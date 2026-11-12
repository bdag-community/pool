package server

import (
	"context"
	"encoding/binary"
	"log"
	"strings"
	"time"

	"asicPool/internal/pool/storage"
)

const blockSubmissionPersistTimeout = 2 * time.Second

func recordBlockSubmission(db storage.Storage, job *JobData, candidateHash string, result interface{}, submitErr error) {
	if db == nil || candidateHash == "" {
		return
	}

	submission := buildBlockSubmission(job, candidateHash, result, submitErr)
	ctx, cancel := context.WithTimeout(context.Background(), blockSubmissionPersistTimeout)
	defer cancel()
	if err := db.RecordBlockSubmission(ctx, submission); err != nil {
		log.Printf("[SUBMIT] failed to persist block submission candidate=%s outcome=%s backend=%s: %v",
			candidateHash, submission.Outcome, submission.BackendName, err)
	}
}

func recordBlockSubmitBackendOutcome(job *JobData, outcome, reason string) {
	if blockSubmitBackendOutcomes == nil {
		return
	}
	backend := "unknown"
	if job != nil && strings.TrimSpace(job.BackendName) != "" {
		backend = job.BackendName
	}
	if outcome == "" {
		outcome = "unknown"
	}
	if reason == "" {
		reason = "unknown"
	}
	blockSubmitBackendOutcomes.WithLabelValues(backend, outcome, reason).Inc()
}

func buildBlockSubmission(job *JobData, candidateHash string, result interface{}, submitErr error) storage.BlockSubmission {
	submission := storage.BlockSubmission{
		CandidateHash: candidateHash,
		Outcome:       "ok",
		Accepted:      submitErr == nil,
		Message:       "accepted",
	}
	if job != nil {
		submission.BackendName = job.BackendName
		submission.TemplateSeq = int64(job.TemplateSeq)
		if len(job.Height) >= 8 {
			submission.Height = int64(binary.LittleEndian.Uint64(job.Height))
		}
	}
	if submitErr != nil {
		submission.Outcome = classifySubmitErrorReason(submitErr)
		submission.Message = submitErr.Error()
		return submission
	}

	fields, ok := result.(map[string]interface{})
	if !ok {
		return submission
	}

	if accepted, ok := fields["accepted"].(bool); ok {
		submission.Accepted = accepted
	}
	if code, ok := stringField(fields, "code"); ok {
		submission.Outcome = normalizeStructuredSubmitCode(code)
	}
	if msg, ok := stringField(fields, "message"); ok {
		submission.Message = msg
	}
	if blockHash, ok := stringField(fields, "block_hash"); ok {
		submission.NodeBlockHash = blockHash
	}
	if height, ok := numericField(fields, "height"); ok {
		submission.Height = height
	}
	if seq, ok := numericField(fields, "template_seq"); ok {
		submission.TemplateSeq = seq
	}
	if submission.Outcome == "ok" && !submission.Accepted {
		submission.Outcome = "rejected"
	}
	return submission
}

func stringField(fields map[string]interface{}, key string) (string, bool) {
	value, ok := fields[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	text = strings.TrimSpace(text)
	return text, text != ""
}

func numericField(fields map[string]interface{}, key string) (int64, bool) {
	value, ok := fields[key]
	if !ok {
		return 0, false
	}
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case jsonNumber:
		n, err := v.Int64()
		return n, err == nil
	default:
		return 0, false
	}
}

type jsonNumber interface {
	Int64() (int64, error)
}
