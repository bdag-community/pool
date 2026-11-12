package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDiscoveryRecoveryActualDump(t *testing.T) {
	raw := os.Getenv("POOL_DISCOVERY_ACTUAL_DATABASE_URL")
	if raw == "" {
		t.Skip("actual isolated dump required")
	}
	u, e := url.Parse(raw)
	if e != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/pool_submission_journal_test_actual_discovery") {
		t.Fatal("non-isolated fixture")
	}
	s := NewPostgresStorage(raw)
	ctx := context.Background()
	if e = s.Connect(ctx); e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	cidRaw, _ := hex.DecodeString("682c5550eab9725eb33e1fd158cad1e52299ae98f0ab50846e6f1f50f7c7d7c8")
	var cid [32]byte
	copy(cid[:], cidRaw)
	var fid, aid, header, existing, incoming []byte
	if e = s.pool.QueryRow(ctx, `SELECT f.conflict_id,a.attempt_id,c.header_bytes,f.existing_intent_digest,f.incoming_intent_digest FROM accepted_block_accounting_conflicts_v1 f JOIN block_submission_candidates_v1 c USING(candidate_id) JOIN block_submission_attempts_v1 a USING(candidate_id) WHERE f.candidate_id=$1`, cidRaw).Scan(&fid, &aid, &header, &existing, &incoming); e != nil {
		t.Fatal(e)
	}
	fields := map[string]any{"version": "discovery-cut-recovery/v1", "candidateId": hex.EncodeToString(cidRaw), "conflictId": hex.EncodeToString(fid), "attemptId": hex.EncodeToString(aid), "chainId": "1043", "backendId": "node", "blockHash": "0f7a02940ebc6d0d754b6c1a0511ba34b89dbf3fc69f190e89a765165b1ff8e3", "headerHex": hex.EncodeToString(header), "nativeHeight": "109693", "coreTemplateSeq": "109", "checkpointNumber": "112908", "checkpointHash": "85f2c0a83a7795714a2c48f23d5b45edd71836d89373398218785403ac7d9484", "checkpointStateRoot": "9f7cf132079837db3d11527b829c450e8cb03bc6c4a7b23b34184c9bd1cbebdd", "observedAtUtc": time.Now().UTC().Format(time.RFC3339Nano), "witnessPeerIds": []string{"16Uiu2HAmQsvnEUMN6eNpV1dRNYovDaSAehFU4F7eTeBmgXRsbsz1", "16Uiu2HAmJu5NoZDAN5CYWLxMcrRBuZw4yYXiD7NPyNQZjqAzgQ2t"}, "operatorReference": "Owner authorized MINING-005 isolated actual-dump recovery", "originalIntentDigest": hex.EncodeToString(existing), "incomingIntentDigest": hex.EncodeToString(incoming), "additionalWork": "50000000"}
	encoded, _ := json.Marshal(fields)
	var p RefreshedJobRecoveryProofV1
	if e = json.Unmarshal(encoded, &p); e != nil {
		t.Fatal(e)
	}
	tables := []string{"block_submission_candidates_v1", "block_submission_state_v1", "block_submission_attempts_v1", "block_submission_conflicts_v1", "accepted_block_accounting_conflicts_v1", "accepted_block_accounting_intents_v1", "accepted_block_accounting_credit_lines_v1", "block_submission_job_recoveries_v1"}
	before := map[string]string{}
	for _, table := range tables {
		var digest string
		if e = s.pool.QueryRow(ctx, `SELECT md5(coalesce(string_agg(row_to_json(t)::text,'|' ORDER BY row_to_json(t)::text),'')) FROM `+table+` t`).Scan(&digest); e != nil {
			t.Fatal(e)
		}
		before[table] = digest
	}
	if e = s.VerifySubmissionJournalV1(ctx, bigInt(1043), "node"); e == nil {
		t.Fatal("unresolved conflict accepted")
	}
	for _, field := range []string{"additionalWork", "incomingIntentDigest", "originalIntentDigest", "coreTemplateSeq", "headerHex", "witnessPeerIds"} {
		altered := map[string]any{}
		for k, v := range fields {
			altered[k] = v
		}
		switch field {
		case "additionalWork":
			altered[field] = "50000001"
		case "incomingIntentDigest", "originalIntentDigest":
			altered[field] = strings.Repeat("0", 64)
		case "coreTemplateSeq":
			altered[field] = "108"
		case "headerHex":
			altered[field] = "00"
		case "witnessPeerIds":
			altered[field] = []string{"same-peer", "same-peer"}
		}
		bytes, _ := json.Marshal(altered)
		var wrong RefreshedJobRecoveryProofV1
		json.Unmarshal(bytes, &wrong)
		if _, err := s.RecoverRefreshedJobV1(ctx, wrong, true); err == nil {
			t.Fatalf("incorrect evidence accepted: %s", field)
		}
	}
	if _, e = s.RecoverRefreshedJobV1(ctx, p, false); e != nil {
		t.Fatalf("new recovery dry run: %v", e)
	}
	for _, fault := range []struct {
		point string
		after bool
	}{{"recovery_receipt", false}, {"recovery_receipt", true}, {"recovery_commit", false}} {
		submissionFaultHookV1 = func(point string, after bool) error {
			if point == fault.point && after == fault.after {
				return errors.New("simulated interruption")
			}
			return nil
		}
		_, err := s.RecoverRefreshedJobV1(ctx, p, true)
		submissionFaultHookV1 = nil
		if err == nil {
			t.Fatal("recovery interruption not exercised")
		}
		var count int
		if err = s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1)+(SELECT count(*) FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1)+(SELECT count(*) FROM credits WHERE block_hash='ac19e8a3e629a5edfe8722308f57d2515d88bbd3c1cf0f5ebe6650260d000000')`, cidRaw).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial interrupted recovery: %d %v", count, err)
		}
	}
	r, e := s.RecoverRefreshedJobV1(ctx, p, true)
	if e != nil || !r.Applied {
		t.Fatalf("new recovery: %+v %v", r, e)
	}
	r, e = s.RecoverRefreshedJobV1(ctx, p, true)
	if e != nil || !r.AlreadyApplied {
		t.Fatalf("repeat recovery: %+v %v", r, e)
	}
	// Fully qualified recovery includes the original effect atomically.
	if e = s.VerifySubmissionJournalV1(ctx, bigInt(1043), "node"); e != nil {
		t.Fatalf("startup: %v", e)
	}
	if e = s.VerifyAcceptedAccountingV2(ctx); e != nil {
		t.Fatalf("accounting: %v", e)
	}
	result, e := s.ApplyAcceptedAccountingV2(ctx, cid)
	if e != nil || !result.AlreadyApplied {
		t.Fatalf("repeat credit: %+v %v", result, e)
	}
	for _, table := range tables {
		var digest string
		if e = s.pool.QueryRow(ctx, `SELECT md5(coalesce(string_agg(row_to_json(t)::text,'|' ORDER BY row_to_json(t)::text),'')) FROM `+table+` t`).Scan(&digest); e != nil {
			t.Fatal(e)
		}
		if digest != before[table] {
			t.Fatalf("historical evidence modified: %s", table)
		}
	}
	m, e := s.ReadSubmissionMetrics(ctx)
	if e != nil || m.Conflict != 2 || m.RecoveredConflict != 2 || m.AccountingConflictCount != 1 {
		t.Fatalf("metrics: %+v %v", m, e)
	}
	// A new conflict must immediately close the runtime gate.
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = insertAcceptedAccountingConflictTxV1(ctx, tx, cid, "candidate_intent_mismatch", existing, incoming, "another mismatch"); e != nil {
		t.Fatal(e)
	}
	latch, args := s.conflictLatchSQLV1(1)
	var closed bool
	if e = tx.QueryRow(ctx, "SELECT "+latch, args...).Scan(&closed); e != nil {
		t.Fatal(e)
	}
	tx.Rollback(ctx)
	if !closed {
		t.Fatal("new conflict ignored")
	}
	if _, e = s.pool.Exec(ctx, `UPDATE block_submission_discovery_recoveries_v1 SET operator_reference='changed evidence'`); e == nil {
		t.Fatal("proof mutable")
	}
	if _, e = s.pool.Exec(ctx, `DELETE FROM block_submission_discovery_recoveries_v1`); e == nil {
		t.Fatal("proof deletable")
	}
	tx, e = s.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec(ctx, `UPDATE credits SET amount=amount+1 WHERE block_hash='ac19e8a3e629a5edfe8722308f57d2515d88bbd3c1cf0f5ebe6650260d000000'`); e != nil {
		t.Fatal(e)
	}
	if e = tx.QueryRow(ctx, "SELECT "+latch, args...).Scan(&closed); e != nil {
		t.Fatal(e)
	}
	tx.Rollback(ctx)
	if !closed {
		t.Fatal("changed recovered credit ignored")
	}
	t.Log("ACTUAL_DISCOVERY_RECOVERY_PASS: historical rows unchanged, original credit exactly once, both recoveries revalidated, new conflicts still block")
}
