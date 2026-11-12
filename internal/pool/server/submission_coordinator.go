package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"asicPool/internal/pool/storage"
)

const (
	defaultSubmissionLease   = 15 * time.Second
	defaultReconcileInterval = time.Second
	minimumSubmissionLease   = 5 * time.Second
)

type submissionCoordinator struct {
	journal          storage.SubmissionJournal
	owner            [16]byte
	chainID          *big.Int
	backendID        string
	timeout          time.Duration
	lease            time.Duration
	poolFeePercent   float64
	legacyOnly       atomic.Bool
	acceptedEffects  func(FrozenCandidateV1, RPCObservationV1)
	metricsPublisher *submissionMetricsPublisherV1
}

// These hooks are nil in production. They let the owned fault suite inject a
// failure around the final non-SQL boundaries without replacing the
// coordinator, journal, or Stratum response path with a test double.
var submissionEffectFaultHookV1 func(after bool) error
var submissionResponseFaultHookV1 func(after bool) error

type submitCandidateResult struct {
	Candidate     FrozenCandidateV1
	Disposition   storage.PutDisposition
	Transition    storage.TransitionResultV1
	Decision      storage.SubmitDecisionV1
	Observation   storage.RPCObservationV1
	AccountingErr error
	Err           error
}

func newSubmissionCoordinator(journal storage.SubmissionJournal, chainID *big.Int, backendID string, cfg *Config) *submissionCoordinator {
	coordinator := &submissionCoordinator{journal: journal, chainID: new(big.Int), backendID: backendID, timeout: defaultSubmitRPCTimeout, lease: defaultSubmissionLease, metricsPublisher: newSubmissionMetricsPublisherV1()}
	if chainID != nil {
		coordinator.chainID.Set(chainID)
	}
	if cfg != nil {
		coordinator.timeout = configuredSubmitRPCTimeout(cfg.SubmitRPCTimeoutMS)
		coordinator.poolFeePercent = cfg.PoolFeePercentage
		if cfg.SubmissionLeaseMS > 0 {
			coordinator.lease = time.Duration(cfg.SubmissionLeaseMS) * time.Millisecond
		}
	}
	if cfg != nil && !cfg.SubmitBlockHeaderV2Enabled {
		coordinator.legacyOnly.Store(true)
	}
	_, _ = rand.Read(coordinator.owner[:])
	return coordinator
}

// SubmitCandidate is the coordinator entry point used by handleSubmit. The
// existing submit pipeline still owns share validation; its block branch calls
// submitFrozenCandidate after it has constructed the exact candidate bytes.
func (c *submissionCoordinator) SubmitCandidate(client *client, req *stratumRequest) error {
	if c == nil || client == nil {
		return errors.New("submission coordinator is unavailable")
	}
	leaveSubmission, err := activeSubmissionDrain.enterSubmission()
	if err != nil {
		if req != nil {
			sendError(client, req.ID, 20, "pool paused for dataset activation")
		}
		return nil
	}
	defer leaveSubmission()
	return newSubmitPipeline(client, req, time.Now()).run()
}

func (c *submissionCoordinator) submitFrozenCandidate(client *client, job *JobData, header []byte, extraNonce2 []byte, sharePowHash []byte, accountingSnapshot map[string]uint64, accountingTotalWork uint64, requestID interface{}) submitCandidateResult {
	result := submitCandidateResult{Decision: storage.SubmitDecisionPending}
	if c == nil || c.journal == nil {
		result.Err = errors.New("durable submission journal is unavailable")
		return result
	}
	candidate, err := freezeBlockCandidateV1(job, header, extraNonce2, sharePowHash, c.chainID)
	if err != nil {
		result.Err = err
		return result
	}
	result.Candidate = candidate
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	var view storage.CandidateViewV1
	var disposition storage.PutDisposition
	if wide, ok := c.journal.(interface {
		DiscoverCandidateV2(context.Context, storage.FrozenCandidateV1, func() (storage.AcceptedAccountingIntentV2, error)) (storage.CandidateViewV1, storage.PutDisposition, error)
	}); ok {
		view, disposition, err = wide.DiscoverCandidateV2(ctx, candidate, func() (storage.AcceptedAccountingIntentV2, error) {
			return buildAcceptedAccountingIntentV2(candidate, job, accountingSnapshot, accountingTotalWork, c.poolFeePercent)
		})
	} else {
		result.Err = errors.New("durable submission journal lacks required wide accounting v2 implementation")
		return result
	}
	if err != nil {
		result.Err = err
		return result
	}
	result.Disposition = disposition
	if disposition == storage.PutDispositionConflict || view.State == storage.SubmissionStateConflict {
		result.Decision = storage.SubmitDecisionConflict
		return result
	}
	if view.State == storage.SubmissionStateAcceptedFinal || view.State == storage.SubmissionStateRejectedFinal {
		result.Transition.State = view.State
		result.Decision = decisionForFinalState(view)
		if view.State == storage.SubmissionStateAcceptedFinal {
			result.AccountingErr = c.applyAcceptedAccounting(ctx, candidate.CandidateID)
		}
		return result
	}
	leased, err := c.journal.ClaimCandidate(ctx, candidate.CandidateID, c.owner, c.lease)
	if err != nil {
		result.Err = err
		return result
	}
	if leased == nil {
		result.Decision = storage.SubmitDecisionPending
		return result
	}
	method := SubmitMethod(storage.SubmitMethodV2)
	if c.legacyOnly.Load() && len(leased.Candidate.LastAttemptID) == 0 {
		method = SubmitMethod(storage.SubmitMethodLegacy)
	}
	prepared, err := c.journal.PrepareAttempt(ctx, leased.Lease, method)
	if err != nil {
		result.Err = err
		return result
	}
	observation, sendErr := sendPreparedAttemptV1(ctx, prepared)
	if sendErr != nil && strictMethodNotFoundObservationV1(observation) {
		c.legacyOnly.Store(true)
		result.Decision = storage.SubmitDecisionMethodNotFound
	} else {
		result.Decision = classifySubmitDecisionV1(leased.Candidate.Candidate, observation)
	}
	if sendErr != nil && result.Decision != storage.SubmitDecisionConflict && result.Decision != storage.SubmitDecisionMethodNotFound {
		result.Decision = storage.SubmitDecisionPending
	}
	result.Observation = observation
	// An RPC timeout is an observation that must itself be durably recorded.
	// Reusing the expired RPC context abandons completion before SQL can fence
	// the lease. Retain the configured RPC limit and use the existing bounded
	// persistence budget, with all normal decision/lease checks unchanged.
	completionCtx, completionCancel := context.WithTimeout(context.Background(), blockSubmissionPersistTimeout)
	defer completionCancel()
	transition, err := c.journal.CompleteAttempt(completionCtx, leased.Lease, prepared.AttemptID, observation, result.Decision)
	if err != nil {
		recordSubmissionAttemptMetricV1("stratum", "completion_error", observation)
		result.Err = err
		return result
	}
	recordSubmissionAttemptMetricV1("stratum", string(result.Decision), observation)
	result.Transition = transition
	if transition.FirstAccepted {
		result.AccountingErr = c.applyAcceptedAccounting(completionCtx, candidate.CandidateID)
		c.applyFirstAcceptedEffects(leased.Candidate.Candidate, observation)
	}
	_ = requestID
	return result
}

func decisionForFinalState(view CandidateViewV1) storage.SubmitDecisionV1 {
	if view.State == storage.SubmissionStateAcceptedFinal {
		return storage.SubmitDecisionAcceptedFinal
	}
	return storage.SubmitDecisionRejectedFinal
}

func sendPreparedAttemptV1(ctx context.Context, attempt PreparedAttemptV1) (RPCObservationV1, error) {
	var observation RPCObservationV1
	if attempt.Method != storage.SubmitMethodV2 && attempt.Method != storage.SubmitMethodLegacy {
		return observation, errors.New("prepared attempt has unsupported method")
	}
	if len(attempt.RequestBody) == 0 || attempt.CandidateID == [32]byte{} || attempt.AttemptID == [16]byte{} || attempt.LeaseToken == [32]byte{} {
		return observation, errors.New("prepared attempt has invalid identity or empty request")
	}
	if sha256.Sum256(attempt.RequestBody) != attempt.RequestSHA256 {
		return observation, errors.New("prepared attempt request SHA-256 mismatch")
	}
	backend := legacyRPCBackend()
	if activeRPCRouter != nil {
		backend = activeRPCRouter.selectedBackend()
	}
	if backend == nil {
		observation.TransportClass = "no-backend"
		return observation, errors.New("no RPC backend configured")
	}
	if activeRPCRouter != nil {
		if ready, reason := activeRPCRouter.chainIdentityReadiness(backend); !ready {
			observation.TransportClass = "chain-identity"
			return observation, fmt.Errorf("submit blocked: %s", reason)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, backend.url, bytes.NewReader(attempt.RequestBody))
	if err != nil {
		return observation, err
	}
	req.Header.Set("Authorization", backend.authorizationHeader())
	req.Header.Set("Content-Type", "application/json")
	client := backend.submitClient
	if client == nil {
		client = submitHTTPClient
	}
	blocksSubmittedTotal.Inc()
	resp, err := client.Do(req)
	if err != nil {
		observation.TransportClass = "transport"
		return observation, err
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	observation.HTTPStatus = &status
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxSubmitResponseBytesV1+1))
	if readErr != nil {
		observation.TransportClass = "read-response"
		return observation, readErr
	}
	observation.ResponsePrefix = append([]byte(nil), body...)
	if len(body) > maxSubmitResponseBytesV1 {
		observation.ResponsePrefix = body[:maxSubmitResponseBytesV1]
		observation.ResponseTruncated = true
		observation.TransportClass = "oversize"
		return observation, errors.New("oversize submit response")
	}
	observation.ResponsePrefixSHA256 = sha256BytesV1(observation.ResponsePrefix)
	if status != http.StatusOK {
		observation.TransportClass = "http"
		return observation, fmt.Errorf("submit HTTP status %d", status)
	}
	parsed, parseErr := decodeSubmitResponseV1(body)
	parsed.HTTPStatus = &status
	parsed.TransportClass = "http"
	parsed.ResponsePrefixSHA256 = observation.ResponsePrefixSHA256
	parsed.ResponsePrefix = observation.ResponsePrefix
	parsed.ResponseTruncated = observation.ResponseTruncated
	observation = parsed
	if parseErr != nil {
		return observation, parseErr
	}
	return observation, nil
}

func sha256BytesV1(value []byte) [32]byte {
	var out [32]byte
	h := storageDigestV1(value)
	copy(out[:], h[:])
	return out
}
func storageDigestV1(value []byte) [32]byte { return sha256.Sum256(value) }

func strictMethodNotFoundObservationV1(observation RPCObservationV1) bool {
	return observation.MethodNotFound && observation.HTTPStatus != nil && *observation.HTTPStatus == http.StatusOK && !observation.ResponseTruncated
}

func submissionAttemptTransportClassV1(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "":
		return "none"
	case "transport", "read-response", "oversize", "http", "no-backend", "chain-identity":
		return strings.TrimSpace(strings.ToLower(raw))
	default:
		return "other"
	}
}

func submissionAttemptDecisionV1(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "pending", "accepted_final", "rejected_final", "conflict", "method_not_found", "completion_error":
		return strings.TrimSpace(strings.ToLower(raw))
	default:
		return "unknown"
	}
}

func recordSubmissionAttemptMetricV1(source, decision string, observation RPCObservationV1) {
	if submissionAttemptOutcomes == nil {
		return
	}
	if source != "stratum" && source != "reconciler" {
		source = "unknown"
	}
	submissionAttemptOutcomes.WithLabelValues(
		source,
		submissionAttemptDecisionV1(decision),
		submissionAttemptTransportClassV1(observation.TransportClass),
	).Inc()
}

func (c *submissionCoordinator) applyFirstAcceptedEffects(candidate FrozenCandidateV1, observation RPCObservationV1) {
	if submissionEffectFaultHookV1 != nil {
		if err := submissionEffectFaultHookV1(false); err != nil {
			return
		}
	}
	if c != nil && c.acceptedEffects != nil {
		c.acceptedEffects(candidate, observation)
		if submissionEffectFaultHookV1 != nil {
			_ = submissionEffectFaultHookV1(true)
		}
		return
	}
	if c != nil {
		job := &JobData{ParentHash: fmt.Sprintf("%x", candidate.ParentHash[:])}
		_ = advanceCurrentParentAfterAcceptedBlock(job, map[string]any{"block_hash": fmt.Sprintf("%x", candidate.ExpectedBlockHash[:])})
		requestCleanRefreshAfterAcceptedSubmit(job, time.Now())
	}
	if submissionEffectFaultHookV1 != nil {
		_ = submissionEffectFaultHookV1(true)
	}
}

func (c *submissionCoordinator) respondToSubmissionDecision(client *client, req *stratumRequest, result submitCandidateResult) {
	if c == nil || client == nil || req == nil {
		return
	}
	if submissionResponseFaultHookV1 != nil {
		if err := submissionResponseFaultHookV1(false); err != nil {
			return
		}
	}
	switch result.Transition.State {
	case storage.SubmissionStateAcceptedFinal:
		sendSubmitAccepted(client, req.ID, result.Candidate.HeaderBytes[:], result.Candidate.ExpectedBlockHash[:])
	case storage.SubmissionStateRejectedFinal:
		sendError(client, req.ID, 20, "block rejected: "+safeDecisionCodeV1(result.Observation))
	case storage.SubmissionStateConflict:
		sendError(client, req.ID, 20, "block submission conflict")
	default:
		sendError(client, req.ID, 20, "block submission pending reconciliation")
	}
	if submissionResponseFaultHookV1 != nil {
		_ = submissionResponseFaultHookV1(true)
	}
}

func safeDecisionCodeV1(observation RPCObservationV1) string {
	if observation.Code == nil || strings.TrimSpace(*observation.Code) == "" {
		return "unknown"
	}
	return strings.TrimSpace(*observation.Code)
}

func updateSubmissionMetricsV1(ctx context.Context, journal storage.SubmissionJournal) error {
	if journal == nil {
		return errors.New("submission journal is unavailable")
	}
	m, err := journal.ReadSubmissionMetrics(ctx)
	if err != nil {
		return err
	}
	if submissionPendingGauge != nil {
		submissionPendingGauge.Set(float64(m.Pending))
	}
	if submissionInFlightGauge != nil {
		submissionInFlightGauge.Set(float64(m.InFlight))
	}
	if submissionAcceptedGauge != nil {
		submissionAcceptedGauge.Set(float64(m.AcceptedFinal))
	}
	if submissionRejectedGauge != nil {
		submissionRejectedGauge.Set(float64(m.RejectedFinal))
	}
	if submissionConflictGauge != nil {
		submissionConflictGauge.Set(float64(m.Conflict))
	}
	if m.RecoveredConflict < 0 || m.RecoveredConflict > m.Conflict {
		return errors.New("invalid recovered conflict metrics")
	}
	if submissionRecoveredConflictGauge != nil {
		submissionRecoveredConflictGauge.Set(float64(m.RecoveredConflict))
	}
	if submissionUnresolvedConflictGauge != nil {
		submissionUnresolvedConflictGauge.Set(float64(m.Conflict - m.RecoveredConflict))
	}
	if submissionReceiptGauge != nil {
		submissionReceiptGauge.Set(float64(m.AcceptedReceiptCount))
	}
	if accountingIntentGauge != nil {
		accountingIntentGauge.Set(float64(m.AccountingIntentCount))
	}
	if accountingPendingGauge != nil {
		accountingPendingGauge.Set(float64(m.AccountingPendingCount))
	}
	if accountingEffectGauge != nil {
		accountingEffectGauge.Set(float64(m.AccountingEffectCount))
	}
	if accountingConflictGauge != nil {
		accountingConflictGauge.Set(float64(m.AccountingConflictCount))
	}
	if m.RecoveredAccountingConflict < 0 || m.RecoveredAccountingConflict > m.AccountingConflictCount {
		return errors.New("invalid recovered accounting metrics")
	}
	if accountingRecoveredConflictGauge != nil {
		accountingRecoveredConflictGauge.Set(float64(m.RecoveredAccountingConflict))
	}
	if submissionOldestPendingAgeGauge != nil {
		submissionOldestPendingAgeGauge.Set(m.OldestPendingAge.Seconds())
	}
	if submissionLastAcceptedTimestamp != nil && m.LastAcceptedAt != nil {
		submissionLastAcceptedTimestamp.Set(float64(m.LastAcceptedAt.UnixNano()) / 1e9)
	}
	return nil
}

func observationFromErrorV1(err error) RPCObservationV1 {
	msg := stringFieldPtrV1(errStringV1(err))
	return RPCObservationV1{TransportClass: "transport", Message: msg}
}
func errStringV1(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func stringFieldPtrV1(v string) *string { return &v }
