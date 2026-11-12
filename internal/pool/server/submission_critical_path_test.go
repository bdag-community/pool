package server

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"asicPool/internal/pool/storage"
)

type criticalPathJournal struct {
	coordinatorJournal
	metricsEntered   chan struct{}
	metricsRelease   chan struct{}
	completionBudget time.Duration
}

func (j *criticalPathJournal) ReadSubmissionMetrics(ctx context.Context) (storage.SubmissionMetricsV1, error) {
	if j.metricsEntered != nil {
		close(j.metricsEntered)
		<-j.metricsRelease
	}
	return j.coordinatorJournal.ReadSubmissionMetrics(ctx)
}

func (j *criticalPathJournal) CompleteAttempt(ctx context.Context, lease LeaseV1, id [16]byte, o RPCObservationV1, d SubmitDecisionV1) (TransitionResultV1, error) {
	if err := ctx.Err(); err != nil {
		return TransitionResultV1{}, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return TransitionResultV1{}, fmt.Errorf("unbounded completion context")
	}
	j.completionBudget = time.Until(deadline)
	return j.coordinatorJournal.CompleteAttempt(ctx, lease, id, o, d)
}

func installCriticalPathRPC(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	old := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: s.URL, submitClient: s.Client()}}
	t.Cleanup(func() { activeRPCRouter = old })
}

func TestAcceptedParentRefreshDoesNotWaitForMetrics(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1043))
	if err != nil {
		t.Fatal(err)
	}
	installCriticalPathRPC(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"%s","height":7,"template_seq":9}}`, hexDigest(candidate.ExpectedBlockHash[:]))
	})
	j := &criticalPathJournal{metricsEntered: make(chan struct{}), metricsRelease: make(chan struct{})}
	c := newSubmissionCoordinator(j, big.NewInt(1043), "node", nil)
	refreshed := make(chan struct{})
	c.acceptedEffects = func(FrozenCandidateV1, RPCObservationV1) { close(refreshed) }
	done := make(chan submitCandidateResult, 1)
	go func() {
		done <- c.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner": 10}, 10, 1)
	}()
	select {
	case <-refreshed:
		close(j.metricsRelease)
	case <-j.metricsEntered:
		close(j.metricsRelease)
		<-done
		t.Fatal("accepted parent refresh blocked behind a full-ledger metrics scan")
	case <-time.After(time.Second):
		close(j.metricsRelease)
		t.Fatal("accepted path did not finish")
	}
	result := <-done
	if result.Err != nil || result.AccountingErr != nil || !result.Transition.FirstAccepted || !j.accountingApplied {
		t.Fatalf("durable acceptance/accounting changed: %+v", result)
	}
}

func TestRPCTimeoutStillRecordsBoundedPendingObservation(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, share := testSubmissionJob()
	installCriticalPathRPC(t, func(w http.ResponseWriter, r *http.Request) { time.Sleep(80 * time.Millisecond) })
	j := &criticalPathJournal{}
	c := newSubmissionCoordinator(j, big.NewInt(1043), "node", nil)
	c.timeout = 20 * time.Millisecond
	var effects int
	c.acceptedEffects = func(FrozenCandidateV1, RPCObservationV1) { effects++ }
	result := c.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner": 10}, 10, 1)
	if result.Err != nil {
		t.Fatalf("RPC timeout prevented durable pending recording: %v", result.Err)
	}
	if result.Decision != storage.SubmitDecisionPending || result.Transition.State != storage.SubmissionStatePending || len(j.completed) != 1 || j.accountingApplied || effects != 0 {
		t.Fatalf("timeout fabricated acceptance or missed pending recording: result=%+v completed=%v effects=%d", result, j.completed, effects)
	}
	if c.timeout != 20*time.Millisecond || j.completionBudget <= 0 || j.completionBudget > blockSubmissionPersistTimeout {
		t.Fatalf("RPC/completion budgets changed or unbounded: rpc=%v completion=%v", c.timeout, j.completionBudget)
	}
}

func TestRPCTimeoutDurablyCompletesPendingSQLAttempt(t *testing.T) {
	ensureSubmissionTestMetrics()
	journal, databaseURL := serverTestJournal(t)
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1043))
	if err != nil {
		t.Fatal(err)
	}
	installCriticalPathRPC(t, func(w http.ResponseWriter, r *http.Request) { time.Sleep(80 * time.Millisecond) })
	c := newSubmissionCoordinator(journal, big.NewInt(1043), "node", nil)
	c.timeout = 20 * time.Millisecond
	result := c.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner": 10}, 10, 1)
	if result.Err != nil || result.Decision != storage.SubmitDecisionPending {
		t.Fatalf("pending SQL observation was not persisted: %+v", result)
	}
	view, attempts, receipts, _ := reopenServerJournal(t, databaseURL, candidate.CandidateID)
	if view.State != storage.SubmissionStatePending || attempts != 1 || receipts != 0 {
		t.Fatalf("timeout SQL state/attempt/receipt = %q/%d/%d", view.State, attempts, receipts)
	}
}
