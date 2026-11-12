package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"asicPool/internal/pool/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// coordinatorJournal is a deterministic, in-memory journal used only to test
// the ordering contract around the real coordinator. Its methods retain the
// same fencing checks as the SQL implementation: a lease must be active for
// preparation and completion, and only the first committed acceptance wins.
type coordinatorJournal struct {
	mu                sync.Mutex
	candidate         FrozenCandidateV1
	accounting        storage.AcceptedAccountingIntentV1
	accountingV2      storage.AcceptedAccountingIntentV2
	view              CandidateViewV1
	haveCandidate     bool
	prepareErr        error
	putErr            error
	prepared          []PreparedAttemptV1
	completed         []SubmitDecisionV1
	reclaims          int
	firstAccepted     bool
	accountingApplied bool
}

func (j *coordinatorJournal) VerifySubmissionJournalV1(context.Context, *big.Int, string) error {
	return nil
}

func (j *coordinatorJournal) VerifyAcceptedAccountingV1(context.Context) error { return nil }

func (j *coordinatorJournal) PutCandidate(_ context.Context, candidate FrozenCandidateV1, accounting storage.AcceptedAccountingIntentV1) (CandidateViewV1, storage.PutDisposition, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.putErr != nil {
		return CandidateViewV1{}, "", j.putErr
	}
	if !j.haveCandidate {
		j.haveCandidate = true
		j.candidate = candidate
		j.accounting = accounting
		j.view = CandidateViewV1{Candidate: candidate, State: storage.SubmissionStatePending, StateVersion: 1}
		return j.view, storage.PutDispositionInserted, nil
	}
	if !bytes.Equal(j.candidate.FrozenPayload, candidate.FrozenPayload) || j.accounting.IntentDigest != accounting.IntentDigest {
		j.view.State = storage.SubmissionStateConflict
		return j.view, storage.PutDispositionConflict, nil
	}
	return j.view, storage.PutDispositionExisting, nil
}

func (j *coordinatorJournal) VerifyAcceptedAccountingV2(context.Context) error { return nil }

func (j *coordinatorJournal) DiscoverCandidateV2(ctx context.Context, candidate FrozenCandidateV1, freeze func() (storage.AcceptedAccountingIntentV2, error)) (CandidateViewV1, storage.PutDisposition, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.putErr != nil {
		return CandidateViewV1{}, "", j.putErr
	}
	if j.haveCandidate {
		if j.accountingV2.PolicyVersion != storage.AcceptedAccountingPolicyV2 || candidate.CandidateID != j.candidate.CandidateID || candidate.ExpectedBlockHash != j.candidate.ExpectedBlockHash || candidate.HeaderBytes != j.candidate.HeaderBytes {
			j.view.State = storage.SubmissionStateConflict
			return j.view, storage.PutDispositionConflict, nil
		}
		return j.view, storage.PutDispositionExisting, nil
	}
	a, e := freeze()
	if e != nil {
		return CandidateViewV1{}, "", e
	}
	j.haveCandidate = true
	j.candidate = candidate
	j.accountingV2 = a
	j.view = CandidateViewV1{Candidate: candidate, State: storage.SubmissionStatePending, StateVersion: 1}
	return j.view, storage.PutDispositionInserted, nil
}

func (j *coordinatorJournal) PutCandidateV2(_ context.Context, candidate FrozenCandidateV1, accounting storage.AcceptedAccountingIntentV2) (CandidateViewV1, storage.PutDisposition, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.putErr != nil {
		return CandidateViewV1{}, "", j.putErr
	}
	if !j.haveCandidate {
		j.haveCandidate = true
		j.candidate = candidate
		j.accountingV2 = accounting
		j.view = CandidateViewV1{Candidate: candidate, State: storage.SubmissionStatePending, StateVersion: 1}
		return j.view, storage.PutDispositionInserted, nil
	}
	if !bytes.Equal(j.candidate.FrozenPayload, candidate.FrozenPayload) || j.accountingV2.IntentDigest != accounting.IntentDigest {
		j.view.State = storage.SubmissionStateConflict
		return j.view, storage.PutDispositionConflict, nil
	}
	return j.view, storage.PutDispositionExisting, nil
}

func (j *coordinatorJournal) ApplyAcceptedAccountingV1(_ context.Context, candidateID [32]byte) (storage.AcceptedAccountingApplyResultV1, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := storage.AcceptedAccountingApplyResultV1{CandidateID: candidateID, CreditLines: int64(len(j.accounting.CreditLines))}
	if j.view.State != storage.SubmissionStateAcceptedFinal || candidateID != j.candidate.CandidateID {
		return result, errors.New("accepted accounting receipt unavailable")
	}
	if j.accountingApplied {
		result.AlreadyApplied = true
		return result, nil
	}
	j.accountingApplied = true
	result.Applied = true
	return result, nil
}

func (j *coordinatorJournal) ReconcileAcceptedAccountingV1(ctx context.Context) (storage.AcceptedAccountingReconcileResultV1, error) {
	j.mu.Lock()
	pending := j.view.State == storage.SubmissionStateAcceptedFinal && !j.accountingApplied
	id := j.candidate.CandidateID
	j.mu.Unlock()
	if !pending {
		return storage.AcceptedAccountingReconcileResultV1{}, nil
	}
	result, err := j.ApplyAcceptedAccountingV1(ctx, id)
	if err != nil {
		return storage.AcceptedAccountingReconcileResultV1{}, err
	}
	if result.Applied {
		return storage.AcceptedAccountingReconcileResultV1{Applied: 1}, nil
	}
	return storage.AcceptedAccountingReconcileResultV1{}, nil
}

func (j *coordinatorJournal) ApplyAcceptedAccountingV2(_ context.Context, candidateID [32]byte) (storage.AcceptedAccountingApplyResultV2, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := storage.AcceptedAccountingApplyResultV2{CandidateID: candidateID, CreditLines: int64(len(j.accountingV2.CreditLines))}
	if j.view.State != storage.SubmissionStateAcceptedFinal || candidateID != j.candidate.CandidateID {
		return result, errors.New("accepted accounting receipt unavailable")
	}
	if j.accountingApplied {
		result.AlreadyApplied = true
		return result, nil
	}
	j.accountingApplied = true
	result.Applied = true
	return result, nil
}
func (j *coordinatorJournal) ReconcileAcceptedAccountingV2(ctx context.Context) (storage.AcceptedAccountingReconcileResultV2, error) {
	j.mu.Lock()
	pending := j.view.State == storage.SubmissionStateAcceptedFinal && !j.accountingApplied
	id := j.candidate.CandidateID
	j.mu.Unlock()
	if !pending {
		return storage.AcceptedAccountingReconcileResultV2{}, nil
	}
	r, e := j.ApplyAcceptedAccountingV2(ctx, id)
	if e != nil {
		return storage.AcceptedAccountingReconcileResultV2{}, e
	}
	if r.Applied {
		return storage.AcceptedAccountingReconcileResultV2{Applied: 1}, nil
	}
	return storage.AcceptedAccountingReconcileResultV2{}, nil
}

func (j *coordinatorJournal) GetCandidate(context.Context, [32]byte) (CandidateViewV1, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.view, nil
}

func (j *coordinatorJournal) ReclaimExpiredLeases(context.Context) (int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.reclaims++
	if j.view.State == storage.SubmissionStateInFlight && !j.view.NextAttemptAt.IsZero() {
		j.view.State = storage.SubmissionStatePending
	}
	return 0, nil
}

func (j *coordinatorJournal) ClaimCandidate(_ context.Context, id [32]byte, owner [16]byte, lease time.Duration) (*LeasedCandidateV1, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.haveCandidate || j.view.State != storage.SubmissionStatePending || id != j.candidate.CandidateID {
		return nil, nil
	}
	token := [32]byte{1}
	j.view.State = storage.SubmissionStateInFlight
	j.view.AttemptCount++
	j.view.NextAttemptAt = nil
	j.view.StateVersion++
	return &LeasedCandidateV1{Candidate: j.view, Lease: LeaseV1{CandidateID: id, Owner: owner, Token: token, AttemptNo: j.view.AttemptCount, ExpiresAt: time.Now().Add(lease)}}, nil
}

func (j *coordinatorJournal) ClaimNextDueCandidate(ctx context.Context, owner [16]byte, lease time.Duration) (*LeasedCandidateV1, error) {
	j.mu.Lock()
	id := j.candidate.CandidateID
	j.mu.Unlock()
	return j.ClaimCandidate(ctx, id, owner, lease)
}

func (j *coordinatorJournal) PrepareAttempt(_ context.Context, lease LeaseV1, method SubmitMethod) (PreparedAttemptV1, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.prepareErr != nil {
		return PreparedAttemptV1{}, j.prepareErr
	}
	if j.view.State != storage.SubmissionStateInFlight || lease.Token != [32]byte{1} {
		return PreparedAttemptV1{}, errors.New("inactive lease")
	}
	request, err := buildSubmitRequestV1(j.candidate, method)
	if err != nil {
		return PreparedAttemptV1{}, err
	}
	attempt := PreparedAttemptV1{AttemptID: [16]byte{byte(len(j.prepared) + 1)}, CandidateID: lease.CandidateID, AttemptNo: lease.AttemptNo, LeaseToken: lease.Token, Method: method, RequestBody: request, RequestSHA256: sha256.Sum256(request), PreparedAt: time.Now()}
	j.prepared = append(j.prepared, attempt)
	return attempt, nil
}

func (j *coordinatorJournal) CompleteAttempt(_ context.Context, lease LeaseV1, attemptID [16]byte, _ RPCObservationV1, decision SubmitDecisionV1) (TransitionResultV1, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.view.State != storage.SubmissionStateInFlight || lease.Token != [32]byte{1} {
		return TransitionResultV1{CandidateID: lease.CandidateID, AttemptID: attemptID, State: j.view.State}, nil
	}
	j.completed = append(j.completed, decision)
	result := TransitionResultV1{CandidateID: lease.CandidateID, AttemptID: attemptID}
	switch decision {
	case storage.SubmitDecisionAcceptedFinal:
		j.view.State = storage.SubmissionStateAcceptedFinal
		result.FirstAccepted = !j.firstAccepted
		j.firstAccepted = true
	case storage.SubmitDecisionRejectedFinal:
		j.view.State = storage.SubmissionStateRejectedFinal
	case storage.SubmitDecisionConflict:
		j.view.State = storage.SubmissionStateConflict
		result.Conflict = true
	default:
		j.view.State = storage.SubmissionStatePending
	}
	j.view.StateVersion++
	result.State = j.view.State
	return result, nil
}

func (j *coordinatorJournal) ReadSubmissionMetrics(context.Context) (storage.SubmissionMetricsV1, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	metrics := storage.SubmissionMetricsV1{}
	switch j.view.State {
	case storage.SubmissionStatePending:
		metrics.Pending = 1
	case storage.SubmissionStateInFlight:
		metrics.InFlight = 1
	case storage.SubmissionStateAcceptedFinal:
		metrics.AcceptedFinal = 1
	case storage.SubmissionStateRejectedFinal:
		metrics.RejectedFinal = 1
	case storage.SubmissionStateConflict:
		metrics.Conflict = 1
	}
	return metrics, nil
}

func testSubmissionJob() (*JobData, []byte, []byte, []byte) {
	header := make([]byte, frozenHeaderBytesV1)
	for i := range header {
		header[i] = byte(i)
	}
	header[176] = 0x0a
	for i := 177; i < 181; i++ {
		header[i] = 0
	}
	binary.LittleEndian.PutUint64(header[140:148], 7)
	parent := append([]byte(nil), header[4:36]...)
	height := make([]byte, 8)
	binary.LittleEndian.PutUint64(height, 7)
	reward := make([]byte, 8)
	fees := make([]byte, 8)
	binary.LittleEndian.PutUint64(reward, 5)
	binary.LittleEndian.PutUint64(fees, 7)
	job := &JobData{ParentBytes: parent, Height: height, Reward: reward, BlockFees: fees, TemplateSeq: 9, BackendName: "node"}
	extra := []byte{0, 0, 0, 42}
	share := bytes.Repeat([]byte{0x44}, 32)
	return job, header, extra, share
}

func testAcceptedAccounting(t *testing.T, candidate FrozenCandidateV1) storage.AcceptedAccountingIntentV1 {
	t.Helper()
	job, _, _, _ := testSubmissionJob()
	intent, err := buildAcceptedAccountingIntentV1(candidate, job, map[string]uint64{"miner": 10}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestSubmissionCoordinatorCommitsPreparedIntentBeforeHTTP(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	response := `{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"` + hexDigest(candidate.ExpectedBlockHash[:]) + `","height":7,"template_seq":9}}`
	var requestSeen []byte
	var preparedAtRequest bool
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestSeen, _ = ioReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(response))
		preparedAtRequest = true
	}))
	defer httpServer.Close()
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: httpServer.URL, submitClient: httpServer.Client()}}
	defer func() { activeRPCRouter = oldRouter }()

	j := &coordinatorJournal{}
	coordinator := newSubmissionCoordinator(j, big.NewInt(1404), "node", nil)
	var effectCalls int
	coordinator.acceptedEffects = func(FrozenCandidateV1, RPCObservationV1) { effectCalls++ }
	result := coordinator.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner": 10}, 10, 1)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if result.Transition.State != storage.SubmissionStateAcceptedFinal || !result.Transition.FirstAccepted {
		t.Fatalf("transition = %+v", result.Transition)
	}
	if len(j.prepared) != 1 || !preparedAtRequest {
		t.Fatalf("prepared attempts=%d requestSeen=%v", len(j.prepared), requestSeen != nil)
	}
	want, err := buildSubmitRequestV1(candidate, storage.SubmitMethodV2)
	if err != nil || !bytes.Equal(requestSeen, want) || !bytes.Equal(j.prepared[0].RequestBody, want) {
		t.Fatalf("request was not the committed exact prepared body: got=%q want=%q", requestSeen, want)
	}
	if effectCalls != 1 {
		t.Fatalf("accepted effects=%d, want 1", effectCalls)
	}
}

func TestSubmissionCoordinatorPreparationFailureMakesNoHTTPCall(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, _ := testSubmissionJob()
	httpCalls := 0
	httpServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { httpCalls++ }))
	defer httpServer.Close()
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: httpServer.URL, submitClient: httpServer.Client()}}
	defer func() { activeRPCRouter = oldRouter }()
	j := &coordinatorJournal{prepareErr: errors.New("injected prepare failure")}
	coordinator := newSubmissionCoordinator(j, big.NewInt(1404), "node", nil)
	result := coordinator.submitFrozenCandidate(&client{}, job, header, extra, bytes.Repeat([]byte{1}, 32), map[string]uint64{"miner": 10}, 10, 1)
	if result.Err == nil || httpCalls != 0 || len(j.prepared) != 0 {
		t.Fatalf("result=%+v httpCalls=%d prepared=%d", result, httpCalls, len(j.prepared))
	}
}

func TestSubmissionCoordinatorCandidateIntentPersistenceFailureMakesNoHTTPCall(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, _ := testSubmissionJob()
	httpCalls := 0
	httpServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { httpCalls++ }))
	defer httpServer.Close()
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: httpServer.URL, submitClient: httpServer.Client()}}
	defer func() { activeRPCRouter = oldRouter }()
	j := &coordinatorJournal{putErr: errors.New("injected atomic candidate/accounting intent persistence failure")}
	coordinator := newSubmissionCoordinator(j, big.NewInt(1404), "node", nil)
	result := coordinator.submitFrozenCandidate(&client{}, job, header, extra, bytes.Repeat([]byte{1}, 32), map[string]uint64{"miner": 10}, 10, 1)
	if result.Err == nil || httpCalls != 0 || len(j.prepared) != 0 || j.haveCandidate {
		t.Fatalf("result=%+v httpCalls=%d prepared=%d haveCandidate=%t", result, httpCalls, len(j.prepared), j.haveCandidate)
	}
}

func TestSubmissionCoordinatorVariantIntentLatchesBeforeHTTP(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	j := &coordinatorJournal{}
	initial, err := buildAcceptedAccountingIntentV1(candidate, job, map[string]uint64{"miner-a": 10}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, disposition, err := j.PutCandidate(context.Background(), candidate, initial); err != nil || disposition != storage.PutDispositionInserted {
		t.Fatalf("seed candidate: disposition=%q err=%v", disposition, err)
	}
	httpCalls := 0
	httpServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { httpCalls++ }))
	defer httpServer.Close()
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: httpServer.URL, submitClient: httpServer.Client()}}
	defer func() { activeRPCRouter = oldRouter }()
	coordinator := newSubmissionCoordinator(j, big.NewInt(1404), "node", nil)
	result := coordinator.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner-b": 10}, 10, 1)
	if result.Err != nil || result.Disposition != storage.PutDispositionConflict || httpCalls != 0 || len(j.prepared) != 0 || j.view.State != storage.SubmissionStateConflict {
		t.Fatalf("variant result=%+v httpCalls=%d prepared=%d state=%q", result, httpCalls, len(j.prepared), j.view.State)
	}
}

func TestSubmissionAttemptMetricLabelsAreBounded(t *testing.T) {
	for input, want := range map[string]string{
		"":               "none",
		"transport":      "transport",
		"CHAIN-IDENTITY": "chain-identity",
		"arbitrary text": "other",
	} {
		if got := submissionAttemptTransportClassV1(input); got != want {
			t.Fatalf("transport %q = %q, want %q", input, got, want)
		}
	}
	for input, want := range map[string]string{
		"pending":          "pending",
		"accepted_final":   "accepted_final",
		"completion_error": "completion_error",
		"arbitrary text":   "unknown",
	} {
		if got := submissionAttemptDecisionV1(input); got != want {
			t.Fatalf("decision %q = %q, want %q", input, got, want)
		}
	}
}

func hexDigest(value []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(value)*2)
	for i, b := range value {
		out[i*2], out[i*2+1] = digits[b>>4], digits[b&15]
	}
	return string(out)
}

func ioReadAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var out []byte
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out, err
		}
	}
}

func ensureSubmissionTestMetrics() {
	if blocksSubmittedTotal == nil {
		blocksSubmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "pool_submission_test_total", Help: "test submissions"})
	}
}
