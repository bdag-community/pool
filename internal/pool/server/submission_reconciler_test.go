package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"asicPool/internal/pool/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSubmissionReconcilerRetriesOnlyStoredRequestBytes(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	var requests [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := ioReadAll(r.Body)
		if readErr != nil && !strings.Contains(readErr.Error(), "EOF") {
			t.Errorf("read request: %v", readErr)
		}
		requests = append(requests, body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"accepted":false,"final":false,"retryable":true,"code":"queued","message":"later","block_hash":"` + hexDigest(candidate.ExpectedBlockHash[:]) + `","height":7,"template_seq":9}}`))
	}))
	defer server.Close()
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: server.URL, submitClient: server.Client()}}
	defer func() { activeRPCRouter = oldRouter }()

	j := &coordinatorJournal{}
	if _, _, err := j.PutCandidate(nil, candidate, testAcceptedAccounting(t, candidate)); err != nil {
		t.Fatal(err)
	}
	coordinator := newSubmissionCoordinator(j, big.NewInt(1404), "node", nil)
	reconciler := newSubmissionReconciler(j, coordinator, 15*time.Second, time.Second, nil)
	if err := reconciler.reconcileOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || len(j.prepared) != 2 || !bytes.Equal(requests[0], requests[1]) || !bytes.Equal(requests[0], j.prepared[0].RequestBody) {
		t.Fatalf("request retry changed bytes: requests=%q prepared=%d", requests, len(j.prepared))
	}
	if j.reclaims != 2 {
		t.Fatalf("reclaim calls=%d, want 2", j.reclaims)
	}
}

func TestSubmissionReconcilerPersistsMethodNotFoundBeforeLegacyFallback(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioReadAll(r.Body)
		if bytes.Contains(body, []byte(`submitBlockHeaderV2`)) {
			methods = append(methods, storage.SubmitMethodV2)
		} else {
			methods = append(methods, storage.SubmitMethodLegacy)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`))
	}))
	defer server.Close()
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: server.URL, submitClient: server.Client()}}
	defer func() { activeRPCRouter = oldRouter }()

	j := &coordinatorJournal{}
	_, _, _ = j.PutCandidate(t.Context(), candidate, testAcceptedAccounting(t, candidate))
	coordinator := newSubmissionCoordinator(j, big.NewInt(1404), "node", nil)
	reconciler := newSubmissionReconciler(j, coordinator, 15*time.Second, time.Second, nil)
	if err := reconciler.reconcileOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 2 || methods[0] != storage.SubmitMethodV2 || methods[1] != storage.SubmitMethodLegacy || j.view.State != storage.SubmissionStatePending {
		t.Fatalf("methods=%v state=%s", methods, j.view.State)
	}
}

func TestSubmissionReconcilerConfigurationBounds(t *testing.T) {
	if _, err := parseSubmissionChainIDV1(""); err == nil {
		t.Fatal("empty chain id accepted")
	}
	if got, err := parseSubmissionChainIDV1("1404"); err != nil || got.Cmp(big.NewInt(1404)) != 0 {
		t.Fatalf("chain id = %v, err=%v", got, err)
	}
	if _, _, err := submissionDurations(&Config{SubmitRPCTimeoutMS: 10000, SubmissionLeaseMS: 14999}); err == nil {
		t.Fatal("lease below timeout plus minimum accepted")
	}
	if _, _, err := submissionDurations(&Config{SubmitRPCTimeoutMS: 250, SubmissionLeaseMS: 120001}); err == nil {
		t.Fatal("lease above maximum accepted")
	}
	if _, _, err := submissionDurations(&Config{SubmitRPCTimeoutMS: 250, SubmissionLeaseMS: 6000, SubmissionReconcileIntervalMS: 99}); err == nil {
		t.Fatal("interval below minimum accepted")
	}
	lease, interval, err := submissionDurations(&Config{SubmitRPCTimeoutMS: 250, SubmissionLeaseMS: 5250, SubmissionReconcileIntervalMS: 10000})
	if err != nil || lease != 5250*time.Millisecond || interval != 10*time.Second {
		t.Fatalf("bounded durations = %v,%v err=%v", lease, interval, err)
	}
}

type restartAttemptEvidenceV1 struct {
	id, token, body, digest []byte
	attemptNo               int64
	method, phase           string
}

func TestSubmissionReconcilerPostgresProcessRestartPreparedOrphanAccepted(t *testing.T) {
	if phase := os.Getenv("POOL_RESTART_PHASE"); phase != "" {
		runSubmissionRestartChild(t, phase)
		return
	}
	databaseURL := submissionRestartDatabaseURL(t)
	candidate := submissionRestartCandidate(t)
	var calls atomic.Int32
	var outbound []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioReadAll(r.Body)
		if err != nil && !strings.Contains(err.Error(), "EOF") {
			t.Errorf("read outbound body: %v", err)
		}
		outbound = append([]byte(nil), body...)
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"accepted","block_hash":"%s","height":7,"template_seq":9}}`, hexDigest(candidate.ExpectedBlockHash[:]))
	}))
	defer server.Close()

	runSubmissionRestartChildWithEndpoint(t, "phase-a", "")
	pool := openSubmissionRestartPool(t, databaseURL)
	old := readSubmissionRestartAttempt(t, pool, candidate, 1)
	oldBody := append([]byte(nil), old.body...)
	oldDigest := append([]byte(nil), old.digest...)
	if old.phase != "prepared" || old.method != storage.SubmitMethodV2 || len(old.token) != 32 || len(oldBody) == 0 || len(oldDigest) != 32 {
		t.Fatalf("phase-A persisted attempt=%+v", old)
	}
	wantOldBody, err := buildSubmitRequestV1(candidate, storage.SubmitMethodV2)
	if err != nil || !bytes.Equal(oldBody, wantOldBody) {
		t.Fatalf("phase-A prepared bytes got=%q want=%q err=%v", oldBody, wantOldBody, err)
	}
	// Wait on the actual database lease instead of assuming that a fixed sleep
	// covers process scheduling and setup under constrained hosts.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	tag, err := pool.Exec(waitCtx, `SELECT pg_sleep(GREATEST(EXTRACT(EPOCH FROM (lease_expires_at-clock_timestamp())),0)+0.05) FROM block_submission_state_v1 WHERE candidate_id=$1 AND state='in_flight'`, candidate.CandidateID[:])
	if err != nil || tag.RowsAffected() != 1 {
		waitCancel()
		t.Fatalf("wait for real lease expiry: rows=%d err=%v", tag.RowsAffected(), err)
	}
	var expired bool
	if err = pool.QueryRow(waitCtx, `SELECT lease_expires_at <= clock_timestamp() FROM block_submission_state_v1 WHERE candidate_id=$1 AND state='in_flight'`, candidate.CandidateID[:]).Scan(&expired); err != nil || !expired {
		waitCancel()
		t.Fatalf("lease did not expire: expired=%t err=%v", expired, err)
	}
	waitCancel()
	pool.Close()
	runSubmissionRestartChildWithEndpoint(t, "phase-b", server.URL)
	if calls.Load() != 1 || !bytes.Equal(outbound, oldBody) {
		t.Fatalf("phase-B calls/body=%d/%x, want 1/%x", calls.Load(), outbound, oldBody)
	}
	pool = openSubmissionRestartPool(t, databaseURL)
	newAttempt := readSubmissionRestartAttempt(t, pool, candidate, 2)
	if newAttempt.phase != "completed" || bytes.Equal(newAttempt.id, old.id) || bytes.Equal(newAttempt.token, old.token) || newAttempt.attemptNo == old.attemptNo || newAttempt.method != old.method || !bytes.Equal(newAttempt.body, oldBody) || !bytes.Equal(newAttempt.digest, oldDigest) {
		t.Fatalf("phase-B attempt=%+v old=%+v", newAttempt, old)
	}
	computed := sha256BytesV1(newAttempt.body)
	if !bytes.Equal(newAttempt.digest, computed[:]) {
		t.Fatalf("phase-B persisted digest does not match body")
	}
	var oldPhase string
	if err := pool.QueryRow(context.Background(), `SELECT phase FROM block_submission_attempts_v1 WHERE attempt_id=$1`, old.id).Scan(&oldPhase); err != nil {
		t.Fatal(err)
	}
	if oldPhase != "prepared" {
		t.Fatalf("old phase=%q, want prepared", oldPhase)
	}
	var state string
	var receipts, effects, blocks, credits int
	var miner, amount string
	legacyKey := hex.EncodeToString(candidate.SharePowHash[:])
	if err := pool.QueryRow(context.Background(), `SELECT state,
		(SELECT count(*) FROM accepted_block_submission_receipts_v1),
		(SELECT count(*) FROM accepted_block_accounting_effects_v1),
		(SELECT count(*) FROM blocks WHERE hash=$2),
		(SELECT count(*) FROM credits WHERE block_hash=$2),
		(SELECT miner_address FROM credits WHERE block_hash=$2),
		(SELECT amount::text FROM credits WHERE block_hash=$2)
		FROM block_submission_state_v1 WHERE candidate_id=$1`, candidate.CandidateID[:], legacyKey).Scan(&state, &receipts, &effects, &blocks, &credits, &miner, &amount); err != nil {
		t.Fatal(err)
	}
	wantAmount := fmt.Sprintf("%d", uint64(5*10_000_000_000+7))
	if state != storage.SubmissionStateAcceptedFinal || receipts != 1 || effects != 1 || blocks != 1 || credits != 1 || miner != "miner" || amount != wantAmount {
		t.Fatalf("reopened state/receipts/effects/blocks/credits/miner/amount=%q/%d/%d/%d/%d/%q/%q", state, receipts, effects, blocks, credits, miner, amount)
	}
	pool.Close()
	runSubmissionRestartChildWithEndpoint(t, "phase-c", server.URL)
	if calls.Load() != 1 {
		t.Fatalf("final reconciliation added HTTP calls: %d", calls.Load())
	}
}

func submissionRestartDatabaseURL(t *testing.T) string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("POOL_SUBMISSION_JOURNAL_TEST_DATABASE_URL"))
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() == "" || !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "pool_submission_journal_test_") {
		t.Fatalf("invalid disposable PostgreSQL URL %q", raw)
	}
	return raw
}

func submissionRestartCandidate(t *testing.T) FrozenCandidateV1 {
	t.Helper()
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	return candidate
}

func openSubmissionRestartPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}

func readSubmissionRestartAttempt(t *testing.T, pool *pgxpool.Pool, candidate FrozenCandidateV1, attemptNo int64) restartAttemptEvidenceV1 {
	t.Helper()
	var attempt restartAttemptEvidenceV1
	if err := pool.QueryRow(context.Background(), `SELECT attempt_id,attempt_no,lease_token,rpc_method,request_body,request_sha256,phase FROM block_submission_attempts_v1 WHERE candidate_id=$1 AND attempt_no=$2`, candidate.CandidateID[:], attemptNo).Scan(&attempt.id, &attempt.attemptNo, &attempt.token, &attempt.method, &attempt.body, &attempt.digest, &attempt.phase); err != nil {
		t.Fatal(err)
	}
	return attempt
}

func runSubmissionRestartChildWithEndpoint(t *testing.T, phase, endpoint string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run", "^TestSubmissionReconcilerPostgresProcessRestartPreparedOrphanAccepted$", "-test.v")
	command.Env = append(os.Environ(), "POOL_RESTART_PHASE="+phase, "POOL_RESTART_ENDPOINT="+endpoint)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("restart child %s: %v\n%s", phase, err, output)
	}
}

func runSubmissionRestartChild(t *testing.T, phase string) {
	t.Helper()
	ensureSubmissionTestMetrics()
	databaseURL := submissionRestartDatabaseURL(t)
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if phase == "phase-a" {
		defer pool.Close()
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
			t.Fatal(err)
		}
		_, file, _, _ := runtime.Caller(0)
		migration, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "schema.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(context.Background(), string(migration)); err != nil {
			t.Fatal(err)
		}
		journal := storage.NewPostgresStorage(databaseURL)
		if err := journal.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		candidate := submissionRestartCandidate(t)
		if _, disposition, err := journal.PutCandidate(context.Background(), candidate, testAcceptedAccounting(t, candidate)); err != nil || disposition != storage.PutDispositionInserted {
			t.Fatalf("phase-A candidate: %q %v", disposition, err)
		}
		lease, err := journal.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{1}, 5*time.Second)
		if err != nil || lease == nil {
			t.Fatalf("phase-A lease: %#v %v", lease, err)
		}
		if _, err := journal.PrepareAttempt(context.Background(), lease.Lease, storage.SubmitMethodV2); err != nil {
			t.Fatal(err)
		}
		journal.Close()
		return
	}
	pool.Close()
	endpoint := strings.TrimSpace(os.Getenv("POOL_RESTART_ENDPOINT"))
	if endpoint == "" {
		t.Fatal("restart endpoint is required")
	}
	journal := storage.NewPostgresStorage(databaseURL)
	if err := journal.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	coordinator := newSubmissionCoordinator(journal, big.NewInt(1404), "node", &Config{SubmitRPCTimeoutMS: 1000, SubmissionLeaseMS: 10000})
	coordinator.acceptedEffects = func(FrozenCandidateV1, RPCObservationV1) {}
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: endpoint, submitClient: &http.Client{}}}
	defer func() { activeRPCRouter = oldRouter }()
	reconciler := newSubmissionReconciler(journal, coordinator, 10*time.Second, time.Second, nil)
	if err := reconciler.reconcileOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := reconcileAcceptedAccountingAtStartup(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
}
