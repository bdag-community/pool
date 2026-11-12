package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"asicPool/internal/pool/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These six cases use the actual coordinator, prepared-attempt sender,
// net/http client, and PostgreSQL journal for the three transport boundaries
// between SQL transactions.
func TestSubmissionTransportFaultBoundaries(t *testing.T) {
	ensureSubmissionTestMetrics()
	cases := []struct {
		name string
		mode string
	}{
		{"request_write_before", "request-before"},
		{"request_write_after", "request-after"},
		{"timeout_before", "timeout"},
		{"disconnect_after", "disconnect"},
		{"response_read_before", "response-before"},
		{"response_read_after", "response-after"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			journal, databaseURL := serverTestJournal(t)
			job, header, extra, share := testSubmissionJob()
			candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
			if err != nil {
				t.Fatal(err)
			}
			transport := &submissionFaultRoundTripper{mode: tc.mode, response: []byte(`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"` + hexDigest(candidate.ExpectedBlockHash[:]) + `","height":7,"template_seq":9}}`)}
			oldRouter := activeRPCRouter
			activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: "http://pool-submission-fault.invalid", submitClient: &http.Client{Transport: transport}}}
			defer func() { activeRPCRouter = oldRouter }()
			coordinator := newSubmissionCoordinator(journal, big.NewInt(1404), "node", nil)
			result := coordinator.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner": 10}, 10, 1)
			if result.Err != nil {
				t.Fatalf("faulted exchange errored before durable completion: %v", result.Err)
			}
			if result.Transition.State == storage.SubmissionStateAcceptedFinal || result.Transition.FirstAccepted {
				t.Fatalf("transport ambiguity was accepted: %+v", result.Transition)
			}
			if transport.calls.Load() != 1 {
				t.Fatalf("transport calls=%d, want one actual prepared-attempt exchange", transport.calls.Load())
			}
			view, attempts, receipts, request := reopenServerJournal(t, databaseURL, candidate.CandidateID)
			if view.State != storage.SubmissionStatePending || attempts != 1 || receipts != 0 {
				t.Fatalf("reopened transport state=%q attempts=%d receipts=%d, want pending/1/0", view.State, attempts, receipts)
			}
			wantRequest, err := buildSubmitRequestV1(candidate, storage.SubmitMethodV2)
			if err != nil || !bytes.Equal(request, wantRequest) {
				t.Fatalf("reopened prepared bytes changed: got=%q want=%q err=%v", request, wantRequest, err)
			}
			if tc.mode == "request-after" && !bytes.Equal(transport.request, wantRequest) {
				t.Fatalf("request-write fault did not consume exact prepared bytes: got=%q want=%q", transport.request, wantRequest)
			}
		})
	}
}

func TestSubmissionAcceptedEffectFaultBoundaries(t *testing.T) {
	ensureSubmissionTestMetrics()
	for _, after := range []bool{false, true} {
		after := after
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			journal, databaseURL := serverTestJournal(t)
			job, header, extra, share := testSubmissionJob()
			candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
			if err != nil {
				t.Fatal(err)
			}
			response := []byte(`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"` + hexDigest(candidate.ExpectedBlockHash[:]) + `","height":7,"template_seq":9}}`)
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(response) }))
			defer httpServer.Close()
			oldRouter := activeRPCRouter
			activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: httpServer.URL, submitClient: httpServer.Client()}}
			defer func() { activeRPCRouter = oldRouter }()
			var effectCalls, hookCalls int
			oldHook := submissionEffectFaultHookV1
			submissionEffectFaultHookV1 = func(gotAfter bool) error {
				if gotAfter == after {
					hookCalls++
					return errors.New("accepted effect fault")
				}
				return nil
			}
			defer func() { submissionEffectFaultHookV1 = oldHook }()
			coordinator := newSubmissionCoordinator(journal, big.NewInt(1404), "node", nil)
			coordinator.acceptedEffects = func(FrozenCandidateV1, RPCObservationV1) { effectCalls++ }
			result := coordinator.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner": 10}, 10, 1)
			if result.Err != nil || result.Transition.State != storage.SubmissionStateAcceptedFinal || !result.Transition.FirstAccepted {
				t.Fatalf("accepted transition=%+v err=%v", result.Transition, result.Err)
			}
			if hookCalls != 1 || effectCalls != btoiServer(after) {
				t.Fatalf("effect hook/effect calls=%d/%d after=%t", hookCalls, effectCalls, after)
			}
			view, _, receipts, _ := reopenServerJournal(t, databaseURL, candidate.CandidateID)
			if view.State != storage.SubmissionStateAcceptedFinal || receipts != 1 {
				t.Fatalf("effect fault altered durable acceptance: state=%q receipts=%d", view.State, receipts)
			}
		})
	}
}

func TestSubmissionDirectAcceptedMaterializesExactAccountingOnce(t *testing.T) {
	ensureSubmissionTestMetrics()
	journal, databaseURL := serverTestJournal(t)
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), `INSERT INTO payouts(tx_hash,amount) VALUES ('unchanged-direct-payout',7)`); err != nil {
		t.Fatal(err)
	}
	var payoutBefore string
	if err := pool.QueryRow(context.Background(), `SELECT ctid::text||'|'||xmin::text||'|'||tx_hash||'|'||amount::text FROM payouts`).Scan(&payoutBefore); err != nil {
		t.Fatal(err)
	}

	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	response := []byte(`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"` + hexDigest(candidate.ExpectedBlockHash[:]) + `","height":7,"template_seq":9}}`)
	var calls int
	var outbound []byte
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		outbound, _ = io.ReadAll(r.Body)
		_, _ = w.Write(response)
	}))
	defer httpServer.Close()
	oldRouter := activeRPCRouter
	activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: httpServer.URL, submitClient: httpServer.Client()}}
	defer func() { activeRPCRouter = oldRouter }()
	coordinator := newSubmissionCoordinator(journal, big.NewInt(1404), "node", nil)
	coordinator.acceptedEffects = func(FrozenCandidateV1, RPCObservationV1) {}
	snapshot := map[string]uint64{"miner": 10}
	first := coordinator.submitFrozenCandidate(&client{}, job, header, extra, share, snapshot, 10, 1)
	if first.Err != nil || first.AccountingErr != nil || first.Transition.State != storage.SubmissionStateAcceptedFinal || !first.Transition.FirstAccepted {
		t.Fatalf("direct acceptance=%+v err=%v accounting=%v", first.Transition, first.Err, first.AccountingErr)
	}
	wantRequest, err := buildSubmitRequestV1(candidate, storage.SubmitMethodV2)
	if err != nil || calls != 1 || !bytes.Equal(outbound, wantRequest) {
		t.Fatalf("direct outbound calls=%d got=%q want=%q err=%v", calls, outbound, wantRequest, err)
	}
	second := coordinator.submitFrozenCandidate(&client{}, job, header, extra, share, snapshot, 10, 2)
	if second.Err != nil || second.AccountingErr != nil || second.Transition.State != storage.SubmissionStateAcceptedFinal || calls != 1 {
		t.Fatalf("idempotent replay transition=%+v calls=%d err=%v accounting=%v", second.Transition, calls, second.Err, second.AccountingErr)
	}

	legacyKey := fmt.Sprintf("%x", candidate.SharePowHash[:])
	wantGross := fmt.Sprintf("%d", uint64(5*10_000_000_000+7))
	var receipts, effects, blocks, credits int
	var height int64
	var reward, fee, status, miner, amount string
	var paid bool
	if err := pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM accepted_block_submission_receipts_v1),
		(SELECT count(*) FROM accepted_block_accounting_effects_v1),
		(SELECT count(*) FROM blocks WHERE hash=$1),
		(SELECT count(*) FROM credits WHERE block_hash=$1),
		b.height,b.reward::text,b.fees::text,b.status,c.miner_address,c.amount::text,c.is_paid
		FROM blocks b JOIN credits c ON c.block_hash=b.hash WHERE b.hash=$1`, legacyKey).Scan(&receipts, &effects, &blocks, &credits, &height, &reward, &fee, &status, &miner, &amount, &paid); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || effects != 1 || blocks != 1 || credits != 1 || height != 7 || reward != wantGross || fee != "0" || status != "PENDING" || miner != "miner" || amount != wantGross || paid {
		t.Fatalf("direct materialization receipts=%d effects=%d blocks=%d credits=%d height=%d reward=%s fee=%s status=%s miner=%s amount=%s paid=%t", receipts, effects, blocks, credits, height, reward, fee, status, miner, amount, paid)
	}
	var payoutAfter string
	if err := pool.QueryRow(context.Background(), `SELECT ctid::text||'|'||xmin::text||'|'||tx_hash||'|'||amount::text FROM payouts`).Scan(&payoutAfter); err != nil {
		t.Fatal(err)
	}
	if payoutAfter != payoutBefore {
		t.Fatalf("direct acceptance changed payout row: before=%q after=%q", payoutBefore, payoutAfter)
	}
}

func TestSubmissionStratumResponseFaultBoundaries(t *testing.T) {
	ensureSubmissionTestMetrics()
	for _, after := range []bool{false, true} {
		after := after
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			journal, databaseURL := serverTestJournal(t)
			job, header, extra, share := testSubmissionJob()
			candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
			if err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"` + hexDigest(candidate.ExpectedBlockHash[:]) + `","height":7,"template_seq":9}}`))
			}))
			defer httpServer.Close()
			oldRouter := activeRPCRouter
			activeRPCRouter = &rpcRouter{backend: &rpcBackend{url: httpServer.URL, submitClient: httpServer.Client()}}
			defer func() { activeRPCRouter = oldRouter }()
			coordinator := newSubmissionCoordinator(journal, big.NewInt(1404), "node", nil)
			result := coordinator.submitFrozenCandidate(&client{}, job, header, extra, share, map[string]uint64{"miner": 10}, 10, 1)
			if result.Err != nil || result.Transition.State != storage.SubmissionStateAcceptedFinal {
				t.Fatalf("acceptance setup=%+v err=%v", result.Transition, result.Err)
			}
			var output bytes.Buffer
			writer := io.Writer(&output)
			if after {
				writer = failingSubmissionWriter{}
			}
			c := &client{rw: bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(writer))}
			req := &stratumRequest{ID: 1}
			oldHook := submissionResponseFaultHookV1
			calls := 0
			submissionResponseFaultHookV1 = func(gotAfter bool) error {
				if gotAfter == after {
					calls++
					return errors.New("Stratum response fault")
				}
				return nil
			}
			coordinator.respondToSubmissionDecision(c, req, result)
			submissionResponseFaultHookV1 = oldHook
			if calls != 1 {
				t.Fatalf("response hook calls=%d", calls)
			}
			if !after && output.Len() != 0 {
				t.Fatalf("response crossed pre-write fault: %q", output.String())
			}
			view, _, receipts, _ := reopenServerJournal(t, databaseURL, candidate.CandidateID)
			if view.State != storage.SubmissionStateAcceptedFinal || receipts != 1 {
				t.Fatalf("response fault altered acceptance: %q/%d", view.State, receipts)
			}
		})
	}
}

type submissionFaultRoundTripper struct {
	mode     string
	response []byte
	calls    atomic.Int64
	request  []byte
}

func (t *submissionFaultRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	switch t.mode {
	case "request-before":
		return nil, errors.New("fault before HTTP request write")
	case "request-after":
		body, err := io.ReadAll(req.Body)
		t.request = append([]byte(nil), body...)
		if err != nil {
			return nil, err
		}
		return nil, errors.New("fault after HTTP request write")
	case "timeout":
		return nil, context.DeadlineExceeded
	case "disconnect":
		return nil, net.ErrClosed
	case "response-before":
		return &http.Response{StatusCode: http.StatusOK, Body: &faultResponseBody{before: true}, Header: make(http.Header), Request: req}, nil
	case "response-after":
		return &http.Response{StatusCode: http.StatusOK, Body: &faultResponseBody{payload: t.response}, Header: make(http.Header), Request: req}, nil
	default:
		return nil, errors.New("unknown test transport fault")
	}
}

type faultResponseBody struct {
	payload []byte
	before  bool
	done    bool
}

func (b *faultResponseBody) Read(dst []byte) (int, error) {
	if b.before {
		return 0, errors.New("fault before response read")
	}
	if b.done {
		return 0, io.EOF
	}
	b.done = true
	n := copy(dst, b.payload)
	return n, errors.New("fault after response read")
}
func (b *faultResponseBody) Close() error { return nil }

type failingSubmissionWriter struct{}

func (failingSubmissionWriter) Write([]byte) (int, error) {
	return 0, errors.New("failed Stratum write")
}

func btoiServer(value bool) int {
	if value {
		return 1
	}
	return 0
}

var serverDatabaseCounter atomic.Uint64

func serverTestJournal(t *testing.T) (storage.SubmissionJournal, string) {
	t.Helper()
	baseURL := strings.TrimSpace(os.Getenv("POOL_SUBMISSION_JOURNAL_TEST_DATABASE_URL"))
	u, err := url.Parse(baseURL)
	if baseURL == "" || err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.Port() == "" || !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "pool_submission_journal_test_") {
		t.Fatal("POOL_SUBMISSION_JOURNAL_TEST_DATABASE_URL must identify a local pool_submission_journal_test_ database")
	}
	host := strings.ToLower(u.Hostname())
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		t.Fatalf("non-local SQL endpoint %q", host)
	}
	databaseName := fmt.Sprintf("pool_submission_journal_test_server_%d", serverDatabaseCounter.Add(1))
	adminURL := *u
	adminURL.Path = "/postgres"
	adminURL.RawPath = ""
	admin, err := pgxpool.New(context.Background(), adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(context.Background(), `CREATE DATABASE "`+databaseName+`"`); err != nil {
		admin.Close()
		t.Fatalf("create disposable server database: %v", err)
	}
	admin.Close()
	databaseURL := *u
	databaseURL.Path = "/" + databaseName
	databaseURL.RawPath = ""
	journalURL := databaseURL.String()
	t.Cleanup(func() {
		dropAdmin, err := pgxpool.New(context.Background(), adminURL.String())
		if err != nil {
			t.Errorf("reconnect disposable database administrator: %v", err)
			return
		}
		defer dropAdmin.Close()
		if _, err := dropAdmin.Exec(context.Background(), `DROP DATABASE "`+databaseName+`" WITH (FORCE)`); err != nil {
			t.Errorf("drop disposable server database: %v", err)
			return
		}
		var exists bool
		if err := dropAdmin.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1)`, databaseName).Scan(&exists); err != nil {
			t.Errorf("read disposable database absence: %v", err)
		} else if exists {
			t.Errorf("disposable database %q still exists", databaseName)
		}
	})
	ctx := context.Background()
	admin, err = pgxpool.New(ctx, journalURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	migration, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "schema.sql"))
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, string(migration)); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	admin.Close()
	journal := storage.NewPostgresStorage(journalURL)
	if err := journal.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(journal.Close)
	return journal, journalURL
}

func reopenServerJournal(t *testing.T, databaseURL string, candidateID [32]byte) (storage.CandidateViewV1, int, int, []byte) {
	t.Helper()
	j := storage.NewPostgresStorage(databaseURL)
	if err := j.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.Close)
	view, err := j.GetCandidate(context.Background(), candidateID)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var attempts, receipts int
	var request []byte
	if err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM block_submission_attempts_v1 WHERE candidate_id=$1),(SELECT count(*) FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1),(SELECT request_body FROM block_submission_attempts_v1 WHERE candidate_id=$1 ORDER BY attempt_no DESC LIMIT 1)`, candidateID[:]).Scan(&attempts, &receipts, &request); err != nil {
		t.Fatal(err)
	}
	return view, attempts, receipts, request
}
