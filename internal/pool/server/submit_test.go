package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

var submitPayloadBenchSink []byte
var currentJobBenchSink *JobData

func TestAppendSubmitBlockHeaderPayload(t *testing.T) {
	got := appendSubmitBlockHeaderPayload(nil, "submitBlockHeader", "0a0b0c", 42)
	want := `{"jsonrpc":"2.0","id":1,"method":"submitBlockHeader","params":["0a0b0c",42]}`
	if string(got) != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}

	var decoded struct {
		JSONRPC string            `json:"jsonrpc"`
		ID      int               `json:"id"`
		Method  string            `json:"method"`
		Params  []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("payload JSON decode failed: %v", err)
	}
	if decoded.JSONRPC != "2.0" || decoded.ID != 1 || decoded.Method != "submitBlockHeader" || len(decoded.Params) != 2 {
		t.Fatalf("decoded payload = %+v", decoded)
	}
}

func TestBuildSubmitHTTPClientTimeout(t *testing.T) {
	got := buildSubmitHTTPClient(0)
	if got == nil {
		t.Fatal("default submit timeout returned nil client")
	}
	if got == http.DefaultClient {
		t.Fatal("default submit timeout should create a dedicated client")
	}
	if got.Timeout != defaultSubmitRPCTimeout {
		t.Fatalf("default client timeout = %s, want %s", got.Timeout, defaultSubmitRPCTimeout)
	}
	client := buildSubmitHTTPClient(1500)
	if client == nil {
		t.Fatal("configured submit timeout returned nil client")
	}
	if client == http.DefaultClient {
		t.Fatal("configured submit timeout should create a dedicated client")
	}
	if client.Timeout != 1500*time.Millisecond {
		t.Fatalf("client timeout = %s, want 1.5s", client.Timeout)
	}
}

func TestAppendSubmitBlockHeaderV2Payload(t *testing.T) {
	got := appendSubmitBlockHeaderPayload(nil, "submitBlockHeaderV2", "0a0b0c", 42)
	want := `{"jsonrpc":"2.0","id":1,"method":"submitBlockHeaderV2","params":["0a0b0c",42]}`
	if string(got) != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}
}

func TestSubmitBlockHeaderPayloadCacheReusesMethodPayloads(t *testing.T) {
	cache := newSubmitBlockHeaderPayloadCache("0a0b0c", 42)

	legacyFirst := cache.payload("submitBlockHeader")
	legacySecond := cache.payload("submitBlockHeader")
	if len(legacyFirst) == 0 || &legacyFirst[0] != &legacySecond[0] {
		t.Fatal("legacy submit payload was not reused")
	}

	v2First := cache.payload("submitBlockHeaderV2")
	v2Second := cache.payload("submitBlockHeaderV2")
	if len(v2First) == 0 || &v2First[0] != &v2Second[0] {
		t.Fatal("v2 submit payload was not reused")
	}
	if bytes.Equal(legacyFirst, v2First) {
		t.Fatalf("legacy and v2 payloads should differ: %s", legacyFirst)
	}
}

func TestBuildBlockSubmissionFromStructuredAcceptedResult(t *testing.T) {
	height := make([]byte, 8)
	binary.LittleEndian.PutUint64(height, 123)
	submission := buildBlockSubmission(&JobData{
		BackendName: "node",
		TemplateSeq: 77,
		Height:      height,
	}, "candidate-1", map[string]interface{}{
		"accepted":     true,
		"code":         "accepted",
		"message":      "accepted",
		"block_hash":   "node-block-1",
		"height":       float64(124),
		"template_seq": float64(78),
	}, nil)

	if !submission.Accepted ||
		submission.CandidateHash != "candidate-1" ||
		submission.NodeBlockHash != "node-block-1" ||
		submission.BackendName != "node" ||
		submission.Outcome != "ok" ||
		submission.Message != "accepted" ||
		submission.Height != 124 ||
		submission.TemplateSeq != 78 {
		t.Fatalf("submission = %#v", submission)
	}
}

func TestBuildBlockSubmissionFromRejectedError(t *testing.T) {
	height := make([]byte, 8)
	binary.LittleEndian.PutUint64(height, 222)
	err := &submitRPCError{kind: submitErrorSemantic, reason: "tip-overdue", err: errors.New("too late")}
	submission := buildBlockSubmission(&JobData{
		BackendName: "node",
		TemplateSeq: 81,
		Height:      height,
	}, "candidate-2", nil, err)

	if submission.Accepted ||
		submission.CandidateHash != "candidate-2" ||
		submission.BackendName != "node" ||
		submission.Outcome != "tip-overdue" ||
		submission.Message != "too late" ||
		submission.Height != 222 ||
		submission.TemplateSeq != 81 {
		t.Fatalf("submission = %#v", submission)
	}
}

func TestCurrentJobForClientReturnsCurrentJobOnly(t *testing.T) {
	c := &client{}
	current := &JobData{TemplateSeq: 2}
	old := &JobData{TemplateSeq: 1}
	rememberClientCurrentJob(c, "current", current)

	if got, ok := currentJobForClient(c, "current"); !ok || got != current {
		t.Fatalf("currentJobForClient(current) = (%p, %v), want (%p, true)", got, ok, current)
	}
	if got, ok := currentJobForClient(c, "old"); ok || got != nil {
		t.Fatalf("currentJobForClient(old) = (%p, %v), want (nil, false)", got, ok)
	}

	acceptedJobs.Store("old", old)
	t.Cleanup(func() {
		acceptedJobs.Delete("old")
	})
	tplVal, ok := acceptedJobs.Load("old")
	if !ok || tplVal.(*JobData) != old {
		t.Fatal("acceptedJobs fallback fixture was not stored")
	}
}

func TestCanonicalSubmittedJobIDStripsAppendedExtranonce2(t *testing.T) {
	c := &client{}
	c.extranonce1 = [4]byte{0xae, 0x00, 0x00, 0x00}

	job := &JobData{TemplateSeq: 101440}
	rememberClientCurrentJob(c, "101440-18b879c7161e781b_ae000000", job)

	got, aliasKind, aliased := canonicalSubmittedJobID(c, "101440-18b879c7161e781b_ae00000000000000", "00000000")
	if !aliased || aliasKind != "appended_extranonce2" || got != "101440-18b879c7161e781b_ae000000" {
		t.Fatalf("canonicalSubmittedJobID() = %q/%q/%v, want canonical live ASIC job ID", got, aliasKind, aliased)
	}

	if current, ok := currentJobForClient(c, got); !ok || current != job {
		t.Fatalf("currentJobForClient(canonical) = (%p, %v), want current job", current, ok)
	}
}

func TestCanonicalSubmittedJobIDStripsAppendedExtranonce2ForRetainedAcceptedJob(t *testing.T) {
	c := &client{}
	c.extranonce1 = [4]byte{0xae, 0x00, 0x00, 0x00}

	acceptedJobs.Store("101439-18b879c61234abcd_ae000000", &JobData{TemplateSeq: 101439})
	defer acceptedJobs.Delete("101439-18b879c61234abcd_ae000000")

	got, aliasKind, aliased := canonicalSubmittedJobID(c, "101439-18b879c61234abcd_ae00000000000000", "00000000")
	if !aliased || aliasKind != "appended_extranonce2" || got != "101439-18b879c61234abcd_ae000000" {
		t.Fatalf("canonicalSubmittedJobID() = %q/%q/%v, want canonical retained ASIC job ID", got, aliasKind, aliased)
	}
}

func TestCanonicalSubmittedJobIDRequiresClientExtranonceMatch(t *testing.T) {
	c := &client{}
	c.extranonce1 = [4]byte{0xae, 0x00, 0x00, 0x00}
	rememberClientCurrentJob(c, "101440-18b879c7161e781b_ae000000", &JobData{TemplateSeq: 101440})

	got, aliasKind, aliased := canonicalSubmittedJobID(c, "101440-18b879c7161e781b_b000000000000000", "00000000")
	if aliased || got != "101440-18b879c7161e781b_b000000000000000" {
		t.Fatalf("canonicalSubmittedJobID() = %q/%q/%v, want no alias for another client's extranonce1", got, aliasKind, aliased)
	}
}

func TestCanonicalSubmittedJobIDLeavesCanonicalAndMalformedIDsAlone(t *testing.T) {
	c := &client{}
	c.extranonce1 = [4]byte{0xae, 0x00, 0x00, 0x00}
	rememberClientCurrentJob(c, "101440-18b879c7161e781b_ae000000", &JobData{TemplateSeq: 101440})

	for _, tc := range []struct {
		name        string
		jobID       string
		extranonce2 string
	}{
		{name: "canonical", jobID: "101440-18b879c7161e781b_ae000000", extranonce2: "00000000"},
		{name: "bad extranonce2", jobID: "101440-18b879c7161e781b_ae00000000000000", extranonce2: "not-hex!"},
		{name: "missing separator", jobID: "101440-18b879c7161e781bae00000000000000", extranonce2: "00000000"},
		{name: "unassigned alias", jobID: "101441-18b879c7161e781b_ae00000000000000", extranonce2: "00000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, aliasKind, aliased := canonicalSubmittedJobID(c, tc.jobID, tc.extranonce2)
			if aliased || got != tc.jobID {
				t.Fatalf("canonicalSubmittedJobID() = %q/%q/%v, want original %q", got, aliasKind, aliased, tc.jobID)
			}
		})
	}
}

func TestSubmitRequestReceivedAtUsesReadTimestampOrFallback(t *testing.T) {
	fallback := time.Unix(100, 0)
	receivedAt := fallback.Add(-25 * time.Millisecond)

	if got := submitRequestReceivedAt(&stratumRequest{ReceivedAt: receivedAt}, fallback); got != receivedAt {
		t.Fatalf("submitRequestReceivedAt() = %s, want received timestamp %s", got, receivedAt)
	}
	if got := submitRequestReceivedAt(&stratumRequest{}, fallback); got != fallback {
		t.Fatalf("submitRequestReceivedAt(empty) = %s, want fallback %s", got, fallback)
	}
	if got := submitRequestReceivedAt(nil, fallback); got != fallback {
		t.Fatalf("submitRequestReceivedAt(nil) = %s, want fallback %s", got, fallback)
	}
}

func TestSubmitJobClassificationUsesLowCardinalityLifecycleStates(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{cfg: &Config{NonCleanJobGraceMS: 5000, OrdinaryStaleShareGraceMS: 1200}}
	rememberClientCurrentJob(c, "current", &JobData{TemplateKey: "same"})

	current := &JobData{TemplateKey: "same"}
	if got := submitJobClassification(c, "current", current, true, now); got != submitLifecycleCurrent {
		t.Fatalf("current classification = %q, want %q", got, submitLifecycleCurrent)
	}

	retainedGrace := &JobData{TemplateKey: "same", SentAt: now.Add(-time.Second)}
	if got := submitJobClassification(c, "old", retainedGrace, false, now); got != submitLifecycleRetainedNonCleanGrace {
		t.Fatalf("retained grace classification = %q, want %q", got, submitLifecycleRetainedNonCleanGrace)
	}

	retained := &JobData{TemplateKey: "other", SentAt: now.Add(-time.Second)}
	if got := submitJobClassification(c, "old", retained, false, now); got != submitLifecycleRetained {
		t.Fatalf("retained classification = %q, want %q", got, submitLifecycleRetained)
	}

	invalidated := &JobData{TemplateKey: "other", SentAt: now.Add(-time.Second)}
	invalidated.invalidated.Store(true)
	if got := submitJobClassification(c, "old", invalidated, false, now); got != submitLifecycleInvalidated {
		t.Fatalf("invalidated classification = %q, want %q", got, submitLifecycleInvalidated)
	}

	if got := submitJobClassification(c, "missing", nil, false, now); got != submitLifecycleJobNotFound {
		t.Fatalf("missing classification = %q, want %q", got, submitLifecycleJobNotFound)
	}
	if got := submitJobClassification(c, "", nil, false, now); got != submitLifecycleNoJob {
		t.Fatalf("no-job classification = %q, want %q", got, submitLifecycleNoJob)
	}
}

func TestCanAcceptRetainedNonCleanSameTemplateJobWithinGrace(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{cfg: &Config{NonCleanJobGraceMS: 5000}}
	rememberClientCurrentJob(c, "current", &JobData{TemplateKey: "same"})

	old := &JobData{
		TemplateKey: "same",
		Clean:       true,
		SentAt:      now.Add(-2 * time.Second),
	}
	if !canAcceptRetainedJobForClient(c, old, now) {
		t.Fatal("clean job followed by a same-template non-clean notification inside grace should be accepted")
	}
}

func TestCanAcceptRetainedJobRejectsLatestCleanNotification(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{cfg: &Config{NonCleanJobGraceMS: 5000}}
	rememberClientCurrentJob(c, "current", &JobData{TemplateKey: "same", Clean: true})

	old := &JobData{TemplateKey: "same", SentAt: now.Add(-2 * time.Second)}
	if canAcceptRetainedJobForClient(c, old, now) {
		t.Fatal("latest clean notification must revoke every retained job")
	}
}

func TestCanAcceptRetainedJobRejectsDifferentInvalidOrExpired(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{cfg: &Config{NonCleanJobGraceMS: 5000}}
	rememberClientCurrentJob(c, "current", &JobData{TemplateKey: "same"})

	tests := []struct {
		name string
		job  *JobData
	}{
		{
			name: "different-template",
			job:  &JobData{TemplateKey: "other", SentAt: now.Add(-2 * time.Second)},
		},
		{
			name: "invalidated",
			job: func() *JobData {
				job := &JobData{TemplateKey: "same", SentAt: now.Add(-2 * time.Second)}
				job.invalidated.Store(true)
				return job
			}(),
		},
		{
			name: "stale",
			job: func() *JobData {
				job := &JobData{TemplateKey: "same", SentAt: now.Add(-2 * time.Second)}
				job.stale.Store(true)
				return job
			}(),
		},
		{
			name: "expired-grace",
			job:  &JobData{TemplateKey: "same", SentAt: now.Add(-6 * time.Second)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if canAcceptRetainedJobForClient(c, tt.job, now) {
				t.Fatal("retained job should not be accepted")
			}
		})
	}
}

func TestCanAcceptStaleOrdinaryShareWithinInvalidationGrace(t *testing.T) {
	previousInvalidation := lastJobInvalidationUnixNano.Load()
	t.Cleanup(func() {
		lastJobInvalidationUnixNano.Store(previousInvalidation)
	})

	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt: now.Add(-2 * time.Second),
		SentAt:    now.Add(-2 * time.Second),
	}
	markJobInvalidated(job, now.Add(-500*time.Millisecond))

	if !canAcceptStaleOrdinaryShare(&Config{OrdinaryStaleShareGraceMS: 1200}, job, "stale-job", now) {
		t.Fatal("ordinary stale share inside invalidation grace should be accepted")
	}
}

func TestCanEvaluateRetainedInvalidatedJobInsideOrdinaryGrace(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{cfg: &Config{OrdinaryStaleShareGraceMS: 1200}}
	job := &JobData{
		CreatedAt: now.Add(-2 * time.Second),
		SentAt:    now.Add(-2 * time.Second),
	}
	markJobInvalidated(job, now.Add(-500*time.Millisecond))

	if !canEvaluateRetainedJobForClient(c, job, now) {
		t.Fatal("retained invalidated job inside ordinary-share grace should pass strict-current gate for hash classification")
	}
}

func TestInvalidatedJobPreHashRejectHonorsOrdinaryGrace(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt: now.Add(-2 * time.Second),
		SentAt:    now.Add(-2 * time.Second),
	}
	markJobInvalidated(job, now.Add(-500*time.Millisecond))

	c := &client{cfg: &Config{OrdinaryStaleShareGraceMS: 1200}}
	if shouldRejectInvalidatedJobBeforeHash(c, job, now) {
		t.Fatal("ordinary-share grace should allow invalidated job through pre-hash gate")
	}

	c.cfg.OrdinaryStaleShareGraceMS = 250
	if !shouldRejectInvalidatedJobBeforeHash(c, job, now) {
		t.Fatal("expired ordinary-share grace should reject invalidated job before hash")
	}
}

func TestCanAcceptStaleOrdinaryShareRejectsBlocksReasonsAndExpiredGrace(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt: now.Add(-2 * time.Second),
		SentAt:    now.Add(-2 * time.Second),
	}
	markJobInvalidated(job, now.Add(-2*time.Second))
	cfg := &Config{OrdinaryStaleShareGraceMS: 1200}

	if canAcceptStaleOrdinaryShare(cfg, job, "stale-job", now) {
		t.Fatal("ordinary stale share outside invalidation grace should be rejected")
	}
	if canAcceptStaleOrdinaryShare(cfg, job, "stale-parent", now) {
		t.Fatal("stale-parent should never use ordinary stale share grace")
	}
	if canAcceptStaleOrdinaryShare(&Config{}, job, "stale-job", now) {
		t.Fatal("disabled ordinary stale share grace should reject")
	}
}

func TestBlockCandidateRejectContextCapturesLowCardinalityTiming(t *testing.T) {
	previousSeq := lastSeq.Load()
	lastSeq.Store(44)
	t.Cleanup(func() {
		lastSeq.Store(previousSeq)
	})

	now := time.Unix(100, 0)
	ctx := newBlockCandidateRejectContext(&JobData{
		BackendName: "node",
		CreatedAt:   now.Add(-1500 * time.Millisecond),
		TemplateSeq: 41,
		ParentHash:  "0xabc",
	}, now, "0xdef")

	if ctx.Backend != "node" ||
		ctx.JobAge != 1500*time.Millisecond ||
		ctx.TemplateSeq != 41 ||
		ctx.CurrentSeq != 44 ||
		ctx.JobParent != "0xabc" ||
		ctx.CurrentParent != "0xdef" {
		t.Fatalf("context = %#v, want captured backend/timing/template context", ctx)
	}
}

func TestBlockCandidateRejectContextUsesSafeDefaultsAndClampsFutureJobAge(t *testing.T) {
	now := time.Unix(100, 0)
	ctx := newBlockCandidateRejectContext(&JobData{
		CreatedAt: now.Add(time.Second),
	}, now, "")

	if ctx.Backend != "unknown" || ctx.JobAge != 0 {
		t.Fatalf("context = %#v, want unknown backend and clamped zero job age", ctx)
	}

	nilCtx := newBlockCandidateRejectContext(nil, now, "0xparent")
	if nilCtx.Backend != "unknown" || nilCtx.JobAge != 0 || nilCtx.CurrentParent != "0xparent" {
		t.Fatalf("nil context = %#v, want safe defaults", nilCtx)
	}
}

func TestHandleSubmitRejectsUnauthorizedBeforeParsing(t *testing.T) {
	var writes bytes.Buffer
	c := &client{
		addr: "miner-1",
		rw:   bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
	}
	req := &stratumRequest{ID: float64(7), Method: "mining.submit"}

	if err := handleSubmit(c, req); err != nil {
		t.Fatalf("handleSubmit returned error: %v", err)
	}

	var resp stratumResponse
	if err := json.Unmarshal(bytes.TrimSpace(writes.Bytes()), &resp); err != nil {
		t.Fatalf("response decode failed: %v; raw=%q", err, writes.String())
	}
	if resp.Result != nil {
		t.Fatalf("result = %#v, want nil", resp.Result)
	}
	errPayload, ok := resp.Error.([]interface{})
	if !ok || len(errPayload) != 2 {
		t.Fatalf("error payload = %#v, want [24 unauthorized]", resp.Error)
	}
	if errPayload[0] != float64(24) || errPayload[1] != "unauthorized" {
		t.Fatalf("error payload = %#v, want [24 unauthorized]", errPayload)
	}
}

func BenchmarkCurrentJobForClientCache(b *testing.B) {
	c := &client{}
	job := &JobData{TemplateSeq: 42}
	rememberClientCurrentJob(c, "current", job)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, ok := currentJobForClient(c, "current")
		if !ok {
			b.Fatal("current job missing")
		}
		currentJobBenchSink = got
	}
}

func BenchmarkAcceptedJobsSyncMapLoad(b *testing.B) {
	job := &JobData{TemplateSeq: 42}
	acceptedJobs.Store("current", job)
	b.Cleanup(func() {
		acceptedJobs.Delete("current")
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, ok := acceptedJobs.Load("current")
		if !ok {
			b.Fatal("accepted job missing")
		}
		currentJobBenchSink = got.(*JobData)
	}
}

func BenchmarkSubmitBlockHeaderPayloadMapMarshal(b *testing.B) {
	header := "0a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "submitBlockHeader",
			"params":  []interface{}{header, uint64(42)},
		}
		buf, err := json.Marshal(payload)
		if err != nil {
			b.Fatal(err)
		}
		submitPayloadBenchSink = buf
	}
}

func BenchmarkSubmitBlockHeaderPayloadAppend(b *testing.B) {
	header := "0a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		submitPayloadBenchSink = appendSubmitBlockHeaderPayload(make([]byte, 0, len(header)+80), "submitBlockHeader", header, 42)
	}
}

func BenchmarkSubmitBlockHeaderPayloadCacheHit(b *testing.B) {
	header := "0a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	cache := newSubmitBlockHeaderPayloadCache(header, 42)
	submitPayloadBenchSink = cache.payload("submitBlockHeader")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		submitPayloadBenchSink = cache.payload("submitBlockHeader")
	}
}

func TestClassifySubmitRPCError(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "overdue",
			raw:  `{"code":-32000,"message":"block overdue: parent is no longer current"}`,
			want: "tip-overdue",
		},
		{
			name: "expired",
			raw:  `{"message":"template expired"}`,
			want: "expired",
		},
		{
			name: "stale parent",
			raw:  `{"message":"stale parent for submitted block"}`,
			want: "stale-parent",
		},
		{
			name: "parent no longer current",
			raw:  `{"message":"parent is no longer current"}`,
			want: "stale-parent",
		},
		{
			name: "parent not current",
			raw:  `{"message":"parent hash not current for submitted block"}`,
			want: "stale-parent",
		},
		{
			name: "duplicate",
			raw:  `{"message":"duplicate block already known"}`,
			want: "duplicate",
		},
		{
			name: "node syncing initial download",
			raw:  `{"message":"Blockdag is downloading blocks: Client in initial download"}`,
			want: "node-syncing",
		},
		{
			name: "node pool syncing",
			raw:  `{"message":"bdag pool syncing: graph state sync active"}`,
			want: "node-syncing",
		},
		{
			name: "template not ready",
			raw:  `{"message":"mining template not ready during reorg pressure"}`,
			want: "template-not-ready",
		},
		{
			name: "transient tx",
			raw:  `{"message":"nonce too low: tx: 12 state: 13"}`,
			want: "transient-tx",
		},
		{
			name: "missing trie",
			raw:  `{"message":"missing trie node 0xabc"}`,
			want: "missing-trie",
		},
		{
			name: "p2p not current",
			raw:  `{"message":"p2p service not current"}`,
			want: "p2p-not-current",
		},
		{
			name: "miner disabled",
			raw:  `{"message":"You must enable miner by --miner."}`,
			want: "miner-disabled",
		},
		{
			name: "method not found",
			raw:  `{"code":-32601,"message":"Method not found"}`,
			want: "method-not-found",
		},
		{
			name: "invalid pow",
			raw:  `{"message":"invalid proof of work"}`,
			want: "invalid-pow",
		},
		{
			name: "txroot mismatch",
			raw:  `{"message":"txroot mismatch"}`,
			want: "txroot-mismatch",
		},
		{
			name: "state root mismatch",
			raw:  `{"message":"state root mismatch"}`,
			want: "state-root-mismatch",
		},
		{
			name: "too busy",
			raw:  `{"message":"node too busy"}`,
			want: "too-busy",
		},
		{
			name: "unknown rpc error",
			raw:  `{"message":"consensus rejected submitted block"}`,
			want: "rpc-error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifySubmitRPCError(json.RawMessage(test.raw)); got != test.want {
				t.Fatalf("classifySubmitRPCError() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestStructuredSubmitResultAccepted(t *testing.T) {
	raw := json.RawMessage(`{"accepted":true,"code":"accepted","message":"accepted","template_seq":12}`)

	result, err, ok := structuredSubmitResult(raw)
	if !ok {
		t.Fatal("structuredSubmitResult should detect accepted field")
	}
	if err != nil {
		t.Fatalf("structuredSubmitResult returned error: %v", err)
	}
	if result == nil {
		t.Fatal("expected accepted structured result to be returned")
	}
}

func TestStructuredSubmitResultRejected(t *testing.T) {
	raw := json.RawMessage(`{"accepted":false,"code":"tip_overdue","message":"template expired","template_seq":12}`)

	result, err, ok := structuredSubmitResult(raw)
	if !ok {
		t.Fatal("structuredSubmitResult should detect accepted field")
	}
	if result != nil {
		t.Fatalf("rejected structured result = %#v, want nil", result)
	}
	if err == nil {
		t.Fatal("expected rejected structured result to return an error")
	}
	if got := classifySubmitErrorReason(err); got != "tip-overdue" {
		t.Fatalf("classifySubmitErrorReason() = %q, want tip-overdue", got)
	}
}

func TestAcceptedSubmitBlockHash(t *testing.T) {
	hash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	normalized := strings.TrimPrefix(hash, "0x")
	if got := acceptedSubmitBlockHash(map[string]interface{}{"block_hash": hash}); got != normalized {
		t.Fatalf("block_hash result = %q, want %q", got, normalized)
	}
	if got := acceptedSubmitBlockHash(map[string]interface{}{"blockHash": hash}); got != normalized {
		t.Fatalf("blockHash result = %q, want %q", got, normalized)
	}
	if got := acceptedSubmitBlockHash(map[string]interface{}{"block_hash": "not-a-hash"}); got != "" {
		t.Fatalf("invalid hash result = %q", got)
	}
	if got := acceptedSubmitBlockHash("not-a-map"); got != "" {
		t.Fatalf("non-map result = %q", got)
	}
}

func TestNormalizeStructuredSubmitCode(t *testing.T) {
	tests := map[string]string{
		"already_known":        "duplicate",
		"already-known":        "duplicate",
		"duplicate_block":      "duplicate",
		"duplicate-block":      "duplicate",
		"stale_parent":         "stale-parent",
		"stale-parent":         "stale-parent",
		"parent_not_current":   "stale-parent",
		"parent-not-current":   "stale-parent",
		"tip_overdue":          "tip-overdue",
		"tip-overdue":          "tip-overdue",
		"block_overdue":        "tip-overdue",
		"block-overdue":        "tip-overdue",
		"too_late":             "tip-overdue",
		"too-late":             "tip-overdue",
		"template_expired":     "expired",
		"template-expired":     "expired",
		"job_expired":          "expired",
		"job-expired":          "expired",
		"template_seq_unknown": "template-seq-unknown",
		"template-seq-unknown": "template-seq-unknown",
		"invalid_pow":          "invalid-pow",
		"invalid-pow":          "invalid-pow",
		"txroot_mismatch":      "txroot-mismatch",
		"txroot-mismatch":      "txroot-mismatch",
		"missing_trie":         "missing-trie",
		"missing-trie":         "missing-trie",
		"nonce_too_low":        "nonce-too-low",
		"nonce-too-low":        "nonce-too-low",
		"p2p_not_current":      "p2p-not-current",
		"p2p-not-current":      "p2p-not-current",
		" Miner_Disabled ":     "miner-disabled",
	}
	for input, want := range tests {
		if got := normalizeStructuredSubmitCode(input); got != want {
			t.Fatalf("normalizeStructuredSubmitCode(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestClassifySubmitErrorReasonPrefersStructuredSubmitError(t *testing.T) {
	err := &submitRPCError{
		kind:   submitErrorSemantic,
		reason: "duplicate",
		err:    errors.New("RPC error: already known"),
	}
	if got := classifySubmitErrorReason(err); got != "duplicate" {
		t.Fatalf("classifySubmitErrorReason() = %q, want duplicate", got)
	}
}

func TestIsStaleSubmitReason(t *testing.T) {
	staleReasons := []string{"tip-overdue", "expired", "stale-parent", "stale"}
	for _, reason := range staleReasons {
		if !isStaleSubmitReason(reason) {
			t.Fatalf("isStaleSubmitReason(%q) = false, want true", reason)
		}
	}

	nonStaleReasons := []string{"duplicate", "node-syncing", "transient-tx", "invalid-pow", "rpc-error"}
	for _, reason := range nonStaleReasons {
		if isStaleSubmitReason(reason) {
			t.Fatalf("isStaleSubmitReason(%q) = true, want false", reason)
		}
	}
}

func TestObsoleteDepth(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		depth int
		ok    bool
	}{
		{
			name:  "depth from node error",
			err:   errors.New("main tip is overdue. Obsolete depth:425\n)"),
			depth: 425,
			ok:    true,
		},
		{
			name:  "depth one",
			err:   errors.New("obsolete depth:1"),
			depth: 1,
			ok:    true,
		},
		{
			name: "missing depth",
			err:  errors.New("main tip is overdue"),
			ok:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			depth, ok := obsoleteDepth(test.err)
			if depth != test.depth || ok != test.ok {
				t.Fatalf("obsoleteDepth() = %d/%v, want %d/%v", depth, ok, test.depth, test.ok)
			}
		})
	}
}

func TestIsRetryableSubmitErrorForSemanticReasons(t *testing.T) {
	tests := []struct {
		name      string
		reason    string
		message   string
		retryable bool
	}{
		{name: "node syncing", reason: "node-syncing", message: "initial download", retryable: true},
		{name: "transient tx", reason: "transient-tx", message: "nonce too low", retryable: true},
		{name: "nonce too low", reason: "nonce-too-low", message: "nonce too low", retryable: true},
		{name: "missing trie", reason: "missing-trie", message: "missing trie", retryable: true},
		{name: "p2p not current", reason: "p2p-not-current", message: "p2p not current", retryable: true},
		{name: "template seq unknown", reason: "template-seq-unknown", message: "no current mining template", retryable: true},
		{name: "miner disabled", reason: "miner-disabled", message: "You must enable miner by --miner.", retryable: true},
		{name: "tip overdue depth one", reason: "tip-overdue", message: "obsolete depth:1", retryable: false},
		{name: "tip overdue deep stale", reason: "tip-overdue", message: "obsolete depth:425", retryable: false},
		{name: "invalid pow", reason: "invalid-pow", message: "invalid proof of work", retryable: false},
		{name: "duplicate", reason: "duplicate", message: "already known", retryable: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := &submitRPCError{kind: submitErrorSemantic, reason: test.reason, err: errors.New(test.message)}
			if got := isRetryableSubmitError(err); got != test.retryable {
				t.Fatalf("isRetryableSubmitError() = %v, want %v", got, test.retryable)
			}
		})
	}
}

func TestShouldSubmitStaleBlockCandidate(t *testing.T) {
	if !shouldSubmitStaleBlockCandidate(nil) {
		t.Fatal("nil config should preserve legacy stale candidate submit behavior")
	}
	if !shouldSubmitStaleBlockCandidate(&Config{}) {
		t.Fatal("zero config should preserve legacy stale candidate submit behavior")
	}
	if shouldSubmitStaleBlockCandidate(&Config{RejectStaleBlockCandidates: true}) {
		t.Fatal("RejectStaleBlockCandidates should disable stale candidate submission")
	}
}

func TestEvaluateLateBlockCandidateSubmitRejectsStaleCandidateWhenConfigured(t *testing.T) {
	previousSeq := lastSeq.Load()
	t.Cleanup(func() {
		lastSeq.Store(previousSeq)
	})
	lastSeq.Store(44)

	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-3 * time.Second),
		TemplateSeq: 41,
		BackendName: "devnet-node",
		ParentHash:  "old-tip",
	}

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg:            &Config{RejectStaleBlockCandidates: true},
		job:            job,
		now:            now,
		currentTip:     "new-tip",
		jobStaleReason: "stale-parent",
	})

	if decision.action != lateBlockCandidateRejectLocal {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateRejectLocal)
	}
	if decision.reason != "stale-parent" {
		t.Fatalf("reason = %q, want stale-parent", decision.reason)
	}
	if !decision.recordAgeFeedback || !decision.recordTemplatePressure || !decision.recordSubmitRecovery || !decision.recordLocalRejectAge {
		t.Fatalf("stale reject side effects not requested: %#v", decision)
	}
	if !decision.incrementStaleLocalDrop || decision.incrementDuplicateDrop {
		t.Fatalf("drop counters = stale:%v duplicate:%v", decision.incrementStaleLocalDrop, decision.incrementDuplicateDrop)
	}
	if decision.blockSubmitOutcomeReason != "stale-parent" {
		t.Fatalf("block outcome reason = %q, want stale-parent", decision.blockSubmitOutcomeReason)
	}
	if decision.shareRejectLabel != "stale_block_candidate" {
		t.Fatalf("share label = %q, want stale_block_candidate", decision.shareRejectLabel)
	}
	if decision.forceCleanRefreshReason != "stale block candidate: stale-parent" {
		t.Fatalf("refresh reason = %q", decision.forceCleanRefreshReason)
	}
	if !decision.handleStaleTemplateRace {
		t.Fatal("stale reject should request stale-template race handling")
	}
	if decision.rejectContext.Backend != "devnet-node" || decision.rejectContext.JobAge != 3*time.Second || decision.rejectContext.TemplateSeq != 41 || decision.rejectContext.CurrentSeq != 44 {
		t.Fatalf("reject context = %#v", decision.rejectContext)
	}
	if job.blockSubmitted.Load() {
		t.Fatal("stale policy decision should not mark the job as block-submitted")
	}
}

func TestEvaluateLateBlockCandidateSubmitSubmitsRecentOldTemplateCandidateWithinGrace(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-350 * time.Millisecond),
		TemplateSeq: 42,
		BackendName: "devnet-node",
		ParentHash:  "0xabc",
	}

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg: &Config{
			RejectStaleBlockCandidates:             true,
			RecentStaleBlockCandidateSubmitGraceMS: 750,
		},
		job:            job,
		now:            now,
		currentTip:     "abc",
		jobStaleReason: "old-template-sequence",
	})

	if decision.action != lateBlockCandidateSubmitStale {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateSubmitStale)
	}
	if decision.reason != "old-template-sequence" {
		t.Fatalf("reason = %q, want old-template-sequence", decision.reason)
	}
	if !decision.recordTemplatePressure {
		t.Fatal("recent stale submit should still record template pressure")
	}
	if decision.recordSubmitRecovery || decision.recordAgeFeedback || decision.incrementStaleLocalDrop || decision.recordLocalRejectAge {
		t.Fatalf("recent stale submit should not request local reject effects: %#v", decision)
	}
}

func TestEvaluateLateBlockCandidateSubmitSubmitsRecentlyInvalidatedCandidateWithinGrace(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-3 * time.Second),
		TemplateSeq: 42,
		BackendName: "devnet-node",
		ParentHash:  "old-tip",
	}
	markJobInvalidated(job, now.Add(-125*time.Millisecond))

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg: &Config{
			RejectStaleBlockCandidates:             true,
			RecentStaleBlockCandidateSubmitGraceMS: 750,
		},
		job:            job,
		now:            now,
		currentTip:     "new-tip",
		jobStaleReason: "stale-job",
	})

	if decision.action != lateBlockCandidateSubmitStale {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateSubmitStale)
	}
	if decision.reason != "stale-job" {
		t.Fatalf("reason = %q, want stale-job", decision.reason)
	}
	if !decision.recordTemplatePressure {
		t.Fatal("recent invalidated stale submit should still record template pressure")
	}
	if decision.recordSubmitRecovery || decision.recordAgeFeedback || decision.incrementStaleLocalDrop || decision.recordLocalRejectAge {
		t.Fatalf("recent invalidated submit should not request local reject effects: %#v", decision)
	}
}

func TestEvaluateLateBlockCandidateSubmitRejectsOldInvalidatedCandidateAfterGrace(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-3 * time.Second),
		TemplateSeq: 42,
		BackendName: "devnet-node",
		ParentHash:  "old-tip",
	}
	markJobInvalidated(job, now.Add(-2*time.Second))

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg: &Config{
			RejectStaleBlockCandidates:             true,
			RecentStaleBlockCandidateSubmitGraceMS: 750,
		},
		job:            job,
		now:            now,
		currentTip:     "new-tip",
		jobStaleReason: "stale-job",
	})

	if decision.action != lateBlockCandidateRejectLocal {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateRejectLocal)
	}
	if decision.reason != "stale-job" {
		t.Fatalf("reason = %q, want stale-job", decision.reason)
	}
	if !decision.incrementStaleLocalDrop || !decision.recordSubmitRecovery || !decision.recordLocalRejectAge {
		t.Fatalf("old invalidated stale-job reject should keep local reject effects: %#v", decision)
	}
}

func TestEvaluateLateBlockCandidateSubmitRejectsRecentStaleParentDespiteGrace(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-350 * time.Millisecond),
		TemplateSeq: 42,
		BackendName: "devnet-node",
		ParentHash:  "old-tip",
	}

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg: &Config{
			RejectStaleBlockCandidates:             true,
			RecentStaleBlockCandidateSubmitGraceMS: 750,
		},
		job:            job,
		now:            now,
		currentTip:     "new-tip",
		jobStaleReason: "stale-parent",
	})

	if decision.action != lateBlockCandidateRejectLocal {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateRejectLocal)
	}
	if decision.reason != "stale-parent" {
		t.Fatalf("reason = %q, want stale-parent", decision.reason)
	}
	if !decision.incrementStaleLocalDrop || !decision.recordSubmitRecovery || !decision.recordLocalRejectAge {
		t.Fatalf("recent stale-parent reject should keep local reject effects: %#v", decision)
	}
}

func TestEvaluateLateBlockCandidateSubmitRejectsRecentOldTemplateOnDifferentParent(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-350 * time.Millisecond),
		TemplateSeq: 42,
		BackendName: "devnet-node",
		ParentHash:  "old-tip",
	}

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg: &Config{
			RejectStaleBlockCandidates:             true,
			RecentStaleBlockCandidateSubmitGraceMS: 750,
		},
		job:            job,
		now:            now,
		currentTip:     "new-tip",
		jobStaleReason: "old-template-sequence",
	})

	if decision.action != lateBlockCandidateRejectLocal {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateRejectLocal)
	}
	if decision.reason != "old-template-sequence" {
		t.Fatalf("reason = %q, want old-template-sequence", decision.reason)
	}
	if !decision.incrementStaleLocalDrop || !decision.recordSubmitRecovery || !decision.recordLocalRejectAge {
		t.Fatalf("recent different-parent old-template reject should keep local reject effects: %#v", decision)
	}
}

func TestEvaluateLateBlockCandidateSubmitRejectsExpiredCandidateDespiteGrace(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{CreatedAt: now.Add(-350 * time.Millisecond)}

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg: &Config{
			RejectStaleBlockCandidates:             true,
			RecentStaleBlockCandidateSubmitGraceMS: 750,
		},
		job:            job,
		now:            now,
		jobStaleReason: "expired",
	})

	if decision.action != lateBlockCandidateRejectLocal {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateRejectLocal)
	}
	if decision.reason != "expired" {
		t.Fatalf("reason = %q, want expired", decision.reason)
	}
}

func TestEvaluateLateBlockCandidateSubmitSubmitsStaleCandidateByDefault(t *testing.T) {
	job := &JobData{}

	decision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
		cfg:            &Config{},
		job:            job,
		now:            time.Unix(100, 0),
		jobStaleReason: "expired",
	})

	if decision.action != lateBlockCandidateSubmitStale {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateSubmitStale)
	}
	if decision.reason != "expired" {
		t.Fatalf("reason = %q, want expired", decision.reason)
	}
	if !decision.recordTemplatePressure {
		t.Fatal("stale submit should still record template pressure")
	}
	if decision.forceCleanRefreshReason != "stale block candidate submit: expired" {
		t.Fatalf("refresh reason = %q", decision.forceCleanRefreshReason)
	}
	if decision.recordSubmitRecovery || decision.recordAgeFeedback || decision.incrementStaleLocalDrop {
		t.Fatalf("stale submit should not request local reject effects: %#v", decision)
	}
	if job.blockSubmitted.Load() {
		t.Fatal("stale submit decision should not mark the job before duplicate policy runs")
	}
}

func TestEvaluateLateBlockCandidateSubmitMarksFirstBlockCandidate(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(0)
	job := &JobData{}

	decision := evaluateDuplicateBlockCandidateSubmit(lateBlockCandidateInput{
		cfg:      &Config{GlobalBlockDedupeEnabled: true},
		job:      job,
		blockKey: "block-a",
		now:      time.Unix(100, 0),
	})

	if decision.action != lateBlockCandidateSubmit {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateSubmit)
	}
	if !job.blockSubmitted.Load() {
		t.Fatal("accepted block candidate should mark the job as block-submitted")
	}
}

func TestEvaluateLateBlockCandidateSubmitRejectsDuplicateJobCandidate(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(0)
	job := &JobData{}
	now := time.Unix(100, 0)

	first := evaluateDuplicateBlockCandidateSubmit(lateBlockCandidateInput{
		cfg:      &Config{GlobalBlockDedupeEnabled: true},
		job:      job,
		blockKey: "block-a",
		now:      now,
	})
	if first.action != lateBlockCandidateSubmit {
		t.Fatalf("first action = %s, want %s", first.action, lateBlockCandidateSubmit)
	}

	decision := evaluateDuplicateBlockCandidateSubmit(lateBlockCandidateInput{
		cfg:        &Config{GlobalBlockDedupeEnabled: true},
		job:        job,
		blockKey:   "block-b",
		now:        now.Add(time.Second),
		currentTip: "tip",
	})

	if decision.action != lateBlockCandidateRejectLocal {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateRejectLocal)
	}
	if decision.reason != "duplicate-block-job" || !decision.duplicate {
		t.Fatalf("duplicate decision = %#v", decision)
	}
	if decision.shareRejectLabel != "duplicate_block" {
		t.Fatalf("share label = %q, want duplicate_block", decision.shareRejectLabel)
	}
	if !decision.incrementDuplicateDrop || decision.incrementStaleLocalDrop {
		t.Fatalf("drop counters = stale:%v duplicate:%v", decision.incrementStaleLocalDrop, decision.incrementDuplicateDrop)
	}
	if decision.blockSubmitOutcomeReason != "duplicate-block" {
		t.Fatalf("block outcome reason = %q, want duplicate-block", decision.blockSubmitOutcomeReason)
	}
	if decision.forceCleanRefreshReason != "" {
		t.Fatalf("refresh reason = %q, want none for duplicate local drop", decision.forceCleanRefreshReason)
	}
}

func TestEvaluateLateBlockCandidateSubmitRejectsGlobalDuplicateCandidate(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(0)
	cfg := &Config{GlobalBlockDedupeEnabled: true, AllowMultipleBlockCandidatesPerJob: true}
	job := &JobData{}
	now := time.Unix(100, 0)

	first := evaluateDuplicateBlockCandidateSubmit(lateBlockCandidateInput{
		cfg:      cfg,
		job:      job,
		blockKey: "block-a",
		now:      now,
	})
	if first.action != lateBlockCandidateSubmit {
		t.Fatalf("first action = %s, want %s", first.action, lateBlockCandidateSubmit)
	}

	decision := evaluateDuplicateBlockCandidateSubmit(lateBlockCandidateInput{
		cfg:      cfg,
		job:      job,
		blockKey: "block-a",
		now:      now.Add(time.Second),
	})

	if decision.action != lateBlockCandidateRejectLocal {
		t.Fatalf("action = %s, want %s", decision.action, lateBlockCandidateRejectLocal)
	}
	if decision.reason != "duplicate-block-global" || !decision.duplicate {
		t.Fatalf("duplicate decision = %#v", decision)
	}
	if decision.shareRejectLabel != "duplicate_block_global" {
		t.Fatalf("share label = %q, want duplicate_block_global", decision.shareRejectLabel)
	}
	if !decision.recordSubmitRecovery || !decision.recordLocalRejectAge || !decision.duplicateBlockReplayCheck {
		t.Fatalf("duplicate reject side effects not requested: %#v", decision)
	}
}

func TestShouldUseGlobalBlockDedupe(t *testing.T) {
	if shouldUseGlobalBlockDedupe(nil) {
		t.Fatal("nil config should not enable global block dedupe")
	}
	if shouldUseGlobalBlockDedupe(&Config{}) {
		t.Fatal("zero config should not enable global block dedupe")
	}
	if !shouldUseGlobalBlockDedupe(&Config{GlobalBlockDedupeEnabled: true}) {
		t.Fatal("GlobalBlockDedupeEnabled should enable global block dedupe")
	}
}

func TestShouldRequireCurrentJobSubmit(t *testing.T) {
	if shouldRequireCurrentJobSubmit(nil) {
		t.Fatal("nil config should not require strict current-job submit")
	}
	if shouldRequireCurrentJobSubmit(&Config{}) {
		t.Fatal("zero config should preserve legacy acceptedJobs fallback")
	}
	if !shouldRequireCurrentJobSubmit(&Config{StrictCurrentJobSubmit: true}) {
		t.Fatal("StrictCurrentJobSubmit should require per-client current job")
	}
}

func TestBlockCandidateLocalStaleReasonRejectsOldSequenceAfterNewerTemplate(t *testing.T) {
	previousSeq := lastSeq.Load()
	t.Cleanup(func() {
		lastSeq.Store(previousSeq)
	})
	lastSeq.Store(12)

	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-2 * time.Second),
		TemplateSeq: 10,
		ParentHash:  "abc",
	}
	cfg := &Config{MaxBlockCandidateJobAgeMS: 1200}

	if got := blockCandidateLocalStaleReason(cfg, job, now, "abc"); got != "old-template-sequence" {
		t.Fatalf("blockCandidateLocalStaleReason() = %q, want old-template-sequence", got)
	}
}

func TestBlockCandidateLocalStaleReasonAllowsExpiredCurrentSequence(t *testing.T) {
	previousSeq := lastSeq.Load()
	t.Cleanup(func() {
		lastSeq.Store(previousSeq)
	})
	lastSeq.Store(10)

	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now.Add(-2 * time.Second),
		TemplateSeq: 10,
		ParentHash:  "abc",
	}
	cfg := &Config{MaxBlockCandidateJobAgeMS: 1200}

	if got := blockCandidateLocalStaleReason(cfg, job, now, "abc"); got != "" {
		t.Fatalf("blockCandidateLocalStaleReason() = %q, want current-sequence candidate allowed", got)
	}
}

func TestBlockCandidateLocalStaleReasonRejectsStaleParent(t *testing.T) {
	now := time.Unix(100, 0)
	job := &JobData{
		CreatedAt:   now,
		TemplateSeq: 10,
		ParentHash:  "old",
	}

	if got := blockCandidateLocalStaleReason(&Config{MaxBlockCandidateJobAgeMS: 1200}, job, now, "new"); got != "stale-parent" {
		t.Fatalf("blockCandidateLocalStaleReason() = %q, want stale-parent", got)
	}
}

func TestRequestCleanRefreshAfterStaleCandidateRefreshesSupersededCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)

	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousBackendEpochs := drainSyncMap(&backendJobInvalidationEpochs)
	previousInvalidationEpoch := acceptedJobInvalidationEpoch.Load()
	cleanRefreshMu.Lock()
	previousLastCleanRefresh := lastCleanRefresh
	lastCleanRefresh = time.Time{}
	cleanRefreshMu.Unlock()
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	parentMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		drainSyncMap(&backendJobInvalidationEpochs)
		restoreSyncMap(&backendJobInvalidationEpochs, previousBackendEpochs)
		acceptedJobInvalidationEpoch.Store(previousInvalidationEpoch)
		currentParent.Store(previousCurrentParent)
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		cleanRefreshMu.Lock()
		lastCleanRefresh = previousLastCleanRefresh
		cleanRefreshMu.Unlock()
	})

	acceptedJobInvalidationEpoch.Store(0)
	miningWork.rememberParent("new-parent")
	now := time.Unix(100, 0)
	currentJob := &JobData{
		BackendName:              "node",
		ParentHash:               "new-parent",
		CreatedAt:                now,
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch("node"),
	}
	c := &client{authorized: true}
	c.currentJob.Store(&clientCurrentJob{id: "current-job", job: currentJob})
	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()

	if requested := requestCleanRefreshAfterStaleCandidate(&JobData{
		BackendName: "node",
		ParentHash:  "old-parent",
	}, "stale block candidate: stale-job", "stale-job", now); requested {
		cleanTemplateRefreshMu.Lock()
		gotSeq := cleanTemplateRefreshSeq
		cleanTemplateRefreshMu.Unlock()
		if gotSeq == 0 {
			t.Fatal("stale candidate refresh returned true without advancing clean refresh seq")
		}
	} else {
		t.Fatal("stale candidate refresh should be requested even when current jobs are already on the newer parent")
	}
}

func TestRequestCleanRefreshAfterStaleSubmitRefreshesSupersededCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)

	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousCurrentParent := CurrentParent()
	cleanRefreshMu.Lock()
	previousLastCleanRefresh := lastCleanRefresh
	lastCleanRefresh = time.Time{}
	cleanRefreshMu.Unlock()
	parentMu.Lock()
	previousLastParent := lastParent
	parentMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		currentParent.Store(previousCurrentParent)
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		cleanRefreshMu.Lock()
		lastCleanRefresh = previousLastCleanRefresh
		cleanRefreshMu.Unlock()
	})

	miningWork.rememberParent("same-parent")
	now := time.Unix(100, 0)
	currentJob := &JobData{
		BackendName:       "node",
		ParentHash:        "same-parent",
		CreatedAt:         now,
		InvalidationEpoch: acceptedJobInvalidationEpoch.Load(),
	}
	c := &client{authorized: true}
	c.currentJob.Store(&clientCurrentJob{id: "current-job", job: currentJob})
	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()

	if requested := requestCleanRefreshAfterStaleSubmit(&JobData{
		BackendName: "node",
		ParentHash:  "old-parent",
	}, "tip-overdue", now); requested {
		cleanTemplateRefreshMu.Lock()
		gotSeq := cleanTemplateRefreshSeq
		cleanTemplateRefreshMu.Unlock()
		if gotSeq == 0 {
			t.Fatal("stale submit refresh returned true without advancing clean refresh seq")
		}
	} else {
		t.Fatal("stale submit refresh should be requested even when current jobs are already on the newer parent")
	}
}

func TestRequestCleanRefreshAfterStaleSubmitSkipsSupersededInFlightRefresh(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	t.Cleanup(func() {
		cleanTemplateRefreshMu.Lock()
		cleanTemplateRefreshActive = false
		cleanTemplateRefreshLatest = cleanTemplateRefreshRequest{}
		cleanTemplateRefreshMu.Unlock()
	})

	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	parentMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		currentParent.Store(previousCurrentParent)
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
	})

	clientsMu.Lock()
	clients = map[*client]struct{}{}
	clientsMu.Unlock()
	miningWork.rememberParent("new-parent")
	cleanTemplateRefreshMu.Lock()
	cleanTemplateRefreshActive = true
	cleanTemplateRefreshLatest = cleanTemplateRefreshRequest{seq: 1, reason: "block submission accepted"}
	cleanTemplateRefreshMu.Unlock()

	job := &JobData{BackendName: "node", ParentHash: "old-parent"}
	if active, reason := supersededCandidateRefreshInFlight(job); !active || reason != "block submission accepted" {
		t.Fatalf("supersededCandidateRefreshInFlight() = %v/%q, want true/block submission accepted", active, reason)
	}
	if requested := requestCleanRefreshAfterStaleSubmit(job, "tip-overdue", time.Unix(100, 0)); requested {
		t.Fatal("stale submit refresh should be suppressed while a superseding clean refresh is in flight")
	}
}

func TestRecordBlockCandidateAgeFeedbackRaisesOnExpiredAndLowersOnTipOverdue(t *testing.T) {
	previousEnabled := blockCandidateJobAgeOverrideEnabled.Load()
	previousMS := blockCandidateJobAgeOverrideMS.Load()
	t.Cleanup(func() {
		blockCandidateJobAgeOverrideMS.Store(previousMS)
		blockCandidateJobAgeOverrideEnabled.Store(previousEnabled)
	})
	blockCandidateJobAgeOverrideEnabled.Store(false)
	blockCandidateJobAgeOverrideMS.Store(0)

	cfg := &Config{
		MaxBlockCandidateJobAgeMS:    2500,
		AutoTuneBlockCandidateJobAge: true,
	}
	now := time.Unix(100, 0)

	recordBlockCandidateAgeFeedback(cfg, "expired", 3200*time.Millisecond, now)
	if got := maxBlockCandidateJobAge(cfg); got != 3700*time.Millisecond {
		t.Fatalf("maxBlockCandidateJobAge() = %s, want 3.7s", got)
	}

	recordBlockCandidateAgeFeedback(cfg, "tip-overdue", 3200*time.Millisecond, now.Add(time.Second))
	if got := maxBlockCandidateJobAge(cfg); got != 3450*time.Millisecond {
		t.Fatalf("maxBlockCandidateJobAge() = %s, want 3.45s", got)
	}
}

func TestRecordBlockCandidateAgeFeedbackNoopsWhenDisabled(t *testing.T) {
	previousEnabled := blockCandidateJobAgeOverrideEnabled.Load()
	previousMS := blockCandidateJobAgeOverrideMS.Load()
	t.Cleanup(func() {
		blockCandidateJobAgeOverrideMS.Store(previousMS)
		blockCandidateJobAgeOverrideEnabled.Store(previousEnabled)
	})
	blockCandidateJobAgeOverrideEnabled.Store(false)
	blockCandidateJobAgeOverrideMS.Store(0)

	cfg := &Config{MaxBlockCandidateJobAgeMS: 2500}
	recordBlockCandidateAgeFeedback(cfg, "expired", 4*time.Second, time.Unix(100, 0))
	if got := maxBlockCandidateJobAge(cfg); got != 2500*time.Millisecond {
		t.Fatalf("maxBlockCandidateJobAge() = %s, want configured 2.5s", got)
	}
}

func TestSubmitStallRecoveryTriggersAfterZeroSuccessFailures(t *testing.T) {
	tracker := newSubmitStallRecoveryTracker(submitStallRecoveryConfig{
		enabled:            true,
		window:             time.Minute,
		minAge:             time.Second,
		failureThreshold:   3,
		staleThreshold:     10,
		duplicateThreshold: 10,
		cooldown:           time.Minute,
	})
	now := time.Unix(100, 0)

	if _, recover := tracker.recordFailure(now, "rpc-error"); recover {
		t.Fatal("first failure should not trigger recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(time.Second), "duplicate"); recover {
		t.Fatal("second failure should not trigger recovery")
	}
	snapshot, recover := tracker.recordFailure(now.Add(2*time.Second), "tip-overdue")
	if !recover {
		t.Fatal("third zero-success failure should trigger recovery")
	}
	if snapshot.Failures != 3 || snapshot.StaleFailures != 1 || snapshot.DuplicateFailures != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestSubmitStallRecoveryAcceptedSubmitResetsFailures(t *testing.T) {
	tracker := newSubmitStallRecoveryTracker(submitStallRecoveryConfig{
		enabled:            true,
		window:             time.Minute,
		minAge:             time.Second,
		failureThreshold:   3,
		staleThreshold:     10,
		duplicateThreshold: 10,
		cooldown:           time.Minute,
	})
	now := time.Unix(100, 0)

	tracker.recordFailure(now, "rpc-error")
	tracker.recordAccepted(now.Add(time.Second))
	if _, recover := tracker.recordFailure(now.Add(2*time.Second), "rpc-error"); recover {
		t.Fatal("first failure after accepted submit should not trigger recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(3*time.Second), "rpc-error"); recover {
		t.Fatal("second failure after accepted submit should not trigger recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(4*time.Second), "rpc-error"); !recover {
		t.Fatal("third failure after accepted submit should trigger recovery")
	}
}

func TestSubmitStallRecoveryUsesStaleAndDuplicateThresholds(t *testing.T) {
	tracker := newSubmitStallRecoveryTracker(submitStallRecoveryConfig{
		enabled:            true,
		window:             time.Minute,
		minAge:             time.Second,
		failureThreshold:   100,
		staleThreshold:     2,
		duplicateThreshold: 3,
		cooldown:           time.Minute,
	})
	now := time.Unix(100, 0)

	if _, recover := tracker.recordFailure(now, "tip-overdue"); recover {
		t.Fatal("first stale failure should not trigger recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(time.Second), "expired"); !recover {
		t.Fatal("stale threshold should trigger recovery")
	}

	tracker = newSubmitStallRecoveryTracker(submitStallRecoveryConfig{
		enabled:            true,
		window:             time.Minute,
		minAge:             time.Second,
		failureThreshold:   100,
		staleThreshold:     100,
		duplicateThreshold: 3,
		cooldown:           time.Minute,
	})
	if _, recover := tracker.recordFailure(now, "duplicate-block"); recover {
		t.Fatal("first duplicate failure should not trigger recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(time.Second), "duplicate-block-global"); recover {
		t.Fatal("second duplicate failure should not trigger recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(2*time.Second), "duplicate"); !recover {
		t.Fatal("duplicate threshold should trigger recovery")
	}
}

func TestSubmitStallRecoveryRequiresMinimumAge(t *testing.T) {
	tracker := newSubmitStallRecoveryTracker(submitStallRecoveryConfig{
		enabled:            true,
		window:             time.Minute,
		minAge:             30 * time.Second,
		failureThreshold:   2,
		staleThreshold:     100,
		duplicateThreshold: 100,
		cooldown:           time.Minute,
	})
	now := time.Unix(100, 0)

	if _, recover := tracker.recordFailure(now, "rpc-error"); recover {
		t.Fatal("first failure should not trigger recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(time.Second), "rpc-error"); recover {
		t.Fatal("minimum age should suppress an early threshold crossing")
	}
	if _, recover := tracker.recordFailure(now.Add(31*time.Second), "rpc-error"); !recover {
		t.Fatal("threshold should trigger after the minimum age")
	}
}

func TestTemplateConversionStallTriggersDespiteIntermittentAcceptedSubmits(t *testing.T) {
	tracker := newTemplateConversionStallRecoveryTracker(templateConversionStallRecoveryConfig{
		enabled:               true,
		window:                2 * time.Minute,
		minAge:                30 * time.Second,
		minCandidates:         10,
		failureRatioThreshold: 80,
		minActiveMiners:       1,
		cooldown:              time.Minute,
	})
	now := time.Unix(100, 0)

	tracker.recordAccepted(now, 6)
	for i := 1; i <= 7; i++ {
		if _, recover := tracker.recordFailure(now.Add(time.Duration(i)*time.Second), "tip-overdue", 6); recover {
			t.Fatalf("failure %d should not trigger before minimum age", i)
		}
	}
	tracker.recordAccepted(now.Add(20*time.Second), 6)

	snapshot, recover := tracker.recordFailure(now.Add(31*time.Second), "duplicate-block", 6)
	if !recover {
		t.Fatal("high failure ratio should trigger recovery even after intermittent accepted submits")
	}
	if snapshot.Accepted != 2 || snapshot.Failures != 8 || snapshot.TotalCandidates != 10 {
		t.Fatalf("snapshot counts = %#v", snapshot)
	}
	if snapshot.FailureRatio != 80 || snapshot.ActiveMiners != 6 {
		t.Fatalf("snapshot ratio/miners = %#v", snapshot)
	}
}

func TestTemplateConversionStallDoesNotTriggerWhenRatioHealthy(t *testing.T) {
	tracker := newTemplateConversionStallRecoveryTracker(templateConversionStallRecoveryConfig{
		enabled:               true,
		window:                time.Minute,
		minAge:                time.Second,
		minCandidates:         10,
		failureRatioThreshold: 60,
		minActiveMiners:       1,
		cooldown:              time.Minute,
	})
	now := time.Unix(100, 0)

	for i := 0; i < 6; i++ {
		tracker.recordAccepted(now.Add(time.Duration(i)*time.Second), 6)
	}
	for i := 0; i < 4; i++ {
		if _, recover := tracker.recordFailure(now.Add(time.Duration(10+i)*time.Second), "tip-overdue", 6); recover {
			t.Fatal("healthy candidate conversion ratio should not trigger recovery")
		}
	}
}

func TestTemplateConversionStallRequiresCandidatesAgeAndActiveMiners(t *testing.T) {
	tracker := newTemplateConversionStallRecoveryTracker(templateConversionStallRecoveryConfig{
		enabled:               true,
		window:                time.Minute,
		minAge:                10 * time.Second,
		minCandidates:         3,
		failureRatioThreshold: 50,
		minActiveMiners:       2,
		cooldown:              time.Minute,
	})
	now := time.Unix(100, 0)

	if _, recover := tracker.recordFailure(now, "tip-overdue", 1); recover {
		t.Fatal("insufficient active miners should suppress recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(time.Second), "tip-overdue", 2); recover {
		t.Fatal("minimum candidate count should suppress recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(2*time.Second), "tip-overdue", 2); recover {
		t.Fatal("minimum age should suppress early recovery")
	}
	if _, recover := tracker.recordFailure(now.Add(11*time.Second), "tip-overdue", 2); !recover {
		t.Fatal("threshold should trigger once candidates, age, and active miners are sufficient")
	}
}

func TestMarkGlobalBlockCandidate(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(0)
	now := time.Unix(100, 0)

	if !markGlobalBlockCandidate("abc", now) {
		t.Fatal("first block candidate mark should be accepted")
	}
	if markGlobalBlockCandidate("abc", now.Add(time.Second)) {
		t.Fatal("duplicate block candidate inside TTL should be rejected")
	}
	if !markGlobalBlockCandidate("abc", now.Add(submittedBlockMarkTTL+time.Second)) {
		t.Fatal("block candidate should be accepted again after TTL")
	}
	if !markGlobalBlockCandidate("", now) {
		t.Fatal("empty block keys should not block submission")
	}
}

func TestMarkBlockCandidateForSubmitRejectsSecondJobCandidateByDefault(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(0)
	cfg := &Config{GlobalBlockDedupeEnabled: true}
	job := &JobData{}
	now := time.Unix(100, 0)

	if reason := markBlockCandidateForSubmit(cfg, job, "block-a", now); reason != "" {
		t.Fatalf("first block candidate should be accepted, got %s", reason)
	}
	if reason := markBlockCandidateForSubmit(cfg, job, "block-b", now.Add(time.Second)); reason != "duplicate-block-job" {
		t.Fatalf("second block candidate should preserve legacy per-job suppression, got %s", reason)
	}
}

func TestMarkBlockCandidateForSubmitAllowsUniqueBlocksForSameJobWhenEnabled(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(0)
	cfg := &Config{GlobalBlockDedupeEnabled: true, AllowMultipleBlockCandidatesPerJob: true}
	job := &JobData{}
	now := time.Unix(100, 0)

	if reason := markBlockCandidateForSubmit(cfg, job, "block-a", now); reason != "" {
		t.Fatalf("first block candidate should be accepted, got %s", reason)
	}
	if !job.blockSubmitted.Load() {
		t.Fatal("job should record that at least one block was submitted")
	}
	if reason := markBlockCandidateForSubmit(cfg, job, "block-b", now.Add(time.Second)); reason != "" {
		t.Fatalf("second unique block candidate for same job should be accepted, got %s", reason)
	}
	if reason := markBlockCandidateForSubmit(cfg, job, "block-b", now.Add(2*time.Second)); reason != "duplicate-block-global" {
		t.Fatalf("same block hash should still be rejected by global dedupe, got %s", reason)
	}
}

func TestMaybeCleanupSubmittedBlockMarksThrottlesRange(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(0)
	now := time.Unix(100, 0)

	submittedBlockMarks.Store("expired", now.Add(-submittedBlockMarkTTL-time.Second))
	submittedBlockMarks.Store("fresh", now)
	maybeCleanupSubmittedBlockMarks(now)
	if _, ok := submittedBlockMarks.Load("expired"); ok {
		t.Fatal("expired mark should be cleaned on first cleanup")
	}
	if _, ok := submittedBlockMarks.Load("fresh"); !ok {
		t.Fatal("fresh mark should remain after cleanup")
	}

	submittedBlockMarks.Store("expired-again", now.Add(-submittedBlockMarkTTL-time.Second))
	maybeCleanupSubmittedBlockMarks(now.Add(time.Second))
	if _, ok := submittedBlockMarks.Load("expired-again"); !ok {
		t.Fatal("cleanup inside throttle interval should not range submitted marks")
	}

	maybeCleanupSubmittedBlockMarks(now.Add(submittedBlockMarkCleanupInterval + time.Second))
	if _, ok := submittedBlockMarks.Load("expired-again"); ok {
		t.Fatal("expired mark should be cleaned after throttle interval")
	}
}

func TestMarkGlobalBlockCandidateExpiredOverwriteDoesNotNeedCleanup(t *testing.T) {
	submittedBlockMarks = sync.Map{}
	lastBlockMarkCleanupUnix.Store(time.Unix(100, 0).Unix())
	now := time.Unix(100, 0)

	submittedBlockMarks.Store("abc", now.Add(-submittedBlockMarkTTL-time.Second))
	if !markGlobalBlockCandidate("abc", now) {
		t.Fatal("expired duplicate should be accepted even when global cleanup is throttled")
	}
	got, ok := submittedBlockMarks.Load("abc")
	if !ok {
		t.Fatal("accepted block candidate should leave a fresh mark")
	}
	if ts, ok := got.(time.Time); !ok || !ts.Equal(now) {
		t.Fatalf("mark timestamp = %#v, want %s", got, now)
	}
}

func TestShouldLogDifficultyPush(t *testing.T) {
	t.Setenv("POOL_DIFFICULTY_PUSH_LOG_INTERVAL_SECONDS", "")
	c := &client{}
	now := time.Unix(100, 0)

	if !shouldLogDifficultyPush(c, 0.06, now) {
		t.Fatal("first difficulty push should be logged")
	}
	if shouldLogDifficultyPush(c, 0.06, now.Add(time.Second)) {
		t.Fatal("unchanged difficulty inside log interval should be suppressed")
	}
	if !shouldLogDifficultyPush(c, 0.07, now.Add(2*time.Second)) {
		t.Fatal("changed difficulty should be logged immediately")
	}
	if shouldLogDifficultyPush(c, 0.07, now.Add(3*time.Second)) {
		t.Fatal("same changed difficulty should be suppressed inside interval")
	}
	if !shouldLogDifficultyPush(c, 0.07, now.Add(defaultDifficultyPushLogInterval+3*time.Second)) {
		t.Fatal("unchanged difficulty should be logged again after interval")
	}
}

func TestClientReadyForCurrentJob(t *testing.T) {
	if clientReadyForCurrentJob(nil) {
		t.Fatal("nil client should not be ready")
	}
	c := &client{}
	if clientReadyForCurrentJob(c) {
		t.Fatal("unauthorized client should not be ready")
	}
	c.authorized = true
	if clientReadyForCurrentJob(c) {
		t.Fatal("authorized client without session should not be ready")
	}
	c.sessionID = "session"
	if !clientReadyForCurrentJob(c) {
		t.Fatal("authorized subscribed client should be ready")
	}
}

func TestSubmitExtranonce1ReturnsLockedCopy(t *testing.T) {
	c := &client{}
	c.extranonce1 = [4]byte{1, 2, 3, 4}

	got := submitExtranonce1(c)
	if got != ([4]byte{1, 2, 3, 4}) {
		t.Fatalf("submitExtranonce1() = %x, want 01020304", got)
	}

	got[0] = 0xff
	if c.extranonce1 != ([4]byte{1, 2, 3, 4}) {
		t.Fatalf("submitExtranonce1 returned alias, client extranonce changed to %x", c.extranonce1)
	}
}

func TestDifficultyPushLogIntervalUsesEnvOverride(t *testing.T) {
	t.Setenv("POOL_DIFFICULTY_PUSH_LOG_INTERVAL_SECONDS", "12")
	if got := difficultyPushLogInterval(); got != 12*time.Second {
		t.Fatalf("difficultyPushLogInterval() = %s, want 12s", got)
	}
}

func TestDifficultyPushLogIntervalFallsBackOnInvalidEnv(t *testing.T) {
	t.Setenv("POOL_DIFFICULTY_PUSH_LOG_INTERVAL_SECONDS", "invalid")
	if got := difficultyPushLogInterval(); got != defaultDifficultyPushLogInterval {
		t.Fatalf("difficultyPushLogInterval() = %s, want %s", got, defaultDifficultyPushLogInterval)
	}

	t.Setenv("POOL_DIFFICULTY_PUSH_LOG_INTERVAL_SECONDS", "0")
	if got := difficultyPushLogInterval(); got != defaultDifficultyPushLogInterval {
		t.Fatalf("difficultyPushLogInterval() with zero = %s, want %s", got, defaultDifficultyPushLogInterval)
	}
}

func TestShouldLogRoutineSubmitThrottlesPerClient(t *testing.T) {
	c := &client{}
	now := time.Unix(100, 0)

	shouldLog, suppressed := shouldLogRoutineSubmit(c, now)
	if !shouldLog || suppressed != 0 {
		t.Fatalf("first routine submit log = %v/%d, want true/0", shouldLog, suppressed)
	}

	shouldLog, suppressed = shouldLogRoutineSubmit(c, now.Add(time.Second))
	if shouldLog || suppressed != 0 {
		t.Fatalf("second routine submit log = %v/%d, want false/0", shouldLog, suppressed)
	}
	shouldLog, suppressed = shouldLogRoutineSubmit(c, now.Add(2*time.Second))
	if shouldLog || suppressed != 0 {
		t.Fatalf("third routine submit log = %v/%d, want false/0", shouldLog, suppressed)
	}

	shouldLog, suppressed = shouldLogRoutineSubmit(c, now.Add(routineSubmitLogInterval+time.Second))
	if !shouldLog || suppressed != 2 {
		t.Fatalf("later routine submit log = %v/%d, want true/2", shouldLog, suppressed)
	}
}

func TestShouldLogRoutineSubmitIsPerClient(t *testing.T) {
	now := time.Unix(100, 0)
	first := &client{}
	second := &client{}

	shouldLogRoutineSubmit(first, now)
	if shouldLog, _ := shouldLogRoutineSubmit(first, now.Add(time.Second)); shouldLog {
		t.Fatal("first client should throttle repeated routine submit logs")
	}
	if shouldLog, suppressed := shouldLogRoutineSubmit(second, now.Add(time.Second)); !shouldLog || suppressed != 0 {
		t.Fatalf("second client routine submit log = %v/%d, want true/0", shouldLog, suppressed)
	}
}

func TestShouldLogValidShareThrottlesPerClient(t *testing.T) {
	c := &client{}
	now := time.Unix(100, 0)

	summary := shouldLogValidShare(c, now, 1.5, 98304)
	if !summary.ShouldLog || summary.Suppressed != 0 || summary.SuppressedDiff != 0 || summary.SuppressedWork != 0 {
		t.Fatalf("first valid share log = %+v, want immediate without suppressed values", summary)
	}

	summary = shouldLogValidShare(c, now.Add(time.Second), 2.0, 131072)
	if summary.ShouldLog {
		t.Fatalf("second valid share log = %+v, want suppressed", summary)
	}
	summary = shouldLogValidShare(c, now.Add(2*time.Second), 3.0, 196608)
	if summary.ShouldLog {
		t.Fatalf("third valid share log = %+v, want suppressed", summary)
	}

	summary = shouldLogValidShare(c, now.Add(validShareLogInterval+time.Second), 4.0, 262144)
	if !summary.ShouldLog || summary.Suppressed != 2 {
		t.Fatalf("later valid share log = %+v, want true with 2 suppressed", summary)
	}
	if summary.SuppressedDiff != 5.0 {
		t.Fatalf("suppressed diff = %f, want 5.0", summary.SuppressedDiff)
	}
	if summary.SuppressedWork != 327680 {
		t.Fatalf("suppressed work = %f, want 327680", summary.SuppressedWork)
	}
}

func TestShouldLogValidShareIsPerClient(t *testing.T) {
	now := time.Unix(100, 0)
	first := &client{}
	second := &client{}

	shouldLogValidShare(first, now, 1.0, 65536)
	if summary := shouldLogValidShare(first, now.Add(time.Second), 1.0, 65536); summary.ShouldLog {
		t.Fatalf("first client valid share log = %+v, want suppressed", summary)
	}
	if summary := shouldLogValidShare(second, now.Add(time.Second), 1.0, 65536); !summary.ShouldLog || summary.Suppressed != 0 {
		t.Fatalf("second client valid share log = %+v, want immediate", summary)
	}
}

func TestRecordLowDifficultyRejectDisconnectThreshold(t *testing.T) {
	c := &client{}
	now := time.Unix(100, 0)

	for i := 1; i < defaultLowDiffRejectDisconnectThreshold; i++ {
		if recordLowDifficultyReject(c, now.Add(time.Duration(i)*time.Millisecond)) {
			t.Fatalf("reject %d should not trigger disconnect yet", i)
		}
	}
	if !recordLowDifficultyReject(c, now.Add(defaultLowDiffRejectDisconnectThreshold*time.Millisecond)) {
		t.Fatal("threshold reject should trigger disconnect")
	}
}

func TestRecordLowDifficultyRejectWindowReset(t *testing.T) {
	c := &client{}
	now := time.Unix(100, 0)

	for i := 1; i < defaultLowDiffRejectDisconnectThreshold; i++ {
		recordLowDifficultyReject(c, now.Add(time.Duration(i)*time.Millisecond))
	}
	if recordLowDifficultyReject(c, now.Add(defaultLowDiffRejectWindow+time.Second)) {
		t.Fatal("reject after window expiry should start a new window")
	}
	resetLowDifficultyRejects(c)
	if c.lowDiffRejects != 0 || !c.lowDiffWindowStart.IsZero() {
		t.Fatal("resetLowDifficultyRejects should clear low-difficulty state")
	}
}

func TestRecordLowDifficultyRejectUsesConfiguredThresholdAndWindow(t *testing.T) {
	c := &client{cfg: &Config{LowDiffRejectDisconnectThreshold: 3, LowDiffRejectWindowSeconds: 2}}
	now := time.Unix(100, 0)

	if recordLowDifficultyReject(c, now) {
		t.Fatal("first reject should not trigger disconnect")
	}
	if recordLowDifficultyReject(c, now.Add(time.Second)) {
		t.Fatal("second reject should not trigger disconnect")
	}
	if !recordLowDifficultyReject(c, now.Add(1500*time.Millisecond)) {
		t.Fatal("configured threshold should trigger disconnect")
	}
	if recordLowDifficultyReject(c, now.Add(4*time.Second)) {
		t.Fatal("reject after configured window should reset the counter")
	}
}

func TestLowDifficultyFloodDisconnectEnabledDefaultsTrue(t *testing.T) {
	if !lowDiffFloodDisconnectEnabled(&client{}) {
		t.Fatal("nil config should enable low-difficulty flood disconnect")
	}
	if !lowDiffFloodDisconnectEnabled(&client{cfg: &Config{}}) {
		t.Fatal("zero config should enable low-difficulty flood disconnect")
	}
	if lowDiffFloodDisconnectEnabled(&client{cfg: &Config{DisableLowDiffFloodDisconnect: true}}) {
		t.Fatal("explicit disabled config should disable low-difficulty flood disconnect")
	}
}

func TestRecordDuplicateBlockReplayDisconnectsRepeatedHashAcrossJobs(t *testing.T) {
	c := &client{}
	now := time.Unix(100, 0)

	for i := 1; i < defaultDuplicateBlockReplayDisconnectThreshold; i++ {
		if recordDuplicateBlockReplay(c, "hash-a", fmt.Sprintf("job-%d", i), now.Add(time.Duration(i)*time.Second)) {
			t.Fatalf("duplicate replay %d should not trigger disconnect", i)
		}
	}
	if !recordDuplicateBlockReplay(c, "hash-a", "job-final", now.Add(5*time.Second)) {
		t.Fatal("repeated duplicate hash across fresh jobs should trigger disconnect")
	}
}

func TestRecordDuplicateBlockReplayResetsOnDifferentHashOrWindow(t *testing.T) {
	c := &client{}
	now := time.Unix(100, 0)

	for i := 0; i < defaultDuplicateBlockReplayDisconnectThreshold-1; i++ {
		recordDuplicateBlockReplay(c, "hash-a", fmt.Sprintf("job-a-%d", i), now.Add(time.Duration(i)*time.Second))
	}
	if recordDuplicateBlockReplay(c, "hash-b", "job-b", now.Add(4*time.Second)) {
		t.Fatal("different hash should reset duplicate replay counter")
	}

	for i := 0; i < defaultDuplicateBlockReplayDisconnectThreshold-1; i++ {
		recordDuplicateBlockReplay(c, "hash-b", fmt.Sprintf("job-b-%d", i), now.Add(5*time.Second+time.Duration(i)*time.Second))
	}
	if recordDuplicateBlockReplay(c, "hash-b", "job-late", now.Add(defaultDuplicateBlockReplayWindow+10*time.Second)) {
		t.Fatal("duplicate replay outside the window should not trigger disconnect")
	}
}

func TestShouldLogLowDifficultyShareThrottlesPerClient(t *testing.T) {
	c := &client{}
	now := time.Unix(100, 0)

	shouldLog, suppressed := shouldLogLowDifficultyShare(c, now)
	if !shouldLog || suppressed != 0 {
		t.Fatalf("first low-diff log = %v/%d, want true/0", shouldLog, suppressed)
	}

	shouldLog, suppressed = shouldLogLowDifficultyShare(c, now.Add(time.Second))
	if shouldLog || suppressed != 0 {
		t.Fatalf("second low-diff log = %v/%d, want false/0", shouldLog, suppressed)
	}
	shouldLog, suppressed = shouldLogLowDifficultyShare(c, now.Add(2*time.Second))
	if shouldLog || suppressed != 0 {
		t.Fatalf("third low-diff log = %v/%d, want false/0", shouldLog, suppressed)
	}

	shouldLog, suppressed = shouldLogLowDifficultyShare(c, now.Add(lowDiffShareLogInterval+time.Second))
	if !shouldLog || suppressed != 2 {
		t.Fatalf("later low-diff log = %v/%d, want true/2", shouldLog, suppressed)
	}
}

func TestShouldLogLowDifficultyShareIsPerClient(t *testing.T) {
	now := time.Unix(100, 0)
	first := &client{}
	second := &client{}

	shouldLogLowDifficultyShare(first, now)
	if shouldLog, _ := shouldLogLowDifficultyShare(first, now.Add(time.Second)); shouldLog {
		t.Fatal("first client should throttle repeated low-diff logs")
	}
	if shouldLog, suppressed := shouldLogLowDifficultyShare(second, now.Add(time.Second)); !shouldLog || suppressed != 0 {
		t.Fatalf("second client low-diff log = %v/%d, want true/0", shouldLog, suppressed)
	}
}

func TestClassifyTemplateFetchError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
		wantBenign bool
	}{
		{
			name:       "nonce too low",
			err:        errors.New("RPC error in getBlockTemplate: nonce too low: address 0xabc"),
			wantReason: "nonce-too-low",
			wantBenign: true,
		},
		{
			name:       "node is syncing",
			err:        errors.New(`RPC error in getBlockTemplate: {"code":-32000,"message":"Block Template Request Rejected : Node is Syncing"}`),
			wantReason: "node-syncing",
			wantBenign: true,
		},
		{
			name:       "initial download",
			err:        errors.New("Blockdag is downloading blocks... : Client in initial download"),
			wantReason: "initial-download",
			wantBenign: true,
		},
		{
			name:       "pool syncing",
			err:        errors.New("RPC error in getBlockTemplate: bdag pool syncing"),
			wantReason: "pool-syncing",
			wantBenign: true,
		},
		{
			name:       "graph state sync",
			err:        errors.New("RPC error in getBlockTemplate: graph-state sync active"),
			wantReason: "graph-state-sync",
			wantBenign: true,
		},
		{
			name:       "no consensus peers readiness pause",
			err:        errors.New("getBlockTemplate paused: node-health-no-consensus-peers"),
			wantReason: "no-consensus-peers",
			wantBenign: true,
		},
		{
			name:       "reorg pressure",
			err:        errors.New("RPC error in getBlockTemplate: reorg pressure"),
			wantReason: "reorg-pressure",
			wantBenign: true,
		},
		{
			name:       "tx download",
			err:        errors.New("Blockdag is downloading tx..."),
			wantReason: "tx-download",
			wantBenign: true,
		},
		{
			name:       "transport error",
			err:        errors.New("connection refused"),
			wantReason: "transport",
			wantBenign: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReason, gotBenign := classifyTemplateFetchError(tt.err)
			if gotReason != tt.wantReason || gotBenign != tt.wantBenign {
				t.Fatalf("classifyTemplateFetchError() = (%q, %v), want (%q, %v)", gotReason, gotBenign, tt.wantReason, tt.wantBenign)
			}
		})
	}
}
