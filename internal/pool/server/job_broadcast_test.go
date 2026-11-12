package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var gbtPayloadBenchSink []byte

func TestAppendGetBlockTemplatePayload(t *testing.T) {
	tests := []struct {
		name        string
		poolAddress string
		want        string
		wantParams  int
	}{
		{
			name:       "without pool address",
			want:       `{"jsonrpc":"2.0","id":1,"method":"getBlockTemplate","params":[[],10]}`,
			wantParams: 2,
		},
		{
			name:        "with pool address",
			poolAddress: "0x1234567890abcdef1234567890abcdef12345678",
			want:        `{"jsonrpc":"2.0","id":1,"method":"getBlockTemplate","params":[[],10,"0x1234567890abcdef1234567890abcdef12345678"]}`,
			wantParams:  3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := appendGetBlockTemplatePayload(nil, test.poolAddress)
			if string(got) != test.want {
				t.Fatalf("payload = %s, want %s", got, test.want)
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
			if decoded.JSONRPC != "2.0" || decoded.ID != 1 || decoded.Method != "getBlockTemplate" || len(decoded.Params) != test.wantParams {
				t.Fatalf("decoded payload = %+v", decoded)
			}
		})
	}
}

func TestDecodeSingleJSONRejectsTrailingValues(t *testing.T) {
	var got struct {
		OK bool `json:"ok"`
	}
	if err := decodeSingleJSON(strings.NewReader(`{"ok":true}`), &got); err != nil {
		t.Fatalf("decodeSingleJSON valid object error: %v", err)
	}
	if !got.OK {
		t.Fatal("decoded OK = false, want true")
	}

	err := decodeSingleJSON(strings.NewReader(`{"ok":true} {"ok":false}`), &got)
	if err == nil {
		t.Fatal("expected trailing JSON value to be rejected")
	}
}

func TestTemplateFetchClassifiesNodeOverloadAsTooBusy(t *testing.T) {
	tests := []error{
		errors.New("getBlockTemplate HTTP 503: server busy"),
		errors.New("getBlockTemplate HTTP 503: too many concurrent requests"),
		errors.New(`RPC error in getBlockTemplate: {"code":-32000,"message":"too many requests"}`),
	}
	for _, err := range tests {
		reason, benign := classifyTemplateFetchError(err)
		if !benign || reason != "too-busy" {
			t.Fatalf("classifyTemplateFetchError(%q) = %q/%v, want too-busy/true", err, reason, benign)
		}
		if got := classifyRouterError(err); got != "too-busy" {
			t.Fatalf("classifyRouterError(%q) = %q, want too-busy", err, got)
		}
	}
}

func TestGetBlockTemplateTooBusyBackoffPreventsFlood(t *testing.T) {
	resetRouterTestState(t)

	var calls atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "too many concurrent requests", http.StatusServiceUnavailable)
	}))
	defer node.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: node.URL})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router

	const workers = 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := getBlockTemplate()
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("getBlockTemplate() unexpectedly succeeded during overload")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("getBlockTemplate HTTP calls = %d, want 1 while overloaded", got)
	}

	_, err = getBlockTemplate()
	if err == nil || !strings.Contains(err.Error(), "backoff active") {
		t.Fatalf("second getBlockTemplate() error = %v, want backoff active", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("getBlockTemplate HTTP calls after backoff = %d, want 1", got)
	}
}

func TestGetTemplateHealthTooBusyBackoffSkipsProbe(t *testing.T) {
	resetRouterTestState(t)

	var calls atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "server busy", http.StatusServiceUnavailable)
	}))
	defer node.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     node.URL,
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}

	if got := router.probeNodeHealthBackends(); got != 1 {
		t.Fatalf("first probeNodeHealthBackends() = %d, want 1", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("getTemplateHealth calls = %d, want 1", got)
	}
	backend := router.selectedBackend()
	state := backend.snapshotState()
	if state.nodeHealthReasonCode != "too-busy" {
		t.Fatalf("node health reason = %q, want too-busy", state.nodeHealthReasonCode)
	}

	if got := router.probeNodeHealthBackends(); got != 0 {
		t.Fatalf("second probeNodeHealthBackends() = %d, want skipped 0", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("getTemplateHealth calls after backoff = %d, want 1", got)
	}
}

func TestLifecycleChainHeightTooBusyDoesNotFallbackToGBT(t *testing.T) {
	resetRouterTestState(t)

	var heightCalls atomic.Int32
	var gbtCalls atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req.Method {
		case "getMainChainHeight":
			heightCalls.Add(1)
			http.Error(w, "server busy", http.StatusServiceUnavailable)
		case "getBlockTemplate":
			gbtCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(blockTemplateRPCResponse(123, strings.Repeat("1", 64))))
		default:
			http.Error(w, "unexpected method", http.StatusBadRequest)
		}
	}))
	defer node.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: node.URL})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router

	if _, err := getLifecycleChainHeight(); err == nil {
		t.Fatal("getLifecycleChainHeight() unexpectedly succeeded")
	}
	if heightCalls.Load() != 1 || gbtCalls.Load() != 0 {
		t.Fatalf("calls after overload height=%d gbt=%d, want 1/0", heightCalls.Load(), gbtCalls.Load())
	}

	if _, err := getLifecycleChainHeight(); err == nil || !strings.Contains(err.Error(), "backoff active") {
		t.Fatalf("second getLifecycleChainHeight() error = %v, want backoff active", err)
	}
	if heightCalls.Load() != 1 || gbtCalls.Load() != 0 {
		t.Fatalf("calls after height backoff height=%d gbt=%d, want 1/0", heightCalls.Load(), gbtCalls.Load())
	}
}

func TestForceTemplateRefreshCoalescesActiveRequests(t *testing.T) {
	resetTemplateRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousStartingPdiff := startingPdiff
	previousSeq := lastSeq.Load()
	previousTemplateKey, _ := currentTemplateKey.Load().(string)
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		startingPdiff = previousStartingPdiff
		lastSeq.Store(previousSeq)
		currentTemplateKey.Store(previousTemplateKey)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
	})

	startingPdiff = 0.05
	currentTemplateKey.Store("old-template")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondFetched := make(chan struct{})
	var calls atomic.Int32
	firstParent := strings.Repeat("1", 64)
	secondParent := strings.Repeat("2", 64)
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		switch calls.Add(1) {
		case 1:
			close(firstStarted)
			<-releaseFirst
			return testCleanRefreshBlockTemplate(firstParent, 100), nil
		case 2:
			close(secondFetched)
			return testCleanRefreshBlockTemplate(secondParent, 101), nil
		default:
			return nil, fmt.Errorf("unexpected refresh fetch")
		}
	}

	forceTemplateRefresh("first refresh")
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first refresh fetch did not start")
	}

	forceTemplateRefresh("second refresh")
	select {
	case <-secondFetched:
		t.Fatal("coalesced refresh started a second fetch while the first was active")
	default:
	}

	close(releaseFirst)
	select {
	case <-secondFetched:
	case <-time.After(time.Second):
		t.Fatal("coalesced refresh did not fetch the newest request")
	}
	waitForTemplateRefreshIdle(t)
	waitForTemplateRefreshWorkersIdle(t)

	if got := calls.Load(); got != 2 {
		t.Fatalf("refresh fetch calls = %d, want 2", got)
	}
}

func TestRPCBackendAuthorizationHeader(t *testing.T) {
	cached := basicAuthHeader("cached-user", "cached-pass")
	backend := &rpcBackend{
		rpcUser:       "runtime-user",
		rpcPassword:   "runtime-pass",
		rpcAuthHeader: cached,
	}
	if got := backend.authorizationHeader(); got != cached {
		t.Fatalf("authorizationHeader() = %q, want cached header %q", got, cached)
	}

	backend.rpcAuthHeader = ""
	wantFallback := basicAuthHeader("runtime-user", "runtime-pass")
	if got := backend.authorizationHeader(); got != wantFallback {
		t.Fatalf("fallback authorizationHeader() = %q, want %q", got, wantFallback)
	}
}

func TestRunTemplateSubscriptionFetchesInitialTemplateWithoutEnabledRouter(t *testing.T) {
	resetRouterTestState(t)
	parentMu.Lock()
	lastParent = ""
	parentMu.Unlock()
	currentParent.Store("")
	currentTemplateKey.Store("")
	lastBroadcastTime.Store(time.Time{})

	gbtSeen := make(chan struct{}, 1)
	var gbtCalls atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Logf("websocket upgrade failed: %v", err)
				return
			}
			defer conn.Close()

			var subscribe map[string]interface{}
			if err := conn.ReadJSON(&subscribe); err != nil {
				t.Logf("read subscribe failed: %v", err)
				return
			}
			if err := conn.WriteJSON(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": "sub-test"}); err != nil {
				t.Logf("write subscribe response failed: %v", err)
				return
			}
			select {
			case <-gbtSeen:
			case <-time.After(time.Second):
			}
			return
		}

		gbtCalls.Add(1)
		select {
		case gbtSeen <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, blockTemplateRPCResponse(123, "0xabc"))
	}))
	defer server.Close()

	backend := &rpcBackend{
		name:      "primary",
		url:       server.URL,
		wsURL:     "ws" + strings.TrimPrefix(server.URL, "http"),
		gbtClient: server.Client(),
	}

	err := runTemplateSubscription(backend)
	if gbtCalls.Load() == 0 {
		t.Fatalf("initial getBlockTemplate calls = 0, want at least one; subscription err=%v", err)
	}
}

func TestRunTemplateSubscriptionPausesBeforeWSWhenNodeHealthReportsSyncing(t *testing.T) {
	resetRouterTestState(t)

	var wsCalls atomic.Int32
	var gbtCalls atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			wsCalls.Add(1)
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Logf("websocket upgrade failed: %v", err)
				return
			}
			defer conn.Close()
			var subscribe map[string]interface{}
			_ = conn.ReadJSON(&subscribe)
			_ = conn.WriteJSON(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": "sub-test"})
			return
		}

		gbtCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-32000,"message":"Block Template Request Rejected : Node is Syncing"}}`)
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     server.URL,
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.gbtClient = server.Client()
	recordBackendNodeSyncing(router, backend)

	err = runTemplateSubscription(backend)
	if err == nil || !strings.Contains(err.Error(), "node-health-node-syncing") {
		t.Fatalf("runTemplateSubscription() error = %v, want node-health-node-syncing pause", err)
	}
	if wsCalls.Load() != 0 {
		t.Fatalf("websocket subscription attempts = %d, want 0 while node health reports syncing", wsCalls.Load())
	}
	if gbtCalls.Load() != 0 {
		t.Fatalf("getBlockTemplate validation calls = %d, want 0 while node health reports syncing", gbtCalls.Load())
	}
}

func TestRunTemplateSubscriptionBacksOffAfterRawNodeSyncingErrorWithoutNodeHealth(t *testing.T) {
	resetRouterTestState(t)

	var wsCalls atomic.Int32
	var gbtCalls atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			wsCalls.Add(1)
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Logf("websocket upgrade failed: %v", err)
				return
			}
			defer conn.Close()
			var subscribe map[string]interface{}
			_ = conn.ReadJSON(&subscribe)
			_ = conn.WriteJSON(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": "sub-test"})
			return
		}

		gbtCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-32000,"message":"Block Template Request Rejected : Node is Syncing"}}`)
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: server.URL})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.gbtClient = server.Client()

	err = runTemplateSubscription(backend)
	if err == nil || !strings.Contains(err.Error(), "no usable template") {
		t.Fatalf("runTemplateSubscription() error = %v, want no usable template after node syncing validation", err)
	}
	if wsCalls.Load() != 1 {
		t.Fatalf("websocket subscription attempts = %d, want 1 discovery attempt", wsCalls.Load())
	}
	if gbtCalls.Load() != 1 {
		t.Fatalf("getBlockTemplate validation calls = %d, want 1 discovery attempt", gbtCalls.Load())
	}

	state := backend.snapshotState()
	if held, reason := submitReadinessHoldActive(state, time.Now()); !held || reason != "submit-readiness-template-fetch-node-syncing" {
		t.Fatalf("submitReadinessHoldActive() = %v/%q, want held/template-fetch-node-syncing", held, reason)
	}

	_, err = getBlockTemplate()
	if err == nil || !strings.Contains(err.Error(), "submit-readiness-template-fetch-node-syncing") {
		t.Fatalf("getBlockTemplate() error = %v, want template-fetch node-syncing pause", err)
	}
	if gbtCalls.Load() != 1 {
		t.Fatalf("getBlockTemplate calls after hold = %d, want still 1", gbtCalls.Load())
	}

	err = probeTemplateSubscription(backend)
	if err == nil || !strings.Contains(err.Error(), "submit-readiness-template-fetch-node-syncing") {
		t.Fatalf("probeTemplateSubscription() error = %v, want template-fetch node-syncing pause", err)
	}
	if wsCalls.Load() != 1 {
		t.Fatalf("websocket attempts after hold = %d, want still 1", wsCalls.Load())
	}
}

func TestFetchBlockTemplatePreservesHTTPSequence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, blockTemplateRPCResponseWithSeq(123, "0xabc", 42))
	}))
	defer server.Close()

	backend := &rpcBackend{
		name:      "node",
		url:       server.URL,
		gbtClient: server.Client(),
	}

	tpl, _, err := fetchBlockTemplateFromBackend(backend)
	if err != nil {
		t.Fatalf("fetchBlockTemplateFromBackend() error = %v", err)
	}
	if tpl.SourceSeq != 42 {
		t.Fatalf("SourceSeq = %d, want 42", tpl.SourceSeq)
	}
}

func TestHTTPBlockTemplateSequenceProtectsRouterOrdering(t *testing.T) {
	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	first := &BlockTemplate{PreviousHash: "0xabc", Height: 123, SourceSeq: 42}
	if !router.recordTemplateSuccess(backend, first, time.Millisecond) {
		t.Fatal("recordTemplateSuccess() rejected initial HTTP template sequence")
	}

	second := &BlockTemplate{PreviousHash: "0xabc", Height: 123, SourceSeq: 42}
	if router.recordTemplateSuccess(backend, second, time.Millisecond) {
		t.Fatal("recordTemplateSuccess() accepted duplicate HTTP template sequence")
	}
}

func TestGetBlockTemplatePausesWhenNodeHealthReportsSyncing(t *testing.T) {
	resetRouterTestState(t)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, blockTemplateRPCResponse(123, "0xabc"))
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     server.URL,
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.gbtClient = server.Client()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "node is syncing",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		MainOrder:                  101,
	}, time.Millisecond)

	_, err = getBlockTemplate()
	if err == nil || !strings.Contains(err.Error(), "node-health-node-syncing") {
		t.Fatalf("getBlockTemplate() error = %v, want node-health-node-syncing pause", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("getBlockTemplate HTTP calls = %d, want 0 while node health reports syncing", calls.Load())
	}
}

func TestGetBlockTemplateRefreshesWhenNodeHealthReportsTemplateParentStale(t *testing.T) {
	resetRouterTestState(t)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, blockTemplateRPCResponse(124, "0x"+strings.Repeat("f", 64)))
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           server.URL,
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.gbtClient = server.Client()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "template_parent_stale",
		Reason:                     "cached template parent is stale",
		MineableNow:                false,
		SubmitReady:                false,
		TemplateAvailable:          true,
		TemplateUsable:             false,
		TemplateAgeMs:              250,
		P2PFreshnessMaxAgeMs:       3000,
		P2PCurrent:                 true,
		P2PConsensusPeerCount:      5,
		P2PFreshConsensusPeerCount: 4,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
		MainOrder:                  101,
	}, time.Millisecond)
	// The stale-parent signal starts a clean refresh itself. Let that fetch
	// release the global single-flight token before issuing the explicit probe.
	waitForCleanRefreshIdle(t)
	waitForCleanRefreshWorkersIdle(t)

	tpl, err := getBlockTemplate()
	if err != nil {
		t.Fatalf("getBlockTemplate() error = %v, want refresh allowed", err)
	}
	if tpl == nil || tpl.Height != 124 {
		t.Fatalf("getBlockTemplate() template = %#v, want replacement height 124", tpl)
	}
	if calls.Load() == 0 {
		t.Fatal("getBlockTemplate HTTP calls = 0, want refresh fetch")
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); !ok || reason != "ok" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want true/ok after replacement template", ok, reason)
	}
}

func TestGetBlockTemplatePausesUntilNodeHealthSample(t *testing.T) {
	resetRouterTestState(t)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, blockTemplateRPCResponse(123, "0xabc"))
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     server.URL,
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.gbtClient = server.Client()

	_, err = getBlockTemplate()
	if err == nil || !strings.Contains(err.Error(), "node-health-pending") {
		t.Fatalf("getBlockTemplate() error = %v, want node-health-pending pause", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("getBlockTemplate HTTP calls = %d, want 0 before node health sample", calls.Load())
	}
}

func TestGetBlockTemplatePausesWhenNodeHealthStale(t *testing.T) {
	resetRouterTestState(t)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, blockTemplateRPCResponse(123, "0xabc"))
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           server.URL,
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 1,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.gbtClient = server.Client()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:        "ok",
		MineableNow:       true,
		SubmitReady:       true,
		TemplateAvailable: true,
		TemplateUsable:    true,
		TemplateAgeMs:     250,
		MainOrder:         101,
	}, time.Millisecond)
	backend.mu.Lock()
	backend.state.lastNodeHealthAt = time.Now().Add(-2 * time.Second)
	backend.mu.Unlock()

	_, err = getBlockTemplate()
	if err == nil || !strings.Contains(err.Error(), "node-health-stale") {
		t.Fatalf("getBlockTemplate() error = %v, want node-health-stale pause", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("getBlockTemplate HTTP calls = %d, want 0 with stale node health", calls.Load())
	}
}

func TestGetBlockTemplatePausesBeforeReturningCachedTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	cacheBlockTemplate(backend.name, &BlockTemplate{PreviousHash: "0xcached", Height: 123})

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "node is syncing",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		MainOrder:                  101,
	}, time.Millisecond)

	tpl, err := getBlockTemplate()
	if err == nil || !strings.Contains(err.Error(), "node-health-node-syncing") {
		t.Fatalf("getBlockTemplate() = (%#v, %v), want node-health-node-syncing pause", tpl, err)
	}
}

func TestCleanTemplateRefreshPausesWhenNodeHealthReportsSyncing(t *testing.T) {
	resetRouterTestState(t)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, blockTemplateRPCResponse(123, "0xabc"))
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     server.URL,
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.gbtClient = server.Client()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "node is syncing",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		MainOrder:                  101,
	}, time.Millisecond)

	_, err = getUncachedRefreshBlockTemplate()
	if err == nil || !strings.Contains(err.Error(), "node-health-node-syncing") {
		t.Fatalf("getUncachedRefreshBlockTemplate() error = %v, want node-health-node-syncing pause", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("clean refresh HTTP calls = %d, want 0 while node health reports syncing", calls.Load())
	}
}

func recordBackendNodeSyncing(router *rpcRouter, backend *rpcBackend) {
	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "node is syncing",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		MainOrder:                  101,
	}, time.Millisecond)
}

func TestCleanTemplateRefreshCoalescesInFlightRequests(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousStartingPdiff := startingPdiff
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		startingPdiff = previousStartingPdiff
	})

	startingPdiff = 0.05
	clientsMu.Lock()
	clients = map[*client]struct{}{}
	clientsMu.Unlock()
	currentTemplateKey.Store("test-old-template")
	lastBroadcastTime.Store(time.Time{})

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondFetched := make(chan struct{})
	var calls atomic.Int32
	firstParent := strings.Repeat("1", 64)
	secondParent := strings.Repeat("2", 64)
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		switch calls.Add(1) {
		case 1:
			close(firstStarted)
			<-releaseFirst
			return testCleanRefreshBlockTemplate(firstParent, 100), nil
		case 2:
			close(secondFetched)
			return testCleanRefreshBlockTemplate(secondParent, 101), nil
		default:
			return nil, fmt.Errorf("unexpected refresh fetch")
		}
	}

	startTemplateSeq := templateSeqCounter.Load()
	forceCleanTemplateRefresh("first refresh")
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first refresh fetch did not start")
	}

	forceCleanTemplateRefresh("second refresh")
	close(releaseFirst)
	select {
	case <-secondFetched:
	case <-time.After(time.Second):
		t.Fatal("coalesced refresh did not fetch the newest request")
	}
	waitForCleanRefreshIdle(t)

	if got := calls.Load(); got != 2 {
		t.Fatalf("refresh fetch calls = %d, want 2", got)
	}
	if got := lastSeq.Load(); got != startTemplateSeq+1 {
		t.Fatalf("broadcast sequence = %d, want exactly one broadcast after %d", got, startTemplateSeq)
	}
	baseJob, _ := lastBaseJobVal.Load().(*JobData)
	if baseJob == nil {
		t.Fatal("last base job was not stored")
	}
	if baseJob.ParentHash != secondParent {
		t.Fatalf("broadcast parent = %s, want newest parent %s", baseJob.ParentHash, secondParent)
	}
}

func TestCleanTemplateRefreshFetchErrorDoesNotWedgeCoordinator(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousStartingPdiff := startingPdiff
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		startingPdiff = previousStartingPdiff
	})

	startingPdiff = 0.05
	var writes bytes.Buffer
	oldJob := &JobData{
		CreatedAt:         time.Now(),
		InvalidationEpoch: acceptedJobInvalidationEpoch.Load(),
	}
	c := &client{
		addr:  "miner-1",
		rw:    bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
		pdiff: 1.0,
	}
	c.extranonce1 = [4]byte{1, 2, 3, 4}
	c.currentJob.Store(&clientCurrentJob{id: "old-job", job: oldJob})
	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()

	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		return nil, errors.New("temporary template failure")
	}
	forceCleanTemplateRefresh("fetch failure")
	waitForCleanRefreshIdle(t)
	if oldJob.invalidated.Load() || jobInvalidatedByEpoch(oldJob) {
		t.Fatal("fetch failure invalidated the current miner job before a replacement template existed")
	}

	startTemplateSeq := templateSeqCounter.Load()
	parent := strings.Repeat("3", 64)
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		return testCleanRefreshBlockTemplate(parent, 102), nil
	}
	forceCleanTemplateRefresh("after failure")
	waitForCleanRefreshIdle(t)

	if got := lastSeq.Load(); got != startTemplateSeq+1 {
		t.Fatalf("broadcast sequence after retry = %d, want %d", got, startTemplateSeq+1)
	}
	if !oldJob.invalidated.Load() || !jobInvalidatedByEpoch(oldJob) {
		t.Fatal("successful clean refresh did not invalidate the replaced miner job")
	}
	current, _ := c.currentJob.Load().(*clientCurrentJob)
	if current == nil || current.job == nil {
		t.Fatal("client current job was not replaced after successful clean refresh")
	}
	if !current.job.Clean {
		t.Fatal("successful clean refresh did not send a clean replacement job")
	}
}

func TestPreemptiveBlockRefreshBroadcastsOnlyAfterParentAdvance(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetPreemptiveBlockRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousStartingPdiff := startingPdiff
	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	oldParent := strings.Repeat("1", 64)
	newParent := strings.Repeat("2", 64)
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	lastParent = oldParent
	parentMu.Unlock()
	currentParent.Store(oldParent)
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		startingPdiff = previousStartingPdiff
		lastSeq.Store(previousSeq)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		currentParent.Store(previousCurrentParent)
	})

	startingPdiff = 0.05
	oldJob := &JobData{
		BackendName:              "node",
		ParentHash:               oldParent,
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch("node"),
	}
	var writes bytes.Buffer
	c := &client{
		addr: "miner-1",
		rw:   bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
	}
	c.extranonce1 = [4]byte{1, 2, 3, 4}
	c.pdiff = 1.0
	c.currentJob.Store(&clientCurrentJob{id: "old-job", job: oldJob})
	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()

	var calls atomic.Int32
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		if calls.Add(1) == 1 {
			return testCleanRefreshBlockTemplate(oldParent, 200), nil
		}
		return testCleanRefreshBlockTemplate(newParent, 201), nil
	}

	startSeq := templateSeqCounter.Load()
	requestPreemptiveBlockRefresh(preemptiveBlockRefreshRequest{
		oldParent:   oldParent,
		backendName: "node",
		delay:       0,
		interval:    time.Millisecond,
		timeout:     100 * time.Millisecond,
	})
	waitForPreemptiveBlockRefreshIdle(t)

	if got := calls.Load(); got < 2 {
		t.Fatalf("refresh fetch calls = %d, want at least 2", got)
	}
	if got := lastSeq.Load(); got != startSeq+1 {
		t.Fatalf("broadcast sequence = %d, want %d", got, startSeq+1)
	}
	baseJob, _ := lastBaseJobVal.Load().(*JobData)
	if baseJob == nil || baseJob.ParentHash != newParent {
		t.Fatalf("broadcast parent = %#v, want %s", baseJob, newParent)
	}
	if !oldJob.invalidated.Load() || !jobInvalidatedByEpoch(oldJob) {
		t.Fatal("preemptive clean refresh did not invalidate the replaced miner job")
	}
	current, _ := c.currentJob.Load().(*clientCurrentJob)
	if current == nil || current.job == nil || !current.job.Clean {
		t.Fatal("preemptive refresh did not send a clean replacement job")
	}
}

func TestPreemptiveBlockRefreshDoesNotBroadcastSameParent(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetPreemptiveBlockRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		lastSeq.Store(previousSeq)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
	})

	parent := strings.Repeat("3", 64)
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	lastParent = parent
	parentMu.Unlock()
	currentParent.Store(parent)
	t.Cleanup(func() {
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		currentParent.Store(previousCurrentParent)
	})

	var calls atomic.Int32
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		calls.Add(1)
		return testCleanRefreshBlockTemplate(parent, 300), nil
	}

	requestPreemptiveBlockRefresh(preemptiveBlockRefreshRequest{
		oldParent: parent,
		delay:     0,
		interval:  time.Millisecond,
		timeout:   10 * time.Millisecond,
	})
	waitForPreemptiveBlockRefreshIdle(t)

	if calls.Load() == 0 {
		t.Fatal("preemptive refresh did not fetch")
	}
	if got := lastSeq.Load(); got != previousSeq {
		t.Fatalf("broadcast sequence = %d, want unchanged %d", got, previousSeq)
	}
}

func TestPreemptiveBlockRefreshDefaultsAreBounded(t *testing.T) {
	if got := preemptiveBlockCandidateRefreshDelay(nil); got < 250*time.Millisecond {
		t.Fatalf("default preemptive delay = %s, want at least 250ms", got)
	}
	if got := preemptiveBlockCandidateRefreshInterval(nil); got < defaultGBTMinInterval {
		t.Fatalf("default preemptive interval = %s, want at least %s", got, defaultGBTMinInterval)
	}
	if got := preemptiveBlockCandidateRefreshInterval(&Config{PreemptiveBlockCandidateRefreshIntervalMS: 1}); got < defaultGBTMinInterval {
		t.Fatalf("clamped preemptive interval = %s, want at least %s", got, defaultGBTMinInterval)
	}
	if got := preemptiveBlockCandidateRefreshTimeout(nil); got < 1500*time.Millisecond {
		t.Fatalf("default preemptive timeout = %s, want at least 1500ms", got)
	}
	if got := preemptiveBlockCandidateRefreshTimeout(&Config{PreemptiveBlockCandidateRefreshTimeoutMS: 10000}); got > maxPreemptiveBlockRefreshTimeout {
		t.Fatalf("clamped preemptive timeout = %s, want at most %s", got, maxPreemptiveBlockRefreshTimeout)
	}
}

func TestPreemptiveBlockRefreshCleanReissuesSameParentWhenEnabled(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetPreemptiveBlockRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousStartingPdiff := startingPdiff
	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	parent := strings.Repeat("3", 64)
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	lastParent = parent
	parentMu.Unlock()
	currentParent.Store(parent)
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		startingPdiff = previousStartingPdiff
		lastSeq.Store(previousSeq)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		currentParent.Store(previousCurrentParent)
	})

	startingPdiff = 0.05
	oldJob := &JobData{
		BackendName:              "node",
		ParentHash:               parent,
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch("node"),
	}
	var writes bytes.Buffer
	c := &client{
		addr: "miner-1",
		rw:   bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
	}
	c.extranonce1 = [4]byte{1, 2, 3, 4}
	c.pdiff = 1.0
	c.currentJob.Store(&clientCurrentJob{id: "old-job", job: oldJob})
	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()

	var calls atomic.Int32
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		calls.Add(1)
		return testCleanRefreshBlockTemplate(parent, 300), nil
	}

	startSeq := templateSeqCounter.Load()
	requestPreemptiveBlockRefresh(preemptiveBlockRefreshRequest{
		oldParent:    parent,
		backendName:  "node",
		cleanReissue: true,
		delay:        0,
		interval:     time.Millisecond,
		timeout:      10 * time.Millisecond,
	})
	waitForPreemptiveBlockRefreshIdle(t)

	if calls.Load() == 0 {
		t.Fatal("preemptive refresh did not fetch")
	}
	if got := lastSeq.Load(); got != startSeq+1 {
		t.Fatalf("broadcast sequence = %d, want %d", got, startSeq+1)
	}
	baseJob, _ := lastBaseJobVal.Load().(*JobData)
	if baseJob == nil || baseJob.ParentHash != parent {
		t.Fatalf("broadcast parent = %#v, want %s", baseJob, parent)
	}
	if !oldJob.invalidated.Load() || !jobInvalidatedByEpoch(oldJob) {
		t.Fatal("preemptive clean reissue did not invalidate the replaced miner job")
	}
	current, _ := c.currentJob.Load().(*clientCurrentJob)
	if current == nil || current.job == nil || !current.job.Clean {
		t.Fatal("preemptive clean reissue did not send a clean replacement job")
	}
}

func TestPreemptiveBlockRefreshSkipsWhenCurrentParentAlreadyAdvanced(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetPreemptiveBlockRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousSeq := lastSeq.Load()
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	parentMu.Unlock()
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		lastSeq.Store(previousSeq)
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		currentParent.Store(previousCurrentParent)
	})

	oldParent := strings.Repeat("4", 64)
	newParent := strings.Repeat("5", 64)
	parentMu.Lock()
	lastParent = newParent
	parentMu.Unlock()
	currentParent.Store(newParent)

	var calls atomic.Int32
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		calls.Add(1)
		return testCleanRefreshBlockTemplate(newParent, 401), nil
	}

	requestPreemptiveBlockRefresh(preemptiveBlockRefreshRequest{
		oldParent: oldParent,
		delay:     0,
		interval:  time.Millisecond,
		timeout:   10 * time.Millisecond,
	})
	waitForPreemptiveBlockRefreshIdle(t)

	if calls.Load() != 0 {
		t.Fatalf("refresh fetch calls = %d, want 0 after parent already advanced", calls.Load())
	}
	if got := lastSeq.Load(); got != previousSeq {
		t.Fatalf("broadcast sequence = %d, want unchanged %d", got, previousSeq)
	}
}

func TestPreemptiveBlockRefreshIntervalUsesGBTFloor(t *testing.T) {
	if got := preemptiveBlockCandidateRefreshInterval(nil); got != defaultGBTMinInterval {
		t.Fatalf("default interval = %s, want %s", got, defaultGBTMinInterval)
	}
	if got := preemptiveBlockCandidateRefreshInterval(&Config{PreemptiveBlockCandidateRefreshIntervalMS: 50}); got != defaultGBTMinInterval {
		t.Fatalf("low configured interval = %s, want floor %s", got, defaultGBTMinInterval)
	}
	if got := preemptiveBlockCandidateRefreshInterval(&Config{PreemptiveBlockCandidateRefreshIntervalMS: 1500}); got != 1500*time.Millisecond {
		t.Fatalf("configured interval = %s, want 1.5s", got)
	}
}

func TestPreemptiveBlockRefreshDoesNotFetchAfterDeadline(t *testing.T) {
	resetPreemptiveBlockRefreshTestState(t)

	previousFetcher := refreshBlockTemplateFetcher
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	parentMu.Unlock()
	t.Cleanup(func() {
		refreshBlockTemplateFetcher = previousFetcher
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		currentParent.Store(previousCurrentParent)
	})

	parent := strings.Repeat("6", 64)
	parentMu.Lock()
	lastParent = parent
	parentMu.Unlock()
	currentParent.Store(parent)

	var calls atomic.Int32
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		calls.Add(1)
		return testCleanRefreshBlockTemplate(parent, 600), nil
	}

	runPreemptiveBlockRefreshAttempt(preemptiveBlockRefreshRequest{
		oldParent: parent,
		delay:     0,
		interval:  time.Millisecond,
		timeout:   0,
	})

	if got := calls.Load(); got != 0 {
		t.Fatalf("refresh fetch calls = %d, want 0 after deadline", got)
	}
}

func TestCleanTemplateRefreshSyncErrorPreservesFreshCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node.invalid:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.nodeHealthTemplate = true
	backend.state.nodeHealthTemplateUsable = true
	backend.state.nodeHealthLastGoodTemplateAvailable = true
	backend.state.nodeHealthLastGoodTemplateAge = 250 * time.Millisecond
	backend.state.nodeHealthP2PContextObserved = true
	backend.state.nodeHealthP2PCurrent = true
	backend.state.nodeHealthP2PMiningFresh = true
	backend.mu.Unlock()

	clientsMu.Lock()
	previousClients := clients
	job := &JobData{
		BackendName:              backend.name,
		ParentHash:               "old-parent",
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch(backend.name),
	}
	miningWork.rememberParent(job.ParentHash)
	c := &client{addr: "miner-1"}
	c.currentJob.Store(&clientCurrentJob{id: "job-1", job: job})
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = previousClients
		clientsMu.Unlock()
	})

	handleCleanTemplateRefreshFetchError(cleanTemplateRefreshRequest{
		reason:      "test refresh",
		backendName: backend.name,
	}, errors.New("RPC error in getBlockTemplate: bdag pool syncing"))

	if job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("tip-chase sync-pressure template fetch failure invalidated current work")
	}
	state := backend.snapshotState()
	if !state.healthy || !strings.Contains(state.reason, "advisory") {
		t.Fatalf("backend state = %#v, want tip-chase advisory after sync-pressure fetch failure", state)
	}
}

func TestCleanTemplateRefreshSyncErrorInvalidatesFreshCurrentJobsAfterParentAdvance(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	previousCurrentParent := CurrentParent()
	currentParent.Store("new-parent")
	t.Cleanup(func() {
		currentParent.Store(previousCurrentParent)
	})

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node.invalid:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	clientsMu.Lock()
	previousClients := clients
	job := &JobData{
		BackendName:              backend.name,
		ParentHash:               "old-parent",
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch(backend.name),
	}
	c := &client{addr: "miner-1"}
	c.currentJob.Store(&clientCurrentJob{id: "job-1", job: job})
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = previousClients
		clientsMu.Unlock()
	})

	handleCleanTemplateRefreshFetchError(cleanTemplateRefreshRequest{
		reason:      "test refresh",
		backendName: backend.name,
	}, errors.New("RPC error in getBlockTemplate: bdag node syncing"))

	if !job.invalidated.Load() || !jobInvalidatedByEpoch(job) {
		t.Fatal("sync-pressure template fetch failure did not invalidate current work after parent advanced")
	}
}

func TestCleanTemplateRefreshNodeSyncingPreservesMatchingCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node.invalid:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.nodeHealthTemplate = true
	backend.state.nodeHealthTemplateUsable = true
	backend.state.nodeHealthLastGoodTemplateAvailable = true
	backend.state.nodeHealthLastGoodTemplateAge = 250 * time.Millisecond
	backend.state.nodeHealthP2PContextObserved = true
	backend.state.nodeHealthP2PCurrent = true
	backend.state.nodeHealthP2PMiningFresh = true
	backend.mu.Unlock()
	job := seedFreshCurrentJobForRouterTest(t, backend.name)

	handleCleanTemplateRefreshFetchError(cleanTemplateRefreshRequest{
		reason:      "test refresh",
		backendName: backend.name,
	}, errors.New("RPC error in getBlockTemplate: bdag node syncing"))

	if job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("tip-chase node-syncing template fetch failure invalidated matching current work")
	}
	state := backend.snapshotState()
	if !state.healthy || !strings.Contains(state.reason, "advisory") {
		t.Fatalf("backend state = %#v, want tip-chase advisory after node-syncing fetch failure", state)
	}
}

func TestCleanTemplateRefreshNodeSyncingInvalidatesAfterParentAdvance(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node.invalid:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	job := seedFreshCurrentJobForRouterTest(t, backend.name)
	miningWork.rememberParent(strings.Repeat("c", 64))

	handleCleanTemplateRefreshFetchError(cleanTemplateRefreshRequest{
		reason:      "test refresh",
		backendName: backend.name,
	}, errors.New("RPC error in getBlockTemplate: bdag node syncing"))

	if !job.invalidated.Load() && !jobInvalidatedByEpoch(job) {
		t.Fatal("node-syncing template fetch failure preserved stale-parent work after parent advanced")
	}
}

func TestCleanTemplateRefreshP2PNotCurrentInvalidatesMatchingCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node.invalid:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	job := seedFreshCurrentJobForRouterTest(t, backend.name)

	handleCleanTemplateRefreshFetchError(cleanTemplateRefreshRequest{
		reason:      "test refresh",
		backendName: backend.name,
	}, errors.New("RPC error in getBlockTemplate: p2p-not-current"))

	if !job.invalidated.Load() && !jobInvalidatedByEpoch(job) {
		t.Fatal("p2p-not-current template fetch failure should invalidate matching current work")
	}
}

func TestCleanTemplateRefreshP2PNotCurrentInvalidatesAfterParentAdvance(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node.invalid:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	job := seedFreshCurrentJobForRouterTest(t, backend.name)
	miningWork.rememberParent(strings.Repeat("c", 64))

	handleCleanTemplateRefreshFetchError(cleanTemplateRefreshRequest{
		reason:      "test refresh",
		backendName: backend.name,
	}, errors.New("RPC error in getBlockTemplate: p2p-not-current"))

	if !job.invalidated.Load() && !jobInvalidatedByEpoch(job) {
		t.Fatal("p2p-not-current template fetch failure preserved stale-parent work after parent advanced")
	}
}

func TestCleanTemplateRefreshSyncErrorInvalidatesExpiredCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node.invalid:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	clientsMu.Lock()
	previousClients := clients
	job := &JobData{
		BackendName:              backend.name,
		ParentHash:               "old-parent",
		CreatedAt:                time.Now().Add(-jobTTL - time.Second),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch(backend.name),
	}
	c := &client{addr: "miner-1"}
	c.currentJob.Store(&clientCurrentJob{id: "job-1", job: job})
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = previousClients
		clientsMu.Unlock()
	})

	handleCleanTemplateRefreshFetchError(cleanTemplateRefreshRequest{
		reason:      "test refresh",
		backendName: backend.name,
	}, errors.New("RPC error in getBlockTemplate: bdag pool syncing"))

	if !job.invalidated.Load() || !jobInvalidatedByEpoch(job) {
		t.Fatal("expired current work was not invalidated after sync-pressure template fetch failure")
	}
}

func TestInvalidatedLatestBroadcastIsNotResent(t *testing.T) {
	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	previousNotify, previousNotifyOK := lastBaseNotifyVal.Load().([]interface{})
	previousGlobal := acceptedJobInvalidationEpoch.Load()
	previousBackendEpochs := snapshotBackendJobInvalidationEpochs()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	t.Cleanup(func() {
		lastSeq.Store(previousSeq)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
		lastBaseNotifyVal = atomic.Value{}
		if previousNotifyOK {
			lastBaseNotifyVal.Store(previousNotify)
		}
		acceptedJobInvalidationEpoch.Store(previousGlobal)
		restoreBackendJobInvalidationEpochs(previousBackendEpochs)
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
	})

	acceptedJobInvalidationEpoch.Store(0)
	backendJobInvalidationEpochs = sync.Map{}

	baseNotify := []interface{}{"base-job", "prev", "ver", "bits", "time", false}
	baseJob := &JobData{
		BackendName:              "node",
		NBits:                    0x1d00ffff,
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch("node"),
	}
	miningWork.rememberLatestBroadcast(baseNotify, baseJob, 99)

	incrementBackendInvalidationEpoch("node")
	if latestBroadcastReadyForResend() {
		t.Fatal("invalidated latest broadcast remained eligible for resend")
	}

	var writes bytes.Buffer
	c := &client{
		addr:       "miner-1",
		authorized: true,
		sessionID:  "session-1",
		rw:         bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
		pdiff:      1.0,
		prevPdiff:  0.5,
	}
	c.extranonce1 = [4]byte{1, 2, 3, 4}

	ResendJobToClientClean(c, true)

	if writes.Len() != 0 {
		t.Fatalf("invalidated latest broadcast wrote Stratum messages: %q", writes.String())
	}
	if current := currentClientJob(c); current != nil {
		t.Fatalf("invalidated latest broadcast was stored as current job: %#v", current)
	}
	if miningWork.acceptedJobExists("base-job_01020304") {
		t.Fatal("invalidated latest broadcast was stored as an accepted job")
	}
}

func TestCleanRefreshBroadcastLeavesLatestWorkResendable(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)

	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	previousNotify, previousNotifyOK := lastBaseNotifyVal.Load().([]interface{})
	previousTemplateKey, _ := currentTemplateKey.Load().(string)
	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousStartingPdiff := startingPdiff
	t.Cleanup(func() {
		lastSeq.Store(previousSeq)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
		lastBaseNotifyVal = atomic.Value{}
		if previousNotifyOK {
			lastBaseNotifyVal.Store(previousNotify)
		}
		currentTemplateKey.Store(previousTemplateKey)
		clientsMu.Lock()
		clients = map[*client]struct{}{}
		for _, c := range previousClients {
			clients[c] = struct{}{}
		}
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		startingPdiff = previousStartingPdiff
	})

	startingPdiff = 0.05
	currentTemplateKey.Store("old-template")
	acceptedJobInvalidationEpoch.Store(0)
	backendJobInvalidationEpochs = sync.Map{}

	oldJob := &JobData{
		BackendName:              "node",
		ParentHash:               "old-parent",
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: backendInvalidationEpoch("node"),
	}
	activeClient := &client{
		addr:       "active-miner",
		authorized: true,
		sessionID:  "active-session",
		rw:         bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&bytes.Buffer{})),
		pdiff:      1.0,
	}
	activeClient.extranonce1 = [4]byte{1, 2, 3, 4}
	activeClient.currentJob.Store(&clientCurrentJob{id: "old-job", job: oldJob})
	clientsMu.Lock()
	clients = map[*client]struct{}{activeClient: {}}
	clientsMu.Unlock()

	tpl := testCleanRefreshBlockTemplate(strings.Repeat("4", 64), 103)
	tpl.BackendName = "node"
	if invalidated := maybeBroadcastCleanTemplate(tpl, "test clean refresh", "node"); invalidated != 1 {
		t.Fatalf("invalidated jobs = %d, want 1", invalidated)
	}
	if !oldJob.invalidated.Load() || !jobInvalidatedByEpoch(oldJob) {
		t.Fatal("clean refresh did not invalidate the replaced current job")
	}
	if !latestBroadcastReadyForResend() {
		t.Fatal("clean refresh latest broadcast was not eligible for resend")
	}
	baseJob, _ := lastBaseJobVal.Load().(*JobData)
	if baseJob == nil {
		t.Fatal("clean refresh did not store latest base job")
	}
	if baseJob.InvalidationEpoch != acceptedJobInvalidationEpoch.Load() {
		t.Fatalf("base job global epoch = %d, want %d", baseJob.InvalidationEpoch, acceptedJobInvalidationEpoch.Load())
	}
	if baseJob.BackendInvalidationEpoch != backendInvalidationEpoch("node") {
		t.Fatalf("base job backend epoch = %d, want %d", baseJob.BackendInvalidationEpoch, backendInvalidationEpoch("node"))
	}

	var writes bytes.Buffer
	staleClient := &client{
		addr:       "stale-miner",
		authorized: true,
		sessionID:  "stale-session",
		rw:         bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
		pdiff:      1.0,
		prevPdiff:  0.5,
	}
	staleClient.extranonce1 = [4]byte{5, 6, 7, 8}
	staleClient.currentJob.Store(&clientCurrentJob{id: "old-job", job: oldJob})

	if !currentJobReadyForResend(staleClient) {
		t.Fatal("stale client with fresh latest work should be eligible for resend")
	}
	ResendJobToClientClean(staleClient, true)

	messages := decodeStratumPushes(t, writes.Bytes())
	if len(messages) != 2 {
		t.Fatalf("resend wrote %d messages, want difficulty then notify; messages=%q", len(messages), writes.String())
	}
	if messages[1].Method != "mining.notify" {
		t.Fatalf("second resend method = %s, want mining.notify", messages[1].Method)
	}
	current := currentClientJob(staleClient)
	if current == nil || current.job == nil {
		t.Fatal("resend did not replace stale client's current job")
	}
	if current.job == oldJob || jobInvalidatedByEpoch(current.job) || current.job.invalidated.Load() {
		t.Fatalf("resend current job was not fresh: %#v", current.job)
	}
	if !current.job.Clean {
		t.Fatal("clean resend did not mark replacement job clean")
	}
}

func TestTemplateKeyNormalizesFields(t *testing.T) {
	tpl := &BlockTemplate{
		PreviousHash:    "0xABCDEF",
		StateRoot:       "0x001122",
		TxRoot:          "334455",
		Version:         7,
		CurTime:         100,
		Height:          42,
		CoinbaseAddress: "0xAABBCC",
		Reward:          1234,
		BlockFees:       56,
	}
	tpl.PoWDiffReference.Nbits = "0x1A2B3C4D"

	got := templateKey(tpl)
	want := "abcdef|001122|334455|7|1a2b3c4d|42|aabbcc|1234|56"
	if got != want {
		t.Fatalf("templateKey() = %q, want %q", got, want)
	}

	ntimeRefresh := *tpl
	ntimeRefresh.CurTime++
	if refreshKey := templateKey(&ntimeRefresh); refreshKey != got {
		t.Fatalf("ntime-only refresh key = %q, want unchanged %q", refreshKey, got)
	}

	consensusChanges := []struct {
		name   string
		mutate func(*BlockTemplate)
	}{
		{name: "parent", mutate: func(v *BlockTemplate) { v.PreviousHash = "ff" }},
		{name: "state-root", mutate: func(v *BlockTemplate) { v.StateRoot = "ff" }},
		{name: "tx-root", mutate: func(v *BlockTemplate) { v.TxRoot = "ff" }},
		{name: "version", mutate: func(v *BlockTemplate) { v.Version++ }},
		{name: "difficulty", mutate: func(v *BlockTemplate) { v.PoWDiffReference.Nbits = "1a2b3c4e" }},
		{name: "height", mutate: func(v *BlockTemplate) { v.Height++ }},
		{name: "coinbase", mutate: func(v *BlockTemplate) { v.CoinbaseAddress = "dd" }},
		{name: "reward", mutate: func(v *BlockTemplate) { v.Reward++ }},
		{name: "fees", mutate: func(v *BlockTemplate) { v.BlockFees++ }},
	}
	for _, test := range consensusChanges {
		t.Run(test.name, func(t *testing.T) {
			changed := *tpl
			test.mutate(&changed)
			if changedKey := templateKey(&changed); changedKey == got {
				t.Fatalf("templateKey() did not bind %s", test.name)
			}
		})
	}
}

func TestTemplateIngressRejectsSignedMonetaryValues(t *testing.T) {
	if _, err := (&remoteGBTResult{Reward: uint64(math.MaxInt64) + 1}).toBlockTemplate(); err == nil {
		t.Fatal("remote template accepted reward above signed wire boundary")
	}
	if _, err := (&remoteGBTResult{Reward: 1, BlockFees: -1}).toBlockTemplate(); err == nil {
		t.Fatal("remote template accepted negative block fees")
	}
	if _, _, err := buildNotify("negative", &BlockTemplate{Reward: -1}, true); err == nil {
		t.Fatal("buildNotify accepted negative reward")
	}
}

func TestSnapshotClientsReturnsCopy(t *testing.T) {
	clientsMu.Lock()
	previous := clients
	first := &client{addr: "first"}
	second := &client{addr: "second"}
	clients = map[*client]struct{}{first: {}, second: {}}
	clientsMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = previous
		clientsMu.Unlock()
	})

	got := snapshotClients()
	if len(got) != 2 {
		t.Fatalf("snapshotClients() length = %d, want 2", len(got))
	}

	clientsMu.Lock()
	delete(clients, first)
	clientsMu.Unlock()
	if len(got) != 2 {
		t.Fatal("snapshot should not alias clients map")
	}
}

func TestPushToAllMinersAppliesVarDiffBeforeNotify(t *testing.T) {
	clientsMu.Lock()
	previousClients := clients
	clientsMu.Unlock()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousMinPdiff := minPdiff
	previousStartingPdiff := startingPdiff
	t.Cleanup(func() {
		clientsMu.Lock()
		clients = previousClients
		clientsMu.Unlock()
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		minPdiff = previousMinPdiff
		startingPdiff = previousStartingPdiff
	})

	minPdiff = 0.05
	startingPdiff = 0.05

	var writes bytes.Buffer
	c := &client{
		addr:         "miner-1",
		rw:           bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
		pdiff:        1.0,
		prevPdiff:    0.5,
		windowStart:  time.Now().Add(-windowSize - time.Second),
		windowShares: int(desiredSharesPerWindow() * 4),
	}
	c.extranonce1 = [4]byte{1, 2, 3, 4}

	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()

	baseNotify := []interface{}{"base-job", "prev", "ver", "bits", "time", false}
	baseJob := &JobData{NBits: 0x1d00ffff, CreatedAt: time.Now()}
	pushToAllMinersWithVarDiff(baseNotify, baseJob, 99)

	messages := decodeStratumPushes(t, writes.Bytes())
	if len(messages) != 2 {
		t.Fatalf("broadcast wrote %d messages, want difficulty then notify; messages=%q", len(messages), writes.String())
	}
	if messages[0].Method != "mining.set_difficulty" {
		t.Fatalf("first method = %s, want mining.set_difficulty", messages[0].Method)
	}
	if messages[1].Method != "mining.notify" {
		t.Fatalf("second method = %s, want mining.notify", messages[1].Method)
	}

	var pushedDiff float64
	if err := json.Unmarshal(messages[0].Params[0], &pushedDiff); err != nil {
		t.Fatalf("decode pushed difficulty: %v", err)
	}
	if pushedDiff <= 1.0 {
		t.Fatalf("pushed difficulty = %.6f, want timer-adjusted value above old difficulty", pushedDiff)
	}

	var jobID string
	if err := json.Unmarshal(messages[1].Params[0], &jobID); err != nil {
		t.Fatalf("decode notify job id: %v", err)
	}
	job, ok := currentJobForClient(c, jobID)
	if !ok {
		t.Fatalf("current job %q was not recorded", jobID)
	}
	if !almostEqual(job.Difficulty, pushedDiff) {
		t.Fatalf("job difficulty = %.6f, pushed difficulty = %.6f", job.Difficulty, pushedDiff)
	}
	if !almostEqual(job.PrevDiff, 1.0) {
		t.Fatalf("job previous difficulty = %.6f, want old difficulty 1.0", job.PrevDiff)
	}
}

func TestPushJobToClientDeletesAcceptedJobOnNotifyFailure(t *testing.T) {
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	t.Cleanup(func() {
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
	})

	writes := &rejectNotifyWriter{}
	c := &client{
		addr:      "miner-1",
		rw:        bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(writes)),
		pdiff:     1.0,
		prevPdiff: 0.5,
	}
	c.extranonce1 = [4]byte{1, 2, 3, 4}

	baseNotify := []interface{}{"base-job", "prev", "ver", "bits", "time", false}
	baseJob := &JobData{NBits: 0x1d00ffff, CreatedAt: time.Now()}
	err := pushJobToClient(c, baseNotify, baseJob, 99, "base-job", preserveNotifyClean)
	if err == nil {
		t.Fatal("pushJobToClient returned nil error")
	}

	if _, ok := acceptedJobs.Load("base-job_01020304"); ok {
		t.Fatal("accepted job entry remained after notify failure")
	}
	if !c.closed {
		t.Fatal("client was not marked closed after notify failure")
	}

	messages := decodeStratumPushes(t, writes.Bytes())
	if len(messages) != 1 || messages[0].Method != "mining.set_difficulty" {
		t.Fatalf("writes = %+v, want only successful mining.set_difficulty before notify failure", messages)
	}
}

type rejectNotifyWriter struct {
	bytes.Buffer
}

func (w *rejectNotifyWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(`"mining.notify"`)) {
		return 0, errors.New("forced notify failure")
	}
	return w.Buffer.Write(p)
}

type stratumPushMessage struct {
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

func decodeStratumPushes(t *testing.T, data []byte) []stratumPushMessage {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var messages []stratumPushMessage
	for scanner.Scan() {
		var msg stratumPushMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			t.Fatalf("decode stratum push %q: %v", scanner.Text(), err)
		}
		messages = append(messages, msg)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan stratum pushes: %v", err)
	}
	return messages
}

func BenchmarkGetBlockTemplatePayloadMapMarshal(b *testing.B) {
	poolAddress := "0x1234567890abcdef1234567890abcdef12345678"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params := []interface{}{[]interface{}{}, 10, poolAddress}
		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getBlockTemplate",
			"params":  params,
		}
		buf, err := json.Marshal(payload)
		if err != nil {
			b.Fatal(err)
		}
		gbtPayloadBenchSink = buf
	}
}

func BenchmarkGetBlockTemplatePayloadAppend(b *testing.B) {
	poolAddress := "0x1234567890abcdef1234567890abcdef12345678"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		gbtPayloadBenchSink = appendGetBlockTemplatePayload(make([]byte, 0, 96+len(poolAddress)), poolAddress)
	}
}

func TestTemplateStreamKeepalivePrecedesIdleTimeout(t *testing.T) {
	if templateStreamPingInterval >= templateStreamIdleTimeout {
		t.Fatalf("templateStreamPingInterval=%s must be shorter than templateStreamIdleTimeout=%s", templateStreamPingInterval, templateStreamIdleTimeout)
	}
	if templateStreamIdleTimeout < 3*templateStreamPingInterval {
		t.Fatalf("templateStreamIdleTimeout=%s should allow at least three ping intervals of %s", templateStreamIdleTimeout, templateStreamPingInterval)
	}
}

func TestProbeTemplateSubscriptionRequiresSubscribeAck(t *testing.T) {
	var upgrader websocket.Upgrader
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		defer conn.Close()

		var req struct {
			Method string `json:"method"`
		}
		if err := conn.ReadJSON(&req); err != nil {
			t.Errorf("read subscribe: %v", err)
			return
		}
		if req.Method != "Blockdag_subscribe" {
			t.Errorf("method = %q, want Blockdag_subscribe", req.Method)
		}
		if err := conn.WriteJSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  "sub-1",
			"error":   nil,
		}); err != nil {
			t.Errorf("write subscribe ack: %v", err)
		}
	}))
	defer server.Close()

	backend := &rpcBackend{name: "node", wsURL: "ws" + strings.TrimPrefix(server.URL, "http")}
	if err := probeTemplateSubscription(backend); err != nil {
		t.Fatalf("probeTemplateSubscription() error = %v", err)
	}
}

func TestProbeTemplateSubscriptionPausesWhenNodeHealthReportsSyncing(t *testing.T) {
	resetRouterTestState(t)

	var wsCalls atomic.Int32
	var upgrader websocket.Upgrader
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsCalls.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		defer conn.Close()
	}))
	defer server.Close()

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     server.URL,
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	activeRPCRouter = router
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	recordBackendNodeSyncing(router, backend)

	err = probeTemplateSubscription(backend)
	if err == nil || !strings.Contains(err.Error(), "node-health-node-syncing") {
		t.Fatalf("probeTemplateSubscription() error = %v, want node-health-node-syncing pause", err)
	}
	if wsCalls.Load() != 0 {
		t.Fatalf("websocket probe attempts = %d, want 0 while node health reports syncing", wsCalls.Load())
	}
}

func TestProbeTemplateSubscriptionRejectsSubscribeError(t *testing.T) {
	var upgrader websocket.Upgrader
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		defer conn.Close()
		if err := conn.WriteJSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  nil,
			"error": map[string]interface{}{
				"code":    -32601,
				"message": "method blockTemplate does not exist",
			},
		}); err != nil {
			t.Errorf("write subscribe error: %v", err)
		}
	}))
	defer server.Close()

	backend := &rpcBackend{name: "node", wsURL: "ws" + strings.TrimPrefix(server.URL, "http")}
	err := probeTemplateSubscription(backend)
	if err == nil {
		t.Fatal("probeTemplateSubscription() error = nil, want subscribe error")
	}
	if !isWSUnsupportedSubscriptionError(err) {
		t.Fatalf("error %q should be classified as unsupported subscription", err)
	}
}

func TestWSUnsupportedSubscriptionError(t *testing.T) {
	err := errors.New("subscribe error: map[code:-32601 message:The method blockTemplate does not exist/is not available]")
	if !isWSUnsupportedSubscriptionError(err) {
		t.Fatal("expected blockTemplate method-not-found subscription error to be classified unsupported")
	}
	if isWSUnsupportedSubscriptionError(errors.New("subscribe error: temporary EOF")) {
		t.Fatal("temporary subscription errors should not be treated as unsupported capability")
	}
	if isWSUnsupportedSubscriptionError(errors.New("RPC error in getBlockTemplate: nonce too low")) {
		t.Fatal("HTTP template errors should not be treated as unsupported WebSocket subscription")
	}
}

func resetTemplateTTLRefreshConfigForTest(t *testing.T) {
	t.Helper()

	previousTTL := templateTTLRefreshDur
	previousOverride := templateTTLRefreshOverride.Load()

	templateTTLRefreshOnce = sync.Once{}
	templateTTLRefreshDur = 0
	templateTTLRefreshOverride.Store(0)

	t.Cleanup(func() {
		templateTTLRefreshOnce = sync.Once{}
		templateTTLRefreshDur = previousTTL
		templateTTLRefreshOverride.Store(previousOverride)
	})
}

func TestTemplateTTLRefreshForcesInitialBroadcast(t *testing.T) {
	previousLastBroadcastTime := lastBroadcastTime
	resetTemplateTTLRefreshConfigForTest(t)
	t.Cleanup(func() {
		lastBroadcastTime = previousLastBroadcastTime
	})

	now := time.Now()
	lastBroadcastTime = atomic.Value{}
	t.Setenv("POOL_TEMPLATE_TTL_REFRESH_MS", "")
	if !shouldForceTemplateTTLRefresh(now) {
		t.Fatal("missing prior broadcast should force an initial template refresh")
	}

	lastBroadcastTime.Store(now.Add(-defaultTemplateTTLDur - time.Second))
	if !shouldForceTemplateTTLRefresh(now) {
		t.Fatal("expired prior broadcast should force a template refresh")
	}

	lastBroadcastTime.Store(now.Add(-(defaultTemplateTTLDur / 2)))
	if shouldForceTemplateTTLRefresh(now) {
		t.Fatal("recent prior broadcast should not force a template refresh")
	}
}

func TestTemplateTTLRefreshDefaultTracksFastDAG(t *testing.T) {
	resetTemplateTTLRefreshConfigForTest(t)

	t.Setenv("POOL_TEMPLATE_TTL_REFRESH_MS", "")

	if got := configuredTemplateTTLRefresh(); got != defaultTemplateTTLDur {
		t.Fatalf("default TTL refresh = %s, want %s", got, defaultTemplateTTLDur)
	}
}

func TestTemplateTTLRefreshUsesConfiguredInterval(t *testing.T) {
	previousLastBroadcastTime := lastBroadcastTime
	resetTemplateTTLRefreshConfigForTest(t)
	t.Cleanup(func() {
		lastBroadcastTime = previousLastBroadcastTime
	})

	now := time.Now()
	lastBroadcastTime = atomic.Value{}
	t.Setenv("POOL_TEMPLATE_TTL_REFRESH_MS", "700")

	lastBroadcastTime.Store(now.Add(-800 * time.Millisecond))
	if !shouldForceTemplateTTLRefresh(now) {
		t.Fatal("configured expired prior broadcast should force a template refresh")
	}

	lastBroadcastTime.Store(now.Add(-600 * time.Millisecond))
	if shouldForceTemplateTTLRefresh(now) {
		t.Fatal("configured recent prior broadcast should not force a template refresh")
	}
}

func TestTemplateTTLRefreshCapsSlowRuntimeOverrideForFastDAG(t *testing.T) {
	resetTemplateTTLRefreshConfigForTest(t)
	t.Setenv("POOL_TEMPLATE_TTL_REFRESH_MS", "5000")

	if got := configuredTemplateTTLRefresh(); got != time.Second {
		t.Fatalf("configured TTL = %s, want fast-DAG cap 1s", got)
	}
}

func TestTemplateTTLRefreshOverride(t *testing.T) {
	resetTemplateTTLRefreshConfigForTest(t)
	t.Setenv("POOL_TEMPLATE_TTL_REFRESH_MS", "5000")

	if got := configuredTemplateTTLRefresh(); got != time.Second {
		t.Fatalf("configured TTL = %s, want fast-DAG cap 1s", got)
	}
	setTemplateTTLRefreshOverride(1500 * time.Millisecond)
	if got := configuredTemplateTTLRefresh(); got != 1500*time.Millisecond {
		t.Fatalf("override TTL = %s, want 1.5s", got)
	}
	setTemplateTTLRefreshOverride(0)
	if got := configuredTemplateTTLRefresh(); got != time.Second {
		t.Fatalf("cleared override TTL = %s, want fast-DAG cap 1s", got)
	}
}

func TestJobEpochInvalidationMarksJobsStaleWithoutScanning(t *testing.T) {
	previousGlobal := acceptedJobInvalidationEpoch.Load()
	previousBackendEpochs := snapshotBackendJobInvalidationEpochs()
	t.Cleanup(func() {
		acceptedJobInvalidationEpoch.Store(previousGlobal)
		restoreBackendJobInvalidationEpochs(previousBackendEpochs)
	})
	acceptedJobInvalidationEpoch.Store(0)
	backendJobInvalidationEpochs = sync.Map{}

	job := &JobData{
		ParentHash:               "parent",
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendName:              "node",
		BackendInvalidationEpoch: backendInvalidationEpoch("node"),
	}

	acceptedJobInvalidationEpoch.Add(1)

	if job.invalidated.Load() || job.stale.Load() {
		t.Fatal("epoch invalidation should not need per-job flag writes")
	}
	if !jobInvalidatedByEpoch(job) {
		t.Fatal("job was not invalidated by global epoch")
	}
	if got := staleShareReason(job, "parent"); got != "stale-job" {
		t.Fatalf("stale reason = %q, want stale-job", got)
	}
}

func TestBackendEpochInvalidationOnlyMarksMatchingBackend(t *testing.T) {
	previousGlobal := acceptedJobInvalidationEpoch.Load()
	previousBackendEpochs := snapshotBackendJobInvalidationEpochs()
	t.Cleanup(func() {
		acceptedJobInvalidationEpoch.Store(previousGlobal)
		restoreBackendJobInvalidationEpochs(previousBackendEpochs)
	})
	acceptedJobInvalidationEpoch.Store(0)
	backendJobInvalidationEpochs = sync.Map{}

	activeJob := &JobData{
		BackendName:              "node",
		BackendInvalidationEpoch: backendInvalidationEpoch("node"),
	}
	otherJob := &JobData{
		BackendName:              "other-source",
		BackendInvalidationEpoch: backendInvalidationEpoch("other-source"),
	}

	incrementBackendInvalidationEpoch("node")

	if !jobInvalidatedByEpoch(activeJob) {
		t.Fatal("node job was not invalidated by node epoch")
	}
	if jobInvalidatedByEpoch(otherJob) {
		t.Fatal("other-source job should not be invalidated by node epoch")
	}
}

func TestRememberTemplateParentUpdatesCurrentParent(t *testing.T) {
	parentMu.Lock()
	previousLastParent := lastParent
	lastParent = ""
	parentMu.Unlock()
	previousCurrentParent := currentParent
	currentParent = atomic.Value{}
	t.Cleanup(func() {
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		currentParent = previousCurrentParent
	})

	tpl := &BlockTemplate{PreviousHash: "0xabcdef"}
	if !rememberTemplateParent(tpl) {
		t.Fatal("first parent should be treated as changed")
	}
	if got := CurrentParent(); got != "abcdef" {
		t.Fatalf("CurrentParent() = %q, want abcdef", got)
	}
	if rememberTemplateParent(tpl) {
		t.Fatal("same parent should not be treated as changed")
	}
}

func TestTemplatePressureWindowControlsActiveState(t *testing.T) {
	previousUntil := templatePressureUntil.Load()
	t.Cleanup(func() {
		templatePressureUntil.Store(previousUntil)
	})

	now := time.Unix(100, 0)
	templatePressureUntil.Store(0)
	if templatePressureActive(now) {
		t.Fatal("empty template pressure deadline should not be active")
	}

	templatePressureUntil.Store(now.Add(time.Second).UnixNano())
	if !templatePressureActive(now) {
		t.Fatal("future template pressure deadline should be active")
	}

	if templatePressureActive(now.Add(2 * time.Second)) {
		t.Fatal("expired template pressure deadline should not be active")
	}
}

type backendJobInvalidationEpochSnapshot map[string]uint64

func snapshotBackendJobInvalidationEpochs() backendJobInvalidationEpochSnapshot {
	snapshot := backendJobInvalidationEpochSnapshot{}
	backendJobInvalidationEpochs.Range(func(key, value interface{}) bool {
		name, _ := key.(string)
		counter, _ := value.(*atomic.Uint64)
		if name == "" || counter == nil {
			return true
		}
		snapshot[name] = counter.Load()
		return true
	})
	return snapshot
}

func restoreBackendJobInvalidationEpochs(snapshot backendJobInvalidationEpochSnapshot) {
	backendJobInvalidationEpochs = sync.Map{}
	for name, value := range snapshot {
		counter := &atomic.Uint64{}
		counter.Store(value)
		backendJobInvalidationEpochs.Store(name, counter)
	}
}

func resetTemplateRefreshTestState(t *testing.T) {
	t.Helper()

	waitForTemplateRefreshIdle(t)
	waitForTemplateRefreshWorkersIdle(t)

	templateRefreshMu.Lock()
	previousLatest := templateRefreshLatest
	previousActive := templateRefreshActive
	previousSeq := templateRefreshSeq
	templateRefreshLatest = templateRefreshRequest{}
	templateRefreshActive = false
	templateRefreshSeq = 0
	templateRefreshMu.Unlock()

	t.Cleanup(func() {
		waitForTemplateRefreshIdle(t)
		waitForTemplateRefreshWorkersIdle(t)
		templateRefreshMu.Lock()
		templateRefreshLatest = previousLatest
		templateRefreshActive = previousActive
		templateRefreshSeq = previousSeq
		templateRefreshMu.Unlock()
	})
}

func waitForTemplateRefreshIdle(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		templateRefreshMu.Lock()
		active := templateRefreshActive
		templateRefreshMu.Unlock()
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("template refresh coordinator did not become idle")
}

func waitForTemplateRefreshWorkersIdle(t *testing.T) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		templateRefreshWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("template refresh worker did not exit")
	}
}

func resetCleanTemplateRefreshTestState(t *testing.T) {
	t.Helper()

	waitForCleanRefreshIdle(t)
	waitForCleanRefreshWorkersIdle(t)

	cleanTemplateRefreshMu.Lock()
	previousLatest := cleanTemplateRefreshLatest
	previousActive := cleanTemplateRefreshActive
	previousSeq := cleanTemplateRefreshSeq
	previousInvalidationEpoch := acceptedJobInvalidationEpoch.Load()
	previousBackendEpochs := snapshotBackendJobInvalidationEpochs()
	previousLastInvalidation := lastJobInvalidationUnixNano.Load()
	cleanTemplateRefreshLatest = cleanTemplateRefreshRequest{}
	cleanTemplateRefreshActive = false
	cleanTemplateRefreshSeq = 0
	backendJobInvalidationEpochs = sync.Map{}
	cleanTemplateRefreshMu.Unlock()

	t.Cleanup(func() {
		waitForCleanRefreshIdle(t)
		waitForCleanRefreshWorkersIdle(t)
		cleanTemplateRefreshMu.Lock()
		cleanTemplateRefreshLatest = previousLatest
		cleanTemplateRefreshActive = previousActive
		cleanTemplateRefreshSeq = previousSeq
		cleanTemplateRefreshMu.Unlock()
		acceptedJobInvalidationEpoch.Store(previousInvalidationEpoch)
		restoreBackendJobInvalidationEpochs(previousBackendEpochs)
		lastJobInvalidationUnixNano.Store(previousLastInvalidation)
	})
}

func waitForCleanRefreshIdle(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		cleanTemplateRefreshMu.Lock()
		active := cleanTemplateRefreshActive
		cleanTemplateRefreshMu.Unlock()
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("clean template refresh coordinator did not become idle")
}

func waitForCleanRefreshWorkersIdle(t *testing.T) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		cleanTemplateRefreshWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clean template refresh worker did not exit")
	}
}

func resetPreemptiveBlockRefreshTestState(t *testing.T) {
	t.Helper()

	waitForPreemptiveBlockRefreshIdle(t)
	waitForPreemptiveBlockRefreshWorkersIdle(t)

	preemptiveBlockRefreshMu.Lock()
	previousLatest := preemptiveBlockRefreshLatest
	previousActive := preemptiveBlockRefreshActive
	previousSeq := preemptiveBlockRefreshSeq
	preemptiveBlockRefreshLatest = preemptiveBlockRefreshRequest{}
	preemptiveBlockRefreshActive = false
	preemptiveBlockRefreshSeq = 0
	preemptiveBlockRefreshMu.Unlock()

	t.Cleanup(func() {
		waitForPreemptiveBlockRefreshIdle(t)
		waitForPreemptiveBlockRefreshWorkersIdle(t)
		preemptiveBlockRefreshMu.Lock()
		preemptiveBlockRefreshLatest = previousLatest
		preemptiveBlockRefreshActive = previousActive
		preemptiveBlockRefreshSeq = previousSeq
		preemptiveBlockRefreshMu.Unlock()
	})
}

func waitForPreemptiveBlockRefreshIdle(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		preemptiveBlockRefreshMu.Lock()
		active := preemptiveBlockRefreshActive
		preemptiveBlockRefreshMu.Unlock()
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("preemptive block refresh coordinator did not become idle")
}

func waitForPreemptiveBlockRefreshWorkersIdle(t *testing.T) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		preemptiveBlockRefreshWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("preemptive block refresh worker did not exit")
	}
}

func testCleanRefreshBlockTemplate(parent string, height int64) *BlockTemplate {
	return &BlockTemplate{
		Version:      1,
		TxRoot:       strings.Repeat("a", 64),
		StateRoot:    strings.Repeat("b", 64),
		CurTime:      int(time.Now().Unix()),
		PreviousHash: parent,
		Height:       height,
		PoWDiffReference: struct {
			Nbits string `json:"nbits"`
		}{Nbits: "1d00ffff"},
		CoinbaseAddress: testPoolCoinbase,
		Reward:          1,
		BlockFees:       0,
	}
}
