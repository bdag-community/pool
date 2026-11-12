package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestNewRPCRouterFromConfigBuildsSingleNodeBackend(t *testing.T) {
	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:      "http://bdag-miner-node-1:38131",
		RPCUser:     "user",
		RPCPassword: "pass",
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	if router.backend == nil {
		t.Fatal("router.backend = nil, want configured node backend")
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	if backend.name != "node" {
		t.Fatalf("backend name = %q, want node", backend.name)
	}
	if backend.url != "http://bdag-miner-node-1:38131/" {
		t.Fatalf("backend URL = %q", backend.url)
	}
	if backend.wsURL != "ws://bdag-miner-node-1:38131/ws" {
		t.Fatalf("backend wsURL = %q", backend.wsURL)
	}
}

func TestBuildRPCRouterConfigNodeHealthProbeCadence(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		wantProbe  time.Duration
		wantErr    bool
	}{
		{name: "default", configured: 0, wantProbe: 15 * time.Second},
		{name: "ten seconds", configured: 10, wantProbe: 10 * time.Second},
		{name: "fifteen seconds", configured: 15, wantProbe: 15 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildRPCRouterConfig(Config{
				RPCURL:                          "http://node:38131",
				RPCRouterNodeHealthProbeSeconds: tt.configured,
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildRPCRouterConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if cfg.nodeHealthProbe != tt.wantProbe {
				t.Fatalf("node health probe = %s, want %s", cfg.nodeHealthProbe, tt.wantProbe)
			}
		})
	}
}

func TestBuildRPCRouterConfigRejectsSubMinimumNodeHealthProbeCadence(t *testing.T) {
	for configured := 1; configured <= 9; configured++ {
		t.Run(fmt.Sprintf("%d seconds", configured), func(t *testing.T) {
			_, err := buildRPCRouterConfig(Config{
				RPCURL:                          "http://node:38131",
				RPCRouterNodeHealthProbeSeconds: configured,
			})
			if err == nil {
				t.Fatalf("buildRPCRouterConfig() accepted %d-second node health probe cadence", configured)
			}
			if !strings.Contains(err.Error(), "at least 10 seconds") {
				t.Fatalf("buildRPCRouterConfig() error = %q, want minimum-cadence explanation", err)
			}
		})
	}
}

func TestRPCRouterUsesGlobalRPCURLFallback(t *testing.T) {
	previous := rpcURL
	rpcURL = "http://fallback-node:38131"
	t.Cleanup(func() {
		rpcURL = previous
	})

	router, err := newTestRPCRouterFromConfig(t, Config{})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	if backend.url != "http://fallback-node:38131/" {
		t.Fatalf("backend URL = %q, want fallback", backend.url)
	}
}

func TestRPCRouterTemplateSequenceRejectsDuplicatePush(t *testing.T) {
	router, err := newTestRPCRouterFromConfig(t, Config{RPCURL: "http://node:38131"})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	if !router.recordTemplateSuccess(backend, &BlockTemplate{
		PreviousHash: "0xabc",
		Height:       100,
		SourceSeq:    1,
	}, time.Millisecond) {
		t.Fatal("recordTemplateSuccess() rejected initial template")
	}
	if router.recordTemplateSuccess(backend, &BlockTemplate{
		PreviousHash: "0xabc",
		Height:       100,
		SourceSeq:    1,
	}, time.Millisecond) {
		t.Fatal("recordTemplateSuccess() accepted duplicate pushed template")
	}
}

func TestNodeHealthReadyBootstrapsBackendWithoutPoolTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.mu.Lock()
	backend.state.healthy = false
	backend.state.score = 0
	backend.state.reason = "template-error"
	backend.mu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:        "ok",
		MineableNow:       true,
		SubmitReady:       true,
		TemplateAvailable: true,
		TemplateUsable:    true,
		TemplateAgeMs:     250,
		LatestTemplateSeq: 22,
		MainOrder:         100,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if !state.healthy || state.reason != "node-health-ok-awaiting-template" {
		t.Fatalf("node health ready without pool template state = %#v, want bootstrap-ready awaiting-template", state)
	}
	if !state.lastTemplateAt.IsZero() {
		t.Fatalf("node health should not synthesize router template freshness, state = %#v", state)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); !ok || reason != "ok" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want true/ok for first getBlockTemplate", ok, reason)
	}

	backend.mu.Lock()
	backend.state.lastTemplateAt = time.Now()
	backend.mu.Unlock()
	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:        "ok",
		MineableNow:       true,
		SubmitReady:       true,
		TemplateAvailable: true,
		TemplateUsable:    true,
		TemplateAgeMs:     250,
		LatestTemplateSeq: 23,
		MainOrder:         101,
	}, 10*time.Millisecond)
	state = backend.snapshotState()
	if !state.healthy || state.reason != "node-health-ok" {
		t.Fatalf("node health ready with pool template state = %#v, want healthy", state)
	}
}

func TestNodeHealthCanonicalMiningFreshAllowsBoundedAncestorTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                               "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:           true,
		RPCRouterNodeHealthMinConsensusPeers: 2,
	})
	if err != nil {
		t.Fatalf("newTestRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		Reason:                     "ready",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(true),
		GetBlockTemplateReasonCode: "ok",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		LastGoodTemplateAvailable:  true,
		LastGoodTemplateAgeMs:      250,
		P2PFreshnessMaxAgeMs:       3000,
		P2PPeerLeadToleranceBlocks: 512,
		P2PCurrent:                 false,
		P2PConsensusPeerCount:      5,
		P2PFreshConsensusPeerCount: 5,
		P2PBestPeerLeadBlocks:      288,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
		ChainCurrent:               true,
		MainOrder:                  17852408,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if !state.healthy || state.reason != "node-health-ok-awaiting-template" {
		t.Fatalf("state = %#v, want canonical mining-fresh backend ready", state)
	}
	if got := router.templateFetchPauseReason(backend); got != "" {
		t.Fatalf("templateFetchPauseReason() = %q, want empty", got)
	}
}

func TestNodeHealthMiningFreshDoesNotOverrideNonCurrentLocalChain(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                               "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:           true,
		RPCRouterNodeHealthMinConsensusPeers: 2,
	})
	if err != nil {
		t.Fatalf("newTestRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		Reason:                     "ready",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(true),
		GetBlockTemplateReasonCode: "ok",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		P2PFreshnessMaxAgeMs:       3000,
		P2PCurrent:                 false,
		P2PConsensusPeerCount:      5,
		P2PFreshConsensusPeerCount: 5,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
		ChainCurrent:               false,
		MainOrder:                  17852408,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-p2p-not-current" {
		t.Fatalf("state = %#v, want fail-closed p2p-not-current", state)
	}
}

func TestNodeHealthGetBlockTemplateReadyBootstrapsMissingTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "template_unavailable",
		Reason:                     "no mining template is available",
		MineableNow:                false,
		SubmitReady:                false,
		GetBlockTemplateReady:      boolPtr(true),
		GetBlockTemplateReasonCode: "ok",
		GetBlockTemplateReason:     "getBlockTemplate sync preflight is ready",
		WorkerReady:                true,
		WorkerRunning:              false,
		TemplateAvailable:          false,
		TemplateUsable:             false,
		P2PFreshnessMaxAgeMs:       3000,
		P2PCurrent:                 true,
		P2PConsensusPeerCount:      5,
		P2PFreshConsensusPeerCount: 4,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
		SyncAllowed:                true,
		SyncReasonCode:             "ok",
		ChainCurrent:               true,
		MainOrder:                  10432988,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if !state.healthy || state.reason != "node-health-get-block-template-ready-awaiting-template" {
		t.Fatalf("state = %#v, want healthy bootstrap-ready awaiting-template", state)
	}
	if state.nodeHealthConsecutiveUnready != 0 {
		t.Fatalf("nodeHealthConsecutiveUnready = %d, want 0", state.nodeHealthConsecutiveUnready)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); !ok || reason != "ok" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want true/ok for first getBlockTemplate", ok, reason)
	}
	if ok, reason := router.backendReadyForSubmitRecovery(backend); !ok || reason != "ok" {
		t.Fatalf("backendReadyForSubmitRecovery() = %v/%q, want true/ok for first getBlockTemplate", ok, reason)
	}
}

func TestNodeHealthGetBlockTemplateReadyVetoesWithoutConsensusPeers(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "template_unavailable",
		Reason:                     "no mining template is available",
		MineableNow:                false,
		SubmitReady:                false,
		GetBlockTemplateReady:      boolPtr(true),
		GetBlockTemplateReasonCode: "ok",
		GetBlockTemplateReason:     "getBlockTemplate sync preflight is ready",
		WorkerReady:                true,
		TemplateAvailable:          false,
		TemplateUsable:             false,
		P2PFreshnessMaxAgeMs:       3000,
		P2PConsensusPeerCount:      0,
		P2PFreshConsensusPeerCount: 0,
		P2PMiningFresh:             false,
		P2PMiningFreshReasonCode:   "no_consensus_peers",
		SyncAllowed:                true,
		SyncReasonCode:             "ok",
		ChainCurrent:               true,
		MainOrder:                  10432988,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-not-enough-consensus-peers" {
		t.Fatalf("state = %#v, want unhealthy not-enough-consensus-peers", state)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); ok || reason != "node-health-not-enough-consensus-peers" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want false/node-health-not-enough-consensus-peers", ok, reason)
	}
	if got := router.templateFetchPauseReason(backend); got != "node-health-not-enough-consensus-peers" {
		t.Fatalf("templateFetchPauseReason() = %q, want node-health-not-enough-consensus-peers", got)
	}
}

func TestNodeHealthVetoesSingleFreshConsensusPeer(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                                       "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:                   true,
		RPCRouterNodeHealthMaxAgeSeconds:             10,
		RPCRouterNodeHealthMinConsensusPeers:         2,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.mu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		Reason:                     "ready",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(true),
		GetBlockTemplateReasonCode: "ok",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		LastGoodTemplateAvailable:  true,
		LastGoodTemplateAgeMs:      250,
		P2PFreshnessMaxAgeMs:       3000,
		P2PCurrent:                 true,
		P2PConsensusPeerCount:      1,
		P2PFreshConsensusPeerCount: 1,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
		SyncAllowed:                true,
		SyncReasonCode:             "ok",
		ChainCurrent:               true,
		MainOrder:                  10432988,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-not-enough-consensus-peers" {
		t.Fatalf("state = %#v, want unhealthy not-enough-consensus-peers", state)
	}
	if state.nodeHealthP2PMiningFresh || state.nodeHealthP2PMiningFreshReasonCode != "not-enough-consensus-peers" {
		t.Fatalf("effective P2P freshness = %v/%q, want false/not-enough-consensus-peers", state.nodeHealthP2PMiningFresh, state.nodeHealthP2PMiningFreshReasonCode)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); ok || reason != "node-health-not-enough-consensus-peers" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want false/node-health-not-enough-consensus-peers", ok, reason)
	}
	if got := router.templateFetchPauseReason(backend); got != "node-health-not-enough-consensus-peers" {
		t.Fatalf("templateFetchPauseReason() = %q, want node-health-not-enough-consensus-peers", got)
	}
}

func TestNodeHealthGetBlockTemplateReadyFalseVetoesWithoutFreshPoolTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "getBlockTemplate sync preflight is not ready",
		TemplateAvailable:          true,
		TemplateAgeMs:              250,
		LatestTemplateSeq:          24,
		MainOrder:                  101,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-node-syncing" {
		t.Fatalf("state = %#v, want hard node-syncing veto without fresh pool template", state)
	}
}

func TestNodeHealthSyncVetoesEvenWithFreshPoolTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.lastTemplateParent = "fresh-parent"
	backend.mu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "bdag pool syncing",
		TemplateAvailable:          true,
		TemplateAgeMs:              250,
		LatestTemplateSeq:          24,
		MainOrder:                  101,
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-node-syncing" {
		t.Fatalf("state = %#v, want hard node-syncing veto even with fresh pool template", state)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); ok || reason != "node-health-node-syncing" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want false/node-health-node-syncing", ok, reason)
	}
}

func TestNodeHealthSyncSubmitReadyWithCachedTemplateAndP2PMiningFresh(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.lastTemplateParent = "fresh-parent"
	backend.state.nodeHealthP2PContextObserved = true
	backend.state.nodeHealthP2PCurrent = true
	backend.state.nodeHealthP2PMiningFresh = true
	backend.mu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "transient initial download during fresh mining",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		LastGoodTemplateAvailable:  true,
		LastGoodTemplateAgeMs:      250,
		LatestTemplateSeq:          24,
		MainOrder:                  101,
		P2PFreshnessMaxAgeMs:       3000,
		P2PCurrent:                 true,
		P2PConsensusPeerCount:      5,
		P2PFreshConsensusPeerCount: 4,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if !state.healthy || state.reason != "node-health-submit-ready-cached-template" {
		t.Fatalf("state = %#v, want cached-template submit-ready backend", state)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); !ok || reason != "ok" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want true/ok", ok, reason)
	}
	if ok, reason := router.backendReadyForSubmitRecovery(backend); !ok || reason != "ok" {
		t.Fatalf("backendReadyForSubmitRecovery() = %v/%q, want true/ok", ok, reason)
	}
}

func TestNodeHealthTipChaseSyncPreservesYoungCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousCurrentParent := CurrentParent()
	previousGlobalEpoch := acceptedJobInvalidationEpoch.Load()
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
		acceptedJobInvalidationEpoch.Store(previousGlobalEpoch)
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
	})
	acceptedJobInvalidationEpoch.Store(0)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
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

	parent := strings.Repeat("b", 64)
	miningWork.rememberParent(parent)
	job := &JobData{
		BackendName:              backend.name,
		ParentHash:               parent,
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: miningWork.backendInvalidationEpoch(backend.name),
	}
	acceptedJobs.Store("tip-chase-job", job)
	c := &client{addr: "miner-1", authorized: true}
	c.currentJob.Store(&clientCurrentJob{id: "tip-chase-job", job: job})
	clientsMu.Lock()
	clients[c] = struct{}{}
	clientsMu.Unlock()

	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.lastTemplateParent = parent
	backend.mu.Unlock()

	// Live tip-chase shape: node_syncing with mineable_now=false, but a young
	// last-good template and tip-aligned P2P freshness.
	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "node_syncing",
		Reason:                     "node pending-template backend is syncing",
		MineableNow:                false,
		SubmitReady:                false,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "node pending-template backend is syncing",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              800,
		LastGoodTemplateAvailable:  true,
		LastGoodTemplateAgeMs:      800,
		LatestTemplateSeq:          189,
		MainOrder:                  15_450_418,
		P2PFreshnessMaxAgeMs:       3000,
		P2PPeerLeadToleranceBlocks: 10,
		P2PBestPeerLeadBlocks:      0,
		P2PCurrent:                 true,
		P2PConsensusPeerCount:      3,
		P2PFreshConsensusPeerCount: 3,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if !state.healthy {
		t.Fatalf("state = %#v, want healthy tip-chase advisory", state)
	}
	if !strings.Contains(state.reason, "advisory") && state.reason != "node-health-submit-ready-cached-template" {
		t.Fatalf("state.reason = %q, want advisory tip-chase reason", state.reason)
	}
	if job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("tip-chase node_syncing invalidated young current job")
	}
	current, _ := c.currentJob.Load().(*clientCurrentJob)
	if current == nil || current.job != job {
		t.Fatal("tip-chase node_syncing cleared miner current job")
	}
}

func TestNodeHealthCatchUpSyncStillInvalidatesCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousGlobalEpoch := acceptedJobInvalidationEpoch.Load()
	t.Cleanup(func() {
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		acceptedJobInvalidationEpoch.Store(previousGlobalEpoch)
	})
	acceptedJobInvalidationEpoch.Store(0)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	job := &JobData{
		BackendName:              backend.name,
		CreatedAt:                time.Now(),
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: miningWork.backendInvalidationEpoch(backend.name),
	}
	acceptedJobs.Store("catchup-job", job)

	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.lastTemplateParent = "stale-parent"
	backend.mu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "node_syncing",
		Reason:                     "node is still syncing with peers",
		MineableNow:                false,
		SubmitReady:                false,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              800,
		LastGoodTemplateAvailable:  true,
		LastGoodTemplateAgeMs:      800,
		LatestTemplateSeq:          10,
		MainOrder:                  100,
		P2PFreshnessMaxAgeMs:       3000,
		P2PPeerLeadToleranceBlocks: 10,
		P2PBestPeerLeadBlocks:      50,
		P2PCurrent:                 false,
		P2PConsensusPeerCount:      3,
		P2PFreshConsensusPeerCount: 2,
		P2PMiningFresh:             false,
		P2PMiningFreshReasonCode:   "peer-lag",
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy {
		t.Fatalf("state = %#v, want hard catch-up veto", state)
	}
	if !job.invalidated.Load() && !jobInvalidatedByEpoch(job) {
		t.Fatal("catch-up node_syncing should invalidate current jobs")
	}
}

func TestNodeHealthSyncStillVetoesWithStaleP2PMiningFreshness(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.lastTemplateParent = "fresh-parent"
	backend.mu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "ok",
		MineableNow:                true,
		SubmitReady:                true,
		GetBlockTemplateReady:      boolPtr(false),
		GetBlockTemplateReasonCode: "node_syncing",
		GetBlockTemplateReason:     "stale peer freshness",
		TemplateAvailable:          true,
		TemplateUsable:             true,
		TemplateAgeMs:              250,
		LastGoodTemplateAvailable:  true,
		LastGoodTemplateAgeMs:      250,
		LatestTemplateSeq:          24,
		MainOrder:                  101,
		P2PFreshnessMaxAgeMs:       3000,
		P2PCurrent:                 true,
		P2PConsensusPeerCount:      5,
		P2PFreshConsensusPeerCount: 1,
		P2PMiningFresh:             false,
		P2PMiningFreshReasonCode:   "peer-lag",
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-node-syncing" {
		t.Fatalf("state = %#v, want hard node-syncing veto with stale P2P", state)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); ok || reason != "node-health-node-syncing" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want false/node-health-node-syncing", ok, reason)
	}
}

func TestNodeHealthTemplateParentStaleVetoesFreshPoolTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                           "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:       true,
		RPCRouterNodeHealthMaxAgeSeconds: 10,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.state.lastTemplateParent = "fresh-parent"
	backend.mu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                 "template_parent_stale",
		Reason:                     "template parent is no longer selected",
		MineableNow:                false,
		SubmitReady:                false,
		TemplateAvailable:          true,
		TemplateUsable:             false,
		TemplateAgeMs:              250,
		LastGoodTemplateAvailable:  true,
		LastGoodTemplateAgeMs:      250,
		LatestTemplateSeq:          25,
		MainOrder:                  101,
		P2PFreshnessMaxAgeMs:       3000,
		P2PCurrent:                 true,
		P2PConsensusPeerCount:      5,
		P2PFreshConsensusPeerCount: 4,
		P2PMiningFresh:             true,
		P2PMiningFreshReasonCode:   "ok",
	}, 10*time.Millisecond)

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-template-parent-stale" {
		t.Fatalf("state = %#v, want hard template-parent-stale veto", state)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); ok || reason != "node-health-template-parent-stale" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want false/node-health-template-parent-stale", ok, reason)
	}
	if got := router.templateFetchPauseReason(backend); got != "" {
		t.Fatalf("templateFetchPauseReason() = %q, want refresh allowed for replacement template", got)
	}
}

func TestNodeHealthRPCErrorInvalidatesLatestWorkForSelectedBackend(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled: true,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}

	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	previousNotify, previousNotifyOK := lastBaseNotifyVal.Load().([]interface{})
	previousGlobalEpoch := acceptedJobInvalidationEpoch.Load()
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
		acceptedJobInvalidationEpoch.Store(previousGlobalEpoch)
		restoreBackendJobInvalidationEpochs(previousBackendEpochs)
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
	})

	acceptedJobInvalidationEpoch.Store(0)
	restoreBackendJobInvalidationEpochs(backendJobInvalidationEpochSnapshot{})
	baseNotify := []interface{}{"stale-base-job", "prev", "ver", "bits", "time", false}
	baseJob := &JobData{
		BackendName:              backend.name,
		ParentHash:               "old-parent",
		CreatedAt:                time.Now(),
		InvalidationEpoch:        miningWork.currentInvalidationEpoch(),
		BackendInvalidationEpoch: miningWork.backendInvalidationEpoch(backend.name),
	}
	miningWork.rememberLatestBroadcast(baseNotify, baseJob, 101)
	miningWork.storeAcceptedJob("stale-base-job", baseJob)
	if !latestBroadcastReadyForResend() {
		t.Fatal("seeded latest broadcast should be resendable before node health error")
	}

	router.recordNodeTemplateHealthError(backend, &nodeHealthRPCError{
		reason: "timeout",
		err:    errors.New("context deadline exceeded"),
	})

	state := backend.snapshotState()
	if state.healthy || state.reason != "node-health-timeout" {
		t.Fatalf("state = %#v, want unhealthy node-health-timeout", state)
	}
	if !jobInvalidatedByEpoch(baseJob) || !baseJob.invalidated.Load() {
		t.Fatalf("latest backend job was not invalidated after node health timeout: job=%#v", baseJob)
	}
	if latestBroadcastReadyForResend() {
		t.Fatal("node health timeout left latest backend broadcast eligible for stale resend")
	}
}

func TestNodeHealthTransientErrorsStayAdvisoryWithFreshTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                            "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled:        true,
		RPCRouterNodeHealthErrorThreshold: 3,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
	backend := router.selectedBackend()
	if backend == nil {
		t.Fatal("selectedBackend() = nil")
	}
	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.lastTemplateAt = time.Now()
	backend.mu.Unlock()

	errHealth := &nodeHealthRPCError{reason: "timeout", err: fmt.Errorf("timeout fetching template health")}
	router.recordNodeTemplateHealthError(backend, errHealth)
	state := backend.snapshotState()
	if !state.healthy || state.reason != "node-health-advisory-timeout" || state.nodeHealthConsecutiveErrors != 1 {
		t.Fatalf("first transient error state = %#v, want healthy advisory timeout", state)
	}

	router.recordNodeTemplateHealthError(backend, errHealth)
	state = backend.snapshotState()
	if !state.healthy || state.reason != "node-health-advisory-timeout" || state.nodeHealthConsecutiveErrors != 2 {
		t.Fatalf("second transient error state = %#v, want healthy advisory timeout", state)
	}

	router.recordNodeTemplateHealthError(backend, errHealth)
	state = backend.snapshotState()
	if state.healthy || state.reason != "node-health-timeout" || state.nodeHealthConsecutiveErrors != 3 {
		t.Fatalf("third transient error state = %#v, want unhealthy timeout", state)
	}
}

func TestTemplateRefreshReadinessHoldFailsClosedWithFreshP2PTemplate(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled: true,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
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

	router.recordTemplateRefreshReadinessHold(backend, "initial-download", fmt.Errorf("Client in initial download"))

	state := backend.snapshotState()
	if state.healthy || state.reason != "submit-readiness-template-refresh-initial-download" {
		t.Fatalf("state = %#v, want hard template-refresh initial-download", state)
	}
	if held, reason := submitReadinessHoldActive(state, time.Now()); !held || reason != "submit-readiness-template-refresh-initial-download" {
		t.Fatalf("submitReadinessHoldActive() = %v/%q, want held/template-refresh-initial-download", held, reason)
	}
	if ok, reason := router.backendReadyForActiveTemplateFallback(backend); ok || reason != "submit-readiness-template-refresh-initial-download" {
		t.Fatalf("backendReadyForActiveTemplateFallback() = %v/%q, want false/template-refresh initial-download", ok, reason)
	}
}

func TestTemplateRefreshReadinessHoldRequiresLastGoodTemplateEvidence(t *testing.T) {
	resetRouterTestState(t)

	router, err := newTestRPCRouterFromConfig(t, Config{
		RPCURL:                     "http://node.invalid:38131",
		RPCRouterNodeHealthEnabled: true,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: 15,
	})
	if err != nil {
		t.Fatalf("newRPCRouterFromConfig() error = %v", err)
	}
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
	backend.state.nodeHealthP2PContextObserved = true
	backend.state.nodeHealthP2PCurrent = true
	backend.state.nodeHealthP2PMiningFresh = true
	backend.mu.Unlock()

	router.recordTemplateRefreshReadinessHold(backend, "initial-download", fmt.Errorf("Client in initial download"))

	state := backend.snapshotState()
	if state.healthy || state.reason != "submit-readiness-template-refresh-initial-download" {
		t.Fatalf("state = %#v, want hard template-refresh initial-download without last-good template evidence", state)
	}
	if held, reason := submitReadinessHoldActive(state, time.Now()); !held || reason != "submit-readiness-template-refresh-initial-download" {
		t.Fatalf("submitReadinessHoldActive() = %v/%q, want held/template-refresh-initial-download", held, reason)
	}
}

func TestSubmitNodeSyncingStaysAdvisoryWithFreshSubmitEvidence(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousGlobalEpoch := acceptedJobInvalidationEpoch.Load()
	t.Cleanup(func() {
		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		acceptedJobInvalidationEpoch.Store(previousGlobalEpoch)
	})
	acceptedJobInvalidationEpoch.Store(0)

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
	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.reason = "ok"
	backend.state.nodeHealthP2PContextObserved = true
	backend.state.nodeHealthP2PCurrent = true
	backend.state.nodeHealthP2PMiningFresh = true
	backend.mu.Unlock()

	job := &JobData{
		BackendName:              backend.name,
		InvalidationEpoch:        acceptedJobInvalidationEpoch.Load(),
		BackendInvalidationEpoch: miningWork.backendInvalidationEpoch(backend.name),
	}
	acceptedJobs.Store("node-job", job)

	router.recordSubmitResult(backend, "ok", time.Millisecond, nil)
	router.recordSubmitResult(backend, "ok", time.Millisecond, &submitRPCError{
		kind:   submitErrorSemantic,
		reason: "node-syncing",
		err:    fmt.Errorf("node is still syncing"),
	})

	if job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("tip-chase node-syncing submit readiness should preserve accepted jobs")
	}
	state := backend.snapshotState()
	if !state.healthy || state.reason != "submit-readiness-advisory-node-syncing" {
		t.Fatalf("state = %#v, want advisory node-syncing submit readiness", state)
	}
	if held, _ := submitReadinessHoldActive(state, time.Now()); held {
		t.Fatalf("submitReadinessHoldActive() held, want advisory without hold")
	}
	if submitBackendReadinessRequiresRefresh(job, "node-syncing") {
		t.Fatal("tip-chase node-syncing submit readiness should not force a clean job refresh")
	}
}

func TestSubmitNodeSyncingPreservesFreshCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
	resetRouterTestState(t)

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

	parent := strings.Repeat("a", 64)
	miningWork.rememberParent(parent)
	job := &JobData{
		BackendName:              backend.name,
		ParentHash:               parent,
		CreatedAt:                time.Now(),
		InvalidationEpoch:        miningWork.currentInvalidationEpoch(),
		BackendInvalidationEpoch: miningWork.backendInvalidationEpoch(backend.name),
	}
	c := &client{authorized: true}
	c.currentJob.Store(&clientCurrentJob{id: "job-1", job: job})
	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()
	acceptedJobs.Store("job-1", job)

	router.recordSubmitResult(backend, "ok", time.Millisecond, &submitRPCError{
		kind:   submitErrorSemantic,
		reason: "node-syncing",
		err:    fmt.Errorf("node is still syncing"),
	})

	if job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("tip-chase node-syncing submit readiness should preserve current jobs")
	}
	state := backend.snapshotState()
	if !state.healthy || !strings.Contains(state.reason, "advisory-node-syncing") {
		t.Fatalf("state = %#v, want advisory node-syncing submit readiness", state)
	}
	if held, _ := submitReadinessHoldActive(state, time.Now()); held {
		t.Fatalf("submitReadinessHoldActive() held, want advisory without hold")
	}
}

func TestRepeatedSubmitNodeSyncingStaysAdvisoryWhileJobsPreserved(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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
	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.nodeHealthP2PContextObserved = true
	backend.state.nodeHealthP2PCurrent = true
	backend.state.nodeHealthP2PMiningFresh = true
	backend.state.lastTemplateAt = time.Now()
	backend.state.nodeHealthTemplate = true
	backend.state.nodeHealthTemplateUsable = true
	backend.state.nodeHealthLastGoodTemplateAvailable = true
	backend.state.nodeHealthLastGoodTemplateAge = 250 * time.Millisecond
	backend.mu.Unlock()

	job := seedFreshCurrentJobForRouterTest(t, backend.name)
	syncErr := &submitRPCError{
		kind:   submitErrorSemantic,
		reason: "node-syncing",
		err:    fmt.Errorf("node is still syncing"),
	}

	router.recordSubmitResult(backend, "ok", time.Millisecond, nil)
	router.recordSubmitResult(backend, "ok", time.Millisecond, syncErr)
	if job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("first tip-chase node-syncing submit should preserve current work")
	}

	router.recordSubmitResult(backend, "ok", time.Millisecond, syncErr)
	if job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("repeated tip-chase node-syncing submit should keep current work")
	}
	state := backend.snapshotState()
	if !state.healthy || !strings.Contains(state.reason, "advisory-node-syncing") {
		t.Fatalf("state = %#v, want advisory node-syncing", state)
	}
	if held, _ := submitReadinessHoldActive(state, time.Now()); held {
		t.Fatalf("submitReadinessHoldActive() held, want no hold during tip-chase advisory")
	}
}

func TestNodeHealthTransportPreservesFreshCurrentJobs(t *testing.T) {
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
	job := seedFreshCurrentJobForRouterTest(t, backend.name)

	backend.mu.Lock()
	backend.state.healthy = true
	backend.state.score = 100
	backend.state.reason = "ok"
	backend.state.lastTemplateAt = time.Now()
	backend.mu.Unlock()

	router.recordNodeTemplateHealthError(backend, &nodeHealthRPCError{
		reason: "transport",
		err:    errors.New("dial tcp: i/o timeout"),
	})

	if job.stale.Load() || job.invalidated.Load() || jobInvalidatedByEpoch(job) {
		t.Fatal("fresh current jobs should be preserved during transient node health transport errors")
	}
	state := backend.snapshotState()
	if !state.healthy || state.reason != "node-health-advisory-transport" {
		t.Fatalf("state = %#v, want advisory transport with backend still usable", state)
	}
}

func TestNodeHealthP2PNotCurrentInvalidatesFreshCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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
	job := seedFreshCurrentJobForRouterTest(t, backend.name)

	cleanTemplateRefreshMu.Lock()
	startSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()

	router.invalidateBackendJobsFromNodeHealth(backend, "node health p2p unready: p2p-not-current")
	waitForCleanRefreshIdle(t)

	if !job.invalidated.Load() && !jobInvalidatedByEpoch(job) {
		t.Fatal("p2p-not-current node health should invalidate current jobs")
	}
	cleanTemplateRefreshMu.Lock()
	gotSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()
	if gotSeq == startSeq {
		t.Fatalf("clean refresh seq = %d, want refresh", gotSeq)
	}
}

func TestNodeHealthP2PNotCurrentRefreshesAfterParentAdvance(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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
	seedFreshCurrentJobForRouterTest(t, backend.name)
	miningWork.rememberParent(strings.Repeat("c", 64))

	cleanTemplateRefreshMu.Lock()
	startSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()

	router.invalidateBackendJobsFromNodeHealth(backend, "node health p2p unready: p2p-not-current")
	waitForCleanRefreshIdle(t)

	cleanTemplateRefreshMu.Lock()
	gotSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()
	if gotSeq == startSeq {
		t.Fatalf("clean refresh seq = %d, want refresh after parent advance", gotSeq)
	}
}

func TestNodeHealthNodeSyncingInvalidatesFreshCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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
	job := seedFreshCurrentJobForRouterTest(t, backend.name)

	cleanTemplateRefreshMu.Lock()
	startSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()

	router.invalidateBackendJobsFromNodeHealth(backend, "submit backend unready: node-syncing")
	waitForCleanRefreshIdle(t)

	if !job.invalidated.Load() && !jobInvalidatedByEpoch(job) {
		t.Fatal("node-syncing node health should invalidate current jobs")
	}
	cleanTemplateRefreshMu.Lock()
	gotSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()
	if gotSeq == startSeq {
		t.Fatalf("clean refresh seq = %d, want refresh", gotSeq)
	}
}

func TestNodeHealthNodeSyncingRefreshesAfterParentAdvance(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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
	seedFreshCurrentJobForRouterTest(t, backend.name)
	miningWork.rememberParent(strings.Repeat("c", 64))

	cleanTemplateRefreshMu.Lock()
	startSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()

	router.invalidateBackendJobsFromNodeHealth(backend, "submit backend unready: node-syncing")
	waitForCleanRefreshIdle(t)

	cleanTemplateRefreshMu.Lock()
	gotSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()
	if gotSeq == startSeq {
		t.Fatalf("clean refresh seq = %d, want refresh after parent advance", gotSeq)
	}
}

func TestNodeTemplateSubmitObsoleteDepthRefreshesCurrentJobs(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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
	job := seedFreshCurrentJobForRouterTest(t, backend.name)

	cleanTemplateRefreshMu.Lock()
	startSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()

	router.handleNodeTemplateInvalidation(backend, 99, "submit-obsolete-depth", "submit_obsolete_depth", "")

	waitForCleanRefreshIdle(t)

	if !jobInvalidatedByEpoch(job) {
		t.Fatal("current jobs should be invalidated after submit-obsolete-depth invalidation")
	}
	cleanTemplateRefreshMu.Lock()
	gotSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()
	if gotSeq == startSeq {
		t.Fatalf("clean refresh seq = %d, want refresh", gotSeq)
	}
}

func TestShouldHandleNodeTemplateInvalidationBaselinesFirstObservation(t *testing.T) {
	tests := []struct {
		name              string
		observed          bool
		previous, current uint64
		cause             string
		want              bool
	}{
		{
			name:     "historical invalidation on first probe",
			observed: false,
			current:  91,
			cause:    "parent-changed",
			want:     false,
		},
		{
			name:     "first live transition from zero",
			observed: true,
			current:  1,
			cause:    "parent-changed",
			want:     true,
		},
		{
			name:     "later live transition",
			observed: true,
			previous: 91,
			current:  92,
			cause:    "state-changed",
			want:     true,
		},
		{
			name:     "unchanged sequence",
			observed: true,
			previous: 92,
			current:  92,
			cause:    "state-changed",
			want:     false,
		},
		{
			name:     "non-actionable cause",
			observed: true,
			previous: 92,
			current:  93,
			cause:    "manual-refresh",
			want:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldHandleNodeTemplateInvalidation(test.observed, test.previous, test.current, test.cause); got != test.want {
				t.Fatalf("shouldHandleNodeTemplateInvalidation() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRecordNodeTemplateHealthDoesNotReplayHistoricalInvalidation(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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

	previousFetcher := refreshBlockTemplateFetcher
	refreshBlockTemplateFetcher = func() (*BlockTemplate, error) {
		return nil, errors.New("unexpected startup template refresh")
	}
	t.Cleanup(func() {
		waitForCleanRefreshIdle(t)
		waitForCleanRefreshWorkersIdle(t)
		refreshBlockTemplateFetcher = previousFetcher
	})

	cleanTemplateRefreshMu.Lock()
	startSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()

	router.recordNodeTemplateHealth(backend, &nodeTemplateHealthResult{
		ReasonCode:                     "ok",
		MineableNow:                    true,
		SubmitReady:                    true,
		TemplateAvailable:              true,
		TemplateUsable:                 true,
		TemplateAgeMs:                  100,
		LatestTemplateSeq:              500,
		LastTemplateInvalidationSeq:    91,
		LastTemplateInvalidationCause:  "parent_changed",
		LastTemplateInvalidationReason: "parent changed before pool startup",
	}, 10*time.Millisecond)

	cleanTemplateRefreshMu.Lock()
	gotSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()
	if gotSeq != startSeq {
		t.Fatalf("clean refresh seq = %d, want unchanged %d for first health baseline", gotSeq, startSeq)
	}
	state := backend.snapshotState()
	if state.nodeHealthLastTemplateInvalidationSeq != 91 {
		t.Fatalf("recorded invalidation seq = %d, want baseline 91", state.nodeHealthLastTemplateInvalidationSeq)
	}
}

func TestNodeTemplateParentChangedInvalidatesFreshCurrentJobsWhenParentAlreadyCurrent(t *testing.T) {
	resetCleanTemplateRefreshTestState(t)
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
	job := seedFreshCurrentJobForRouterTest(t, backend.name)

	cleanTemplateRefreshMu.Lock()
	startSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()

	router.handleNodeTemplateInvalidation(backend, 100, "parent-changed", "parent_changed", "0x"+job.ParentHash)

	if !jobInvalidatedByEpoch(job) {
		t.Fatal("current jobs should be invalidated after parent-changed invalidation")
	}
	cleanTemplateRefreshMu.Lock()
	gotSeq := cleanTemplateRefreshSeq
	cleanTemplateRefreshMu.Unlock()
	if gotSeq == startSeq {
		t.Fatalf("clean refresh seq = %d, want refresh", gotSeq)
	}
}

func seedFreshCurrentJobForRouterTest(t *testing.T, backendName string) *JobData {
	t.Helper()

	previousClients := snapshotClients()
	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousCurrentParent := CurrentParent()
	parentMu.Lock()
	previousLastParent := lastParent
	parentMu.Unlock()
	previousInvalidationEpoch := acceptedJobInvalidationEpoch.Load()
	previousBackendEpochs := snapshotBackendJobInvalidationEpochs()
	previousLastInvalidation := lastJobInvalidationUnixNano.Load()

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
		acceptedJobInvalidationEpoch.Store(previousInvalidationEpoch)
		restoreBackendJobInvalidationEpochs(previousBackendEpochs)
		lastJobInvalidationUnixNano.Store(previousLastInvalidation)
	})

	parent := strings.Repeat("b", 64)
	miningWork.rememberParent(parent)
	job := &JobData{
		BackendName:              backendName,
		ParentHash:               parent,
		CreatedAt:                time.Now(),
		InvalidationEpoch:        miningWork.currentInvalidationEpoch(),
		BackendInvalidationEpoch: miningWork.backendInvalidationEpoch(backendName),
	}
	c := &client{authorized: true}
	c.currentJob.Store(&clientCurrentJob{id: "job-1", job: job})
	clientsMu.Lock()
	clients = map[*client]struct{}{c: {}}
	clientsMu.Unlock()
	acceptedJobs.Store("job-1", job)
	return job
}

func resetRouterTestState(t *testing.T) {
	t.Helper()

	waitForCleanRefreshIdle(t)
	waitForCleanRefreshWorkersIdle(t)

	oldRouter := activeRPCRouter
	oldRPCURL := rpcURL
	oldRPCUser := rpcUser
	oldRPCPassword := rpcPassword
	oldPoolAddress := poolAddress
	oldSubmitBlockHeaderV2Enabled := submitBlockHeaderV2Enabled
	oldLegacySubmitV2Probed := legacySubmitV2Probed.Load()
	oldLegacySubmitV2Unsupported := legacySubmitV2Unsupported.Load()

	activeRPCRouter = nil
	rpcURL = ""
	rpcUser = ""
	rpcPassword = ""
	poolAddress = common.HexToAddress(testPoolCoinbase).Hex()
	rememberVerifiedTemplateCoinbase(poolAddress)
	submitBlockHeaderV2Enabled = false
	legacySubmitV2Probed.Store(false)
	legacySubmitV2Unsupported.Store(false)
	lastGBTRequestMu.Lock()
	lastGBTRequest = time.Time{}
	lastCachedTemplate = nil
	lastCachedBackendName = ""
	templateFetchInFlight = false
	templateFetchBackoffUntil = time.Time{}
	templateFetchBackoffReason = ""
	templateFetchConsecutive = 0
	lastGBTRequestMu.Unlock()
	mainChainHeightMu.Lock()
	mainChainHeightBackoffUntil = time.Time{}
	mainChainHeightBackoffReason = ""
	mainChainHeightConsecutive = 0
	mainChainHeightMu.Unlock()

	t.Cleanup(func() {
		waitForCleanRefreshIdle(t)
		waitForCleanRefreshWorkersIdle(t)
		activeRPCRouter = oldRouter
		rpcURL = oldRPCURL
		rpcUser = oldRPCUser
		rpcPassword = oldRPCPassword
		poolAddress = oldPoolAddress
		submitBlockHeaderV2Enabled = oldSubmitBlockHeaderV2Enabled
		legacySubmitV2Probed.Store(oldLegacySubmitV2Probed)
		legacySubmitV2Unsupported.Store(oldLegacySubmitV2Unsupported)
		lastGBTRequestMu.Lock()
		lastGBTRequest = time.Time{}
		lastCachedTemplate = nil
		lastCachedBackendName = ""
		templateFetchInFlight = false
		templateFetchBackoffUntil = time.Time{}
		templateFetchBackoffReason = ""
		templateFetchConsecutive = 0
		lastGBTRequestMu.Unlock()
		mainChainHeightMu.Lock()
		mainChainHeightBackoffUntil = time.Time{}
		mainChainHeightBackoffReason = ""
		mainChainHeightConsecutive = 0
		mainChainHeightMu.Unlock()
	})
}

func newTestRPCRouterFromConfig(t *testing.T, cfg Config) (*rpcRouter, error) {
	t.Helper()
	router, err := newRPCRouterFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	// Router tests that predate the Core identity contract exercise downstream
	// readiness behavior. Mark their synthetic backend as already preflighted;
	// chain_identity_test.go covers the fail-closed unverified state directly.
	backend := router.selectedBackend()
	backend.mu.Lock()
	backend.state.chainIdentity = testMainnetChainIdentityV1()
	backend.state.chainIdentityPinned = true
	backend.state.chainIdentityVerified = true
	backend.state.lastChainIdentityAt = time.Now()
	backend.mu.Unlock()
	return router, nil
}

func healthyEVMRPCServer(t *testing.T) *httptest.Server {
	t.Helper()
	return jsonRPCServer(t, func(method string) interface{} {
		switch method {
		case "eth_syncing":
			return false
		case "eth_blockNumber":
			return "0x64"
		default:
			return nil
		}
	})
}

func jsonRPCServer(t *testing.T, result func(method string) interface{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  result(request.Method),
		})
	}))
}

func boolPtr(v bool) *bool {
	return &v
}

func blockTemplateRPCResponse(height int64, parent string) string {
	return blockTemplateRPCResponseAt(height, parent, 1760000000)
}

func blockTemplateRPCResponseWithSeq(height int64, parent string, seq uint64) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"seq":%d,"version":1,"parents":[],"txroot":"%064x","stateroot":"%064x","pow_diff_reference":{"nbits":"1d00ffff"},"curtime":1760000000,"previousblockhash":%q,"height":%d,"coinbase_address":%q,"reward":1,"block_fees":0},"error":null}`,
		seq, 1, 2, parent, height, testPoolCoinbase)
}

func blockTemplateRPCResponseAt(height int64, parent string, curtime int64) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"version":1,"parents":[],"txroot":"%064x","stateroot":"%064x","pow_diff_reference":{"nbits":"1d00ffff"},"curtime":%d,"previousblockhash":%q,"height":%d,"coinbase_address":%q,"reward":1,"block_fees":0},"error":null}`,
		1, 2, curtime, parent, height, testPoolCoinbase)
}
