package server

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type runtimeTimingTestState struct {
	ageOverrideEnabled   bool
	ageOverrideMS        int64
	ttlOverride          int64
	allowOverrideEnabled bool
	allowOverrideValue   bool
	staleGraceEnabled    bool
	staleGraceMS         int64
	jobAgeGauge          prometheus.Gauge
	templateTTLGauge     prometheus.Gauge
	staleGraceGauge      prometheus.Gauge
}

func saveRuntimeTimingTestState(t *testing.T) {
	t.Helper()
	state := runtimeTimingTestState{
		ageOverrideEnabled:   blockCandidateJobAgeOverrideEnabled.Load(),
		ageOverrideMS:        blockCandidateJobAgeOverrideMS.Load(),
		ttlOverride:          templateTTLRefreshOverride.Load(),
		allowOverrideEnabled: allowMultipleBlockOverrideEnabled.Load(),
		allowOverrideValue:   allowMultipleBlockOverrideValue.Load(),
		staleGraceEnabled:    recentStaleGraceOverrideEnabled.Load(),
		staleGraceMS:         recentStaleGraceOverrideMS.Load(),
		jobAgeGauge:          blockTimingControllerJobAgeMS,
		templateTTLGauge:     blockTimingControllerTemplateTTLMS,
		staleGraceGauge:      blockTimingControllerStaleGraceMS,
	}
	t.Cleanup(func() {
		blockCandidateJobAgeOverrideMS.Store(state.ageOverrideMS)
		blockCandidateJobAgeOverrideEnabled.Store(state.ageOverrideEnabled)
		templateTTLRefreshOverride.Store(state.ttlOverride)
		allowMultipleBlockOverrideValue.Store(state.allowOverrideValue)
		allowMultipleBlockOverrideEnabled.Store(state.allowOverrideEnabled)
		recentStaleGraceOverrideMS.Store(state.staleGraceMS)
		recentStaleGraceOverrideEnabled.Store(state.staleGraceEnabled)
		blockTimingControllerJobAgeMS = state.jobAgeGauge
		blockTimingControllerTemplateTTLMS = state.templateTTLGauge
		blockTimingControllerStaleGraceMS = state.staleGraceGauge
	})
}

func testBlockTimingController(defaultAge time.Duration) *blockTimingController {
	return newBlockTimingController(blockTimingControllerConfig{
		enabled:                 true,
		window:                  10 * time.Second,
		minCandidates:           5,
		targetWaste:             5,
		minJobAge:               250 * time.Millisecond,
		maxJobAge:               2 * time.Second,
		minTTL:                  100 * time.Millisecond,
		maxTTL:                  time.Second,
		step:                    250 * time.Millisecond,
		cooldown:                0,
		defaultJobAge:           defaultAge,
		defaultRecentStaleGrace: 750 * time.Millisecond,
		maxRecentStaleGrace:     750 * time.Millisecond,
	})
}

func TestBlockTimingControllerTightensOnTipOverdueWaste(t *testing.T) {
	saveRuntimeTimingTestState(t)
	controller := testBlockTimingController(time.Second)
	setBlockCandidateJobAgeOverride(time.Second)
	setTemplateTTLRefreshOverride(500 * time.Millisecond)
	setAllowMultipleBlockCandidatesOverride(false, true)

	now := time.Unix(100, 0)
	controller.recordAccepted(now, 4)
	for i := 0; i < 4; i++ {
		controller.recordFailure(now.Add(11*time.Second+time.Duration(i)*time.Millisecond), "tip-overdue", 4)
	}

	if got := maxBlockCandidateJobAge(nil); got != 750*time.Millisecond {
		t.Fatalf("job age = %s, want 750ms", got)
	}
	if got := configuredTemplateTTLRefresh(); got != 250*time.Millisecond {
		t.Fatalf("template TTL = %s, want 250ms", got)
	}
	if !shouldAllowMultipleBlockCandidatesPerJob(nil) {
		t.Fatal("timing controller should keep multiple block candidates enabled")
	}
}

func TestBlockTimingControllerRelaxesOnLocalExpiryWaste(t *testing.T) {
	saveRuntimeTimingTestState(t)
	controller := testBlockTimingController(500 * time.Millisecond)
	setBlockCandidateJobAgeOverride(500 * time.Millisecond)
	setTemplateTTLRefreshOverride(250 * time.Millisecond)

	now := time.Unix(200, 0)
	controller.recordAccepted(now, 4)
	for i := 0; i < 4; i++ {
		controller.recordFailure(now.Add(11*time.Second+time.Duration(i)*time.Millisecond), "expired", 4)
	}

	if got := maxBlockCandidateJobAge(nil); got != 750*time.Millisecond {
		t.Fatalf("job age = %s, want 750ms", got)
	}
	if got := configuredTemplateTTLRefresh(); got != 250*time.Millisecond {
		t.Fatalf("template TTL = %s, want unchanged 250ms", got)
	}
}

func TestBlockTimingControllerHoldsDuringNodeSyncing(t *testing.T) {
	saveRuntimeTimingTestState(t)
	controller := testBlockTimingController(time.Second)
	setBlockCandidateJobAgeOverride(time.Second)
	setTemplateTTLRefreshOverride(500 * time.Millisecond)

	now := time.Unix(300, 0)
	controller.recordAccepted(now, 4)
	for i := 0; i < 4; i++ {
		controller.recordFailure(now.Add(11*time.Second+time.Duration(i)*time.Millisecond), "node-syncing", 4)
	}

	if got := maxBlockCandidateJobAge(nil); got != time.Second {
		t.Fatalf("job age = %s, want unchanged 1s", got)
	}
	if got := configuredTemplateTTLRefresh(); got != 500*time.Millisecond {
		t.Fatalf("template TTL = %s, want unchanged 500ms", got)
	}
}

func TestBlockTimingControllerWithinTargetDoesNotDeadlock(t *testing.T) {
	saveRuntimeTimingTestState(t)
	controller := testBlockTimingController(time.Second)
	blockTimingControllerJobAgeMS = prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_block_timing_job_age_ms"})
	blockTimingControllerTemplateTTLMS = prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_block_timing_template_ttl_ms"})
	setBlockCandidateJobAgeOverride(250 * time.Millisecond)
	setTemplateTTLRefreshOverride(100 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		now := time.Unix(400, 0)
		for i := 0; i < 4; i++ {
			controller.recordAccepted(now.Add(time.Duration(i)*time.Millisecond), 4)
		}
		controller.recordAccepted(now.Add(11*time.Second), 4)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("within-target timing window deadlocked")
	}
}

func TestBlockTimingControllerReducesRecentStaleGraceOnBackendRisk(t *testing.T) {
	saveRuntimeTimingTestState(t)
	controller := testBlockTimingController(time.Second)
	setRecentStaleBlockCandidateSubmitGraceOverride(750*time.Millisecond, true)

	now := time.Unix(500, 0)
	controller.recordAccepted(now, 4)
	for i := 0; i < 4; i++ {
		controller.recordFailure(now.Add(11*time.Second+time.Duration(i)*time.Millisecond), "tip-overdue", 4)
	}

	if got := currentRecentStaleBlockCandidateSubmitGrace(&Config{RecentStaleBlockCandidateSubmitGraceMS: 750}); got != 500*time.Millisecond {
		t.Fatalf("recent stale grace = %s, want 500ms", got)
	}
}

func TestBlockTimingControllerKeepsRecentStaleGraceFloor(t *testing.T) {
	saveRuntimeTimingTestState(t)
	controller := newBlockTimingController(blockTimingControllerConfig{
		enabled:                 true,
		window:                  10 * time.Second,
		minCandidates:           5,
		targetWaste:             5,
		minJobAge:               250 * time.Millisecond,
		maxJobAge:               2 * time.Second,
		minTTL:                  100 * time.Millisecond,
		maxTTL:                  time.Second,
		step:                    250 * time.Millisecond,
		cooldown:                0,
		defaultJobAge:           time.Second,
		defaultRecentStaleGrace: 250 * time.Millisecond,
		maxRecentStaleGrace:     250 * time.Millisecond,
	})
	setRecentStaleBlockCandidateSubmitGraceOverride(250*time.Millisecond, true)

	now := time.Unix(550, 0)
	controller.recordAccepted(now, 4)
	for i := 0; i < 4; i++ {
		controller.recordFailure(now.Add(11*time.Second+time.Duration(i)*time.Millisecond), "tip-overdue", 4)
	}

	if got := currentRecentStaleBlockCandidateSubmitGrace(&Config{RecentStaleBlockCandidateSubmitGraceMS: 250}); got != 250*time.Millisecond {
		t.Fatalf("recent stale grace = %s, want floor 250ms", got)
	}
}

func TestBlockTimingControllerRestoresRecentStaleGraceForLocalRace(t *testing.T) {
	saveRuntimeTimingTestState(t)
	controller := testBlockTimingController(time.Second)
	setRecentStaleBlockCandidateSubmitGraceOverride(250*time.Millisecond, true)

	now := time.Unix(600, 0)
	controller.recordAccepted(now, 4)
	for i := 0; i < 4; i++ {
		controller.recordFailure(now.Add(11*time.Second+time.Duration(i)*time.Millisecond), "old-template-sequence", 4)
	}

	if got := currentRecentStaleBlockCandidateSubmitGrace(&Config{RecentStaleBlockCandidateSubmitGraceMS: 750}); got != 500*time.Millisecond {
		t.Fatalf("recent stale grace = %s, want 500ms", got)
	}
}
