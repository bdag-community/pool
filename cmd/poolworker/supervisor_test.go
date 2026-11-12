package main

import (
	"testing"
	"time"
)

func testThresholds() thresholds {
	return thresholds{
		driftTimeout:           45 * time.Second,
		poolUnreachableTimeout: 60 * time.Second,
		stratumDownTimeout:     60 * time.Second,
	}
}

// The rule that keeps two supervisors from fighting over one outage: if the
// node cannot be mined against, restarting the pool only drops the miners. The
// node's own supervisor owns that failure.
func TestNodeUnhealthyNeverRestartsPool(t *testing.T) {
	for _, tc := range []struct {
		name string
		st   probeState
	}{
		{"node unreachable", probeState{nodeReachable: false, poolParentAge: time.Hour, stratumDownFor: time.Hour}},
		{"node not mineable", probeState{nodeReachable: true, nodeMineable: false, poolParentAge: time.Hour}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if restart, reason := wedgeDecision(tc.st, testThresholds()); restart {
				t.Fatalf("restarted the pool for a node problem: %s", reason)
			}
		})
	}
}

// Template drift must NEVER restart the pool. The pool's current_parent lags its
// live broadcast parent, so drift measured against it is a false positive; the
// ws-coalescing fix prevents the real backlog at the source. Restarting on drift
// only dropped miners in a loop.
func TestDriftNeverRestarts(t *testing.T) {
	st := probeState{
		nodeReachable: true,
		nodeMineable:  true,
		poolReachable: true,
		stratumOK:     true,
		poolParentAge: time.Hour, // absurd drift
	}
	if restart, reason := wedgeDecision(st, testThresholds()); restart {
		t.Fatalf("drift must not restart the pool, got: %s", reason)
	}
}

func TestDriftWithinTimeoutDoesNotRestart(t *testing.T) {
	st := probeState{
		nodeReachable: true,
		nodeMineable:  true,
		poolReachable: true,
		stratumOK:     true,
		poolParentAge: 44 * time.Second,
	}
	if restart, reason := wedgeDecision(st, testThresholds()); restart {
		t.Fatalf("restarted below the drift timeout: %s", reason)
	}
}

// Drift is only meaningful when we could read the pool's parent at all.
func TestDriftIgnoredWhenPoolUnreachable(t *testing.T) {
	st := probeState{
		nodeReachable:      true,
		nodeMineable:       true,
		poolReachable:      false,
		stratumOK:          true,
		poolParentAge:      time.Hour,
		poolUnreachableFor: 10 * time.Second,
	}
	if restart, reason := wedgeDecision(st, testThresholds()); restart {
		t.Fatalf("restarted on stale drift while the pool was unreachable: %s", reason)
	}
}

func TestSustainedFailuresRestartOnlyAfterTimeout(t *testing.T) {
	base := probeState{nodeReachable: true, nodeMineable: true, poolReachable: true, stratumOK: true}

	brief := base
	brief.stratumOK = false
	brief.stratumDownFor = 59 * time.Second
	if restart, _ := wedgeDecision(brief, testThresholds()); restart {
		t.Fatal("a briefly refused stratum listener must not restart the pool")
	}

	sustainedDown := base
	sustainedDown.stratumOK = false
	sustainedDown.stratumDownFor = 60 * time.Second
	if restart, _ := wedgeDecision(sustainedDown, testThresholds()); !restart {
		t.Fatal("a stratum listener down past the timeout must restart the pool")
	}

	poolDown := base
	poolDown.poolReachable = false
	poolDown.poolUnreachableFor = 61 * time.Second
	if restart, _ := wedgeDecision(poolDown, testThresholds()); !restart {
		t.Fatal("an unreachable pool past the timeout must restart it")
	}
}

// A single dropped probe must not accumulate toward a restart.
func TestSustainedResetsOnSuccess(t *testing.T) {
	var s sustained
	start := time.Now()

	if got := s.fail(start); got != 0 {
		t.Fatalf("first failure elapsed = %s, want 0", got)
	}
	if got := s.fail(start.Add(30 * time.Second)); got != 30*time.Second {
		t.Fatalf("sustained failure = %s, want 30s", got)
	}
	s.ok()
	if got := s.fail(start.Add(31 * time.Second)); got != 0 {
		t.Fatalf("after a success the counter must restart, got %s", got)
	}
}

// Restarting cures a wedge whose cause has passed. It cannot cure a defect that
// reproduces immediately — an unbounded template backlog re-wedges the moment
// the pool comes back. A fixed cooldown would restart forever, dropping the
// miners each time; the backoff makes the defect visible instead.
func TestCooldownBacksOffOnConsecutiveRestarts(t *testing.T) {
	base, ceiling := time.Minute, 30*time.Minute
	for _, tc := range []struct {
		consecutive int
		want        time.Duration
	}{
		{0, base}, // a zero streak still waits the base cooldown
		{1, time.Minute},
		{2, 2 * time.Minute},
		{3, 4 * time.Minute},
		{5, 16 * time.Minute},
		{6, ceiling},  // 32m would exceed the ceiling
		{40, ceiling}, // and it must not overflow on a long streak
	} {
		if got := cooldownAfterRestart(tc.consecutive, base, ceiling); got != tc.want {
			t.Fatalf("consecutive=%d cooldown = %s, want %s", tc.consecutive, got, tc.want)
		}
	}
}

func TestRestartStreakResetsAfterHealthyStretch(t *testing.T) {
	now := time.Now()
	healthy := 10 * time.Minute

	if got := restartStreak(0, time.Time{}, now, healthy); got != 1 {
		t.Fatalf("first ever restart streak = %d, want 1", got)
	}
	if got := restartStreak(3, now.Add(-time.Minute), now, healthy); got != 4 {
		t.Fatalf("a restart hard on the last must extend the streak, got %d", got)
	}
	if got := restartStreak(7, now.Add(-11*time.Minute), now, healthy); got != 1 {
		t.Fatalf("a restart after a healthy stretch must reset the streak, got %d", got)
	}
}

// The node's miner enables lazily on the first getBlockTemplate request, so a
// synced-but-unpoked node reports mineable_now=false, reason_code=miner_disabled.
// Gating the pool start on mineable_now would deadlock — the pool is what pokes
// the node. The gate must therefore start on get_block_template_ready.
func TestStartGateOnTemplateReadyNotMineable(t *testing.T) {
	// Synced, template-ready, miner not yet awake: must start.
	if !nodeReadyToStartPool(&nodeHealth{
		MineableNow:           false,
		GetBlockTemplateReady: true,
		ReasonCode:            "miner_disabled",
	}) {
		t.Fatal("a synced, template-ready node must not deadlock the pool start")
	}

	// Already mineable: must start.
	if !nodeReadyToStartPool(&nodeHealth{MineableNow: true, GetBlockTemplateReady: true}) {
		t.Fatal("a mineable node must start the pool")
	}

	// Still syncing: get_block_template_ready is false, so keep waiting.
	if nodeReadyToStartPool(&nodeHealth{
		MineableNow:           false,
		GetBlockTemplateReady: false,
		ReasonCode:            "node_syncing",
	}) {
		t.Fatal("a syncing node must not start the pool")
	}
}
