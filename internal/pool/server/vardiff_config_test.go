package server

import (
	"math"
	"testing"
	"time"
)

func TestApplyConfigSetsStartingPdiff(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()

	applyConfig(Config{StartingPdiff: 0.075})

	c := &client{}
	changed, pdiff, _ := c.recordShareAndAdjust(time.Now())
	if changed {
		t.Fatal("first share should not immediately retarget")
	}
	if math.Abs(pdiff-0.075) > 0.0000001 {
		t.Fatalf("expected configured starting pdiff, got %.8f", pdiff)
	}
	if math.Abs(c.pdiff-0.075) > 0.0000001 {
		t.Fatalf("client pdiff was not bootstrapped from configured value: %.8f", c.pdiff)
	}
}

func TestDefaultStartingPdiffIsIndependentFromRecoveryFloor(t *testing.T) {
	if defaultStartingPdiff != 0.05 {
		t.Fatalf("default starting pdiff = %.8f, want 0.05", defaultStartingPdiff)
	}
	if defaultMinPdiff != 0.0001 {
		t.Fatalf("default minimum pdiff = %.8f, want 0.0001", defaultMinPdiff)
	}
	if defaultStartingPdiff <= defaultMinPdiff {
		t.Fatalf("starting pdiff %.8f must remain above recovery floor %.8f", defaultStartingPdiff, defaultMinPdiff)
	}
}

func TestApplyConfigClampsStartingPdiff(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()

	applyConfig(Config{StartingPdiff: maxPdiff * 10})

	if startingPdiff != maxPdiff {
		t.Fatalf("expected starting pdiff to clamp to max, got %.8f", startingPdiff)
	}
}

func TestApplyConfigSetsMinPdiff(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()

	applyConfig(Config{MinPdiff: 0.08, StartingPdiff: 0.01})

	if math.Abs(minPdiff-0.08) > 0.0000001 {
		t.Fatalf("expected configured min pdiff, got %.8f", minPdiff)
	}
	if math.Abs(startingPdiff-0.08) > 0.0000001 {
		t.Fatalf("starting pdiff should clamp to configured min, got %.8f", startingPdiff)
	}
}

func TestApplyConfigSetsVarDiffTargetSharesPerMinute(t *testing.T) {
	previousTarget := vardiffTargetSharesMilli.Load()
	previousInit := vardiffTargetSharesInitOnce.Load()
	t.Cleanup(func() {
		vardiffTargetSharesMilli.Store(previousTarget)
		vardiffTargetSharesInitOnce.Store(previousInit)
	})

	applyConfig(Config{VarDiffTargetSharesPerMinute: 8})

	if got := desiredSharesPerWindow(); math.Abs(got-8) > 0.0000001 {
		t.Fatalf("desiredSharesPerWindow() = %.8f, want 8", got)
	}
}

func TestSetVarDiffTargetSharesPerMinuteClamps(t *testing.T) {
	previousTarget := vardiffTargetSharesMilli.Load()
	previousInit := vardiffTargetSharesInitOnce.Load()
	t.Cleanup(func() {
		vardiffTargetSharesMilli.Store(previousTarget)
		vardiffTargetSharesInitOnce.Store(previousInit)
	})

	setVarDiffTargetSharesPerMinute(1000)
	if got := desiredSharesPerWindow(); got != maxSharesPerWindow {
		t.Fatalf("desiredSharesPerWindow() = %.8f, want max %.8f", got, maxSharesPerWindow)
	}
}

func TestVarDiffDoesNotDropBelowMinPdiff(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()
	setMinPdiff(0.05)

	now := time.Unix(100, 0)
	c := &client{
		pdiff:       0.06,
		windowStart: now.Add(-windowSize),
	}

	changed, newPdiff, _ := c.adjustOnTimer(now)
	if !changed {
		t.Fatal("expected no-share timer to lower difficulty to the floor")
	}
	if math.Abs(newPdiff-0.05) > 0.0000001 {
		t.Fatalf("expected pdiff floor 0.05, got %.8f", newPdiff)
	}
}

func TestVarDiffTimerCanRecoverBelowStartingPdiff(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()
	startingPdiff = defaultStartingPdiff
	minPdiff = defaultMinPdiff

	now := time.Unix(100, 0)
	c := &client{pdiff: startingPdiff, windowStart: now.Add(-windowSize)}
	changed, newPdiff, _ := c.adjustOnTimer(now)
	if !changed {
		t.Fatal("expected no-share timer to lower difficulty")
	}
	if newPdiff >= startingPdiff || newPdiff <= minPdiff {
		t.Fatalf("timer pdiff = %.8f, want between floor %.8f and start %.8f", newPdiff, minPdiff, startingPdiff)
	}
}

func TestLowDifficultyRecoveryStepsCurrentLaneToFloor(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()
	startingPdiff = defaultStartingPdiff
	minPdiff = defaultMinPdiff

	now := time.Unix(100, 0)
	c := &client{addr: "miner", pdiff: startingPdiff}
	assigned := startingPdiff
	step := func(start time.Time, want float64) time.Time {
		for i := 0; i < lowDiffRecoveryMinRejects-1; i++ {
			if changed, got := c.recordLowDifficultyAndAdjust(start.Add(time.Duration(i)*time.Second), assigned); changed {
				t.Fatalf("recovered before evidence threshold: pdiff %.8f", got)
			}
		}
		at := start.Add(time.Duration(lowDiffRecoveryMinRejects-1) * time.Second)
		changed, got := c.recordLowDifficultyAndAdjust(at, assigned)
		if !changed || math.Abs(got-want) > 0.0000001 {
			t.Fatalf("recovery from %.8f = changed=%t pdiff=%.8f, want %.8f", assigned, changed, got, want)
		}
		if math.Abs(c.prevPdiff-assigned) > 0.0000001 {
			t.Fatalf("previous pdiff = %.8f, want %.8f", c.prevPdiff, assigned)
		}
		assigned = got
		return at.Add(minFastRetargetInterval)
	}

	now = step(now, 0.005)
	if changed, got := c.recordLowDifficultyAndAdjust(now, startingPdiff); changed || !almostEqual(got, 0.005) {
		t.Fatalf("old-job difficulty caused recovery cascade: changed=%t pdiff=%.8f", changed, got)
	}
	now = step(now.Add(time.Second), 0.0005)
	_ = step(now.Add(time.Second), minPdiff)
	if changed, got := c.recordLowDifficultyAndAdjust(now.Add(time.Minute), minPdiff); changed || !almostEqual(got, minPdiff) {
		t.Fatalf("recovery moved below floor: changed=%t pdiff=%.8f", changed, got)
	}
}

func TestVarDiffExpiredRejectsRaiseInsteadOfLowering(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()
	setMinPdiff(0.05)

	now := time.Unix(100, 0)
	c := &client{
		pdiff:             0.08,
		windowStart:       now.Add(-windowSize),
		expiredJobRejects: 4,
	}

	changed, newPdiff, _ := c.adjustOnTimer(now)
	if !changed {
		t.Fatal("expected expired-job anti-spiral to retarget")
	}
	if newPdiff <= 0.08 {
		t.Fatalf("expected expired rejects to raise/hold, got %.8f", newPdiff)
	}
	if c.expiredJobRejects != 0 {
		t.Fatal("expected expired reject counter to reset after retarget")
	}
}

func TestVarDiffExpiredRejectsDominatingSharesHoldDifficulty(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{
		pdiff:             0.08,
		windowStart:       now.Add(-windowSize),
		windowShares:      1,
		expiredJobRejects: 4,
	}

	changed, newPdiff, _ := c.adjustOnTimer(now)
	if changed {
		t.Fatal("expected expired rejects dominating useful shares to hold difficulty")
	}
	if math.Abs(newPdiff-0.08) > 0.0000001 {
		t.Fatalf("expected pdiff hold at 0.08, got %.8f", newPdiff)
	}
}

func TestRecordExpiredJobRejectInitializesVarDiffWindow(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{}

	c.recordExpiredJobReject(now)

	if c.windowStart != now {
		t.Fatalf("expected expired reject to initialize window start, got %v", c.windowStart)
	}
	if c.expiredJobRejects != 1 {
		t.Fatalf("expected one expired reject, got %d", c.expiredJobRejects)
	}
}

func TestRecordExpiredJobRejectFeedsTimerAntiSpiral(t *testing.T) {
	originalStart := startingPdiff
	originalMin := minPdiff
	defer func() {
		startingPdiff = originalStart
		minPdiff = originalMin
	}()
	setMinPdiff(0.05)

	now := time.Unix(100, 0)
	c := &client{pdiff: 0.08}
	c.recordExpiredJobReject(now)

	changed, newPdiff, _ := c.adjustOnTimer(now.Add(windowSize + time.Second))
	if !changed {
		t.Fatal("expected expired reject signal to trigger anti-spiral retarget")
	}
	if newPdiff <= 0.08 {
		t.Fatalf("expected expired reject signal to raise difficulty, got %.8f", newPdiff)
	}
	if c.expiredJobRejects != 0 {
		t.Fatal("expected expired reject counter to reset after retarget")
	}
}
