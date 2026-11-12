package server

import (
	"log"
	"math"
	"sync/atomic"
	"time"
)

const (
	windowSize                = 60 * time.Second // Slower adjustment for stability
	defaultSharesPerWindow    = 20.0             // Expect enough shares before adjusting.
	minSharesPerWindow        = 4.0
	maxSharesPerWindow        = 40.0
	toleranceFraction         = 0.5              // 50% tolerance
	noShareEasierFactor       = 0.3              // Drop 70% if no shares
	expiredRejectHarderFactor = 1.25             // Raise gently when expired jobs dominate
	stepExponent              = 0.2              // Very slow adjustments
	defaultMinPdiff           = 0.0001           // Recovery floor for low-target firmware
	defaultStartingPdiff      = 0.05             // Conservative target for new connections
	maxPdiff                  = 10000.0          // Allow difficulty to rise high for ASICs
	minFastRetargetInterval   = 10 * time.Second // Allow fast adjustment (e.g. for ASICs joining)
	lowDiffRecoveryWindow     = 30 * time.Second
	lowDiffRecoveryMinAge     = 5 * time.Second
	lowDiffRecoveryMinRejects = 12
	lowDiffRecoveryFactor     = 0.1
)

var (
	minPdiff                    = defaultMinPdiff
	startingPdiff               = defaultStartingPdiff
	vardiffTargetSharesMilli    atomic.Uint64
	vardiffTargetSharesInitOnce atomic.Bool
)

func setMinPdiff(value float64) {
	if value <= 0 {
		return
	}
	next := clamp(value, 0.0001, maxPdiff)
	if !almostEqual(minPdiff, next) {
		log.Printf("[vardiff] minimum pdiff configured %.8f", next)
	}
	minPdiff = next
	startingPdiff = clamp(startingPdiff, minPdiff, maxPdiff)
}

func setStartingPdiff(value float64) {
	if value <= 0 {
		return
	}
	next := clamp(value, minPdiff, maxPdiff)
	if !almostEqual(startingPdiff, next) {
		log.Printf("[vardiff] starting pdiff configured %.8f", next)
	}
	startingPdiff = next
}

func setVarDiffTargetSharesPerMinute(value float64) {
	if value <= 0 {
		return
	}
	next := clamp(value, minSharesPerWindow, maxSharesPerWindow)
	vardiffTargetSharesMilli.Store(uint64(next * 1000))
	log.Printf("[vardiff] target shares configured %.3f/min/client", next)
}

func desiredSharesPerWindow() float64 {
	if !vardiffTargetSharesInitOnce.Load() {
		if vardiffTargetSharesInitOnce.CompareAndSwap(false, true) {
			vardiffTargetSharesMilli.Store(uint64(defaultSharesPerWindow * 1000))
		}
	}
	base := float64(vardiffTargetSharesMilli.Load()) / 1000
	if base <= 0 {
		base = defaultSharesPerWindow
	}
	active := activeClientCount()
	if active > 2 {
		base *= math.Sqrt(2.0 / float64(active))
	}
	return clamp(base, minSharesPerWindow, maxSharesPerWindow)
}

// Called on each accepted share
// - increments window counter
// - if the window has elapsed, performs an adjustment immediately
func (c *client) recordShareAndAdjust(now time.Time) (changed bool, pdiff float64, avgInterval time.Duration) {
	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	// bootstrap
	if c.pdiff <= 0 {
		c.pdiff = startingPdiff
	}
	if c.windowStart.IsZero() {
		c.windowStart = now
	}

	c.windowShares++
	pdiff = c.pdiff

	// early exit if window not complete yet
	winDur := now.Sub(c.windowStart)
	targetShares := desiredSharesPerWindow()
	if winDur < windowSize {
		// FAST RETARGET: If we have WAY too many shares already, don't wait for the full window.
		// e.g. if we have > 2x desired shares, adjust NOW.
		// BUT: Respect a cooldown to let the miner catch up (avoid rejecting valid shares).
		if float64(c.windowShares) > targetShares*2 {
			if now.Sub(c.lastAdjust) >= minFastRetargetInterval {
				// Trigger adjustment immediately
				return c.adjustLocked(now, winDur)
			}
		}

		// Provide an interval hint for logs/dashboards
		if c.windowShares > 0 {
			avgInterval = time.Duration(float64(winDur) / float64(c.windowShares))
		}
		return false, pdiff, avgInterval
	}

	// Window completed → adjust
	changed, newP, avg := c.adjustLocked(now, winDur)
	return changed, newP, avg
}

// Called periodically even if NO shares arrived in the window.
// Use this from your broadcaster/timer loop.
func (c *client) adjustOnTimer(now time.Time) (changed bool, newPdiff float64, avgInterval time.Duration) {
	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	// Bootstrap: start a fresh window
	if c.windowStart.IsZero() {
		c.windowStart = now
		return false, c.pdiff, 0
	}

	winDur := now.Sub(c.windowStart)
	if winDur < windowSize {
		return false, c.pdiff, 0
	}
	return c.adjustLocked(now, winDur)
}

func (c *client) recordExpiredJobReject(now time.Time) {
	if c == nil {
		return
	}
	c.diffMu.Lock()
	defer c.diffMu.Unlock()
	if c.windowStart.IsZero() {
		c.windowStart = now
	}
	c.expiredJobRejects++
}

// recordLowDifficultyAndAdjust uses sustained, fully validated low-difficulty
// submissions as evidence that this physical lane cannot meet its current
// target. Only submissions for the lane's current difficulty count, so work
// that was already in flight before a retarget cannot cascade it to the floor.
func (c *client) recordLowDifficultyAndAdjust(now time.Time, assignedPdiff float64) (changed bool, newPdiff float64) {
	if c == nil || assignedPdiff <= 0 {
		return false, 0
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	old := c.pdiff
	if old <= 0 {
		old = startingPdiff
		c.pdiff = old
	}
	if !almostEqual(assignedPdiff, old) || old <= minPdiff ||
		(!c.lastLowDiffRecovery.IsZero() && now.Sub(c.lastLowDiffRecovery) < minFastRetargetInterval) {
		return false, old
	}

	if c.lowDiffRecoveryWindowStart.IsZero() || now.Before(c.lowDiffRecoveryWindowStart) ||
		now.Sub(c.lowDiffRecoveryWindowStart) > lowDiffRecoveryWindow {
		c.lowDiffRecoveryWindowStart = now
		c.lowDiffRecoveryRejects = 1
		return false, old
	}
	c.lowDiffRecoveryRejects++
	if c.lowDiffRecoveryRejects < lowDiffRecoveryMinRejects ||
		now.Sub(c.lowDiffRecoveryWindowStart) < lowDiffRecoveryMinAge {
		return false, old
	}

	newPdiff = clampRuntimePdiff(old * lowDiffRecoveryFactor)
	c.lowDiffRecoveryWindowStart = time.Time{}
	c.lowDiffRecoveryRejects = 0
	if almostEqual(newPdiff, old) {
		return false, old
	}

	c.prevPdiff = old
	c.pdiff = newPdiff
	c.windowShares = 0
	c.expiredJobRejects = 0
	c.windowStart = now
	c.lastAdjust = now
	c.lastLowDiffRecovery = now
	incVarDiffAdjustment("low-diff-recovery")
	log.Printf("[vardiff] %s low-difficulty recovery pdiff %.6f → %.6f after %d validated rejects",
		c.addr, old, newPdiff, lowDiffRecoveryMinRejects)
	return true, newPdiff
}

// Core adjust logic (expects diffMu held)
func (c *client) adjustLocked(now time.Time, winDur time.Duration) (changed bool, newPdiff float64, avgInterval time.Duration) {
	old := c.pdiff
	shares := float64(c.windowShares)
	expiredRejects := c.expiredJobRejects

	// Compute ratio against target
	var ratio float64
	if shares <= 0 {
		if expiredRejects > 0 {
			// Expired-job bursts mean the miner is submitting work for old jobs.
			// Lowering difficulty here amplifies the flood, so hold/raise gently.
			newPdiff = clampRuntimePdiff(math.Max(old, minPdiff) * expiredRejectHarderFactor)
		} else {
			// No shares and no stale-job signal: ease difficulty aggressively.
			newPdiff = clampRuntimePdiff(old * noShareEasierFactor)
		}
		ratio = 0.0
		avgInterval = 0
	} else {
		// Calculate expected shares for the actual window duration
		targetShares := desiredSharesPerWindow()
		expectedShares := targetShares * (winDur.Seconds() / windowSize.Seconds())
		ratio = shares / expectedShares
		// Deadband to avoid tiny oscillations
		lower := 1.0 - toleranceFraction
		upper := 1.0 + toleranceFraction

		if ratio < lower {
			if expiredRejects > int(shares) {
				// Expired rejects dominate useful shares. Treat this as a stale-job
				// synchronization problem, not proof that the miner needs easier work.
				newPdiff = old
				incVarDiffAdjustment("anti-spiral-hold")
			} else {
				// Too few shares → make easier
				scale := math.Pow(ratio, stepExponent) // < 1.0
				newPdiff = clampRuntimePdiff(old * scale)
			}
		} else if ratio > upper {
			// Too many shares → make harder
			scale := math.Pow(ratio, stepExponent) // > 1.0
			newPdiff = clampRuntimePdiff(old * scale)
		} else {
			// Within deadband → keep pdiff
			newPdiff = old
		}

		avgInterval = time.Duration(float64(winDur) / shares)
	}

	// Reset window
	c.windowShares = 0
	c.expiredJobRejects = 0
	c.lastAdjust = now
	c.windowStart = now

	if almostEqual(newPdiff, old) {
		log.Printf("[vardiff DEBUG] hold pdiff %.6f (shares=%.0f expired=%d in %ds, ratio=%.2f, lower=%.2f upper=%.2f)",
			old, shares, expiredRejects, int(winDur.Seconds()), ratio, 1.0-toleranceFraction, 1.0+toleranceFraction)
		incVarDiffAdjustment("hold")
		return false, old, avgInterval
	}

	c.prevPdiff = old
	c.pdiff = newPdiff
	c.lowDiffRecoveryWindowStart = time.Time{}
	c.lowDiffRecoveryRejects = 0

	dir := "hold"
	if newPdiff > old {
		dir = "increase"
	} else if newPdiff < old {
		dir = "decrease"
	}
	incVarDiffAdjustment(dir)
	log.Printf("[vardiff] %s %s pdiff %.6f → %.6f (shares=%.0f expired=%d in %ds, target=%.0f/%ds)",
		c.addr, dir, old, newPdiff, shares, expiredRejects, int(winDur.Seconds()),
		desiredSharesPerWindow(), int(windowSize.Seconds()))
	return true, newPdiff, avgInterval
}

func clampRuntimePdiff(value float64) float64 {
	if value < minPdiff {
		incVarDiffFloorHit()
	}
	return clamp(value, minPdiff, maxPdiff)
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func almostEqual(a, b float64) bool {
	if a == b {
		return true
	}
	// relative epsilon
	den := math.Max(math.Abs(a), math.Abs(b))
	if den == 0 {
		return false
	}
	return math.Abs(a-b)/den < 1e-3
}

func incVarDiffAdjustment(direction string) {
	if vardiffAdjustments != nil {
		vardiffAdjustments.WithLabelValues(direction).Inc()
	}
}

func incVarDiffFloorHit() {
	if vardiffFloorHits != nil {
		vardiffFloorHits.Inc()
	}
}
