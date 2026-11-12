package pplns

import (
	"math/big"
	"sync"
	"time"
)

// PPLNS_SCALE is the multiplier to convert float64 difficulty to fixed-point uint64 work units.
// Using 1e9 as recommended to preserve precision.
const PPLNS_SCALE = 1_000_000_000

// Share represents a single valid share execution.
type Share struct {
	Account   string
	Work      uint64 // Difficulty * 1e9
	Timestamp int64  // Unix nano
}

// CalculateCreditsWei is the discovery-time PPLNS allocation used by the
// wide accounting path.  It intentionally performs no float conversion: the
// remainder stays undistributed, exactly as the legacy path did.
func CalculateCreditsWei(workByAccount map[string]uint64, totalWork uint64, distributable *big.Int) map[string]*big.Int {
	credits := make(map[string]*big.Int, len(workByAccount))
	if totalWork == 0 || distributable == nil || distributable.Sign() < 0 {
		return credits
	}
	denominator := new(big.Int).SetUint64(totalWork)
	for account, work := range workByAccount {
		if work == 0 {
			continue
		}
		amount := new(big.Int).Mul(distributable, new(big.Int).SetUint64(work))
		amount.Quo(amount, denominator)
		credits[account] = amount
	}
	return credits
}

// Window manages the rolling window of PPLNS shares.
type Window struct {
	mu            sync.RWMutex
	shares        []Share
	head          int
	totalWork     uint64
	targetWork    uint64 // The 'N' in PPLNS (scaled)
	workByAccount map[string]uint64
}

// NewWindow creates a new PPLNS window with the specified target work (N).
// targetWork is the raw difficulty sum (e.g., 2000.0), which will be scaled internally.
func NewWindow(targetWork float64) *Window {
	return &Window{
		shares:        make([]Share, 0, 10000), // Pre-allocate some capacity
		targetWork:    uint64(targetWork * PPLNS_SCALE),
		workByAccount: make(map[string]uint64),
	}
}

// PushShare adds a new share to the window and evicts old ones if necessary.
// assignedPdiff should be the difficulty assigned to the miner (VarDiff).
func (w *Window) PushShare(account string, assignedPdiff float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pushShareLocked(account, assignedPdiff, time.Now())
}

// PushShareAndSnapshot atomically adds a valid block share and copies the
// resulting discovery-time distribution while holding the same lock.
func (w *Window) PushShareAndSnapshot(account string, assignedPdiff float64) (map[string]uint64, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pushShareLocked(account, assignedPdiff, time.Now())
	return w.snapshotLocked()
}

func (w *Window) pushShareLocked(account string, assignedPdiff float64, now time.Time) {
	work := uint64(assignedPdiff * PPLNS_SCALE)
	if work == 0 {
		return // Ignore 0 work
	}

	share := Share{
		Account:   account,
		Work:      work,
		Timestamp: now.UnixNano(),
	}

	w.shares = append(w.shares, share)
	w.totalWork += work
	w.workByAccount[account] += work

	// Evict oldest shares if we exceed targetWork.
	// We strictly follow "last N work", so we pop until totalWork <= targetWork.
	//
	// NOTE: This implementation uses a "Strict Limit": TotalWork will never exceed TargetWork.
	// If removing the oldest share drops TotalWork below TargetWork, we still remove it.
	// This means the window size might fluctuate slightly below N_work temporarily.
	//
	// Alternative "Soft Limit" Approach:
	// Some implementations prefer to keep *at least* N work (Stop evicting if total < N).
	// We are currently using the Strict Limit to ensure we don't overpay for history.

	for w.liveShareCountLocked() > 0 && w.totalWork > w.targetWork {
		removed := w.shares[w.head]
		// Safety check: ensure we don't underflow
		if w.totalWork < removed.Work {
			w.totalWork = 0
		} else {
			// Proceed with eviction to maintain strict limit (TotalWork <= TargetWork)

			w.totalWork -= removed.Work
			w.workByAccount[removed.Account] -= removed.Work
			if w.workByAccount[removed.Account] == 0 {
				delete(w.workByAccount, removed.Account)
			}
			w.shares[w.head] = Share{}
			w.head++
		}
	}

	w.compactLocked()
}

// Snapshot returns a copy of the current work distribution and total work.
// Useful for calculating rewards safely without holding the lock.
// Returns: map[account]workUnits, totalWorkUnits
func (w *Window) Snapshot() (map[string]uint64, uint64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.snapshotLocked()
}

func (w *Window) snapshotLocked() (map[string]uint64, uint64) {
	// Deep copy map
	snapshot := make(map[string]uint64, len(w.workByAccount))
	for k, v := range w.workByAccount {
		snapshot[k] = v
	}

	return snapshot, w.totalWork
}

// Stats returns lightweight read-only counters for monitoring.
func (w *Window) Stats() (shareCount int, totalWork uint64, minerCount int) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.liveShareCountLocked(), w.totalWork, len(w.workByAccount)
}

func (w *Window) liveShareCountLocked() int {
	if w.head >= len(w.shares) {
		return 0
	}
	return len(w.shares) - w.head
}

func (w *Window) compactLocked() {
	if w.head == 0 {
		return
	}
	if w.head >= len(w.shares) {
		w.shares = w.shares[:0]
		w.head = 0
		return
	}
	if w.head < 4096 || w.head*2 < len(w.shares) {
		return
	}
	copy(w.shares, w.shares[w.head:])
	live := len(w.shares) - w.head
	clear(w.shares[live:])
	w.shares = w.shares[:live]
	w.head = 0
}

// CalculateCredits computes the reward for each account based on the snapshot.
// reward is the total block reward (in minimal units, e.g. satoshis/wei).
// Returns map[account]rewardAmount.
func CalculateCredits(workByAccount map[string]uint64, totalWork uint64, blockReward uint64) map[string]uint64 {
	credits := make(map[string]uint64)
	if totalWork == 0 {
		return credits
	}

	// Use big.Int for calculation to avoid overflow during multiply
	// credit = (accountWork * blockReward) / totalWork
	// We don't need fixed-point scaling here because work/totalWork is the fraction.

	// Note: integer division truncates. The remainder is dust.
	// In a real pool, dust is often collected or ignored.

	for acc, work := range workByAccount {
		if work == 0 {
			continue
		}
		// val = work * reward
		// val = val / totalWork

		// 128 bit math check:
		// work (u64) * reward (u64) can exceed u64.
		// Go doesn't have native u128.
		// We need to implement manual multiplication or split.
		// Since we didn't import math/big in signature, let's just use it inside.
		// Or simply assume uint64 is enough?
		// If reward is 50 BTC = 50 * 1e8 = 5,000,000,000.
		// If work is 1e9 * difficulty (say 1,000,000) = 1e15.
		// 5e9 * 1e15 = 5e24. Exceeds uint64 (1.8e19).
		// Use float64 or big.Int for credit calculation.
		// NOTE: totals can use uint64 with overflow checks or big.Int if needed.
		// We will use float64 for simplicity as PPLNS credit calc usually tolerates epsilon errors.
		// Let's use float64 for the calculation: credit = reward * (float(work)/float(total))

		frac := float64(work) / float64(totalWork)
		credit := uint64(float64(blockReward) * frac)
		credits[acc] = credit
	}
	return credits
}
