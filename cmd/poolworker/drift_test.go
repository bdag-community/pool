package main

import (
	"fmt"
	"testing"
	"time"
)

func newParent(i int) string { return fmt.Sprintf("parent-%04d", i) }

// newDriftAt builds a detector whose warm-up has already elapsed, so tests can
// exercise the steady-state behaviour without simulating the warm-up first.
func newDriftAt(window time.Duration, maxParents int, start time.Time) *templateDrift {
	d := newTemplateDrift(window, maxParents)
	d.observingSince = start.Add(-window)
	return d
}

// The backlog that motivated this worker: the node publishes a fresh parent
// roughly once a second, the pool keeps broadcasting jobs built from a parent
// it received minutes ago, and the pool's own health looks fine throughout.
// The reported age must track how stale the pool actually is.
func TestPoolParentAgeGrowsWhilePoolIsStuck(t *testing.T) {
	start := time.Now()
	d := newDriftAt(90*time.Second, 512, start)

	stuck := newParent(0)
	d.observeNodeParent(stuck, start)
	if got := d.poolParentAge(stuck, start); got != 0 {
		t.Fatalf("pool on the node's newest parent: age = %s, want 0", got)
	}

	// The node moves on once per second; the pool stays put.
	for i := 1; i <= 40; i++ {
		now := start.Add(time.Duration(i) * time.Second)
		d.observeNodeParent(newParent(i), now)
		want := time.Duration(i) * time.Second
		if got := d.poolParentAge(stuck, now); got != want {
			t.Fatalf("t+%ds: age = %s, want %s", i, got, want)
		}
	}
}

// Once the stuck parent falls out of the window we can no longer time it
// exactly, but it is at least window old — and that lower bound must still
// exceed any sane drift timeout.
func TestPoolParentAgeReportsWindowForEvictedParent(t *testing.T) {
	start := time.Now()
	window := 30 * time.Second
	d := newDriftAt(window, 512, start)

	stuck := newParent(0)
	d.observeNodeParent(stuck, start)

	for i := 1; i <= 60; i++ {
		d.observeNodeParent(newParent(i), start.Add(time.Duration(i)*time.Second))
	}
	got := d.poolParentAge(stuck, start.Add(60*time.Second))
	if got != window {
		t.Fatalf("evicted parent age = %s, want the window %s as a lower bound", got, window)
	}
}

func TestPoolParentAgeIsZeroWhenPoolTracksNode(t *testing.T) {
	start := time.Now()
	d := newDriftAt(90*time.Second, 512, start)

	for i := range 60 {
		now := start.Add(time.Duration(i) * time.Second)
		d.observeNodeParent(newParent(i), now)
		if got := d.poolParentAge(newParent(i), now); got != 0 {
			t.Fatalf("tick %d: age = %s, want 0", i, got)
		}
	}
}

// The node's parent changes about once a second, so a pool a few seconds behind
// is normal and must report a small age, well under any drift timeout.
func TestPoolLaggingSecondsReportsSmallAge(t *testing.T) {
	start := time.Now()
	d := newDriftAt(90*time.Second, 512, start)

	for i := range 60 {
		now := start.Add(time.Duration(i) * time.Second)
		d.observeNodeParent(newParent(i), now)
		if i < 5 {
			continue
		}
		got := d.poolParentAge(newParent(i-5), now)
		if got != 5*time.Second {
			t.Fatalf("tick %d: a 5s-old parent reported age %s, want 5s", i, got)
		}
	}
}

// Before a full window has elapsed, an unknown parent is indistinguishable from
// a cold start. Reporting drift then would restart a perfectly healthy pool the
// moment the worker came up.
func TestNoDriftDuringWarmUp(t *testing.T) {
	start := time.Now()
	d := newTemplateDrift(90*time.Second, 512) // warm-up begins now
	d.observingSince = start

	if got := d.poolParentAge("never-seen", start.Add(30*time.Second)); got != 0 {
		t.Fatalf("age during warm-up = %s, want 0", got)
	}
	if got := d.poolParentAge("never-seen", start.Add(91*time.Second)); got != 90*time.Second {
		t.Fatalf("age after warm-up = %s, want the window", got)
	}
}

func TestDriftCapsRememberedParents(t *testing.T) {
	const limit = 8
	start := time.Now()
	d := newDriftAt(time.Hour, limit, start)

	for i := range 100 {
		d.observeNodeParent(newParent(i), start.Add(time.Duration(i)*time.Millisecond))
	}
	if len(d.firstSeen) > limit {
		t.Fatalf("remembered %d parents, want at most %d", len(d.firstSeen), limit)
	}
	if got := d.poolParentAge(newParent(99), start.Add(99*time.Millisecond)); got != 0 {
		t.Fatalf("newest parent evicted by the cap (age %s)", got)
	}
}

// After a restart the pool needs a window to catch up. Reset must restart the
// warm-up, or the next tick restarts it again for the parent it held before.
func TestResetRestartsWarmUp(t *testing.T) {
	start := time.Now()
	window := 90 * time.Second
	d := newDriftAt(window, 512, start)

	d.observeNodeParent(newParent(0), start)
	later := start.Add(200 * time.Second)
	d.observeNodeParent(newParent(1), later)
	if got := d.poolParentAge("ancient", later); got != window {
		t.Fatalf("expected window-bound drift before reset, got %s", got)
	}

	d.reset(later)
	if got := d.poolParentAge("ancient", later); got != 0 {
		t.Fatalf("age immediately after reset = %s, want 0", got)
	}
	if got := d.poolParentAge("ancient", later.Add(window+time.Second)); got != window {
		t.Fatalf("age once the fresh warm-up elapses = %s, want the window", got)
	}
}

// The node reports template_parent_root 0x-prefixed; the pool reports
// current_parent as bare lowercase hex. If the detector compared them raw they
// would never match, and it would report permanent drift and restart the pool
// forever. Both sides must be normalised before comparison.
func TestDriftNormalisesParentFormats(t *testing.T) {
	start := time.Now()
	d := newDriftAt(90*time.Second, 512, start)

	// Node publishes 0x-prefixed, mixed case.
	d.observeNodeParent("0xABCDEF0123456789", start)

	// Pool reports the same parent bare and lowercase — must read as current.
	if got := d.poolParentAge("abcdef0123456789", start); got != 0 {
		t.Fatalf("0x-prefixed node parent vs bare pool parent must match, got drift %s", got)
	}
}
