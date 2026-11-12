package main

import (
	"strings"
	"sync"
	"time"
)

// templateDrift measures how old the template the pool is mining actually is,
// by timing it against the node's own stream of parent roots.
//
// It exists because the pool cannot see this on its own. During the July 2026
// backlog the pool broadcast a job every ~1.8s and reported itself healthy
// throughout — it was simply broadcasting jobs built from templates the node
// had published minutes earlier. Only a comparison against the node's parents
// reveals it.
//
// The measure is the age of the pool's current parent: the time since the node
// first published it. A pool a few seconds behind is normal, because the node
// republishes a new parent roughly once a second. A pool minutes behind is
// mining work that can never be won.
type templateDrift struct {
	// window bounds how far back parents are remembered. A pool parent that has
	// fallen out of the window is at least this old, which is the lower bound we
	// report for it. Keep window >= the caller's drift timeout, or an evicted
	// parent could report an age below the threshold and never trigger.
	window time.Duration
	// maxParents caps memory if the node churns parents faster than expected.
	maxParents int

	mu sync.Mutex
	// firstSeen maps a node parent root to when the node first published it.
	firstSeen map[string]time.Time
	// observingSince is when this detector started (or was last reset). Before a
	// full window has elapsed we cannot distinguish "parent is ancient" from
	// "we have not been watching long enough", so we report no drift.
	observingSince time.Time
}

func newTemplateDrift(window time.Duration, maxParents int) *templateDrift {
	return &templateDrift{
		window:         window,
		maxParents:     maxParents,
		firstSeen:      make(map[string]time.Time),
		observingSince: time.Now(),
	}
}

// observeNodeParent records a parent root the node is currently building on.
// normalizeParent canonicalises a parent root for comparison. The node reports
// template_parent_root 0x-prefixed (ethcommon.Hash.String), while the pool
// reports current_parent as bare lowercase hex. Without this, the two never
// match and the detector reports permanent drift — restarting the pool forever.
func normalizeParent(p string) string {
	return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(p)), "0x")
}

func (d *templateDrift) observeNodeParent(parent string, now time.Time) {
	parent = normalizeParent(parent)
	if parent == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.firstSeen[parent]; !ok {
		d.firstSeen[parent] = now
	}
	d.pruneLocked(now)
}

// pruneLocked drops parents older than the window, then the oldest survivors
// above maxParents. d.mu must be held.
func (d *templateDrift) pruneLocked(now time.Time) {
	cutoff := now.Add(-d.window)
	for parent, at := range d.firstSeen {
		if at.Before(cutoff) {
			delete(d.firstSeen, parent)
		}
	}
	for len(d.firstSeen) > d.maxParents {
		var oldest string
		var oldestAt time.Time
		for parent, at := range d.firstSeen {
			if oldest == "" || at.Before(oldestAt) {
				oldest, oldestAt = parent, at
			}
		}
		delete(d.firstSeen, oldest)
	}
}

// poolParentAge reports how long ago the node first published the parent the
// pool is currently mining.
//
// A parent we have never seen is either older than the window or from a chain
// we are not tracking; either way it is at least window old, so we report the
// window as a lower bound. Until we have observed for a full window we return
// zero, because an unknown parent is indistinguishable from a cold start.
func (d *templateDrift) poolParentAge(parent string, now time.Time) time.Duration {
	parent = normalizeParent(parent)
	d.mu.Lock()
	defer d.mu.Unlock()

	if at, ok := d.firstSeen[parent]; ok && parent != "" {
		age := now.Sub(at)
		if age < 0 {
			return 0
		}
		return age
	}
	if now.Sub(d.observingSince) < d.window {
		return 0
	}
	return d.window
}

// reset forgets all history and restarts the warm-up. Called after a child
// restart, so the fresh pool is given a window to catch up rather than being
// restarted again on the next tick for the parent it held before.
func (d *templateDrift) reset(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.firstSeen = make(map[string]time.Time)
	d.observingSince = now
}
