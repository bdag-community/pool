package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// sustained tracks how long a condition has continuously failed. A condition
// that flaps never accumulates, which is what keeps a single dropped RPC from
// restarting a healthy pool.
type sustained struct {
	since time.Time
}

func (s *sustained) fail(now time.Time) time.Duration {
	if s.since.IsZero() {
		s.since = now
	}
	return now.Sub(s.since)
}

func (s *sustained) ok() { s.since = time.Time{} }

// probeState is everything the restart decision depends on, gathered once per
// tick. Separated from the decision so the decision can be tested without a
// node, a pool, or a clock.
type probeState struct {
	nodeReachable bool
	nodeMineable  bool
	nodeReason    string

	poolReachable bool
	stratumOK     bool

	// poolParentAge is how long ago the node first published the parent the pool
	// is currently mining. Only meaningful when the node is mineable; a node
	// that is not publishing cannot make the pool fresh.
	poolParentAge time.Duration

	poolUnreachableFor time.Duration
	stratumDownFor     time.Duration
}

type thresholds struct {
	driftTimeout           time.Duration
	poolUnreachableTimeout time.Duration
	stratumDownTimeout     time.Duration
}

// wedgeDecision reports whether the pool child should be restarted, and why.
//
// Node health is never a reason to restart the pool. If the node is not
// mineable the pool has nothing to mine, and restarting it would only drop the
// miners; the node's own supervisor owns that failure. This is the rule that
// keeps two supervisors from fighting over one outage.
func wedgeDecision(s probeState, t thresholds) (restart bool, reason string) {
	if !s.nodeReachable || !s.nodeMineable {
		return false, ""
	}
	if !s.stratumOK && s.stratumDownFor >= t.stratumDownTimeout {
		return true, fmt.Sprintf("stratum listener not accepting for %s", s.stratumDownFor.Truncate(time.Second))
	}
	if !s.poolReachable && s.poolUnreachableFor >= t.poolUnreachableTimeout {
		return true, fmt.Sprintf("pool health endpoint unreachable for %s", s.poolUnreachableFor.Truncate(time.Second))
	}
	// Template drift is deliberately NOT a restart trigger. The pool's job-state
	// current_parent lags its live broadcast parent, so drift measured against it
	// is a false positive that restart-loops a healthy pool. The ws-coalescing
	// fix in jobBroadcast.go prevents the stale-template backlog at the source;
	// restarting here only dropped miners. The reliable signals above (stratum
	// listener down, health endpoint unreachable) still catch a wedged pool.
	return false, ""
}

type supervisor struct {
	node  *nodeClient
	pool  *poolClient
	child *child
	drift *templateDrift

	stratumAddr        string
	probeInterval      time.Duration
	dialTimeout        time.Duration
	stopGrace          time.Duration
	restartCooldown    time.Duration
	maxRestartCooldown time.Duration
	thresholds         thresholds

	poolDown    sustained
	stratumDown sustained

	// consecutiveRestarts drives the backoff. Restarting cures a wedge whose
	// cause has passed; it cannot cure a defect that reproduces immediately. A
	// pool with, say, an unbounded template backlog re-wedges the moment it comes
	// back, so a fixed cooldown would restart it forever and drop the miners each
	// time. Backing off keeps the pool mining for progressively longer stretches
	// and makes the underlying defect visible in the logs instead of hiding it.
	consecutiveRestarts int
	lastRestart         time.Time
}

// cooldownAfterRestart doubles the quiet period for each restart that follows
// hard on the last one, and resets once the pool has stayed healthy for a
// stretch. Exported logic kept pure for testing.
func cooldownAfterRestart(consecutive int, base, max time.Duration) time.Duration {
	if consecutive < 1 {
		consecutive = 1
	}
	cooldown := base
	for range consecutive - 1 {
		cooldown *= 2
		if cooldown >= max {
			return max
		}
	}
	return cooldown
}

// restartStreak reports how many restarts have happened back-to-back. A restart
// that follows a long healthy stretch starts the streak over.
func restartStreak(previous int, lastRestart, now time.Time, healthyFor time.Duration) int {
	if lastRestart.IsZero() || now.Sub(lastRestart) > healthyFor {
		return 1
	}
	return previous + 1
}

// nodeReadyToStartPool reports whether the node is ready for the pool to start
// mining against it.
//
// It gates on get_block_template_ready, not mineable_now. The node's miner
// enables lazily on the first getBlockTemplate request, so a synced node that
// no pool has poked yet reports mineable_now=false, reason_code=miner_disabled.
// Waiting for mineable_now would deadlock — the pool is what pokes it. A node
// that is template-ready is one the pool can start against; its own first
// template request wakes the miner.
func nodeReadyToStartPool(h *nodeHealth) bool {
	return h.MineableNow || h.GetBlockTemplateReady
}

// awaitNodeReady blocks until the node is ready for the pool to start.
//
// This replaces the pool start lease. The pool is already an RPC client of the
// node; it does not need a third party to vouch for the node's health, and
// making it wait on one coupled mining availability to watchdog liveness.
func (s *supervisor) awaitNodeReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := time.Second
	const maxBackoff = 15 * time.Second

	for {
		health, err := s.node.templateHealth(ctx)
		switch {
		case err != nil:
			log.Printf("[poolworker] start gate: node unreachable: %v", err)
		case health.MineableNow:
			log.Printf("[poolworker] start gate: node mineable (seq=%d parent=%s)",
				health.LatestTemplateSeq, short(health.TemplateParentRoot))
			return nil
		case health.GetBlockTemplateReady:
			// Synced and able to build templates; the miner just has not been
			// asked yet. Start the pool — its first getBlockTemplate wakes it.
			log.Printf("[poolworker] start gate: node template-ready (reason_code=%s); starting pool to wake the miner",
				health.ReasonCode)
			return nil
		default:
			// reason_code is the node's own explanation. Logging it every attempt
			// is the difference between "the pool won't start" and knowing why.
			log.Printf("[poolworker] start gate: waiting on node: template_ready=%v mineable=%v reason_code=%s reason=%s",
				health.GetBlockTemplateReady, health.MineableNow, health.ReasonCode, health.Reason)
		}

		if timeout > 0 && time.Now().After(deadline) {
			return fmt.Errorf("start gate: node did not become template-ready within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// probeOnce gathers one tick of state.
func (s *supervisor) probeOnce(ctx context.Context, now time.Time) probeState {
	var st probeState

	if health, err := s.node.templateHealth(ctx); err == nil {
		st.nodeReachable = true
		st.nodeMineable = health.MineableNow && health.SubmitReady
		st.nodeReason = health.ReasonCode
		s.drift.observeNodeParent(health.TemplateParentRoot, now)
	}

	if health, err := s.pool.jobState(ctx); err == nil {
		st.poolReachable = true
		s.poolDown.ok()
		st.poolParentAge = s.drift.poolParentAge(health.CurrentParent, now)
	} else {
		st.poolUnreachableFor = s.poolDown.fail(now)
	}

	if err := dialStratum(ctx, s.stratumAddr, s.dialTimeout); err == nil {
		st.stratumOK = true
		s.stratumDown.ok()
	} else {
		st.stratumDownFor = s.stratumDown.fail(now)
	}

	return st
}

// run supervises the child until ctx is cancelled or the child exits on its own.
func (s *supervisor) run(ctx context.Context) error {
	ticker := time.NewTicker(s.probeInterval)
	defer ticker.Stop()

	for {
		// Re-read each iteration: a restart replaces the child's exit channel.
		select {
		case <-ctx.Done():
			s.child.stop(s.stopGrace)
			return ctx.Err()

		case <-s.child.exited():
			// The child died on its own. Surface it and let the container's
			// restart policy decide, rather than looping here.
			return fmt.Errorf("pool exited: %w", s.child.exitError())

		case now := <-ticker.C:
			st := s.probeOnce(ctx, now)
			if st.nodeReachable && !st.nodeMineable {
				log.Printf("[poolworker] node not mineable (reason_code=%s); not restarting pool", st.nodeReason)
			}
			restart, reason := wedgeDecision(st, s.thresholds)
			if !restart {
				continue
			}

			// A restart streak means restarting is not curing the cause. Say so:
			// the streak in the log is the signal that the pool has a defect, not
			// a transient wedge.
			healthyFor := 10 * s.restartCooldown
			s.consecutiveRestarts = restartStreak(s.consecutiveRestarts, s.lastRestart, now, healthyFor)
			cooldown := cooldownAfterRestart(s.consecutiveRestarts, s.restartCooldown, s.maxRestartCooldown)
			s.lastRestart = now

			log.Printf("[poolworker] restarting pool (streak=%d, next probe in %s): %s",
				s.consecutiveRestarts, cooldown, reason)
			s.child.stop(s.stopGrace)

			// Forget drift history: the fresh child deserves a fresh window, or
			// we would restart it again on the next tick for the old parent.
			s.drift.reset(time.Now())
			s.poolDown.ok()
			s.stratumDown.ok()

			if err := s.child.start(); err != nil {
				return fmt.Errorf("restart pool: %w", err)
			}

			select {
			case <-ctx.Done():
				s.child.stop(s.stopGrace)
				return ctx.Err()
			case <-time.After(cooldown):
			}
		}
	}
}

func short(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}
