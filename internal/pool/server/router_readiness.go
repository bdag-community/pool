package server

import "time"

type miningReadinessPurpose uint8

const (
	readinessSubmitRecovery miningReadinessPurpose = iota
	readinessActiveTemplateFallback
	readinessTemplateFetch
)

type miningReadinessDecision struct {
	ready  bool
	reason string
}

func newMiningReadinessDecision(ready bool, reason string) miningReadinessDecision {
	if reason == "" {
		if ready {
			reason = "ok"
		} else {
			reason = "not-ready"
		}
	}
	return miningReadinessDecision{ready: ready, reason: reason}
}

func (d miningReadinessDecision) readyReason() (bool, string) {
	return d.ready, d.reason
}

func (r *rpcRouter) backendReadyForSubmitRecovery(backend *rpcBackend) (bool, string) {
	return r.backendReadyForPurpose(backend, readinessSubmitRecovery)
}

func (r *rpcRouter) backendReadyForActiveTemplateFallback(backend *rpcBackend) (bool, string) {
	return r.backendReadyForPurpose(backend, readinessActiveTemplateFallback)
}

func (r *rpcRouter) backendReadyForPurpose(backend *rpcBackend, purpose miningReadinessPurpose) (bool, string) {
	return r.miningReadiness(backend, purpose, time.Now()).readyReason()
}

func (r *rpcRouter) miningReadiness(backend *rpcBackend, purpose miningReadinessPurpose, now time.Time) miningReadinessDecision {
	if r == nil || backend == nil {
		return newMiningReadinessDecision(false, "no-backend")
	}
	if ready, reason := r.chainIdentityReadiness(backend); !ready {
		return newMiningReadinessDecision(false, reason)
	}
	state := backend.snapshotState()
	if held, reason := submitReadinessHoldActive(state, now); held {
		return newMiningReadinessDecision(false, reason)
	}
	if purpose == readinessTemplateFetch {
		return r.templateFetchReadiness(state, now)
	}
	if blocked, reason := r.nodeHealthReadinessBlock(state, now, purpose); blocked {
		return newMiningReadinessDecision(false, reason)
	}
	if !state.healthy {
		reason := state.reason
		if reason == "" {
			reason = "unhealthy"
		}
		return newMiningReadinessDecision(false, reason)
	}
	return newMiningReadinessDecision(true, "ok")
}

func (r *rpcRouter) templateFetchReadiness(state backendState, now time.Time) miningReadinessDecision {
	if r == nil || !r.config.nodeHealthEnabled {
		return newMiningReadinessDecision(true, "ok")
	}
	if state.lastNodeHealthAt.IsZero() {
		return newMiningReadinessDecision(false, "node-health-pending")
	}
	if nodeHealthAgeExceeded(state, now, r.config.nodeHealthMaxAge) {
		return newMiningReadinessDecision(false, "node-health-stale")
	}
	if state.nodeHealthGetBlockTemplateReadyKnown && !state.nodeHealthGetBlockTemplateReady {
		reason := getTemplateUnreadyReason(state)
		if templateFetchReasonShouldPause(reason) {
			return newMiningReadinessDecision(false, "node-health-"+reason)
		}
		return newMiningReadinessDecision(true, "ok")
	}
	if !state.nodeHealthMineable || !state.nodeHealthSubmitReady || !state.nodeHealthTemplateUsable {
		reason := mineableUnreadyReason(state)
		if templateFetchReasonShouldPause(reason) {
			return newMiningReadinessDecision(false, "node-health-"+reason)
		}
	}
	if state.nodeHealthP2PContextObserved && !nodeHealthP2PMiningReady(state.nodeHealthChainCurrent, state.nodeHealthP2PCurrent, state.nodeHealthP2PMiningFresh) {
		return newMiningReadinessDecision(false, "node-health-"+nodeHealthP2PUnreadyReason(state.nodeHealthChainCurrent, state.nodeHealthP2PCurrent, state.nodeHealthP2PMiningFresh, state.nodeHealthP2PMiningFreshReasonCode))
	}
	return newMiningReadinessDecision(true, "ok")
}

func (r *rpcRouter) templateFetchPauseReason(backend *rpcBackend) string {
	if r == nil || backend == nil {
		return ""
	}
	decision := r.miningReadiness(backend, readinessTemplateFetch, time.Now())
	if decision.ready {
		return ""
	}
	return decision.reason
}

func templateFetchReasonShouldPause(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "node-syncing", "pool-syncing", "bdag-pool-syncing",
		"initial-download", "block-download", "tx-download",
		"graph-state-sync", "reorg-pressure", "template-not-ready",
		"get-block-template-not-ready", "p2p-not-fresh", "p2p-not-current", "peer-lag",
		"no-consensus-peers", "no-fresh-consensus-peers",
		"not-enough-consensus-peers", "not-enough-fresh-consensus-peers",
		"p2p-unavailable",
		"timeout", "transport", "too-busy", "http-retryable", "http-error",
		"rpc-error", "request-build", "read-response", "decode-response",
		"empty-result":
		return true
	default:
		return false
	}
}

func (r *rpcRouter) nodeHealthReadinessBlock(state backendState, now time.Time, purpose miningReadinessPurpose) (bool, string) {
	if r == nil || !r.config.nodeHealthEnabled || state.lastNodeHealthAt.IsZero() {
		return false, ""
	}
	if nodeHealthAgeExceeded(state, now, r.config.nodeHealthMaxAge) {
		return true, "node-health-stale"
	}
	if blocked, reason := r.getTemplateReadinessBlock(state, now, purpose); blocked {
		return true, reason
	}
	if blocked, reason := r.mineableReadinessBlock(state, now, purpose); blocked {
		return true, reason
	}
	if state.nodeHealthP2PContextObserved && !nodeHealthP2PMiningReady(state.nodeHealthChainCurrent, state.nodeHealthP2PCurrent, state.nodeHealthP2PMiningFresh) {
		return true, "node-health-" + nodeHealthP2PUnreadyReason(state.nodeHealthChainCurrent, state.nodeHealthP2PCurrent, state.nodeHealthP2PMiningFresh, state.nodeHealthP2PMiningFreshReasonCode)
	}
	return false, ""
}

func nodeHealthAgeExceeded(state backendState, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 {
		maxAge = time.Duration(defaultRPCRouterNodeHealthMaxAgeSeconds) * time.Second
	}
	healthAge := now.Sub(state.lastNodeHealthAt)
	if healthAge < 0 {
		healthAge = 0
	}
	return healthAge > maxAge
}

func (r *rpcRouter) getTemplateReadinessBlock(state backendState, now time.Time, purpose miningReadinessPurpose) (bool, string) {
	if !state.nodeHealthGetBlockTemplateReadyKnown || state.nodeHealthGetBlockTemplateReady {
		return false, ""
	}
	reason := getTemplateUnreadyReason(state)
	if backendStateAllowsCachedTemplateSubmitOnly(state, reason) {
		return false, ""
	}
	freshAdvisory := backendStateAllowsFreshTemplateReadinessAdvisory(state, reason, now, r.config.nodeHealthFreshTemplateGrace)
	if purpose == readinessActiveTemplateFallback {
		if freshAdvisory {
			return false, ""
		}
		return true, "node-health-" + reason
	}

	thresholdReached := state.nodeHealthConsecutiveUnready >= intOrDefault(r.config.nodeHealthUnreadyThreshold, defaultRPCRouterNodeHealthUnreadyThreshold)
	freshRouterTemplate := backendStateHasFreshRouterTemplate(state, now, r.config.nodeHealthFreshTemplateGrace)
	if !freshAdvisory && (!state.healthy || thresholdReached || !freshRouterTemplate) {
		return true, "node-health-" + reason
	}
	return false, ""
}

func (r *rpcRouter) mineableReadinessBlock(state backendState, now time.Time, purpose miningReadinessPurpose) (bool, string) {
	if state.nodeHealthMineable && state.nodeHealthSubmitReady && state.nodeHealthTemplateUsable {
		return false, ""
	}
	reason := mineableUnreadyReason(state)
	if state.nodeHealthGetBlockTemplateReadyKnown &&
		state.nodeHealthGetBlockTemplateReady &&
		nodeHealthCachedTemplateOnlyReason(reason) {
		return false, ""
	}
	if purpose == readinessActiveTemplateFallback &&
		nodeHealthTemplateRefreshableReason(reason) &&
		backendStateHasFreshRouterTemplate(state, now, r.config.nodeHealthFreshTemplateGrace) &&
		(!state.nodeHealthP2PContextObserved || (state.nodeHealthP2PCurrent && state.nodeHealthP2PMiningFresh)) {
		return false, ""
	}
	if purpose == readinessActiveTemplateFallback &&
		backendStateAllowsFreshTemplateReadinessAdvisory(state, reason, now, r.config.nodeHealthFreshTemplateGrace) {
		return false, ""
	}
	return true, "node-health-" + reason
}

func backendStateAllowsCachedTemplateSubmitOnly(state backendState, reason string) bool {
	if !nodeHealthCachedTemplateSubmitOnlyReason(reason) {
		return false
	}
	if !state.nodeHealthMineable || !state.nodeHealthSubmitReady || !state.nodeHealthTemplate || !state.nodeHealthTemplateUsable {
		return false
	}
	return !state.nodeHealthP2PContextObserved || (state.nodeHealthP2PCurrent && state.nodeHealthP2PMiningFresh)
}

func getTemplateUnreadyReason(state backendState) string {
	reason := state.nodeHealthGetBlockTemplateReasonCode
	if reason == "" || reason == "unknown" || reason == "ok" {
		reason = state.nodeHealthReasonCode
	}
	if reason == "" || reason == "unknown" || reason == "ok" {
		reason = "get-block-template-not-ready"
	}
	return reason
}

func mineableUnreadyReason(state backendState) string {
	reason := state.nodeHealthReasonCode
	if reason == "" {
		reason = state.reason
	}
	if reason == "" || reason == "ok" {
		reason = "not-submit-ready"
	}
	return reason
}
