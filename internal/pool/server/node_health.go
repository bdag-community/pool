package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type nodeTemplateHealthResult struct {
	CheckedAtUnixMs                                int64  `json:"checked_at_unix_ms"`
	Enabled                                        bool   `json:"enabled"`
	Started                                        bool   `json:"started"`
	Shutdown                                       bool   `json:"shutdown"`
	ReasonCode                                     string `json:"reason_code"`
	Reason                                         string `json:"reason"`
	MineableNow                                    bool   `json:"mineable_now"`
	SubmitReady                                    bool   `json:"submit_ready"`
	GetBlockTemplateReady                          *bool  `json:"get_block_template_ready"`
	GetBlockTemplateReasonCode                     string `json:"get_block_template_reason_code"`
	GetBlockTemplateReason                         string `json:"get_block_template_reason"`
	WorkerReady                                    bool   `json:"worker_ready"`
	WorkerRunning                                  bool   `json:"worker_running"`
	TemplateAvailable                              bool   `json:"template_available"`
	TemplateUsable                                 bool   `json:"template_usable"`
	TemplateAgeMs                                  int64  `json:"template_age_ms"`
	TemplateMaxAgeMs                               int64  `json:"template_max_age_ms"`
	TemplateTimestampUnix                          int64  `json:"template_timestamp_unix"`
	LatestTemplateSeq                              uint64 `json:"latest_template_seq"`
	TemplateHeight                                 int64  `json:"template_height"`
	TemplateParentRoot                             string `json:"template_parent_root"`
	TemplateTxCount                                int    `json:"template_tx_count"`
	LastGoodTemplateAvailable                      bool   `json:"last_good_template_available"`
	LastGoodTemplateAgeMs                          int64  `json:"last_good_template_age_ms"`
	LastTemplateBuildAgeMs                         int64  `json:"last_template_build_age_ms"`
	LastTemplateBuildDurationMs                    int64  `json:"last_template_build_duration_ms"`
	LastTemplateBuildErrorCode                     string `json:"last_template_build_error_code"`
	LastTemplateBuildError                         string `json:"last_template_build_error"`
	LastTemplateBuildReason                        string `json:"last_template_build_reason"`
	LastTemplateBuildBlocking                      bool   `json:"last_template_build_error_blocking"`
	LastTemplateInvalidationAtUnixMs               int64  `json:"last_template_invalidation_at_unix_ms"`
	LastTemplateInvalidationSeq                    uint64 `json:"last_template_invalidation_seq"`
	LastTemplateInvalidationCause                  string `json:"last_template_invalidation_cause"`
	LastTemplateInvalidationReason                 string `json:"last_template_invalidation_reason"`
	LastTemplateInvalidationSubmitObsoleteDepth    int64  `json:"last_template_invalidation_submit_obsolete_depth"`
	PendingTemplateBuild                           bool   `json:"pending_template_build"`
	PendingTemplateBuildReason                     string `json:"pending_template_build_reason"`
	PendingTemplateBuildPriority                   int    `json:"pending_template_build_priority"`
	PendingTemplateInvalidationCause               string `json:"pending_template_invalidation_cause"`
	PendingTemplateInvalidationSubmitObsoleteDepth int64  `json:"pending_template_invalidation_submit_obsolete_depth"`
	P2PCurrent                                     bool   `json:"p2p_current"`
	P2PNearlySynced                                bool   `json:"p2p_nearly_synced"`
	P2PFreshnessMaxAgeMs                           int64  `json:"p2p_freshness_max_age_ms"`
	P2PPeerLeadToleranceBlocks                     uint64 `json:"p2p_peer_lead_tolerance_blocks"`
	P2PConsensusPeerCount                          int    `json:"p2p_consensus_peer_count"`
	P2PFreshConsensusPeerCount                     int    `json:"p2p_fresh_consensus_peer_count"`
	P2PStaleConsensusPeerCount                     int    `json:"p2p_stale_consensus_peer_count"`
	P2PBestPeerMainOrder                           uint64 `json:"p2p_best_peer_main_order"`
	P2PBestPeerLeadBlocks                          int64  `json:"p2p_best_peer_lead_blocks"`
	P2PBestPeerGraphStateAgeMs                     int64  `json:"p2p_best_peer_graph_state_age_ms"`
	P2PAvgGraphStateAgeMs                          int64  `json:"p2p_avg_graph_state_age_ms"`
	P2PMaxGraphStateAgeMs                          int64  `json:"p2p_max_graph_state_age_ms"`
	P2PSyncPeerPresent                             bool   `json:"p2p_sync_peer_present"`
	P2PSyncPeerFresh                               bool   `json:"p2p_sync_peer_fresh"`
	P2PSyncPeerGraphStateAgeMs                     int64  `json:"p2p_sync_peer_graph_state_age_ms"`
	P2PMiningFresh                                 bool   `json:"p2p_mining_fresh"`
	P2PMiningFreshReasonCode                       string `json:"p2p_mining_fresh_reason_code"`
	SyncAllowed                                    bool   `json:"sync_allowed"`
	SyncReasonCode                                 string `json:"sync_reason_code"`
	SyncReason                                     string `json:"sync_reason"`
	ChainCurrent                                   bool   `json:"chain_current"`
	MainOrder                                      uint64 `json:"main_order"`
	SelfAdd                                        int64  `json:"self_add"`
	SubmitNoSynced                                 bool   `json:"submit_no_synced"`
	NearlySyncMining                               bool   `json:"nearly_sync_mining"`
}

const nodeHealthJobInvalidationThrottle = 30 * time.Second

const (
	defaultNodeHealthErrorBackoff = 500 * time.Millisecond
	maxNodeHealthErrorBackoff     = 15 * time.Second
)

type nodeHealthRPCError struct {
	reason string
	err    error
}

func (e *nodeHealthRPCError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *nodeHealthRPCError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func fetchNodeTemplateHealthFromBackend(backend *rpcBackend) (*nodeTemplateHealthResult, time.Duration, error) {
	if backend == nil {
		return nil, 0, &nodeHealthRPCError{reason: "no-backend", err: errors.New("no RPC backend configured")}
	}

	start := time.Now()
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTemplateHealth",
		"params":  []interface{}{},
	}
	buf, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", backend.url, bytes.NewReader(buf))
	if err != nil {
		return nil, time.Since(start), &nodeHealthRPCError{reason: "request-build", err: err}
	}
	req.SetBasicAuth(backend.rpcUser, backend.rpcPassword)
	req.Header.Set("Content-Type", "application/json")

	client := backend.gbtClient
	if client == nil {
		client = gbtHTTPClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, time.Since(start), &nodeHealthRPCError{reason: "transport", err: err}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, time.Since(start), &nodeHealthRPCError{reason: "read-response", err: fmt.Errorf("read getTemplateHealth response body: %w", err)}
	}
	if resp.StatusCode != http.StatusOK {
		reason := "http-error"
		bodyText := strings.ToLower(string(body))
		if messageIndicatesNodeOverloaded(bodyText) {
			reason = "too-busy"
		} else if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			reason = "http-retryable"
		}
		err := fmt.Errorf("getTemplateHealth HTTP %d: %s", resp.StatusCode, truncateForLog(body, 256))
		return nil, time.Since(start), &nodeHealthRPCError{reason: reason, err: err}
	}

	var rpcResp struct {
		Result *nodeTemplateHealthResult `json:"result"`
		Error  json.RawMessage           `json:"error"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, time.Since(start), &nodeHealthRPCError{reason: "decode-response", err: err}
	}
	if len(rpcResp.Error) > 0 && string(rpcResp.Error) != "null" {
		reason := classifyNodeHealthRPCError(rpcResp.Error)
		err := fmt.Errorf("RPC error in getTemplateHealth: %s", truncateForLog(rpcResp.Error, 512))
		return nil, time.Since(start), &nodeHealthRPCError{reason: reason, err: err}
	}
	if rpcResp.Result == nil {
		return nil, time.Since(start), &nodeHealthRPCError{reason: "empty-result", err: errors.New("getTemplateHealth returned empty result")}
	}
	return rpcResp.Result, time.Since(start), nil
}

func classifyNodeHealthRPCError(raw json.RawMessage) string {
	msg := strings.ToLower(string(raw))
	switch {
	case strings.Contains(msg, "method") && strings.Contains(msg, "not") && strings.Contains(msg, "found"):
		return "method-not-found"
	case strings.Contains(msg, "not found"):
		return "method-not-found"
	case messageIndicatesNodeSyncing(msg):
		return "node-syncing"
	case strings.Contains(msg, "initial") && strings.Contains(msg, "download"):
		return "node-syncing"
	case strings.Contains(msg, "bdag pool syncing") || strings.Contains(msg, "pool syncing"):
		return "node-syncing"
	case strings.Contains(msg, "graph-state") || strings.Contains(msg, "graph state"):
		return "node-syncing"
	case strings.Contains(msg, "reorg"):
		return "reorg-pressure"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case messageIndicatesNodeOverloaded(msg):
		return "too-busy"
	default:
		return "rpc-error"
	}
}

func messageIndicatesNodeSyncing(msg string) bool {
	return strings.Contains(msg, "node-health-node-syncing") ||
		strings.Contains(msg, "node-syncing") ||
		strings.Contains(msg, "node_syncing") ||
		strings.Contains(msg, "node syncing") ||
		strings.Contains(msg, "node is syncing")
}

func messageIndicatesNodeOverloaded(msg string) bool {
	msg = strings.ToLower(msg)
	return strings.Contains(msg, "too busy") ||
		strings.Contains(msg, "server busy") ||
		strings.Contains(msg, "too many concurrent") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "temporarily overloaded") ||
		strings.Contains(msg, "service unavailable")
}

func nodeHealthErrorReason(err error) string {
	var healthErr *nodeHealthRPCError
	if errors.As(err, &healthErr) && healthErr.reason != "" {
		return healthErr.reason
	}
	return classifyRouterError(err)
}

func nodeHealthUnsupported(err error) bool {
	return nodeHealthErrorReason(err) == "method-not-found"
}

func normalizeNodeHealthReason(reason string) string {
	reason = strings.TrimSpace(strings.ToLower(reason))
	if reason == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("_", "-", " ", "-", "/", "-", ".", "-")
	reason = replacer.Replace(reason)
	for strings.Contains(reason, "--") {
		reason = strings.ReplaceAll(reason, "--", "-")
	}
	return strings.Trim(reason, "-")
}

func normalizeOptionalNodeHealthReason(reason string) string {
	if strings.TrimSpace(reason) == "" {
		return ""
	}
	return normalizeNodeHealthReason(reason)
}

func nodeHealthTemplateAge(health *nodeTemplateHealthResult) time.Duration {
	if health == nil || health.TemplateAgeMs <= 0 {
		return 0
	}
	return time.Duration(health.TemplateAgeMs) * time.Millisecond
}

func nodeHealthDurationFromMs(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

func nodeHealthTimeFromUnixMs(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func (r *rpcRouter) recordNodeTemplateHealth(backend *rpcBackend, health *nodeTemplateHealthResult, latency time.Duration) {
	if r == nil || backend == nil || health == nil {
		return
	}

	now := time.Now()
	reasonCode := normalizeNodeHealthReason(health.ReasonCode)
	if reasonCode == "unknown" && health.MineableNow && health.SubmitReady {
		reasonCode = "ok"
	}
	getTemplateReadyKnown := health.GetBlockTemplateReady != nil
	getTemplateReady := false
	if getTemplateReadyKnown {
		getTemplateReady = *health.GetBlockTemplateReady
	}
	getTemplateReasonCode := normalizeNodeHealthReason(health.GetBlockTemplateReasonCode)
	if getTemplateReasonCode == "unknown" && getTemplateReadyKnown {
		if getTemplateReady {
			getTemplateReasonCode = "ok"
		} else if reasonCode != "unknown" {
			getTemplateReasonCode = reasonCode
		}
	}
	getTemplateReason := health.GetBlockTemplateReason
	if getTemplateReason == "" {
		getTemplateReason = health.Reason
	}
	templateAge := nodeHealthTemplateAge(health)
	templateStale := bool(
		health.TemplateAvailable &&
			r.config.nodeHealthMaxAge > 0 &&
			templateAge > r.config.nodeHealthMaxAge,
	)
	buildContextObserved := health.TemplateMaxAgeMs > 0 ||
		health.LastGoodTemplateAvailable ||
		health.LastGoodTemplateAgeMs > 0 ||
		health.LastTemplateBuildAgeMs > 0 ||
		health.LastTemplateBuildDurationMs > 0 ||
		health.LastTemplateBuildReason != "" ||
		health.LastTemplateBuildErrorCode != "" ||
		health.LastTemplateInvalidationSeq > 0 ||
		health.LastTemplateInvalidationCause != "" ||
		health.PendingTemplateInvalidationCause != "" ||
		health.PendingTemplateBuild ||
		health.PendingTemplateBuildReason != "" ||
		health.PendingTemplateBuildPriority != 0
	p2pContextObserved := health.P2PFreshnessMaxAgeMs > 0 ||
		health.P2PPeerLeadToleranceBlocks > 0 ||
		health.P2PConsensusPeerCount > 0 ||
		health.P2PFreshConsensusPeerCount > 0 ||
		health.P2PStaleConsensusPeerCount > 0 ||
		health.P2PBestPeerMainOrder > 0 ||
		health.P2PBestPeerLeadBlocks != 0 ||
		health.P2PBestPeerGraphStateAgeMs > 0 ||
		health.P2PAvgGraphStateAgeMs > 0 ||
		health.P2PMaxGraphStateAgeMs > 0 ||
		health.P2PSyncPeerPresent ||
		health.P2PSyncPeerGraphStateAgeMs > 0 ||
		health.P2PCurrent ||
		health.P2PMiningFresh ||
		health.P2PMiningFreshReasonCode != ""
	p2pMiningFresh := health.P2PMiningFresh
	p2pMiningFreshReasonCode := normalizeNodeHealthReason(health.P2PMiningFreshReasonCode)
	if p2pContextObserved {
		if peerFloorReason := nodeHealthPeerFloorReason(
			r.config.nodeHealthMinConsensusPeers,
			health.P2PConsensusPeerCount,
			health.P2PFreshConsensusPeerCount,
		); peerFloorReason != "" {
			p2pMiningFresh = false
			p2pMiningFreshReasonCode = peerFloorReason
		}
	}
	p2pMiningReady := !p2pContextObserved || nodeHealthP2PMiningReady(
		health.ChainCurrent,
		health.P2PCurrent,
		p2pMiningFresh,
	)
	ready := health.MineableNow &&
		health.SubmitReady &&
		health.TemplateAvailable &&
		health.TemplateUsable &&
		!templateStale &&
		((!getTemplateReadyKnown || getTemplateReady) ||
			(health.MineableNow &&
				health.SubmitReady &&
				getTemplateReadyKnown &&
				!getTemplateReady &&
				nodeHealthCachedTemplateSubmitOnlyReason(getTemplateReasonCode))) &&
		p2pMiningReady
	cachedTemplateSubmitOnly := health.MineableNow &&
		health.SubmitReady &&
		health.TemplateAvailable &&
		health.TemplateUsable &&
		!templateStale &&
		getTemplateReadyKnown &&
		!getTemplateReady &&
		nodeHealthCachedTemplateSubmitOnlyReason(getTemplateReasonCode) &&
		p2pMiningReady
	p2pUnsafe := p2pContextObserved && !p2pMiningReady
	p2pUnreadyReason := ""
	if p2pUnsafe {
		p2pUnreadyReason = nodeHealthP2PUnreadyReason(health.ChainCurrent, health.P2PCurrent, p2pMiningFresh, p2pMiningFreshReasonCode)
	}
	templateBootstrapReady := getTemplateReadyKnown &&
		getTemplateReady &&
		nodeHealthCachedTemplateOnlyReason(reasonCode) &&
		!p2pUnsafe
	stateReason := "node-health-" + reasonCode
	if ready {
		stateReason = "node-health-ok"
		if cachedTemplateSubmitOnly {
			stateReason = "node-health-submit-ready-cached-template"
		}
	} else if templateBootstrapReady {
		stateReason = "node-health-get-block-template-ready"
	} else if templateStale {
		stateReason = "node-health-template-stale"
	}
	immediateHardReason := nodeHealthImmediateUnhealthy(reasonCode)
	invalidationCause := normalizeOptionalNodeHealthReason(health.LastTemplateInvalidationCause)
	pendingInvalidationCause := normalizeOptionalNodeHealthReason(health.PendingTemplateInvalidationCause)
	invalidateJobs := false
	invalidateJobsReason := ""

	backend.mu.Lock()
	previousNodeHealthObserved := !backend.state.lastNodeHealthAt.IsZero()
	previousInvalidationSeq := backend.state.nodeHealthLastTemplateInvalidationSeq
	backend.state.lastNodeHealthAt = now
	backend.state.lastNodeHealthLatency = latency
	backend.state.nodeHealthMineable = health.MineableNow
	backend.state.nodeHealthSubmitReady = health.SubmitReady
	backend.state.nodeHealthGetBlockTemplateReadyKnown = getTemplateReadyKnown
	backend.state.nodeHealthGetBlockTemplateReady = getTemplateReady
	backend.state.nodeHealthGetBlockTemplateReasonCode = getTemplateReasonCode
	backend.state.nodeHealthGetBlockTemplateReason = getTemplateReason
	backend.state.nodeHealthTemplate = health.TemplateAvailable
	backend.state.nodeHealthTemplateUsable = health.TemplateUsable
	backend.state.nodeHealthTemplateAge = templateAge
	backend.state.nodeHealthTemplateMaxAge = nodeHealthDurationFromMs(health.TemplateMaxAgeMs)
	backend.state.nodeHealthTemplateSeq = health.LatestTemplateSeq
	backend.state.nodeHealthMainOrder = health.MainOrder
	backend.state.nodeHealthBuildContextObserved = buildContextObserved
	backend.state.nodeHealthLastGoodTemplateAvailable = health.LastGoodTemplateAvailable
	backend.state.nodeHealthLastGoodTemplateAge = nodeHealthDurationFromMs(health.LastGoodTemplateAgeMs)
	backend.state.nodeHealthLastTemplateBuildAge = nodeHealthDurationFromMs(health.LastTemplateBuildAgeMs)
	backend.state.nodeHealthLastTemplateBuildDuration = nodeHealthDurationFromMs(health.LastTemplateBuildDurationMs)
	backend.state.nodeHealthLastTemplateBuildErrorCode = normalizeNodeHealthReason(health.LastTemplateBuildErrorCode)
	backend.state.nodeHealthLastTemplateBuildBlocking = health.LastTemplateBuildBlocking
	backend.state.nodeHealthLastTemplateInvalidationAt = nodeHealthTimeFromUnixMs(health.LastTemplateInvalidationAtUnixMs)
	backend.state.nodeHealthLastTemplateInvalidationSeq = health.LastTemplateInvalidationSeq
	backend.state.nodeHealthLastTemplateInvalidationCause = invalidationCause
	backend.state.nodeHealthLastTemplateInvalidationReason = health.LastTemplateInvalidationReason
	backend.state.nodeHealthLastTemplateInvalidationSubmitObsoleteDepth = health.LastTemplateInvalidationSubmitObsoleteDepth
	backend.state.nodeHealthPendingTemplateBuild = health.PendingTemplateBuild
	backend.state.nodeHealthPendingTemplateBuildReason = normalizeNodeHealthReason(health.PendingTemplateBuildReason)
	backend.state.nodeHealthPendingTemplateBuildPriority = health.PendingTemplateBuildPriority
	backend.state.nodeHealthPendingTemplateInvalidationCause = pendingInvalidationCause
	backend.state.nodeHealthPendingTemplateInvalidationSubmitObsoleteDepth = health.PendingTemplateInvalidationSubmitObsoleteDepth
	backend.state.nodeHealthP2PContextObserved = p2pContextObserved
	backend.state.nodeHealthChainCurrent = health.ChainCurrent
	backend.state.nodeHealthP2PCurrent = health.P2PCurrent
	backend.state.nodeHealthP2PMiningFresh = p2pMiningFresh
	backend.state.nodeHealthP2PMiningFreshReasonCode = p2pMiningFreshReasonCode
	backend.state.nodeHealthP2PFreshnessMaxAge = nodeHealthDurationFromMs(health.P2PFreshnessMaxAgeMs)
	backend.state.nodeHealthP2PPeerLeadToleranceBlocks = health.P2PPeerLeadToleranceBlocks
	backend.state.nodeHealthP2PConsensusPeerCount = health.P2PConsensusPeerCount
	backend.state.nodeHealthP2PFreshConsensusPeerCount = health.P2PFreshConsensusPeerCount
	backend.state.nodeHealthP2PStaleConsensusPeerCount = health.P2PStaleConsensusPeerCount
	backend.state.nodeHealthP2PBestPeerMainOrder = health.P2PBestPeerMainOrder
	backend.state.nodeHealthP2PBestPeerLeadBlocks = health.P2PBestPeerLeadBlocks
	backend.state.nodeHealthP2PBestPeerGraphStateAge = nodeHealthDurationFromMs(health.P2PBestPeerGraphStateAgeMs)
	backend.state.nodeHealthP2PAvgGraphStateAge = nodeHealthDurationFromMs(health.P2PAvgGraphStateAgeMs)
	backend.state.nodeHealthP2PMaxGraphStateAge = nodeHealthDurationFromMs(health.P2PMaxGraphStateAgeMs)
	backend.state.nodeHealthP2PSyncPeerPresent = health.P2PSyncPeerPresent
	backend.state.nodeHealthP2PSyncPeerFresh = health.P2PSyncPeerFresh
	backend.state.nodeHealthP2PSyncPeerGraphStateAge = nodeHealthDurationFromMs(health.P2PSyncPeerGraphStateAgeMs)
	backend.state.nodeHealthReasonCode = reasonCode
	backend.state.nodeHealthReason = health.Reason
	backend.state.nodeHealthError = ""
	backend.state.nodeHealthUnsupported = false
	backend.state.nodeHealthBackoffUntil = time.Time{}
	backend.state.nodeHealthBackoffConsecutive = 0
	backend.state.nodeHealthBackoffReason = ""
	if ready {
		backend.state.nodeHealthConsecutiveUnready = 0
		backend.state.nodeHealthConsecutiveErrors = 0
		backend.state.nodeHealthLastReadyAt = now
		if backendStateHasFreshRouterTemplate(backend.state, now, r.config.nodeHealthFreshTemplateGrace) {
			if !backend.state.healthy {
				backend.state.healthySince = now
			}
			backend.state.healthy = true
			if backend.state.score < 95 {
				backend.state.score = 95
			}
			if backend.state.reason != "ok" {
				backend.state.reason = stateReason
			}
		} else {
			if !backend.state.healthy {
				backend.state.healthySince = now
			}
			backend.state.healthy = true
			if backend.state.score < 85 {
				backend.state.score = 85
			}
			backend.state.reason = "node-health-ok-awaiting-template"
		}
	} else if templateBootstrapReady {
		backend.state.nodeHealthConsecutiveUnready = 0
		backend.state.nodeHealthConsecutiveErrors = 0
		if backendStateHasFreshRouterTemplate(backend.state, now, r.config.nodeHealthFreshTemplateGrace) {
			if !backend.state.healthy {
				backend.state.healthySince = now
			}
			backend.state.healthy = true
			if backend.state.score < 90 {
				backend.state.score = 90
			}
			backend.state.reason = stateReason
		} else {
			if !backend.state.healthy {
				backend.state.healthySince = now
			}
			backend.state.healthy = true
			if backend.state.score < 80 {
				backend.state.score = 80
			}
			backend.state.reason = "node-health-get-block-template-ready-awaiting-template"
		}
	} else {
		backend.state.nodeHealthConsecutiveUnready++
		backend.state.nodeHealthConsecutiveErrors = 0
		if health.LastTemplateBuildError != "" {
			backend.state.lastTemplateError = health.LastTemplateBuildError
		} else {
			backend.state.lastTemplateError = health.Reason
		}
		freshEvidence := backendStateHasFreshRouterTemplate(backend.state, now, r.config.nodeHealthFreshTemplateGrace)
		getTemplateVeto := getTemplateReadyKnown && !getTemplateReady
		unreadyReasonCode := reasonCode
		if getTemplateVeto && getTemplateReasonCode != "unknown" {
			unreadyReasonCode = getTemplateReasonCode
		}
		if !getTemplateVeto && p2pUnsafe && nodeHealthCachedTemplateOnlyReason(unreadyReasonCode) {
			unreadyReasonCode = p2pUnreadyReason
		}
		lastGoodAge := nodeHealthDurationFromMs(health.LastGoodTemplateAgeMs)
		if lastGoodAge < 0 {
			lastGoodAge = 0
		}
		grace := r.config.nodeHealthFreshTemplateGrace
		if grace <= 0 {
			grace = time.Duration(defaultRPCRouterNodeHealthFreshGraceSeconds) * time.Second
		}
		tipChaseAdvisory := nodeHealthTipChaseAdvisory(
			unreadyReasonCode,
			freshEvidence,
			health,
			p2pContextObserved,
			p2pMiningFresh,
			lastGoodAge,
			grace,
			templateStale,
		)
		freshTemplateOverride := tipChaseAdvisory || (freshEvidence &&
			nodeHealthFreshTemplateOverridesUnready(unreadyReasonCode) &&
			health.MineableNow &&
			health.SubmitReady &&
			health.TemplateAvailable &&
			health.TemplateUsable &&
			health.LastGoodTemplateAvailable &&
			lastGoodAge <= grace &&
			!templateStale &&
			p2pContextObserved &&
			p2pMiningReady)
		templateReadinessVeto := getTemplateVeto &&
			!cachedTemplateSubmitOnly &&
			nodeHealthTemplateReadinessBlocksMining(unreadyReasonCode) &&
			!freshTemplateOverride
		staleEvidenceVeto := !freshTemplateOverride &&
			(immediateHardReason || (!freshEvidence && (getTemplateVeto || !health.TemplateAvailable || reasonCode != "ok")))
		immediateUnhealthy := p2pUnsafe || templateReadinessVeto || staleEvidenceVeto
		thresholdReached := !freshTemplateOverride && backend.state.nodeHealthConsecutiveUnready >= intOrDefault(r.config.nodeHealthUnreadyThreshold, defaultRPCRouterNodeHealthUnreadyThreshold)
		submitUnsafe := (getTemplateVeto && !cachedTemplateSubmitOnly) || !health.SubmitReady || p2pUnsafe
		if !freshTemplateOverride && submitUnsafe && (backend.state.nodeHealthLastJobInvalidationAt.IsZero() || now.Sub(backend.state.nodeHealthLastJobInvalidationAt) >= nodeHealthJobInvalidationThrottle) {
			backend.state.nodeHealthLastJobInvalidationAt = now
			invalidateJobs = true
			invalidateJobsReason = "node health unready: " + unreadyReasonCode
			if p2pUnsafe {
				invalidateJobsReason = "node health p2p unready: " + p2pUnreadyReason
			}
		}
		if immediateUnhealthy || thresholdReached {
			backend.state.healthy = false
			backend.state.score = 0
			backend.state.reason = "node-health-" + unreadyReasonCode
		} else if freshTemplateOverride {
			if !backend.state.healthy {
				backend.state.healthySince = now
			}
			backend.state.healthy = true
			if backend.state.score < 90 {
				backend.state.score = 90
			}
			backend.state.reason = "node-health-advisory-" + unreadyReasonCode
		} else {
			if backend.state.healthy && backend.state.score > 90 {
				backend.state.score = 90
			}
			if backend.state.healthy {
				backend.state.reason = "node-health-advisory-" + unreadyReasonCode
			}
		}
	}
	backend.mu.Unlock()

	if invalidateJobs {
		r.invalidateBackendJobsFromNodeHealth(backend, invalidateJobsReason)
	}
	if shouldHandleNodeTemplateInvalidation(previousNodeHealthObserved, previousInvalidationSeq, health.LastTemplateInvalidationSeq, invalidationCause) {
		r.handleNodeTemplateInvalidation(backend, health.LastTemplateInvalidationSeq, invalidationCause, health.LastTemplateInvalidationReason, health.TemplateParentRoot)
	}
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) invalidateBackendJobsFromNodeHealth(backend *rpcBackend, reason string) {
	if r == nil || backend == nil {
		return
	}
	if reason == "" {
		reason = "node health unready"
	}
	if r.preserveCurrentJobsForTransientNodeHealth(backend, reason) {
		return
	}
	invalidated := invalidateAcceptedJobsForBackend(backend.name, reason)
	if invalidated > 0 {
		log.Printf("[ROUTER] invalidated %d accepted jobs for backend=%s reason=%s", invalidated, backend.name, reason)
	}
	if backend == r.selectedBackend() {
		forceCleanTemplateRefreshForBackend(reason, backend.name)
		return
	}
}

func shouldHandleNodeTemplateInvalidation(previousHealthObserved bool, previousSeq, currentSeq uint64, cause string) bool {
	// The node reports its most recent invalidation as durable health state. On
	// the first probe that sequence is a baseline, not an event that occurred
	// during this pool process. Replaying it can populate the pool with a cached
	// template before a long startup preflight has finished. A later 0 -> 1
	// transition remains observable because previousHealthObserved is then true.
	if !previousHealthObserved {
		return false
	}
	if currentSeq == 0 || currentSeq <= previousSeq {
		return false
	}
	switch cause {
	case "parent-changed", "state-changed", "mempool-root-changed", "template-sequence-obsolete", "submit-obsolete-depth":
		return true
	default:
		return false
	}
}

func nodeHealthCachedTemplateOnlyReason(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "ok",
		"template-unavailable":
		return true
	default:
		return false
	}
}

func nodeHealthCachedTemplateSubmitOnlyReason(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "node-syncing",
		"get-block-template-not-ready",
		"template-not-ready",
		"pool-syncing",
		"graph-state-sync",
		"reorg-pressure":
		return true
	default:
		return false
	}
}

func nodeHealthTemplateRefreshableReason(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "template-expired",
		"template-parent-stale",
		"template-stale":
		return true
	default:
		return false
	}
}

func nodeHealthPeerFloorReason(minPeers, consensusPeers, freshPeers int) string {
	if minPeers <= 0 {
		return ""
	}
	if consensusPeers < minPeers {
		return "not-enough-consensus-peers"
	}
	if freshPeers < minPeers {
		return "not-enough-fresh-consensus-peers"
	}
	return ""
}

func nodeHealthP2PMiningReady(chainCurrent, current, miningFresh bool) bool {
	return miningFresh && (current || chainCurrent)
}

func nodeHealthP2PUnreadyReason(chainCurrent, current, miningFresh bool, reason string) string {
	reason = normalizeNodeHealthReason(reason)
	if !miningFresh {
		if reason == "unknown" || reason == "ok" {
			return "p2p-not-fresh"
		}
		return reason
	}
	if !current && !chainCurrent {
		if reason == "unknown" || reason == "ok" {
			return "p2p-not-current"
		}
		return reason
	}
	return reason
}

func (r *rpcRouter) handleNodeTemplateInvalidation(backend *rpcBackend, seq uint64, cause, reason, templateParent string) {
	if r == nil || backend == nil {
		return
	}
	if rpcBackendNodeHealthTemplateInvalidations != nil {
		rpcBackendNodeHealthTemplateInvalidations.WithLabelValues(backend.name, cause).Inc()
	}
	if rpcBackendNodeHealthLastTemplateInvalidationSeq != nil {
		rpcBackendNodeHealthLastTemplateInvalidationSeq.WithLabelValues(backend.name, cause).Set(float64(seq))
	}
	if backend != r.selectedBackend() {
		return
	}
	if reason == "" {
		reason = cause
	}
	if nodeTemplateInvalidationCanPreserveCurrentJobs(cause, templateParent) {
		preserved := preservableCurrentJobsForBackend(backend.name, time.Now())
		if preserved > 0 {
			log.Printf("[ROUTER] preserving %d current jobs for backend=%s during node template invalidation cause=%s reason=%s",
				preserved, backend.name, cause, reason)
			return
		}
	}
	refreshReason := fmt.Sprintf("node template invalidation: %s", reason)
	invalidated := invalidateAcceptedJobsForBackend(backend.name, refreshReason)
	if invalidated > 0 {
		log.Printf("[ROUTER] invalidated %d accepted jobs for backend=%s reason=%s", invalidated, backend.name, refreshReason)
	}
	forceCleanTemplateRefreshForBackend(refreshReason, backend.name)
}

func (r *rpcRouter) recordNodeTemplateHealthError(backend *rpcBackend, err error) {
	if r == nil || backend == nil || err == nil {
		return
	}
	reason := nodeHealthErrorReason(err)
	unsupported := nodeHealthUnsupported(err)
	now := time.Now()

	backend.mu.Lock()
	backend.state.lastNodeHealthAt = now
	backend.state.nodeHealthMineable = false
	backend.state.nodeHealthSubmitReady = false
	backend.state.nodeHealthReasonCode = reason
	backend.state.nodeHealthReason = err.Error()
	backend.state.nodeHealthError = err.Error()
	backend.state.nodeHealthUnsupported = unsupported
	if unsupported {
		backend.state.nodeHealthConsecutiveErrors = 0
		if backend.state.score > 85 {
			backend.state.score = 85
		}
		backend.state.reason = "node-health-unsupported"
	} else {
		backend.state.lastTemplateError = err.Error()
		backend.state.nodeHealthConsecutiveErrors++
		backend.state.nodeHealthConsecutiveUnready = 0
		freshEvidence := backendStateHasFreshRouterTemplate(backend.state, now, r.config.nodeHealthFreshTemplateGrace)
		thresholdReached := backend.state.nodeHealthConsecutiveErrors >= intOrDefault(r.config.nodeHealthErrorThreshold, defaultRPCRouterNodeHealthErrorThreshold)
		if !freshEvidence || thresholdReached {
			backend.state.score = 0
			backend.state.healthy = false
			backend.state.reason = "node-health-" + reason
		} else if backend.state.healthy {
			if backend.state.score > 85 {
				backend.state.score = 85
			}
			backend.state.reason = "node-health-advisory-" + reason
		}
	}
	backend.mu.Unlock()

	if !unsupported {
		r.invalidateBackendJobsFromNodeHealthError(backend, "node health error: "+reason)
	}
	if rpcBackendNodeHealthErrors != nil {
		rpcBackendNodeHealthErrors.WithLabelValues(backend.name, reason).Inc()
	}
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) invalidateBackendJobsFromNodeHealthError(backend *rpcBackend, reason string) {
	if r == nil || backend == nil {
		return
	}
	if reason == "" {
		reason = "node health error"
	}
	if r.preserveCurrentJobsForTransientNodeHealth(backend, reason) {
		return
	}
	if backend == r.selectedBackend() {
		clearCachedBlockTemplate()
	}
	invalidated := invalidateAcceptedJobsForBackend(backend.name, reason)
	if invalidated > 0 {
		log.Printf("[ROUTER] invalidated %d accepted jobs for backend=%s reason=%s", invalidated, backend.name, reason)
	}
}

func (r *rpcRouter) preserveCurrentJobsForTransientNodeHealth(backend *rpcBackend, reason string) bool {
	if r == nil || backend == nil || !nodeHealthSignalCanPreserveCurrentJobs(reason) {
		return false
	}
	preserved := preservableCurrentJobsForBackend(backend.name, time.Now())
	if preserved == 0 {
		return false
	}
	log.Printf("[ROUTER] preserving %d current jobs for backend=%s during transient node health signal reason=%s",
		preserved, backend.name, reason)
	return true
}

func nodeHealthSignalCanPreserveCurrentJobs(reason string) bool {
	raw := strings.ToLower(strings.TrimSpace(reason))
	if strings.Contains(raw, "transport") ||
		strings.Contains(raw, "timeout") ||
		strings.Contains(raw, "too busy") ||
		strings.Contains(raw, "too-busy") ||
		strings.Contains(raw, "http-retryable") {
		return true
	}
	switch normalizeNodeHealthReason(reason) {
	case "transport",
		"timeout",
		"http-retryable",
		"too-busy",
		"node-health-transport",
		"node-health-timeout",
		"node-health-http-retryable",
		"node-health-too-busy",
		"node-health-error-transport",
		"node-health-error-timeout",
		"node-health-error-http-retryable",
		"node-health-error-too-busy":
		return true
	default:
		return false
	}
}

func nodeTemplateInvalidationCanPreserveCurrentJobs(cause, templateParent string) bool {
	return false
}

func nodeHealthImmediateUnhealthy(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "miner-disabled",
		"miner-not-started",
		"miner-shutdown",
		"worker-unavailable",
		"worker-stopped",
		"p2p-unavailable",
		"p2p-not-fresh",
		"peer-lag",
		"no-consensus-peers",
		"no-fresh-consensus-peers",
		"chain-unavailable",
		"template-expired",
		"template-parent-stale",
		"template-stale":
		return true
	default:
		return false
	}
}

func nodeHealthFreshTemplateOverridesUnready(reason string) bool {
	return nodeHealthCachedTemplateSubmitOnlyReason(reason)
}

// nodeHealthTipChaseAdvisory is true when the node is only briefly unable to
// build a *new* template (tip-chase micro-sync) while ASICs still have young,
// tip-aligned work. Real catch-up (peer lead / stale P2P) stays hard-gated.
func nodeHealthTipChaseAdvisory(
	reason string,
	freshEvidence bool,
	health *nodeTemplateHealthResult,
	p2pContextObserved bool,
	p2pMiningFresh bool,
	lastGoodAge time.Duration,
	grace time.Duration,
	templateStale bool,
) bool {
	if health == nil || !freshEvidence || templateStale {
		return false
	}
	if !nodeHealthFreshTemplateOverridesUnready(reason) {
		return false
	}
	if !health.LastGoodTemplateAvailable || lastGoodAge > grace {
		return false
	}
	if !health.TemplateAvailable || !health.TemplateUsable {
		return false
	}
	if !p2pContextObserved || !health.P2PCurrent || !p2pMiningFresh {
		return false
	}
	return nodeHealthPeerLeadAllowsTipChase(health)
}

func nodeHealthPeerLeadAllowsTipChase(health *nodeTemplateHealthResult) bool {
	if health == nil {
		return false
	}
	tolerance := health.P2PPeerLeadToleranceBlocks
	if tolerance == 0 {
		tolerance = 10
	}
	lead := health.P2PBestPeerLeadBlocks
	if lead < 0 {
		lead = 0
	}
	return lead <= int64(tolerance)
}

func nodeHealthProbeBackoffReason(err error) (string, bool) {
	reason := nodeHealthErrorReason(err)
	switch normalizeNodeHealthReason(reason) {
	case "too-busy", "timeout", "transport", "http-retryable":
		return normalizeNodeHealthReason(reason), true
	default:
		return "", false
	}
}

func nodeHealthBackoffDuration(consecutive int) time.Duration {
	if consecutive < 1 {
		consecutive = 1
	}
	backoff := defaultNodeHealthErrorBackoff
	for i := 1; i < consecutive && backoff < maxNodeHealthErrorBackoff; i++ {
		backoff *= 2
	}
	if backoff > maxNodeHealthErrorBackoff {
		backoff = maxNodeHealthErrorBackoff
	}
	return backoff + jitterDuration(backoff/2)
}

func recordNodeHealthProbeBackoff(backend *rpcBackend, reason string) {
	if backend == nil {
		return
	}
	reason = normalizeNodeHealthReason(reason)
	if reason == "" || reason == "unknown" {
		reason = "retryable"
	}
	now := time.Now()
	backend.mu.Lock()
	backend.state.nodeHealthBackoffConsecutive++
	backoff := nodeHealthBackoffDuration(backend.state.nodeHealthBackoffConsecutive)
	backend.state.nodeHealthBackoffUntil = now.Add(backoff)
	backend.state.nodeHealthBackoffReason = reason
	backend.mu.Unlock()
}

func nodeHealthProbeBackoffActive(backend *rpcBackend, now time.Time) (time.Duration, string, bool) {
	if backend == nil {
		return 0, "", false
	}
	backend.mu.RLock()
	until := backend.state.nodeHealthBackoffUntil
	reason := backend.state.nodeHealthBackoffReason
	backend.mu.RUnlock()
	if until.IsZero() || !now.Before(until) {
		return 0, "", false
	}
	return until.Sub(now), reason, true
}

func nodeHealthTemplateReadinessBlocksMining(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "node-syncing",
		"initial-download",
		"get-block-template-not-ready",
		"template-not-ready",
		"pool-syncing",
		"graph-state-sync",
		"reorg-pressure",
		"template-expired",
		"template-parent-stale",
		"template-stale",
		"p2p-unavailable",
		"p2p-not-fresh",
		"p2p-not-current",
		"peer-lag",
		"no-consensus-peers",
		"no-fresh-consensus-peers":
		return true
	default:
		return false
	}
}

func backendStateHasFreshRouterTemplate(state backendState, now time.Time, grace time.Duration) bool {
	if grace <= 0 {
		grace = time.Duration(defaultRPCRouterNodeHealthFreshGraceSeconds) * time.Second
	}
	if !state.lastTemplateAt.IsZero() {
		age := now.Sub(state.lastTemplateAt)
		if age < 0 {
			age = 0
		}
		if age <= grace {
			return true
		}
	}
	return false
}

func startRouterNodeHealthProbe(router *rpcRouter, stop <-chan struct{}) {
	if router == nil || !router.config.nodeHealthEnabled || router.config.nodeHealthProbe <= 0 {
		return
	}
	ticker := time.NewTicker(router.config.nodeHealthProbe)
	go func() {
		defer ticker.Stop()
		probeRouterNodeHealthLoop(router, stop, ticker.C, func(r *rpcRouter) {
			r.probeNodeHealthBackends()
		})
	}()
}

func primeRouterNodeHealth(router *rpcRouter) int {
	if router == nil || !router.config.nodeHealthEnabled {
		return 0
	}
	return router.probeNodeHealthBackends()
}

func probeRouterNodeHealthLoop(router *rpcRouter, stop <-chan struct{}, ticks <-chan time.Time, probe func(*rpcRouter)) {
	if probe == nil {
		return
	}
	for {
		select {
		case <-stop:
			return
		case <-ticks:
			probe(router)
		}
	}
}

func (r *rpcRouter) probeNodeHealthBackends() int {
	if r == nil || !r.config.nodeHealthEnabled {
		return 0
	}
	backend := r.selectedBackend()
	if backend == nil {
		return 0
	}
	if delay, reason, active := nodeHealthProbeBackoffActive(backend, time.Now()); active {
		if reason == "" {
			reason = "retryable"
		}
		log.Printf("[ROUTER] skipping getTemplateHealth during backoff backend=%s reason=%s remaining=%s",
			backend.name, reason, delay.Round(time.Millisecond))
		return 0
	}
	health, latency, err := fetchNodeTemplateHealthFromBackend(backend)
	if err != nil {
		r.recordNodeTemplateHealthError(backend, err)
		if reason, ok := nodeHealthProbeBackoffReason(err); ok {
			recordNodeHealthProbeBackoff(backend, reason)
		}
		return 1
	}
	r.recordNodeTemplateHealth(backend, health, latency)
	return 1
}
