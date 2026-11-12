package server

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultRPCRouterTemplateMaxAgeSeconds       = 5
	defaultRPCRouterFreezeMaxTemplateAgeSeconds = 90
	defaultRouterMetricsRefreshInterval         = 2 * time.Second
	defaultRPCRouterNodeHealthProbeSeconds      = 15
	minRPCRouterNodeHealthProbeSeconds          = 10
	defaultRPCRouterNodeHealthMaxAgeSeconds     = 10
	defaultRPCRouterNodeHealthUnreadyThreshold  = 3
	defaultRPCRouterNodeHealthErrorThreshold    = 3
	defaultRPCRouterNodeHealthFreshGraceSeconds = 15
	defaultRPCRouterNodeHealthMinConsensusPeers = 2
	defaultRPCRouterSubmitReadinessHold         = 3 * time.Second
	defaultRPCRouterTemplateFetchReadinessHold  = 15 * time.Second
	defaultRPCRouterSubmitReadinessFreshGrace   = 3 * time.Second
)

type rpcRouterConfig struct {
	templateMaxAge               time.Duration
	freezeMaxTemplateAge         time.Duration
	submitTimeout                time.Duration
	nodeHealthEnabled            bool
	nodeHealthProbe              time.Duration
	nodeHealthMaxAge             time.Duration
	nodeHealthUnreadyThreshold   int
	nodeHealthErrorThreshold     int
	nodeHealthFreshTemplateGrace time.Duration
	nodeHealthMinConsensusPeers  int
	backend                      rpcBackendConfig
}

type rpcBackendConfig struct {
	name        string
	url         string
	wsURL       string
	rpcUser     string
	rpcPassword string
}

type rpcRouter struct {
	config                rpcRouterConfig
	backend               *rpcBackend
	chainIdentityEnforced bool
}

type rpcBackend struct {
	name                string
	url                 string
	wsURL               string
	rpcUser             string
	rpcPassword         string
	rpcAuthHeader       string
	gbtClient           *http.Client
	submitClient        *http.Client
	submitV2Probed      atomic.Bool
	submitV2Unsupported atomic.Bool

	mu    sync.RWMutex
	state backendState
}

type backendState struct {
	chainIdentity         chainIdentityV1
	chainIdentityPinned   bool
	chainIdentityVerified bool
	chainIdentityFatal    bool
	chainIdentityError    string
	lastChainIdentityAt   time.Time
	lastTemplateAt        time.Time
	lastTemplateHeight    int64
	lastTemplateParent    string
	lastTemplateAge       time.Duration
	lastTemplateSeq       uint64
	lastTemplateError     string
	lastTemplateLatency   time.Duration
	healthySince          time.Time

	wsConnected     bool
	lastWSAt        time.Time
	lastReconnectAt time.Time
	reconnects      uint64

	lastSubmitAt              time.Time
	lastSubmitSuccessAt       time.Time
	lastSubmitError           string
	recentSubmitErrors        uint64
	submitReadinessHoldUntil  time.Time
	submitReadinessHoldReason string

	lastNodeHealthAt                                         time.Time
	lastNodeHealthLatency                                    time.Duration
	nodeHealthMineable                                       bool
	nodeHealthSubmitReady                                    bool
	nodeHealthGetBlockTemplateReadyKnown                     bool
	nodeHealthGetBlockTemplateReady                          bool
	nodeHealthGetBlockTemplateReasonCode                     string
	nodeHealthGetBlockTemplateReason                         string
	nodeHealthTemplate                                       bool
	nodeHealthTemplateUsable                                 bool
	nodeHealthTemplateAge                                    time.Duration
	nodeHealthTemplateMaxAge                                 time.Duration
	nodeHealthTemplateSeq                                    uint64
	nodeHealthMainOrder                                      uint64
	nodeHealthBuildContextObserved                           bool
	nodeHealthLastGoodTemplateAvailable                      bool
	nodeHealthLastGoodTemplateAge                            time.Duration
	nodeHealthLastTemplateBuildAge                           time.Duration
	nodeHealthLastTemplateBuildDuration                      time.Duration
	nodeHealthLastTemplateBuildErrorCode                     string
	nodeHealthLastTemplateBuildBlocking                      bool
	nodeHealthLastTemplateInvalidationAt                     time.Time
	nodeHealthLastTemplateInvalidationSeq                    uint64
	nodeHealthLastTemplateInvalidationCause                  string
	nodeHealthLastTemplateInvalidationReason                 string
	nodeHealthLastTemplateInvalidationSubmitObsoleteDepth    int64
	nodeHealthPendingTemplateBuild                           bool
	nodeHealthPendingTemplateBuildReason                     string
	nodeHealthPendingTemplateBuildPriority                   int
	nodeHealthPendingTemplateInvalidationCause               string
	nodeHealthPendingTemplateInvalidationSubmitObsoleteDepth int64
	nodeHealthP2PContextObserved                             bool
	nodeHealthChainCurrent                                   bool
	nodeHealthP2PCurrent                                     bool
	nodeHealthP2PMiningFresh                                 bool
	nodeHealthP2PMiningFreshReasonCode                       string
	nodeHealthP2PFreshnessMaxAge                             time.Duration
	nodeHealthP2PPeerLeadToleranceBlocks                     uint64
	nodeHealthP2PConsensusPeerCount                          int
	nodeHealthP2PFreshConsensusPeerCount                     int
	nodeHealthP2PStaleConsensusPeerCount                     int
	nodeHealthP2PBestPeerMainOrder                           uint64
	nodeHealthP2PBestPeerLeadBlocks                          int64
	nodeHealthP2PBestPeerGraphStateAge                       time.Duration
	nodeHealthP2PAvgGraphStateAge                            time.Duration
	nodeHealthP2PMaxGraphStateAge                            time.Duration
	nodeHealthP2PSyncPeerPresent                             bool
	nodeHealthP2PSyncPeerFresh                               bool
	nodeHealthP2PSyncPeerGraphStateAge                       time.Duration
	nodeHealthReasonCode                                     string
	nodeHealthReason                                         string
	nodeHealthError                                          string
	nodeHealthUnsupported                                    bool
	nodeHealthConsecutiveUnready                             int
	nodeHealthConsecutiveErrors                              int
	nodeHealthBackoffUntil                                   time.Time
	nodeHealthBackoffConsecutive                             int
	nodeHealthBackoffReason                                  string
	nodeHealthLastReadyAt                                    time.Time
	nodeHealthLastJobInvalidationAt                          time.Time
	score                                                    float64
	healthy                                                  bool
	reason                                                   string
}

func newRPCRouterFromConfig(cfg Config) (*rpcRouter, error) {
	routerCfg, err := buildRPCRouterConfig(cfg)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	backendCfg := routerCfg.backend
	backend := &rpcBackend{
		name:          backendCfg.name,
		url:           backendCfg.url,
		wsURL:         backendCfg.wsURL,
		rpcUser:       backendCfg.rpcUser,
		rpcPassword:   backendCfg.rpcPassword,
		rpcAuthHeader: basicAuthHeader(backendCfg.rpcUser, backendCfg.rpcPassword),
		gbtClient:     gbtHTTPClient,
		submitClient:  buildSubmitHTTPClient(int(routerCfg.submitTimeout / time.Millisecond)),
		state: backendState{
			score:        100,
			healthy:      true,
			healthySince: now,
			reason:       "not-yet-measured",
		},
	}
	return &rpcRouter{config: routerCfg, backend: backend, chainIdentityEnforced: true}, nil
}

func buildRPCRouterConfig(cfg Config) (rpcRouterConfig, error) {
	templateMaxAge := secondsOrDefault(cfg.RPCRouterTemplateMaxAgeSeconds, defaultRPCRouterTemplateMaxAgeSeconds)
	freezeMaxTemplateAge := secondsOrDefault(cfg.RPCRouterFreezeMaxTemplateAgeSeconds, defaultRPCRouterFreezeMaxTemplateAgeSeconds)
	if cfg.RPCRouterNodeHealthProbeSeconds > 0 && cfg.RPCRouterNodeHealthProbeSeconds < minRPCRouterNodeHealthProbeSeconds {
		return rpcRouterConfig{}, fmt.Errorf("RPC router node health probe cadence must be at least %d seconds", minRPCRouterNodeHealthProbeSeconds)
	}
	nodeHealthProbe := secondsOrDefault(cfg.RPCRouterNodeHealthProbeSeconds, defaultRPCRouterNodeHealthProbeSeconds)
	nodeHealthMaxAge := secondsOrDefault(cfg.RPCRouterNodeHealthMaxAgeSeconds, defaultRPCRouterNodeHealthMaxAgeSeconds)
	nodeHealthUnreadyThreshold := intOrDefault(cfg.RPCRouterNodeHealthUnreadyThreshold, defaultRPCRouterNodeHealthUnreadyThreshold)
	nodeHealthErrorThreshold := intOrDefault(cfg.RPCRouterNodeHealthErrorThreshold, defaultRPCRouterNodeHealthErrorThreshold)
	nodeHealthFreshTemplateGrace := secondsOrDefault(cfg.RPCRouterNodeHealthFreshTemplateGraceSeconds, defaultRPCRouterNodeHealthFreshGraceSeconds)
	nodeHealthMinConsensusPeers := intOrDefault(cfg.RPCRouterNodeHealthMinConsensusPeers, defaultRPCRouterNodeHealthMinConsensusPeers)

	endpointEntries := make([]namedCoreRPCEndpointInput, 0, len(cfg.RPCEndpointAliases)+1)
	if cfg.RPCURL != "" {
		endpointEntries = append(endpointEntries, namedCoreRPCEndpointInput{name: "RPCURL", value: cfg.RPCURL})
	}
	aliasNames := make([]string, 0, len(cfg.RPCEndpointAliases))
	for name := range cfg.RPCEndpointAliases {
		if !isCoreRPCEndpointAlias(name) {
			return rpcRouterConfig{}, fmt.Errorf("unsupported Core RPC endpoint input %q", name)
		}
		aliasNames = append(aliasNames, name)
	}
	sort.Strings(aliasNames)
	for _, name := range aliasNames {
		endpointEntries = append(endpointEntries, namedCoreRPCEndpointInput{name: name, value: cfg.RPCEndpointAliases[name]})
	}
	if len(endpointEntries) == 0 {
		endpointEntries = append(endpointEntries, namedCoreRPCEndpointInput{name: "RPCURL", value: rpcURL})
	}
	backendURL, err := resolveCoreRPCEndpointEntries(endpointEntries, true)
	if err != nil {
		return rpcRouterConfig{}, err
	}
	backend, err := buildRPCBackendConfig("node", backendURL, cfg.RPCUser, cfg.RPCPassword)
	if err != nil {
		return rpcRouterConfig{}, err
	}

	return rpcRouterConfig{
		templateMaxAge:               templateMaxAge,
		freezeMaxTemplateAge:         freezeMaxTemplateAge,
		submitTimeout:                configuredSubmitRPCTimeout(cfg.SubmitRPCTimeoutMS),
		nodeHealthEnabled:            cfg.RPCRouterNodeHealthEnabled,
		nodeHealthProbe:              nodeHealthProbe,
		nodeHealthMaxAge:             nodeHealthMaxAge,
		nodeHealthUnreadyThreshold:   nodeHealthUnreadyThreshold,
		nodeHealthErrorThreshold:     nodeHealthErrorThreshold,
		nodeHealthFreshTemplateGrace: nodeHealthFreshTemplateGrace,
		nodeHealthMinConsensusPeers:  nodeHealthMinConsensusPeers,
		backend:                      backend,
	}, nil
}

func buildRPCBackendConfig(name, rawURL, rpcUser, rpcPassword string) (rpcBackendConfig, error) {
	normalizedURL, err := normalizeCoreRPCEndpoint(rawURL)
	if err != nil {
		return rpcBackendConfig{}, err
	}
	wsURL, err := rpcToWSURL(normalizedURL)
	if err != nil {
		return rpcBackendConfig{}, err
	}
	return rpcBackendConfig{
		name:        name,
		url:         normalizedURL,
		wsURL:       wsURL,
		rpcUser:     rpcUser,
		rpcPassword: rpcPassword,
	}, nil
}

func secondsOrDefault(value, defaultValue int) time.Duration {
	if value <= 0 {
		value = defaultValue
	}
	return time.Duration(value) * time.Second
}

func intOrDefault(value, defaultValue int) int {
	if value <= 0 {
		return defaultValue
	}
	return value
}

func (r *rpcRouter) selectedBackend() *rpcBackend {
	if r == nil {
		return nil
	}
	return r.backend
}

func (r *rpcRouter) recordTemplateFallbackGuardRejected(backend *rpcBackend, fallback, reason string) {
	if r == nil || backend == nil {
		return
	}
	if reason == "" {
		reason = "template-fallback-backend-not-ready"
	}
	if fallback == "" {
		fallback = "template"
	}
	backend.mu.Lock()
	backend.state.healthy = false
	backend.state.score = 0
	backend.state.reason = reason
	backend.state.lastTemplateError = fallback + " fallback rejected: " + reason
	backend.mu.Unlock()
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) recordTemplateSuccess(backend *rpcBackend, tpl *BlockTemplate, latency time.Duration) bool {
	if r == nil || backend == nil {
		return false
	}

	now := time.Now()
	parent := ""
	height := int64(0)
	sourceSeq := uint64(0)
	if tpl != nil {
		parent = normalizeHex(tpl.PreviousHash)
		height = tpl.Height
		sourceSeq = tpl.SourceSeq
	}

	backend.mu.Lock()
	previousSeq := backend.state.lastTemplateSeq
	accepted, rejectReason := classifyTemplateSequenceUpdate(previousSeq, backend.state.lastTemplateHeight, backend.state.lastTemplateParent, sourceSeq, height, parent)
	if !accepted {
		backend.mu.Unlock()
		publishTemplateSequenceReject(backend.name, rejectReason)
		log.Printf("[WS] Dropping %s template sequence backend=%s seq=%d previous=%d height=%d parent=%s",
			rejectReason, backend.name, sourceSeq, previousSeq, height, parent)
		return false
	}
	backend.state.lastTemplateAt = now
	backend.state.lastTemplateHeight = height
	backend.state.lastTemplateParent = parent
	backend.state.lastTemplateAge = 0
	if sourceSeq > 0 {
		backend.state.lastTemplateSeq = sourceSeq
	}
	backend.state.lastTemplateError = ""
	backend.state.lastTemplateLatency = latency
	if !backend.state.submitReadinessHoldUntil.IsZero() && !now.Before(backend.state.submitReadinessHoldUntil) {
		backend.state.submitReadinessHoldUntil = time.Time{}
		backend.state.submitReadinessHoldReason = ""
	}
	if now.Before(backend.state.submitReadinessHoldUntil) {
		reason := submitReadinessHoldReason(backend.state.submitReadinessHoldReason)
		backend.state.score = 25
		backend.state.healthy = false
		backend.state.reason = reason
	} else {
		backend.state.score = 100
		if !backend.state.healthy {
			backend.state.healthySince = now
		}
		backend.state.healthy = true
		backend.state.reason = "ok"
	}
	backend.mu.Unlock()

	if sourceSeq > 0 {
		publishTemplateSequenceMetrics(backend.name, previousSeq, sourceSeq)
	}
	r.publishBackendMetrics(backend)
	return true
}

func (r *rpcRouter) recordTemplateError(backend *rpcBackend, err error) {
	if r == nil || backend == nil || err == nil {
		return
	}

	reason := classifyRouterError(err)
	backend.mu.Lock()
	backend.state.lastTemplateError = err.Error()
	backend.state.lastTemplateAge = time.Since(backend.state.lastTemplateAt)
	backend.state.score = 0
	backend.state.healthy = false
	backend.state.reason = reason
	backend.mu.Unlock()

	if rpcBackendTemplateErrors != nil {
		rpcBackendTemplateErrors.WithLabelValues(backend.name, reason).Inc()
	}
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) shouldHoldTemplateBackendOnError(backend *rpcBackend, err error) bool {
	if r == nil || backend == nil || err == nil {
		return false
	}
	selected := r.selectedBackend()
	if selected == nil || selected != backend {
		return false
	}
	reason, benign := classifyTemplateFetchError(err)
	if !benign || reason == "hard" {
		return false
	}
	state := backend.snapshotState()
	if !state.wsConnected || state.lastTemplateAt.IsZero() {
		return false
	}
	return time.Since(state.lastTemplateAt) <= r.config.templateMaxAge
}

func (r *rpcRouter) recordTransientTemplateError(backend *rpcBackend, err error) {
	if r == nil || backend == nil || err == nil {
		return
	}

	reason, benign := classifyTemplateFetchError(err)
	if !benign || reason == "hard" {
		reason = classifyRouterError(err)
	}
	reason = "transient-" + reason

	backend.mu.Lock()
	backend.state.lastTemplateError = err.Error()
	backend.state.lastTemplateAge = time.Since(backend.state.lastTemplateAt)
	if backend.state.score > 90 {
		backend.state.score = 90
	}
	backend.state.healthy = true
	backend.state.reason = reason
	backend.mu.Unlock()

	if rpcBackendTemplateErrors != nil {
		rpcBackendTemplateErrors.WithLabelValues(backend.name, reason).Inc()
	}
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) recordTemplateRefreshReadinessHold(backend *rpcBackend, reason string, err error) {
	r.recordTemplateReadinessHold(backend, "template-refresh", reason, err, defaultRPCRouterSubmitReadinessHold)
}

func (r *rpcRouter) recordTemplateFetchReadinessHold(backend *rpcBackend, reason string, err error) {
	r.recordTemplateReadinessHold(backend, "template-fetch", reason, err, defaultRPCRouterTemplateFetchReadinessHold)
}

func (r *rpcRouter) recordTemplateReadinessHold(backend *rpcBackend, source, reason string, err error, hold time.Duration) {
	if r == nil || backend == nil {
		return
	}
	source = strings.TrimSpace(source)
	if source == "" {
		source = "template-fetch"
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "template-not-ready"
	}
	if hold <= 0 {
		hold = defaultRPCRouterSubmitReadinessHold
	}
	now := time.Now()
	backend.mu.Lock()
	advisory := backendStateAllowsFreshTemplateReadinessAdvisory(backend.state, reason, now, r.config.nodeHealthFreshTemplateGrace)
	backend.state.lastTemplateError = ""
	if err != nil {
		backend.state.lastTemplateError = err.Error()
	}
	backend.state.lastTemplateAge = time.Since(backend.state.lastTemplateAt)
	if advisory {
		if backend.state.score > 85 {
			backend.state.score = 85
		}
		if !backend.state.healthy {
			backend.state.healthySince = now
		}
		backend.state.healthy = true
		backend.state.reason = "submit-readiness-advisory-" + source + "-" + reason
		backend.mu.Unlock()
		if rpcBackendTemplateErrors != nil {
			rpcBackendTemplateErrors.WithLabelValues(backend.name, source+"-advisory-"+reason).Inc()
		}
		r.publishBackendMetrics(backend)
		return
	}
	backend.state.submitReadinessHoldUntil = now.Add(hold)
	backend.state.submitReadinessHoldReason = source + "-" + reason
	backend.state.score = 0
	backend.state.healthy = false
	backend.state.reason = submitReadinessHoldReason(backend.state.submitReadinessHoldReason)
	backend.mu.Unlock()

	if rpcBackendTemplateErrors != nil {
		rpcBackendTemplateErrors.WithLabelValues(backend.name, source+"-"+reason).Inc()
	}
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) recordWSConnected(backend *rpcBackend) {
	if r == nil || backend == nil {
		return
	}
	now := time.Now()
	var resetSeq uint64
	backend.mu.Lock()
	wasConnected := backend.state.wsConnected
	backend.state.wsConnected = true
	backend.state.lastWSAt = now
	if !wasConnected && backend.state.lastTemplateSeq > 0 {
		resetSeq = backend.state.lastTemplateSeq
		backend.state.lastTemplateSeq = 0
	}
	if backend.state.reason == "ws-disconnected" {
		backend.state.reason = "ok"
	}
	backend.mu.Unlock()
	if resetSeq > 0 {
		publishTemplateSequenceReset(backend.name)
	}
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) recordWSDisconnected(backend *rpcBackend, reason string) {
	if r == nil || backend == nil {
		return
	}
	if reason == "" {
		reason = "ws-disconnected"
	}
	backend.mu.Lock()
	backend.state.wsConnected = false
	backend.state.lastReconnectAt = time.Now()
	backend.state.reconnects++
	backend.state.reason = classifyRouterError(fmt.Errorf("%s", reason))
	if backend.state.score > 80 {
		backend.state.score = 80
	}
	backend.mu.Unlock()

	if rpcBackendWSReconnects != nil {
		rpcBackendWSReconnects.WithLabelValues(backend.name).Inc()
	}
	r.publishBackendMetrics(backend)
}

func (r *rpcRouter) recordSubmitResult(backend *rpcBackend, result string, latency time.Duration, err error) {
	if r == nil || backend == nil {
		return
	}
	if result == "" {
		result = "unknown"
	}
	if err != nil {
		result = classifySubmitErrorReason(err)
	}
	invalidateBackendJobs := err != nil && isBackendReadinessSubmitReason(result)

	now := time.Now()
	preservableCurrentJobs := 0
	if invalidateBackendJobs && backendReadinessSubmitCanStayAdvisory(result) {
		preservableCurrentJobs = preservableCurrentJobsForBackend(backend.name, now)
	}
	backend.mu.Lock()
	firstReadinessMiss := backend.state.recentSubmitErrors == 0
	readinessAdvisory := invalidateBackendJobs &&
		backendStateAllowsSubmitReadinessAdvisory(backend.state, result, now, r.config.nodeHealthFreshTemplateGrace)
	preserveCurrentJobs := invalidateBackendJobs &&
		!readinessAdvisory &&
		firstReadinessMiss &&
		preservableCurrentJobs > 0
	backend.state.lastSubmitAt = now
	if err != nil {
		backend.state.lastSubmitError = err.Error()
		backend.state.recentSubmitErrors++
		if invalidateBackendJobs && !readinessAdvisory && !preserveCurrentJobs {
			backend.state.submitReadinessHoldUntil = now.Add(defaultRPCRouterSubmitReadinessHold)
			backend.state.submitReadinessHoldReason = result
			backend.state.score = 0
			backend.state.healthy = false
			backend.state.reason = submitReadinessHoldReason(result)
		}
		if isRetryableSubmitError(err) {
			if readinessAdvisory || preserveCurrentJobs {
				// Nodes can briefly report "initial download" while still serving fresh templates
				// and accepting nearby block submits. Keep that contradictory signal advisory
				// so one false negative does not starve all ASICs of current work on a 1s DAG.
				if backend.state.score > 85 {
					backend.state.score = 85
				}
				if !backend.state.healthy {
					backend.state.healthySince = now
				}
				backend.state.healthy = true
				if backend.state.healthy {
					backend.state.reason = "submit-readiness-advisory-" + result
				}
			} else if !invalidateBackendJobs {
				backend.state.score = 25
				backend.state.healthy = false
				backend.state.reason = result
			}
		} else if backend.state.score > 75 {
			backend.state.score = 75
			backend.state.reason = result
		}
	} else {
		backend.state.lastSubmitError = ""
		backend.state.lastSubmitSuccessAt = now
		backend.state.recentSubmitErrors = 0
		backend.state.submitReadinessHoldUntil = time.Time{}
		backend.state.submitReadinessHoldReason = ""
		if backend.state.score < 90 {
			backend.state.score = 90
		}
	}
	backend.mu.Unlock()

	if rpcBackendSubmitTotal != nil {
		rpcBackendSubmitTotal.WithLabelValues(backend.name, result).Inc()
	}
	if rpcBackendSubmitDur != nil {
		rpcBackendSubmitDur.WithLabelValues(backend.name, result).Observe(latency.Seconds())
	}
	if invalidateBackendJobs && !readinessAdvisory && !preserveCurrentJobs {
		r.invalidateBackendJobsFromSubmit(backend, "submit backend unready: "+result)
	} else if preserveCurrentJobs && !readinessAdvisory {
		log.Printf("[ROUTER] preserving current jobs for backend=%s during submit readiness reason=%s", backend.name, result)
	}
	r.publishBackendMetrics(backend)
}

func backendStateHasFreshSuccessfulSubmit(state backendState, now time.Time) bool {
	if state.lastSubmitSuccessAt.IsZero() {
		return false
	}
	age := now.Sub(state.lastSubmitSuccessAt)
	if age < 0 {
		age = 0
	}
	return age <= defaultRPCRouterSubmitReadinessFreshGrace
}

func backendReadinessSubmitCanStayAdvisory(reason string) bool {
	// Tip-chase micro-sync can briefly refuse submits while the current job is
	// still tip-aligned. Treat that as advisory so one flap does not idle ASICs.
	// Parent-stale / real catch-up readiness errors stay hard.
	switch normalizeNodeHealthReason(reason) {
	case "node-syncing",
		"initial-download",
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

func submitBackendReadinessRequiresRefresh(job *JobData, reason string) bool {
	if !isBackendReadinessSubmitReason(reason) {
		return false
	}
	router := activeRPCRouter
	if router == nil || job == nil {
		return true
	}
	backend := router.selectedBackend()
	if backend == nil {
		return true
	}
	state := backend.snapshotState()
	if held, _ := submitReadinessHoldActive(state, time.Now()); held {
		return true
	}
	return !backendStateAllowsSubmitReadinessAdvisory(state, reason, time.Now(), router.config.nodeHealthFreshTemplateGrace)
}

func backendStateAllowsSubmitReadinessAdvisory(state backendState, reason string, now time.Time, grace time.Duration) bool {
	if !backendReadinessSubmitCanStayAdvisory(reason) {
		return false
	}
	if state.nodeHealthP2PContextObserved && !state.nodeHealthP2PMiningFresh {
		return false
	}
	return backendStateHasFreshSuccessfulSubmit(state, now) ||
		backendStateAllowsFreshTemplateReadinessAdvisory(state, reason, now, grace)
}

func backendStateAllowsFreshTemplateReadinessAdvisory(state backendState, reason string, now time.Time, grace time.Duration) bool {
	if !nodeHealthFreshTemplateOverridesUnready(reason) {
		return false
	}
	if state.nodeHealthP2PContextObserved && (!state.nodeHealthP2PCurrent || !state.nodeHealthP2PMiningFresh) {
		return false
	}
	if !backendStateHasFreshRouterTemplate(state, now, grace) {
		return false
	}
	if !state.nodeHealthTemplate || !state.nodeHealthTemplateUsable || !state.nodeHealthLastGoodTemplateAvailable {
		return false
	}
	lastGoodAge := state.nodeHealthLastGoodTemplateAge
	if lastGoodAge < 0 {
		lastGoodAge = 0
	}
	if grace <= 0 {
		grace = time.Duration(defaultRPCRouterNodeHealthFreshGraceSeconds) * time.Second
	}
	if lastGoodAge > grace {
		return false
	}
	return state.nodeHealthP2PContextObserved && state.nodeHealthP2PCurrent && state.nodeHealthP2PMiningFresh
}

func submitReadinessHoldActive(state backendState, now time.Time) (bool, string) {
	if state.submitReadinessHoldUntil.IsZero() || !now.Before(state.submitReadinessHoldUntil) {
		return false, ""
	}
	return true, submitReadinessHoldReason(state.submitReadinessHoldReason)
}

func submitReadinessHoldReason(reason string) string {
	if reason == "" {
		reason = "backend-unready"
	}
	return "submit-readiness-" + reason
}

func (r *rpcRouter) invalidateBackendJobsFromSubmit(backend *rpcBackend, reason string) {
	if r == nil || backend == nil {
		return
	}
	if reason == "" {
		reason = "submit backend unready"
	}
	invalidated := invalidateAcceptedJobsForBackend(backend.name, reason)
	if invalidated > 0 {
		log.Printf("[ROUTER] invalidated %d accepted jobs for backend=%s reason=%s", invalidated, backend.name, reason)
	}
}

func (b *rpcBackend) snapshotState() backendState {
	if b == nil {
		return backendState{}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

func (r *rpcRouter) publishSelectionMetrics() {
	if r == nil || rpcBackendSelected == nil {
		return
	}
	if r.backend != nil {
		rpcBackendSelected.WithLabelValues(r.backend.name).Set(1)
	}
}

func (r *rpcRouter) publishAllMetrics() {
	if r == nil {
		return
	}
	r.publishSelectionMetrics()
	r.publishBackendMetrics(r.backend)
}

func startRouterMetricsPublisher(router *rpcRouter, stop <-chan struct{}) {
	if router == nil {
		return
	}
	ticker := time.NewTicker(defaultRouterMetricsRefreshInterval)
	go func() {
		defer ticker.Stop()
		publishRouterMetricsLoop(router, stop, ticker.C, func(r *rpcRouter) {
			r.publishAllMetrics()
		})
	}()
}

func publishRouterMetricsLoop(router *rpcRouter, stop <-chan struct{}, ticks <-chan time.Time, publish func(*rpcRouter)) {
	if publish == nil {
		return
	}
	for {
		select {
		case <-stop:
			return
		case <-ticks:
			publish(router)
		}
	}
}

func (r *rpcRouter) publishBackendMetrics(backend *rpcBackend) {
	if r == nil || backend == nil {
		return
	}
	state := backend.snapshotState()
	if rpcBackendHealth != nil {
		healthy := 0.0
		if state.healthy {
			healthy = 1
		}
		rpcBackendHealth.WithLabelValues(backend.name).Set(healthy)
	}
	if rpcBackendScore != nil {
		rpcBackendScore.WithLabelValues(backend.name).Set(state.score)
	}
	if rpcBackendTemplateAge != nil {
		age := state.lastTemplateAge
		if !state.lastTemplateAt.IsZero() {
			age = time.Since(state.lastTemplateAt)
		}
		rpcBackendTemplateAge.WithLabelValues(backend.name).Set(age.Seconds())
	}
	if rpcBackendTemplateSeq != nil && state.lastTemplateSeq > 0 {
		rpcBackendTemplateSeq.WithLabelValues(backend.name).Set(float64(state.lastTemplateSeq))
	}
	if rpcBackendWSConnected != nil {
		connected := 0.0
		if state.wsConnected {
			connected = 1
		}
		rpcBackendWSConnected.WithLabelValues(backend.name).Set(connected)
	}
	if rpcBackendNodeHealthMineable != nil {
		rpcBackendNodeHealthMineable.WithLabelValues(backend.name).Set(boolMetricValue(state.nodeHealthMineable))
	}
	if rpcBackendNodeHealthSubmitReady != nil {
		rpcBackendNodeHealthSubmitReady.WithLabelValues(backend.name).Set(boolMetricValue(state.nodeHealthSubmitReady))
	}
	if rpcBackendNodeHealthTemplateAge != nil && state.nodeHealthTemplate {
		rpcBackendNodeHealthTemplateAge.WithLabelValues(backend.name).Set(state.nodeHealthTemplateAge.Seconds())
	}
	if rpcBackendNodeHealthTemplateSeq != nil && state.nodeHealthTemplateSeq > 0 {
		rpcBackendNodeHealthTemplateSeq.WithLabelValues(backend.name).Set(float64(state.nodeHealthTemplateSeq))
	}
	if rpcBackendNodeHealthMainOrder != nil && state.nodeHealthMainOrder > 0 {
		rpcBackendNodeHealthMainOrder.WithLabelValues(backend.name).Set(float64(state.nodeHealthMainOrder))
	}
	if state.nodeHealthBuildContextObserved {
		if rpcBackendNodeHealthLastTemplateBuildAge != nil {
			rpcBackendNodeHealthLastTemplateBuildAge.WithLabelValues(backend.name).Set(state.nodeHealthLastTemplateBuildAge.Seconds())
		}
		if rpcBackendNodeHealthLastTemplateBuildDuration != nil {
			rpcBackendNodeHealthLastTemplateBuildDuration.WithLabelValues(backend.name).Set(state.nodeHealthLastTemplateBuildDuration.Seconds())
		}
		if rpcBackendNodeHealthLastTemplateBuildBlocking != nil {
			rpcBackendNodeHealthLastTemplateBuildBlocking.WithLabelValues(backend.name).Set(boolMetricValue(state.nodeHealthLastTemplateBuildBlocking))
		}
		if rpcBackendNodeHealthLastTemplateInvalidationSeq != nil && state.nodeHealthLastTemplateInvalidationSeq > 0 && state.nodeHealthLastTemplateInvalidationCause != "" {
			rpcBackendNodeHealthLastTemplateInvalidationSeq.WithLabelValues(backend.name, state.nodeHealthLastTemplateInvalidationCause).Set(float64(state.nodeHealthLastTemplateInvalidationSeq))
		}
		if rpcBackendNodeHealthPendingTemplateBuild != nil {
			rpcBackendNodeHealthPendingTemplateBuild.WithLabelValues(backend.name).Set(boolMetricValue(state.nodeHealthPendingTemplateBuild))
		}
		if rpcBackendNodeHealthPendingTemplatePriority != nil {
			rpcBackendNodeHealthPendingTemplatePriority.WithLabelValues(backend.name).Set(float64(state.nodeHealthPendingTemplateBuildPriority))
		}
		if rpcBackendNodeHealthPendingTemplateInvalidation != nil && state.nodeHealthPendingTemplateInvalidationCause != "" {
			rpcBackendNodeHealthPendingTemplateInvalidation.WithLabelValues(backend.name, state.nodeHealthPendingTemplateInvalidationCause).Set(1)
		}
	}
	if state.nodeHealthP2PContextObserved {
		if rpcBackendNodeHealthP2PMiningFresh != nil {
			rpcBackendNodeHealthP2PMiningFresh.WithLabelValues(backend.name).Set(boolMetricValue(state.nodeHealthP2PMiningFresh))
		}
		if rpcBackendNodeHealthP2PConsensusPeerCount != nil {
			rpcBackendNodeHealthP2PConsensusPeerCount.WithLabelValues(backend.name).Set(float64(state.nodeHealthP2PConsensusPeerCount))
		}
		if rpcBackendNodeHealthP2PFreshPeerCount != nil {
			rpcBackendNodeHealthP2PFreshPeerCount.WithLabelValues(backend.name).Set(float64(state.nodeHealthP2PFreshConsensusPeerCount))
		}
		if rpcBackendNodeHealthP2PStalePeerCount != nil {
			rpcBackendNodeHealthP2PStalePeerCount.WithLabelValues(backend.name).Set(float64(state.nodeHealthP2PStaleConsensusPeerCount))
		}
		if rpcBackendNodeHealthP2PBestPeerLeadBlocks != nil {
			rpcBackendNodeHealthP2PBestPeerLeadBlocks.WithLabelValues(backend.name).Set(float64(state.nodeHealthP2PBestPeerLeadBlocks))
		}
		if rpcBackendNodeHealthP2PBestPeerGraphAge != nil {
			rpcBackendNodeHealthP2PBestPeerGraphAge.WithLabelValues(backend.name).Set(state.nodeHealthP2PBestPeerGraphStateAge.Seconds())
		}
		if rpcBackendNodeHealthP2PAvgGraphAge != nil {
			rpcBackendNodeHealthP2PAvgGraphAge.WithLabelValues(backend.name).Set(state.nodeHealthP2PAvgGraphStateAge.Seconds())
		}
		if rpcBackendNodeHealthP2PMaxGraphAge != nil {
			rpcBackendNodeHealthP2PMaxGraphAge.WithLabelValues(backend.name).Set(state.nodeHealthP2PMaxGraphStateAge.Seconds())
		}
		if rpcBackendNodeHealthP2PSyncPeerPresent != nil {
			rpcBackendNodeHealthP2PSyncPeerPresent.WithLabelValues(backend.name).Set(boolMetricValue(state.nodeHealthP2PSyncPeerPresent))
		}
		if rpcBackendNodeHealthP2PSyncPeerFresh != nil {
			rpcBackendNodeHealthP2PSyncPeerFresh.WithLabelValues(backend.name).Set(boolMetricValue(state.nodeHealthP2PSyncPeerFresh))
		}
		if rpcBackendNodeHealthP2PSyncPeerGraphAge != nil {
			rpcBackendNodeHealthP2PSyncPeerGraphAge.WithLabelValues(backend.name).Set(state.nodeHealthP2PSyncPeerGraphStateAge.Seconds())
		}
	}
}

func boolMetricValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func publishTemplateSequenceMetrics(backendName string, previousSeq, currentSeq uint64) {
	if backendName == "" || currentSeq == 0 {
		return
	}
	if rpcBackendTemplateSeq != nil {
		rpcBackendTemplateSeq.WithLabelValues(backendName).Set(float64(currentSeq))
	}
	if previousSeq == 0 {
		return
	}
	if currentSeq == previousSeq+1 {
		return
	}
	if currentSeq > previousSeq+1 {
		if rpcBackendTemplateSeqGap != nil {
			rpcBackendTemplateSeqGap.WithLabelValues(backendName).Add(float64(currentSeq - previousSeq - 1))
		}
		return
	}
	if currentSeq == previousSeq {
		return
	}
	if rpcBackendTemplateSeqResets != nil {
		rpcBackendTemplateSeqResets.WithLabelValues(backendName).Inc()
	}
}

func publishTemplateSequenceReset(backendName string) {
	if backendName == "" || rpcBackendTemplateSeqResets == nil {
		return
	}
	rpcBackendTemplateSeqResets.WithLabelValues(backendName).Inc()
}

func publishTemplateSequenceReject(backendName, reason string) {
	if backendName == "" || reason == "" || rpcBackendTemplateSeqReject == nil {
		return
	}
	rpcBackendTemplateSeqReject.WithLabelValues(backendName, reason).Inc()
}

func classifyTemplateSequenceUpdate(previousSeq uint64, lastHeight int64, lastParent string, currentSeq uint64, height int64, parent string) (bool, string) {
	if currentSeq == 0 || previousSeq == 0 || currentSeq > previousSeq {
		return true, ""
	}
	sameTemplate := height <= lastHeight && normalizeHex(parent) == normalizeHex(lastParent)
	if !sameTemplate {
		return true, ""
	}
	if currentSeq == previousSeq {
		return false, "duplicate"
	}
	return false, "old"
}

func classifyRouterError(err error) string {
	if err == nil {
		return "ok"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case messageIndicatesNodeSyncing(msg):
		return "node-syncing"
	case strings.Contains(msg, "bdag pool syncing") || strings.Contains(msg, "pool syncing"):
		return "node-syncing"
	case strings.Contains(msg, "graph-state") || strings.Contains(msg, "graph state"):
		return "node-syncing"
	case strings.Contains(msg, "reorg"):
		return "reorg-pressure"
	case strings.Contains(msg, "initial") && strings.Contains(msg, "download"):
		return "initial-download"
	case strings.Contains(msg, "overdue") || strings.Contains(msg, "expired"):
		return "stale"
	case messageIndicatesNodeOverloaded(msg):
		return "too-busy"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection reset") || strings.Contains(msg, "no such host"):
		return "transport"
	case strings.Contains(msg, "enable miner") || strings.Contains(msg, "miner disabled"):
		return "miner-disabled"
	case strings.Contains(msg, "http 5") || strings.Contains(msg, "http 429"):
		return "http-retryable"
	case strings.Contains(msg, "http "):
		return "http-error"
	case strings.Contains(msg, "rpc error"):
		return "rpc-error"
	case strings.Contains(msg, "websocket") || strings.Contains(msg, "ws"):
		return "ws-disconnected"
	default:
		return "error"
	}
}
