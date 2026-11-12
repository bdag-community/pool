package server

import (
	"log"
	"strings"
	"sync"
	"time"
)

const (
	defaultBlockTimingControllerWindowSeconds      = 60
	defaultBlockTimingControllerMinCandidates      = 20
	defaultBlockTimingControllerTargetWastePercent = 5
	defaultBlockTimingControllerMinJobAge          = 250 * time.Millisecond
	defaultBlockTimingControllerMaxJobAge          = 2500 * time.Millisecond
	defaultBlockTimingControllerMinTemplateTTL     = 100 * time.Millisecond
	defaultBlockTimingControllerMaxTemplateTTL     = time.Second
	defaultBlockTimingControllerStep               = 250 * time.Millisecond
	defaultBlockTimingControllerCooldown           = 20 * time.Second
	maxBlockTimingControllerWindow                 = 10 * time.Minute
	maxBlockTimingControllerCooldown               = 5 * time.Minute
)

type blockTimingControllerConfig struct {
	enabled                 bool
	window                  time.Duration
	minCandidates           int
	targetWaste             float64
	minJobAge               time.Duration
	maxJobAge               time.Duration
	minTTL                  time.Duration
	maxTTL                  time.Duration
	step                    time.Duration
	cooldown                time.Duration
	defaultJobAge           time.Duration
	defaultRecentStaleGrace time.Duration
	minRecentStaleGrace     time.Duration
	maxRecentStaleGrace     time.Duration
}

type blockTimingWindow struct {
	start        time.Time
	accepted     int
	failed       int
	tipOverdue   int
	staleParent  int
	staleJob     int
	expired      int
	oldTemplate  int
	duplicate    int
	upstream     int
	transport    int
	other        int
	lastReason   string
	activeMiners int
}

type blockTimingController struct {
	mu             sync.Mutex
	cfg            blockTimingControllerConfig
	window         blockTimingWindow
	lastAdjustment time.Time
}

type blockTimingDecision struct {
	action        string
	reason        string
	oldJobAge     time.Duration
	newJobAge     time.Duration
	oldTTL        time.Duration
	newTTL        time.Duration
	oldStaleGrace time.Duration
	newStaleGrace time.Duration
	wasteRatio    float64
	total         int
	failed        int
	accepted      int
	activeMiners  int
}

var globalBlockTimingController = &blockTimingController{
	cfg: blockTimingControllerConfig{enabled: false},
}

func newBlockTimingController(cfg blockTimingControllerConfig) *blockTimingController {
	controller := &blockTimingController{}
	controller.configure(cfg)
	return controller
}

func configureBlockTimingController(cfg Config) {
	globalBlockTimingController.configure(blockTimingControllerConfigFromPoolConfig(cfg))
}

func blockTimingControllerConfigFromPoolConfig(cfg Config) blockTimingControllerConfig {
	controllerCfg := blockTimingControllerConfig{
		enabled:       cfg.BlockTimingControllerEnabled,
		window:        time.Duration(defaultBlockTimingControllerWindowSeconds) * time.Second,
		minCandidates: defaultBlockTimingControllerMinCandidates,
		targetWaste:   float64(defaultBlockTimingControllerTargetWastePercent),
		minJobAge:     defaultBlockTimingControllerMinJobAge,
		maxJobAge:     defaultBlockTimingControllerMaxJobAge,
		minTTL:        defaultBlockTimingControllerMinTemplateTTL,
		maxTTL:        defaultBlockTimingControllerMaxTemplateTTL,
		step:          defaultBlockTimingControllerStep,
		cooldown:      defaultBlockTimingControllerCooldown,
		defaultJobAge: configuredBlockCandidateJobAge(&cfg),
	}
	if cfg.RecentStaleBlockCandidateSubmitGraceMS > 0 {
		controllerCfg.defaultRecentStaleGrace = time.Duration(cfg.RecentStaleBlockCandidateSubmitGraceMS) * time.Millisecond
		controllerCfg.maxRecentStaleGrace = controllerCfg.defaultRecentStaleGrace
	}
	if cfg.BlockTimingControllerWindowSeconds > 0 {
		controllerCfg.window = time.Duration(cfg.BlockTimingControllerWindowSeconds) * time.Second
	}
	if cfg.BlockTimingControllerMinCandidates > 0 {
		controllerCfg.minCandidates = cfg.BlockTimingControllerMinCandidates
	}
	if cfg.BlockTimingControllerTargetWastePercent > 0 {
		controllerCfg.targetWaste = float64(cfg.BlockTimingControllerTargetWastePercent)
	}
	if cfg.BlockTimingControllerMinJobAgeMS > 0 {
		controllerCfg.minJobAge = time.Duration(cfg.BlockTimingControllerMinJobAgeMS) * time.Millisecond
	}
	if cfg.BlockTimingControllerMaxJobAgeMS > 0 {
		controllerCfg.maxJobAge = time.Duration(cfg.BlockTimingControllerMaxJobAgeMS) * time.Millisecond
	}
	if cfg.BlockTimingControllerMinTemplateTTLMS > 0 {
		controllerCfg.minTTL = time.Duration(cfg.BlockTimingControllerMinTemplateTTLMS) * time.Millisecond
	}
	if cfg.BlockTimingControllerMaxTemplateTTLMS > 0 {
		controllerCfg.maxTTL = time.Duration(cfg.BlockTimingControllerMaxTemplateTTLMS) * time.Millisecond
	}
	if cfg.BlockTimingControllerStepMS > 0 {
		controllerCfg.step = time.Duration(cfg.BlockTimingControllerStepMS) * time.Millisecond
	}
	if cfg.BlockTimingControllerCooldownSeconds > 0 {
		controllerCfg.cooldown = time.Duration(cfg.BlockTimingControllerCooldownSeconds) * time.Second
	}
	return controllerCfg
}

func (c *blockTimingController) configure(cfg blockTimingControllerConfig) {
	if cfg.window <= 0 {
		cfg.window = time.Duration(defaultBlockTimingControllerWindowSeconds) * time.Second
	}
	if cfg.window > maxBlockTimingControllerWindow {
		cfg.window = maxBlockTimingControllerWindow
	}
	if cfg.minCandidates <= 0 {
		cfg.minCandidates = defaultBlockTimingControllerMinCandidates
	}
	if cfg.targetWaste <= 0 {
		cfg.targetWaste = float64(defaultBlockTimingControllerTargetWastePercent)
	}
	if cfg.targetWaste > 100 {
		cfg.targetWaste = 100
	}
	if cfg.minJobAge <= 0 {
		cfg.minJobAge = defaultBlockTimingControllerMinJobAge
	}
	if cfg.maxJobAge <= 0 {
		cfg.maxJobAge = defaultBlockTimingControllerMaxJobAge
	}
	if cfg.maxJobAge < cfg.minJobAge {
		cfg.maxJobAge = cfg.minJobAge
	}
	if cfg.minTTL <= 0 {
		cfg.minTTL = defaultBlockTimingControllerMinTemplateTTL
	}
	if cfg.minTTL < minGBTMinInterval {
		cfg.minTTL = minGBTMinInterval
	}
	if cfg.maxTTL <= 0 {
		cfg.maxTTL = defaultBlockTimingControllerMaxTemplateTTL
	}
	if cfg.maxTTL < cfg.minTTL {
		cfg.maxTTL = cfg.minTTL
	}
	if cfg.step <= 0 {
		cfg.step = defaultBlockTimingControllerStep
	}
	if cfg.cooldown < 0 {
		cfg.cooldown = 0
	}
	if cfg.cooldown > maxBlockTimingControllerCooldown {
		cfg.cooldown = maxBlockTimingControllerCooldown
	}
	if cfg.defaultJobAge <= 0 {
		cfg.defaultJobAge = cfg.maxJobAge
	}
	cfg.defaultJobAge = clampDuration(cfg.defaultJobAge, cfg.minJobAge, cfg.maxJobAge)
	if cfg.defaultRecentStaleGrace < 0 {
		cfg.defaultRecentStaleGrace = 0
	}
	if cfg.defaultRecentStaleGrace > 0 && cfg.minRecentStaleGrace <= 0 {
		cfg.minRecentStaleGrace = cfg.step
		if cfg.minRecentStaleGrace > cfg.defaultRecentStaleGrace {
			cfg.minRecentStaleGrace = cfg.defaultRecentStaleGrace
		}
	}
	if cfg.minRecentStaleGrace < 0 {
		cfg.minRecentStaleGrace = 0
	}
	if cfg.maxRecentStaleGrace < 0 {
		cfg.maxRecentStaleGrace = 0
	}
	if cfg.maxRecentStaleGrace < cfg.minRecentStaleGrace {
		cfg.maxRecentStaleGrace = cfg.minRecentStaleGrace
	}
	if cfg.maxRecentStaleGrace > 0 {
		cfg.defaultRecentStaleGrace = clampDuration(cfg.defaultRecentStaleGrace, cfg.minRecentStaleGrace, cfg.maxRecentStaleGrace)
	} else {
		cfg.defaultRecentStaleGrace = 0
	}

	c.mu.Lock()
	c.cfg = cfg
	c.window = blockTimingWindow{}
	c.lastAdjustment = time.Time{}
	c.mu.Unlock()

	if cfg.enabled {
		setAllowMultipleBlockCandidatesOverride(true, true)
		setBlockCandidateJobAgeOverride(cfg.defaultJobAge)
		setRecentStaleBlockCandidateSubmitGraceOverride(cfg.defaultRecentStaleGrace, cfg.maxRecentStaleGrace > 0)
		setTemplateTTLRefreshOverride(clampDuration(configuredTemplateTTLRefresh(), cfg.minTTL, cfg.maxTTL))
		log.Printf("[TIMING-CONTROL] enabled window=%s min_candidates=%d target_waste=%.1f%% job_age=%s ttl=%s stale_grace=%s",
			cfg.window, cfg.minCandidates, cfg.targetWaste, cfg.defaultJobAge, configuredTemplateTTLRefresh(), cfg.defaultRecentStaleGrace)
	} else {
		setRecentStaleBlockCandidateSubmitGraceOverride(0, false)
		log.Printf("[TIMING-CONTROL] disabled")
	}
	publishBlockTimingControllerRuntime()
}

func recordBlockTimingAccepted(activeMiners int) {
	globalBlockTimingController.recordAccepted(time.Now(), activeMiners)
}

func recordBlockTimingFailure(reason string, activeMiners int) {
	globalBlockTimingController.recordFailure(time.Now(), reason, activeMiners)
}

func (c *blockTimingController) recordAccepted(now time.Time, activeMiners int) {
	if now.IsZero() {
		now = time.Now()
	}
	decision, apply := c.record(now, "ok", true, activeMiners)
	if apply {
		applyBlockTimingDecision(decision)
	}
}

func (c *blockTimingController) recordFailure(now time.Time, reason string, activeMiners int) {
	if now.IsZero() {
		now = time.Now()
	}
	decision, apply := c.record(now, reason, false, activeMiners)
	if apply {
		applyBlockTimingDecision(decision)
	}
}

func (c *blockTimingController) record(now time.Time, reason string, accepted bool, activeMiners int) (blockTimingDecision, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cfg := c.cfg
	if !cfg.enabled {
		return blockTimingDecision{}, false
	}
	if c.window.start.IsZero() || now.Sub(c.window.start) > 2*cfg.window {
		c.window = blockTimingWindow{start: now}
	}
	if accepted {
		c.window.accepted++
	} else {
		c.window.failed++
		c.window.lastReason = normalizeBlockTimingReason(reason)
		c.window.addReason(c.window.lastReason)
	}
	if activeMiners > c.window.activeMiners {
		c.window.activeMiners = activeMiners
	}

	total := c.window.total()
	if total < cfg.minCandidates || now.Sub(c.window.start) < cfg.window {
		return blockTimingDecision{}, false
	}

	snapshot := c.window
	c.window = blockTimingWindow{start: now}
	decision, ok := c.evaluateLocked(cfg, snapshot, now)
	if ok {
		c.lastAdjustment = now
	}
	return decision, ok
}

func (w *blockTimingWindow) addReason(reason string) {
	switch reason {
	case "tip-overdue":
		w.tipOverdue++
	case "stale-parent":
		w.staleParent++
	case "stale-job":
		w.staleJob++
	case "expired":
		w.expired++
	case "old-template-sequence":
		w.oldTemplate++
	case "duplicate":
		w.duplicate++
	case "upstream":
		w.upstream++
	case "transport":
		w.transport++
	default:
		w.other++
	}
}

func (w blockTimingWindow) total() int {
	return w.accepted + w.failed
}

func (c *blockTimingController) evaluateLocked(cfg blockTimingControllerConfig, window blockTimingWindow, now time.Time) (blockTimingDecision, bool) {
	total := window.total()
	if total <= 0 {
		return blockTimingDecision{}, false
	}
	wasteRatio := float64(window.failed) * 100 / float64(total)
	if blockTimingControllerWasteRatio != nil {
		blockTimingControllerWasteRatio.Set(wasteRatio / 100)
	}

	oldAge := currentBlockTimingJobAge(cfg)
	oldTTL := clampDuration(configuredTemplateTTLRefresh(), cfg.minTTL, cfg.maxTTL)
	oldGrace := currentBlockTimingRecentStaleGrace(cfg)
	newAge := oldAge
	newTTL := oldTTL
	newGrace := oldGrace

	if wasteRatio <= cfg.targetWaste {
		recordBlockTimingControllerAdjustment("hold", "within-target")
		log.Printf("[TIMING-CONTROL] hold reason=within-target waste=%.1f%% target=%.1f%% accepted=%d failed=%d active_miners=%d",
			wasteRatio, cfg.targetWaste, window.accepted, window.failed, window.activeMiners)
		return blockTimingDecision{}, false
	}
	if !c.lastAdjustment.IsZero() && now.Sub(c.lastAdjustment) < cfg.cooldown {
		recordBlockTimingControllerAdjustment("hold", "cooldown")
		return blockTimingDecision{}, false
	}

	action := "hold"
	reason := "mixed-waste"
	upstreamFailures := window.upstream + window.transport
	if upstreamFailures > 0 && float64(upstreamFailures)*100/float64(total) >= 20 {
		if cfg.maxRecentStaleGrace > 0 && oldGrace > cfg.minRecentStaleGrace {
			action = "reduce-stale-grace"
			reason = "upstream-not-ready"
			newGrace = clampDuration(oldGrace-cfg.step, cfg.minRecentStaleGrace, cfg.maxRecentStaleGrace)
		} else {
			recordBlockTimingControllerAdjustment("hold", "upstream-not-ready")
			log.Printf("[TIMING-CONTROL] hold reason=upstream-not-ready waste=%.1f%% accepted=%d failed=%d upstream=%d transport=%d last_reason=%s active_miners=%d",
				wasteRatio, window.accepted, window.failed, window.upstream, window.transport, window.lastReason, window.activeMiners)
			return blockTimingDecision{}, false
		}
	}

	staleTiming := window.tipOverdue + window.staleParent + window.staleJob
	localExpiry := window.expired + window.oldTemplate
	if action == "hold" {
		switch {
		case staleTiming >= localExpiry && staleTiming > 0:
			action = "freshen-work"
			reason = "stale-submit"
			newAge = clampDuration(oldAge-cfg.step, cfg.minJobAge, cfg.maxJobAge)
			newTTL = clampDuration(oldTTL-cfg.step, cfg.minTTL, cfg.maxTTL)
		case localExpiry > staleTiming:
			action = "relax-job-age"
			reason = "local-expiry"
			newAge = clampDuration(oldAge+cfg.step, cfg.minJobAge, cfg.maxJobAge)
		case window.transport > 0:
			action = "reduce-template-pressure"
			reason = "transport"
			newTTL = clampDuration(oldTTL+cfg.step, cfg.minTTL, cfg.maxTTL)
		case window.duplicate > 0 && window.duplicate >= window.failed/2:
			action = "hold"
			reason = "duplicate-candidates"
		default:
			action = "freshen-work"
			reason = "mixed-waste"
			newAge = clampDuration(oldAge-cfg.step, cfg.minJobAge, cfg.maxJobAge)
			newTTL = clampDuration(oldTTL-cfg.step, cfg.minTTL, cfg.maxTTL)
		}
	}

	if cfg.maxRecentStaleGrace > 0 {
		graceRisk := window.tipOverdue + window.staleParent + window.duplicate + window.upstream
		graceDemand := window.staleJob + window.oldTemplate
		switch {
		case graceRisk > 0 && oldGrace > cfg.minRecentStaleGrace:
			newGrace = clampDuration(oldGrace-cfg.step, cfg.minRecentStaleGrace, cfg.maxRecentStaleGrace)
			if action == "hold" {
				action = "reduce-stale-grace"
				reason = "backend-stale-risk"
			}
		case graceDemand > 0 && graceRisk == 0 && oldGrace < cfg.maxRecentStaleGrace:
			newGrace = clampDuration(oldGrace+cfg.step, cfg.minRecentStaleGrace, cfg.maxRecentStaleGrace)
			if action == "hold" {
				action = "increase-stale-grace"
				reason = "local-stale-race"
			}
		}
	}

	return blockTimingDecision{
		action:        action,
		reason:        reason,
		oldJobAge:     oldAge,
		newJobAge:     newAge,
		oldTTL:        oldTTL,
		newTTL:        newTTL,
		oldStaleGrace: oldGrace,
		newStaleGrace: newGrace,
		wasteRatio:    wasteRatio,
		total:         total,
		failed:        window.failed,
		accepted:      window.accepted,
		activeMiners:  window.activeMiners,
	}, true
}

func applyBlockTimingDecision(decision blockTimingDecision) {
	if decision.action == "" {
		return
	}
	changed := false
	if decision.newJobAge > 0 && decision.newJobAge != decision.oldJobAge {
		setBlockCandidateJobAgeOverride(decision.newJobAge)
		changed = true
	}
	if decision.newTTL > 0 && decision.newTTL != decision.oldTTL {
		setTemplateTTLRefreshOverride(decision.newTTL)
		changed = true
	}
	if decision.newStaleGrace != decision.oldStaleGrace {
		setRecentStaleBlockCandidateSubmitGraceOverride(decision.newStaleGrace, true)
		changed = true
	}
	setAllowMultipleBlockCandidatesOverride(true, true)
	publishBlockTimingControllerRuntime()
	recordBlockTimingControllerAdjustment(decision.action, decision.reason)
	log.Printf("[TIMING-CONTROL] action=%s reason=%s changed=%t waste=%.1f%% accepted=%d failed=%d total=%d active_miners=%d job_age=%s->%s ttl=%s->%s stale_grace=%s->%s",
		decision.action, decision.reason, changed, decision.wasteRatio, decision.accepted, decision.failed, decision.total,
		decision.activeMiners, decision.oldJobAge, decision.newJobAge, decision.oldTTL, decision.newTTL, decision.oldStaleGrace, decision.newStaleGrace)
	if changed && (decision.action == "freshen-work" || decision.action == "reduce-template-pressure") {
		forceCleanTemplateRefreshThrottled("block timing controller: " + decision.reason)
	}
}

func currentBlockTimingJobAge(cfg blockTimingControllerConfig) time.Duration {
	if blockCandidateJobAgeOverrideEnabled.Load() {
		ms := blockCandidateJobAgeOverrideMS.Load()
		if ms > 0 {
			return clampDuration(time.Duration(ms)*time.Millisecond, cfg.minJobAge, cfg.maxJobAge)
		}
	}
	return clampDuration(cfg.defaultJobAge, cfg.minJobAge, cfg.maxJobAge)
}

func currentBlockTimingRecentStaleGrace(cfg blockTimingControllerConfig) time.Duration {
	if cfg.maxRecentStaleGrace <= 0 {
		return 0
	}
	if recentStaleGraceOverrideEnabled.Load() {
		ms := recentStaleGraceOverrideMS.Load()
		if ms <= 0 {
			return 0
		}
		return clampDuration(time.Duration(ms)*time.Millisecond, cfg.minRecentStaleGrace, cfg.maxRecentStaleGrace)
	}
	return clampDuration(cfg.defaultRecentStaleGrace, cfg.minRecentStaleGrace, cfg.maxRecentStaleGrace)
}

func normalizeBlockTimingReason(reason string) string {
	reason = strings.TrimSpace(strings.ToLower(reason))
	switch {
	case reason == "tip-overdue" || reason == "tip_overdue" || reason == "block-overdue" || reason == "too-late":
		return "tip-overdue"
	case reason == "stale-parent" || reason == "stale_parent" || reason == "parent-not-current" || reason == "parent_not_current":
		return "stale-parent"
	case reason == "stale-job" || reason == "stale_job" || reason == "invalidated-job" || reason == "invalidated_job":
		return "stale-job"
	case reason == "expired" || reason == "template-expired" || reason == "template_expired":
		return "expired"
	case reason == "old-template-sequence" || reason == "old_template_sequence":
		return "old-template-sequence"
	case isDuplicateSubmitReason(reason) || strings.Contains(reason, "duplicate"):
		return "duplicate"
	case isBackendReadinessSubmitReason(reason) || reason == "upstream" || strings.Contains(reason, "syncing") || strings.Contains(reason, "not-ready"):
		return "upstream"
	case strings.HasPrefix(reason, "http-") || reason == "transport" || strings.Contains(reason, "timeout") || strings.Contains(reason, "connection"):
		return "transport"
	default:
		if reason == "" {
			return "unknown"
		}
		return reason
	}
}

func clampDuration(value, min, max time.Duration) time.Duration {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func publishBlockTimingControllerRuntime() {
	if blockTimingControllerJobAgeMS != nil {
		globalBlockTimingController.mu.Lock()
		cfg := globalBlockTimingController.cfg
		globalBlockTimingController.mu.Unlock()
		blockTimingControllerJobAgeMS.Set(float64(currentBlockTimingJobAge(cfg) / time.Millisecond))
	}
	if blockTimingControllerTemplateTTLMS != nil {
		blockTimingControllerTemplateTTLMS.Set(float64(configuredTemplateTTLRefresh() / time.Millisecond))
	}
	if blockTimingControllerStaleGraceMS != nil {
		globalBlockTimingController.mu.Lock()
		cfg := globalBlockTimingController.cfg
		globalBlockTimingController.mu.Unlock()
		blockTimingControllerStaleGraceMS.Set(float64(currentBlockTimingRecentStaleGrace(cfg) / time.Millisecond))
	}
}

func recordBlockTimingControllerAdjustment(action, reason string) {
	if blockTimingControllerAdjustments == nil {
		return
	}
	if action == "" {
		action = "unknown"
	}
	if reason == "" {
		reason = "unknown"
	}
	blockTimingControllerAdjustments.WithLabelValues(action, reason).Inc()
}
