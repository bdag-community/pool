package server

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	defaultSubmitStallRecoveryWindowSeconds      = 60
	defaultSubmitStallRecoveryMinAgeSeconds      = 30
	defaultSubmitStallRecoveryFailureThreshold   = 8
	defaultSubmitStallRecoveryStaleThreshold     = 3
	defaultSubmitStallRecoveryDuplicateThreshold = 6
	defaultSubmitStallRecoveryCooldownSeconds    = 20

	defaultTemplateConversionStallRecoveryWindowSeconds       = 120
	defaultTemplateConversionStallRecoveryMinAgeSeconds       = 30
	defaultTemplateConversionStallRecoveryMinCandidates       = 40
	defaultTemplateConversionStallRecoveryFailureRatioPercent = 85
	defaultTemplateConversionStallRecoveryMinActiveMiners     = 1
	defaultTemplateConversionStallRecoveryCooldownSeconds     = 30
	defaultTemplateConversionStallRecoveryReasonPrefix        = "template-conversion-stall"
)

type submitStallRecoveryConfig struct {
	enabled            bool
	window             time.Duration
	minAge             time.Duration
	failureThreshold   int
	staleThreshold     int
	duplicateThreshold int
	cooldown           time.Duration
}

type submitStallRecoverySnapshot struct {
	Reason             string
	WindowStart        time.Time
	Window             time.Duration
	Failures           int
	StaleFailures      int
	DuplicateFailures  int
	FailureThreshold   int
	StaleThreshold     int
	DuplicateThreshold int
}

type submitStallRecoveryTracker struct {
	mu                sync.Mutex
	config            submitStallRecoveryConfig
	windowStart       time.Time
	failures          int
	staleFailures     int
	duplicateFailures int
	lastRecovery      time.Time
}

type templateConversionStallRecoveryConfig struct {
	enabled               bool
	window                time.Duration
	minAge                time.Duration
	minCandidates         int
	failureRatioThreshold float64
	minActiveMiners       int
	cooldown              time.Duration
}

type templateConversionStallRecoverySnapshot struct {
	Reason                string
	WindowStart           time.Time
	Window                time.Duration
	Accepted              int
	Failures              int
	StaleFailures         int
	DuplicateFailures     int
	TotalCandidates       int
	FailureRatio          float64
	FailureRatioThreshold float64
	MinCandidates         int
	ActiveMiners          int
	MinActiveMiners       int
}

type templateConversionStallRecoveryTracker struct {
	mu                sync.Mutex
	config            templateConversionStallRecoveryConfig
	windowStart       time.Time
	accepted          int
	failures          int
	staleFailures     int
	duplicateFailures int
	totalCandidates   int
	activeMiners      int
	lastReason        string
	lastRecovery      time.Time
}

var globalSubmitStallRecovery = newSubmitStallRecoveryTracker(defaultSubmitStallRecoveryConfig())
var globalTemplateConversionStallRecovery = newTemplateConversionStallRecoveryTracker(defaultTemplateConversionStallRecoveryConfig())

func defaultSubmitStallRecoveryConfig() submitStallRecoveryConfig {
	return submitStallRecoveryConfig{
		enabled:            true,
		window:             time.Duration(defaultSubmitStallRecoveryWindowSeconds) * time.Second,
		minAge:             time.Duration(defaultSubmitStallRecoveryMinAgeSeconds) * time.Second,
		failureThreshold:   defaultSubmitStallRecoveryFailureThreshold,
		staleThreshold:     defaultSubmitStallRecoveryStaleThreshold,
		duplicateThreshold: defaultSubmitStallRecoveryDuplicateThreshold,
		cooldown:           time.Duration(defaultSubmitStallRecoveryCooldownSeconds) * time.Second,
	}
}

func defaultTemplateConversionStallRecoveryConfig() templateConversionStallRecoveryConfig {
	return templateConversionStallRecoveryConfig{
		enabled:               true,
		window:                time.Duration(defaultTemplateConversionStallRecoveryWindowSeconds) * time.Second,
		minAge:                time.Duration(defaultTemplateConversionStallRecoveryMinAgeSeconds) * time.Second,
		minCandidates:         defaultTemplateConversionStallRecoveryMinCandidates,
		failureRatioThreshold: float64(defaultTemplateConversionStallRecoveryFailureRatioPercent),
		minActiveMiners:       defaultTemplateConversionStallRecoveryMinActiveMiners,
		cooldown:              time.Duration(defaultTemplateConversionStallRecoveryCooldownSeconds) * time.Second,
	}
}

func newSubmitStallRecoveryTracker(cfg submitStallRecoveryConfig) *submitStallRecoveryTracker {
	tracker := &submitStallRecoveryTracker{}
	tracker.configure(cfg)
	return tracker
}

func newTemplateConversionStallRecoveryTracker(cfg templateConversionStallRecoveryConfig) *templateConversionStallRecoveryTracker {
	tracker := &templateConversionStallRecoveryTracker{}
	tracker.configure(cfg)
	return tracker
}

func configureSubmitStallRecovery(cfg Config) {
	globalSubmitStallRecovery.configure(submitStallRecoveryConfigFromPoolConfig(cfg))
	globalTemplateConversionStallRecovery.configure(templateConversionStallRecoveryConfigFromPoolConfig(cfg))
}

func submitStallRecoveryConfigFromPoolConfig(cfg Config) submitStallRecoveryConfig {
	recoveryCfg := defaultSubmitStallRecoveryConfig()
	recoveryCfg.enabled = !cfg.DisableSubmitStallRecovery
	if cfg.SubmitStallRecoveryWindowSeconds > 0 {
		recoveryCfg.window = time.Duration(cfg.SubmitStallRecoveryWindowSeconds) * time.Second
	}
	if cfg.SubmitStallRecoveryMinAgeSeconds > 0 {
		recoveryCfg.minAge = time.Duration(cfg.SubmitStallRecoveryMinAgeSeconds) * time.Second
	}
	if cfg.SubmitStallRecoveryFailureThreshold > 0 {
		recoveryCfg.failureThreshold = cfg.SubmitStallRecoveryFailureThreshold
	}
	if cfg.SubmitStallRecoveryStaleThreshold > 0 {
		recoveryCfg.staleThreshold = cfg.SubmitStallRecoveryStaleThreshold
	}
	if cfg.SubmitStallRecoveryDuplicateThreshold > 0 {
		recoveryCfg.duplicateThreshold = cfg.SubmitStallRecoveryDuplicateThreshold
	}
	if cfg.SubmitStallRecoveryCooldownSeconds > 0 {
		recoveryCfg.cooldown = time.Duration(cfg.SubmitStallRecoveryCooldownSeconds) * time.Second
	}
	return recoveryCfg
}

func templateConversionStallRecoveryConfigFromPoolConfig(cfg Config) templateConversionStallRecoveryConfig {
	recoveryCfg := defaultTemplateConversionStallRecoveryConfig()
	recoveryCfg.enabled = !cfg.DisableTemplateConversionStallRecovery
	if cfg.TemplateConversionStallWindowSeconds > 0 {
		recoveryCfg.window = time.Duration(cfg.TemplateConversionStallWindowSeconds) * time.Second
	}
	if cfg.TemplateConversionStallMinAgeSeconds > 0 {
		recoveryCfg.minAge = time.Duration(cfg.TemplateConversionStallMinAgeSeconds) * time.Second
	}
	if cfg.TemplateConversionStallMinCandidates > 0 {
		recoveryCfg.minCandidates = cfg.TemplateConversionStallMinCandidates
	}
	if cfg.TemplateConversionStallFailureRatioPercent > 0 {
		recoveryCfg.failureRatioThreshold = float64(cfg.TemplateConversionStallFailureRatioPercent)
	}
	if cfg.TemplateConversionStallMinActiveMiners > 0 {
		recoveryCfg.minActiveMiners = cfg.TemplateConversionStallMinActiveMiners
	}
	if cfg.TemplateConversionStallCooldownSeconds > 0 {
		recoveryCfg.cooldown = time.Duration(cfg.TemplateConversionStallCooldownSeconds) * time.Second
	}
	return recoveryCfg
}

func (t *submitStallRecoveryTracker) configure(cfg submitStallRecoveryConfig) {
	if cfg.window <= 0 {
		cfg.window = time.Duration(defaultSubmitStallRecoveryWindowSeconds) * time.Second
	}
	if cfg.minAge <= 0 {
		cfg.minAge = time.Duration(defaultSubmitStallRecoveryMinAgeSeconds) * time.Second
	}
	if cfg.minAge > cfg.window {
		cfg.minAge = cfg.window
	}
	if cfg.failureThreshold <= 0 {
		cfg.failureThreshold = defaultSubmitStallRecoveryFailureThreshold
	}
	if cfg.staleThreshold <= 0 {
		cfg.staleThreshold = defaultSubmitStallRecoveryStaleThreshold
	}
	if cfg.duplicateThreshold <= 0 {
		cfg.duplicateThreshold = defaultSubmitStallRecoveryDuplicateThreshold
	}
	if cfg.cooldown <= 0 {
		cfg.cooldown = time.Duration(defaultSubmitStallRecoveryCooldownSeconds) * time.Second
	}

	t.mu.Lock()
	t.config = cfg
	t.windowStart = time.Time{}
	t.failures = 0
	t.staleFailures = 0
	t.duplicateFailures = 0
	t.lastRecovery = time.Time{}
	t.mu.Unlock()
}

func (t *templateConversionStallRecoveryTracker) configure(cfg templateConversionStallRecoveryConfig) {
	if cfg.window <= 0 {
		cfg.window = time.Duration(defaultTemplateConversionStallRecoveryWindowSeconds) * time.Second
	}
	if cfg.minAge <= 0 {
		cfg.minAge = time.Duration(defaultTemplateConversionStallRecoveryMinAgeSeconds) * time.Second
	}
	if cfg.minAge > cfg.window {
		cfg.minAge = cfg.window
	}
	if cfg.minCandidates <= 0 {
		cfg.minCandidates = defaultTemplateConversionStallRecoveryMinCandidates
	}
	if cfg.failureRatioThreshold <= 0 {
		cfg.failureRatioThreshold = float64(defaultTemplateConversionStallRecoveryFailureRatioPercent)
	}
	if cfg.failureRatioThreshold > 100 {
		cfg.failureRatioThreshold = 100
	}
	if cfg.minActiveMiners <= 0 {
		cfg.minActiveMiners = defaultTemplateConversionStallRecoveryMinActiveMiners
	}
	if cfg.cooldown <= 0 {
		cfg.cooldown = time.Duration(defaultTemplateConversionStallRecoveryCooldownSeconds) * time.Second
	}

	t.mu.Lock()
	t.config = cfg
	t.windowStart = time.Time{}
	t.accepted = 0
	t.failures = 0
	t.staleFailures = 0
	t.duplicateFailures = 0
	t.totalCandidates = 0
	t.activeMiners = 0
	t.lastReason = ""
	t.lastRecovery = time.Time{}
	t.mu.Unlock()
}

func recordSubmitRecoveryAccepted() {
	now := time.Now()
	activeMiners := activeSubmitRecoveryMinerCount()
	recordBlockTimingAccepted(activeMiners)
	globalSubmitStallRecovery.recordAccepted(now)
	globalTemplateConversionStallRecovery.recordAccepted(now, activeMiners)
}

func recordSubmitRecoveryFailure(reason string, job *JobData) {
	now := time.Now()
	activeMiners := activeSubmitRecoveryMinerCount()
	recordBlockTimingFailure(reason, activeMiners)

	conversionSnapshot, recoverConversion := globalTemplateConversionStallRecovery.recordFailure(now, reason, activeMiners)
	if recoverConversion {
		go recoverFromTemplateConversionStall(conversionSnapshot, job)
	}

	stallSnapshot, recoverStall := globalSubmitStallRecovery.recordFailure(now, reason)
	if recoverStall {
		go recoverFromSubmitStall(stallSnapshot, job)
	}
}

func (t *submitStallRecoveryTracker) recordAccepted(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}

	t.mu.Lock()
	t.windowStart = now
	t.failures = 0
	t.staleFailures = 0
	t.duplicateFailures = 0
	t.mu.Unlock()
}

func (t *templateConversionStallRecoveryTracker) recordAccepted(now time.Time, activeMiners int) {
	if now.IsZero() {
		now = time.Now()
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cfg := t.config
	if !cfg.enabled {
		return
	}
	t.ensureWindowLocked(now, cfg)
	t.accepted++
	t.totalCandidates++
	t.activeMiners = activeMiners
}

func (t *submitStallRecoveryTracker) recordFailure(now time.Time, reason string) (submitStallRecoverySnapshot, bool) {
	if now.IsZero() {
		now = time.Now()
	}
	if reason == "" {
		reason = "unknown"
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cfg := t.config
	if !cfg.enabled {
		return submitStallRecoverySnapshot{}, false
	}
	if t.windowStart.IsZero() || now.Sub(t.windowStart) > cfg.window {
		t.windowStart = now
		t.failures = 0
		t.staleFailures = 0
		t.duplicateFailures = 0
	}

	t.failures++
	if isSubmitStallStaleReason(reason) {
		t.staleFailures++
	}
	if isSubmitStallDuplicateReason(reason) {
		t.duplicateFailures++
	}

	triggered := t.failures >= cfg.failureThreshold ||
		t.staleFailures >= cfg.staleThreshold ||
		t.duplicateFailures >= cfg.duplicateThreshold
	if !triggered {
		return submitStallRecoverySnapshot{}, false
	}
	if now.Sub(t.windowStart) < cfg.minAge {
		return submitStallRecoverySnapshot{}, false
	}
	if !t.lastRecovery.IsZero() && now.Sub(t.lastRecovery) < cfg.cooldown {
		return submitStallRecoverySnapshot{}, false
	}

	snapshot := submitStallRecoverySnapshot{
		Reason:             reason,
		WindowStart:        t.windowStart,
		Window:             cfg.window,
		Failures:           t.failures,
		StaleFailures:      t.staleFailures,
		DuplicateFailures:  t.duplicateFailures,
		FailureThreshold:   cfg.failureThreshold,
		StaleThreshold:     cfg.staleThreshold,
		DuplicateThreshold: cfg.duplicateThreshold,
	}
	t.lastRecovery = now
	t.windowStart = now
	t.failures = 0
	t.staleFailures = 0
	t.duplicateFailures = 0
	return snapshot, true
}

func (t *templateConversionStallRecoveryTracker) recordFailure(now time.Time, reason string, activeMiners int) (templateConversionStallRecoverySnapshot, bool) {
	if now.IsZero() {
		now = time.Now()
	}
	if reason == "" {
		reason = "unknown"
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cfg := t.config
	if !cfg.enabled {
		return templateConversionStallRecoverySnapshot{}, false
	}
	t.ensureWindowLocked(now, cfg)

	t.failures++
	t.totalCandidates++
	t.activeMiners = activeMiners
	t.lastReason = reason
	if isSubmitStallStaleReason(reason) {
		t.staleFailures++
	}
	if isSubmitStallDuplicateReason(reason) {
		t.duplicateFailures++
	}

	if activeMiners < cfg.minActiveMiners {
		return templateConversionStallRecoverySnapshot{}, false
	}
	if t.totalCandidates < cfg.minCandidates {
		return templateConversionStallRecoverySnapshot{}, false
	}
	if now.Sub(t.windowStart) < cfg.minAge {
		return templateConversionStallRecoverySnapshot{}, false
	}
	if !t.lastRecovery.IsZero() && now.Sub(t.lastRecovery) < cfg.cooldown {
		return templateConversionStallRecoverySnapshot{}, false
	}

	failureRatio := t.failureRatioLocked()
	if failureRatio < cfg.failureRatioThreshold {
		return templateConversionStallRecoverySnapshot{}, false
	}

	snapshot := templateConversionStallRecoverySnapshot{
		Reason:                reason,
		WindowStart:           t.windowStart,
		Window:                cfg.window,
		Accepted:              t.accepted,
		Failures:              t.failures,
		StaleFailures:         t.staleFailures,
		DuplicateFailures:     t.duplicateFailures,
		TotalCandidates:       t.totalCandidates,
		FailureRatio:          failureRatio,
		FailureRatioThreshold: cfg.failureRatioThreshold,
		MinCandidates:         cfg.minCandidates,
		ActiveMiners:          activeMiners,
		MinActiveMiners:       cfg.minActiveMiners,
	}
	t.lastRecovery = now
	t.resetWindowLocked(now)
	return snapshot, true
}

func (t *templateConversionStallRecoveryTracker) ensureWindowLocked(now time.Time, cfg templateConversionStallRecoveryConfig) {
	if t.windowStart.IsZero() || now.Sub(t.windowStart) > cfg.window {
		t.resetWindowLocked(now)
	}
}

func (t *templateConversionStallRecoveryTracker) resetWindowLocked(now time.Time) {
	t.windowStart = now
	t.accepted = 0
	t.failures = 0
	t.staleFailures = 0
	t.duplicateFailures = 0
	t.totalCandidates = 0
	t.activeMiners = 0
	t.lastReason = ""
}

func (t *templateConversionStallRecoveryTracker) failureRatioLocked() float64 {
	if t.totalCandidates <= 0 {
		return 0
	}
	return float64(t.failures) * 100 / float64(t.totalCandidates)
}

func activeSubmitRecoveryMinerCount() int {
	count := 0
	for _, c := range snapshotClients() {
		if c == nil {
			continue
		}
		c.mu.Lock()
		authorized := c.authorized
		subscribed := c.sessionID != ""
		c.mu.Unlock()
		if authorized && subscribed {
			count++
		}
	}
	return count
}

func isSubmitStallStaleReason(reason string) bool {
	return isStaleSubmitReason(reason)
}

func isSubmitStallDuplicateReason(reason string) bool {
	return isDuplicateSubmitReason(reason)
}

func recoverFromTemplateConversionStall(snapshot templateConversionStallRecoverySnapshot, job *JobData) {
	reason := snapshot.Reason
	if reason == "" {
		reason = "unknown"
	}

	parent := ""
	backendName := ""
	jobAge := time.Duration(0)
	if job != nil {
		parent = job.ParentHash
		backendName = job.BackendName
		if !job.CreatedAt.IsZero() {
			jobAge = time.Since(job.CreatedAt)
		}
	}

	log.Printf("[TEMPLATE-CONVERSION-STALL] recovery triggered reason=%s accepted=%d failures=%d stale=%d duplicate=%d total=%d failure_ratio=%.1f threshold=%.1f active_miners=%d min_active_miners=%d window=%s backend=%s parent=%s job_age=%s",
		reason, snapshot.Accepted, snapshot.Failures, snapshot.StaleFailures, snapshot.DuplicateFailures, snapshot.TotalCandidates,
		snapshot.FailureRatio, snapshot.FailureRatioThreshold, snapshot.ActiveMiners, snapshot.MinActiveMiners, snapshot.Window,
		backendName, parent, jobAge.Round(time.Second))

	if templateConversionStallRecoveries != nil {
		templateConversionStallRecoveries.WithLabelValues("started", reason).Inc()
	}

	forceCleanTemplateRefresh(fmt.Sprintf("template conversion stall recovery: %s", reason))
	if templateConversionStallRecoveries != nil {
		templateConversionStallRecoveries.WithLabelValues("clean-refresh", reason).Inc()
	}
}

func recoverFromSubmitStall(snapshot submitStallRecoverySnapshot, job *JobData) {
	reason := snapshot.Reason
	if reason == "" {
		reason = "unknown"
	}

	parent := ""
	backendName := ""
	jobAge := time.Duration(0)
	if job != nil {
		parent = job.ParentHash
		backendName = job.BackendName
		if !job.CreatedAt.IsZero() {
			jobAge = time.Since(job.CreatedAt)
		}
	}

	log.Printf("[SUBMIT-STALL] zero-success submit recovery triggered reason=%s failures=%d stale=%d duplicate=%d window=%s backend=%s parent=%s job_age=%s",
		reason, snapshot.Failures, snapshot.StaleFailures, snapshot.DuplicateFailures, snapshot.Window, backendName, parent, jobAge.Round(time.Second))

	if submitStallRecoveries != nil {
		submitStallRecoveries.WithLabelValues("started", reason).Inc()
	}

	forceCleanTemplateRefresh(fmt.Sprintf("submit stall recovery: %s", reason))
	if submitStallRecoveries != nil {
		submitStallRecoveries.WithLabelValues("clean-refresh", reason).Inc()
	}
}
