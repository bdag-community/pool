package server

import (
	"encoding/hex"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	defaultExpiredJobRejectWindowSeconds          = 10
	defaultExpiredJobClientResendThreshold        = 3
	defaultExpiredJobClientReconnectThreshold     = 3
	defaultExpiredJobClientReconnectWindowSeconds = 120
	defaultExpiredJobClientReconnectCooldownSecs  = 60
	defaultExpiredJobGlobalCleanRefreshThreshold  = 100
	defaultExpiredJobGlobalCleanRefreshMinClients = 2
	defaultExpiredJobRecoveryCooldownSeconds      = 15
	defaultStaleRaceRejectWindowSeconds           = 10
	defaultStaleRaceClientResendThreshold         = 3
	defaultStaleRaceClientReconnectThreshold      = 0
	defaultStaleRaceClientReconnectWindowSeconds  = 120
	defaultStaleRaceClientReconnectCooldownSecs   = 60
	defaultStaleRaceRecoveryCooldownSeconds       = 15
)

type expiredJobRecoveryConfig struct {
	enabled                          bool
	window                           time.Duration
	clientResendThreshold            int
	clientReconnectThreshold         int
	clientReconnectWindow            time.Duration
	clientReconnectCooldown          time.Duration
	globalCleanRefreshThreshold      int
	globalCleanRefreshMinClients     int
	cooldown                         time.Duration
	staleRaceWindow                  time.Duration
	staleRaceClientResend            int
	staleRaceClientReconnect         int
	staleRaceClientReconnectWindow   time.Duration
	staleRaceClientReconnectCooldown time.Duration
	staleRaceCooldown                time.Duration
}

type expiredJobRecoveryDecision struct {
	ClientResend     bool
	ClientReconnect  bool
	CleanRefresh     bool
	ClientRejects    int
	ClientRecoveries int
	GlobalRejects    int
}

type expiredJobRecoveryActions struct {
	nowFn           func() time.Time
	resendClient    func(*client)
	reconnectClient func(*client)
	cleanRefresh    func(string)
}

type expiredJobGlobalTracker struct {
	mu           sync.Mutex
	config       expiredJobRecoveryConfig
	windowStart  time.Time
	rejects      int
	clientKeys   map[string]struct{}
	lastRecovery time.Time
}

var globalExpiredJobRecovery = newExpiredJobGlobalTracker(defaultExpiredJobRecoveryConfig())

var defaultExpiredJobRecoveryActions = expiredJobRecoveryActions{
	nowFn: time.Now,
	resendClient: func(c *client) {
		go ResendJobToClientClean(c, true)
	},
	reconnectClient: func(c *client) {
		closeClientConnection(c)
	},
	cleanRefresh: forceCleanTemplateRefresh,
}

func defaultExpiredJobRecoveryConfig() expiredJobRecoveryConfig {
	return expiredJobRecoveryConfig{
		enabled:                          true,
		window:                           time.Duration(defaultExpiredJobRejectWindowSeconds) * time.Second,
		clientResendThreshold:            defaultExpiredJobClientResendThreshold,
		clientReconnectThreshold:         defaultExpiredJobClientReconnectThreshold,
		clientReconnectWindow:            time.Duration(defaultExpiredJobClientReconnectWindowSeconds) * time.Second,
		clientReconnectCooldown:          time.Duration(defaultExpiredJobClientReconnectCooldownSecs) * time.Second,
		globalCleanRefreshThreshold:      defaultExpiredJobGlobalCleanRefreshThreshold,
		globalCleanRefreshMinClients:     defaultExpiredJobGlobalCleanRefreshMinClients,
		cooldown:                         time.Duration(defaultExpiredJobRecoveryCooldownSeconds) * time.Second,
		staleRaceWindow:                  time.Duration(defaultStaleRaceRejectWindowSeconds) * time.Second,
		staleRaceClientResend:            defaultStaleRaceClientResendThreshold,
		staleRaceClientReconnect:         defaultStaleRaceClientReconnectThreshold,
		staleRaceClientReconnectWindow:   time.Duration(defaultStaleRaceClientReconnectWindowSeconds) * time.Second,
		staleRaceClientReconnectCooldown: time.Duration(defaultStaleRaceClientReconnectCooldownSecs) * time.Second,
		staleRaceCooldown:                time.Duration(defaultStaleRaceRecoveryCooldownSeconds) * time.Second,
	}
}

func newExpiredJobGlobalTracker(cfg expiredJobRecoveryConfig) *expiredJobGlobalTracker {
	tracker := &expiredJobGlobalTracker{}
	tracker.configure(cfg)
	return tracker
}

func configureExpiredJobRecovery(cfg Config) {
	globalExpiredJobRecovery.configure(expiredJobRecoveryConfigFromPoolConfig(cfg))
}

func expiredJobRecoveryConfigFromPoolConfig(cfg Config) expiredJobRecoveryConfig {
	recoveryCfg := defaultExpiredJobRecoveryConfig()
	recoveryCfg.enabled = !cfg.DisableExpiredJobRecovery
	if cfg.ExpiredJobRejectWindowSeconds > 0 {
		recoveryCfg.window = time.Duration(cfg.ExpiredJobRejectWindowSeconds) * time.Second
	}
	if cfg.ExpiredJobClientResendThreshold > 0 {
		recoveryCfg.clientResendThreshold = cfg.ExpiredJobClientResendThreshold
	}
	if cfg.ExpiredJobClientReconnectThreshold > 0 {
		recoveryCfg.clientReconnectThreshold = cfg.ExpiredJobClientReconnectThreshold
	}
	if cfg.ExpiredJobClientReconnectWindowSeconds > 0 {
		recoveryCfg.clientReconnectWindow = time.Duration(cfg.ExpiredJobClientReconnectWindowSeconds) * time.Second
	}
	if cfg.ExpiredJobClientReconnectCooldownSeconds > 0 {
		recoveryCfg.clientReconnectCooldown = time.Duration(cfg.ExpiredJobClientReconnectCooldownSeconds) * time.Second
	}
	if cfg.ExpiredJobGlobalCleanRefreshThreshold > 0 {
		recoveryCfg.globalCleanRefreshThreshold = cfg.ExpiredJobGlobalCleanRefreshThreshold
	}
	if cfg.ExpiredJobGlobalCleanRefreshMinClients > 0 {
		recoveryCfg.globalCleanRefreshMinClients = cfg.ExpiredJobGlobalCleanRefreshMinClients
	}
	if cfg.ExpiredJobRecoveryCooldownSeconds > 0 {
		recoveryCfg.cooldown = time.Duration(cfg.ExpiredJobRecoveryCooldownSeconds) * time.Second
	}
	if cfg.StaleRaceRejectWindowSeconds > 0 {
		recoveryCfg.staleRaceWindow = time.Duration(cfg.StaleRaceRejectWindowSeconds) * time.Second
	}
	if cfg.StaleRaceClientResendThreshold > 0 {
		recoveryCfg.staleRaceClientResend = cfg.StaleRaceClientResendThreshold
	}
	if cfg.StaleRaceClientReconnectThreshold > 0 {
		recoveryCfg.staleRaceClientReconnect = cfg.StaleRaceClientReconnectThreshold
	}
	if cfg.StaleRaceClientReconnectWindowSeconds > 0 {
		recoveryCfg.staleRaceClientReconnectWindow = time.Duration(cfg.StaleRaceClientReconnectWindowSeconds) * time.Second
	}
	if cfg.StaleRaceClientReconnectCooldownSeconds > 0 {
		recoveryCfg.staleRaceClientReconnectCooldown = time.Duration(cfg.StaleRaceClientReconnectCooldownSeconds) * time.Second
	}
	if cfg.StaleRaceRecoveryCooldownSeconds > 0 {
		recoveryCfg.staleRaceCooldown = time.Duration(cfg.StaleRaceRecoveryCooldownSeconds) * time.Second
	}
	return recoveryCfg
}

func expiredJobRecoveryConfigForClient(c *client) expiredJobRecoveryConfig {
	if c == nil || c.cfg == nil {
		return defaultExpiredJobRecoveryConfig()
	}
	return expiredJobRecoveryConfigFromPoolConfig(*c.cfg)
}

func (t *expiredJobGlobalTracker) configure(cfg expiredJobRecoveryConfig) {
	if cfg.window <= 0 {
		cfg.window = time.Duration(defaultExpiredJobRejectWindowSeconds) * time.Second
	}
	if cfg.clientResendThreshold <= 0 {
		cfg.clientResendThreshold = defaultExpiredJobClientResendThreshold
	}
	if cfg.clientReconnectThreshold <= 0 {
		cfg.clientReconnectThreshold = defaultExpiredJobClientReconnectThreshold
	}
	if cfg.clientReconnectWindow <= 0 {
		cfg.clientReconnectWindow = time.Duration(defaultExpiredJobClientReconnectWindowSeconds) * time.Second
	}
	if cfg.clientReconnectCooldown <= 0 {
		cfg.clientReconnectCooldown = time.Duration(defaultExpiredJobClientReconnectCooldownSecs) * time.Second
	}
	if cfg.globalCleanRefreshThreshold <= 0 {
		cfg.globalCleanRefreshThreshold = defaultExpiredJobGlobalCleanRefreshThreshold
	}
	if cfg.globalCleanRefreshMinClients <= 0 {
		cfg.globalCleanRefreshMinClients = defaultExpiredJobGlobalCleanRefreshMinClients
	}
	if cfg.cooldown <= 0 {
		cfg.cooldown = time.Duration(defaultExpiredJobRecoveryCooldownSeconds) * time.Second
	}
	if cfg.staleRaceWindow <= 0 {
		cfg.staleRaceWindow = time.Duration(defaultStaleRaceRejectWindowSeconds) * time.Second
	}
	if cfg.staleRaceClientResend <= 0 {
		cfg.staleRaceClientResend = defaultStaleRaceClientResendThreshold
	}
	if cfg.staleRaceClientReconnect < 0 {
		cfg.staleRaceClientReconnect = 0
	}
	if cfg.staleRaceClientReconnectWindow <= 0 {
		cfg.staleRaceClientReconnectWindow = time.Duration(defaultStaleRaceClientReconnectWindowSeconds) * time.Second
	}
	if cfg.staleRaceClientReconnectCooldown <= 0 {
		cfg.staleRaceClientReconnectCooldown = time.Duration(defaultStaleRaceClientReconnectCooldownSecs) * time.Second
	}
	if cfg.staleRaceCooldown <= 0 {
		cfg.staleRaceCooldown = time.Duration(defaultStaleRaceRecoveryCooldownSeconds) * time.Second
	}

	t.mu.Lock()
	t.config = cfg
	t.windowStart = time.Time{}
	t.rejects = 0
	t.clientKeys = nil
	t.lastRecovery = time.Time{}
	t.mu.Unlock()
}

func recordExpiredJobReject(c *client, now time.Time) expiredJobRecoveryDecision {
	if now.IsZero() {
		now = time.Now()
	}

	decision := expiredJobRecoveryDecision{}
	cfg := expiredJobRecoveryConfigForClient(c)

	if c != nil {
		c.recordExpiredJobReject(now)
		c.diffMu.Lock()
		if cfg.enabled {
			if c.expiredJobWindowStart.IsZero() || now.Sub(c.expiredJobWindowStart) > cfg.window {
				c.expiredJobWindowStart = now
				c.expiredJobWindowRejects = 0
			}
			c.expiredJobWindowRejects++
			decision.ClientRejects = c.expiredJobWindowRejects
			if c.expiredJobWindowRejects >= cfg.clientResendThreshold &&
				(c.lastExpiredJobRecovery.IsZero() || now.Sub(c.lastExpiredJobRecovery) >= cfg.cooldown) {
				c.lastExpiredJobRecovery = now
				decision.ClientResend = true
				recordExpiredJobClientRecoveryLocked(c, cfg, now, &decision)
			}
		}
		c.diffMu.Unlock()
	}

	if !cfg.enabled {
		return decision
	}

	globalRejects, cleanRefresh := globalExpiredJobRecovery.record(now, expiredJobClientKey(c))
	decision.GlobalRejects = globalRejects
	decision.CleanRefresh = cleanRefresh
	return decision
}

func recordExpiredJobClientRecoveryLocked(c *client, cfg expiredJobRecoveryConfig, now time.Time, decision *expiredJobRecoveryDecision) {
	if c == nil || decision == nil {
		return
	}
	if cfg.clientReconnectThreshold <= 0 {
		return
	}
	if c.expiredJobRecoveryStart.IsZero() || now.Sub(c.expiredJobRecoveryStart) > cfg.clientReconnectWindow {
		c.expiredJobRecoveryStart = now
		c.expiredJobRecoveryCount = 0
	}
	c.expiredJobRecoveryCount++
	decision.ClientRecoveries = c.expiredJobRecoveryCount
	if c.expiredJobRecoveryCount < cfg.clientReconnectThreshold {
		return
	}
	if !c.lastExpiredJobReconnect.IsZero() && now.Sub(c.lastExpiredJobReconnect) < cfg.clientReconnectCooldown {
		return
	}
	c.lastExpiredJobReconnect = now
	c.expiredJobRecoveryStart = now
	c.expiredJobRecoveryCount = 0
	decision.ClientReconnect = true
}

func (t *expiredJobGlobalTracker) record(now time.Time, clientKey string) (int, bool) {
	if now.IsZero() {
		now = time.Now()
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cfg := t.config
	if !cfg.enabled {
		return 0, false
	}
	if t.windowStart.IsZero() || now.Sub(t.windowStart) > cfg.window {
		t.windowStart = now
		t.rejects = 0
		t.clientKeys = nil
	}
	if t.clientKeys == nil {
		t.clientKeys = make(map[string]struct{}, cfg.globalCleanRefreshMinClients)
	}
	t.clientKeys[normalizeExpiredJobClientKey(clientKey)] = struct{}{}
	t.rejects++
	if t.rejects < cfg.globalCleanRefreshThreshold {
		return t.rejects, false
	}
	if len(t.clientKeys) < cfg.globalCleanRefreshMinClients {
		return t.rejects, false
	}
	if !t.lastRecovery.IsZero() && now.Sub(t.lastRecovery) < cfg.cooldown {
		return t.rejects, false
	}
	t.lastRecovery = now
	return t.rejects, true
}

func expiredJobClientKey(c *client) string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if lane := c.laneIdentity(); strings.HasPrefix(lane, "mac:") {
		return lane
	}
	if c.sessionID != "" {
		return "session:" + c.sessionID
	}
	if c.extranonce1 != ([4]byte{}) {
		return "en1:" + hex.EncodeToString(c.extranonce1[:])
	}
	if lane := c.laneIdentity(); strings.HasPrefix(lane, "ip:") {
		return lane
	}
	if c.username != "" {
		return "user:" + c.username
	}
	if c.addr != "" {
		return "addr:" + c.addr
	}
	return ""
}

func normalizeExpiredJobClientKey(clientKey string) string {
	if clientKey == "" {
		return "unknown"
	}
	return clientKey
}

func handleExpiredJobReject(c *client, jobID string) {
	handleExpiredJobRejectWithActions(c, jobID, defaultExpiredJobRecoveryActions)
}

func handleStaleTemplateRace(c *client, jobID, reason string) {
	if reason == "" {
		reason = "stale-template"
	}
	decision := recordStaleTemplateRace(c, time.Now())
	if decision.ClientReconnect {
		log.Printf("[RECOVERY] reconnecting %s after %d stale-template recoveries (job=%s reason=%s)",
			c.identityLogLabel(), decision.ClientRecoveries, jobID, reason)
		incStaleJobRecovery("client-reconnect", reason)
		go func() {
			time.Sleep(100 * time.Millisecond)
			closeClientConnection(c)
		}()
		return
	}
	if decision.ClientResend && currentJobReadyForResend(c) {
		log.Printf("[RECOVERY] resending current job to %s after %d stale-template races (job=%s reason=%s)",
			c.identityLogLabel(), decision.ClientRejects, jobID, reason)
		incStaleJobRecovery("client-resend", reason)
		go ResendJobToClientClean(c, true)
		return
	}
	incStaleJobRecovery("observed", reason)
}

func handleExpiredJobRejectWithActions(c *client, jobID string, actions expiredJobRecoveryActions) expiredJobRecoveryDecision {
	nowFn := actions.nowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	resendClient := actions.resendClient
	if resendClient == nil {
		resendClient = defaultExpiredJobRecoveryActions.resendClient
	}
	reconnectClient := actions.reconnectClient
	if reconnectClient == nil {
		reconnectClient = defaultExpiredJobRecoveryActions.reconnectClient
	}
	cleanRefresh := actions.cleanRefresh
	if cleanRefresh == nil {
		cleanRefresh = defaultExpiredJobRecoveryActions.cleanRefresh
	}

	decision := recordExpiredJobReject(c, nowFn())
	if decision.ClientReconnect && clientReadyForCurrentJob(c) {
		log.Printf("[RECOVERY] reconnecting stale miner %s after %d expired-job recovery attempts (job=%s)",
			c.identityLogLabel(), decision.ClientRecoveries, jobID)
		incExpiredJobRecovery("client-reconnect")
		cleanRefresh("expired job client reconnect")
		reconnectClient(c)
		return decision
	}
	if decision.ClientResend && currentJobReadyForResend(c) {
		log.Printf("[RECOVERY] resending current job to %s after %d expired job rejects (job=%s)",
			c.identityLogLabel(), decision.ClientRejects, jobID)
		incExpiredJobRecovery("client-resend")
		resendClient(c)
	}
	if decision.CleanRefresh {
		log.Printf("[RECOVERY] forcing clean template refresh after %d global expired job rejects (job=%s)",
			decision.GlobalRejects, jobID)
		incExpiredJobRecovery("clean-refresh")
		cleanRefresh("expired job flood")
	}
	return decision
}

func resetExpiredJobRecoveryState(c *client) {
	if c == nil {
		return
	}
	c.diffMu.Lock()
	c.expiredJobWindowStart = time.Time{}
	c.expiredJobWindowRejects = 0
	c.expiredJobRecoveryStart = time.Time{}
	c.expiredJobRecoveryCount = 0
	c.diffMu.Unlock()
}

func incExpiredJobRecovery(action string) {
	if expiredJobRecoveries != nil {
		expiredJobRecoveries.WithLabelValues(action).Inc()
	}
}

func recordStaleTemplateRace(c *client, now time.Time) expiredJobRecoveryDecision {
	if now.IsZero() {
		now = time.Now()
	}
	decision := expiredJobRecoveryDecision{}
	if c == nil {
		return decision
	}
	cfg := expiredJobRecoveryConfigForClient(c)
	if !cfg.enabled {
		return decision
	}

	c.diffMu.Lock()
	if c.staleRaceWindowStart.IsZero() || now.Sub(c.staleRaceWindowStart) > cfg.staleRaceWindow {
		c.staleRaceWindowStart = now
		c.staleRaceWindowRejects = 0
	}
	c.staleRaceWindowRejects++
	decision.ClientRejects = c.staleRaceWindowRejects
	if c.staleRaceWindowRejects >= cfg.staleRaceClientResend &&
		(c.lastStaleRaceRecovery.IsZero() || now.Sub(c.lastStaleRaceRecovery) >= cfg.staleRaceCooldown) {
		c.lastStaleRaceRecovery = now
		decision.ClientResend = true
		recordStaleRaceClientRecoveryLocked(c, cfg, now, &decision)
	}
	c.diffMu.Unlock()
	return decision
}

func recordStaleRaceClientRecoveryLocked(c *client, cfg expiredJobRecoveryConfig, now time.Time, decision *expiredJobRecoveryDecision) {
	if c == nil || decision == nil || cfg.staleRaceClientReconnect <= 0 {
		return
	}
	if c.staleRaceRecoveryStart.IsZero() || now.Sub(c.staleRaceRecoveryStart) > cfg.staleRaceClientReconnectWindow {
		c.staleRaceRecoveryStart = now
		c.staleRaceRecoveryCount = 0
	}
	c.staleRaceRecoveryCount++
	decision.ClientRecoveries = c.staleRaceRecoveryCount
	if c.staleRaceRecoveryCount < cfg.staleRaceClientReconnect {
		return
	}
	if !c.lastStaleRaceReconnect.IsZero() && now.Sub(c.lastStaleRaceReconnect) < cfg.staleRaceClientReconnectCooldown {
		return
	}
	c.lastStaleRaceReconnect = now
	c.staleRaceRecoveryStart = now
	c.staleRaceRecoveryCount = 0
	decision.ClientReconnect = true
}

func incStaleJobRecovery(action, reason string) {
	if staleJobRecoveries != nil {
		staleJobRecoveries.WithLabelValues(action, reason).Inc()
	}
}
