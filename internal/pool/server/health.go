package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

const (
	poolJobHealthOK                      = "ok"
	poolJobHealthNoActiveMiners          = "no_active_miners"
	poolJobHealthNoAuthorizedMiners      = "no_authorized_miners"
	poolJobHealthMinersWithoutCurrentJob = "miners_without_current_job"
	poolJobHealthStaleCurrentJob         = "stale_current_job"
	poolJobHealthInvalidatedCurrentJob   = "invalidated_current_job"
	poolJobHealthExpiredCurrentJob       = "expired_current_job"

	poolJobIdentityUnknownConnections = "unknown_identity_connections"
	poolJobIdentityDuplicateMACLanes  = "duplicate_mac_lanes"
	poolJobIdentityNoPhysicalMiners   = "no_known_physical_miners"

	// The strict submission parent can advance just before the replacement job
	// is published. That is an expected fast-DAG transition, not a pool outage.
	// If publication does not catch up within this bound, compare clients with
	// the strict parent again so a real stalled refresh remains visible.
	poolJobParentTransitionGrace = 2 * time.Second
)

type poolJobHealthSnapshot struct {
	GeneratedAtUnixMs            int64                             `json:"generated_at_unix_ms"`
	Status                       string                            `json:"status"`
	ReasonCode                   string                            `json:"reason_code"`
	ActiveConnections            int                               `json:"active_connections"`
	AuthorizedConnections        int                               `json:"authorized_connections"`
	SubscribedConnections        int                               `json:"subscribed_connections"`
	ReadyConnections             int                               `json:"ready_connections"`
	ConnectionsWithoutCurrentJob int                               `json:"connections_without_current_job"`
	StaleCurrentJobConnections   int                               `json:"stale_current_job_connections"`
	InvalidCurrentJobConnections int                               `json:"invalid_current_job_connections"`
	ExpiredCurrentJobConnections int                               `json:"expired_current_job_connections"`
	PhysicalMiners               int                               `json:"physical_miners"`
	ReadyPhysicalMiners          int                               `json:"ready_physical_miners"`
	UnknownIdentityConnections   int                               `json:"unknown_identity_connections"`
	DuplicateMACs                int                               `json:"duplicate_macs"`
	DuplicateMACLanes            int                               `json:"duplicate_mac_lanes"`
	IdentityStatus               string                            `json:"identity_status"`
	IdentityReasonCode           string                            `json:"identity_reason_code"`
	CurrentTemplateSeq           uint64                            `json:"current_template_seq"`
	CurrentParent                string                            `json:"current_parent,omitempty"`
	PublishedParent              string                            `json:"published_parent,omitempty"`
	ReadinessParent              string                            `json:"readiness_parent,omitempty"`
	ParentTransition             bool                              `json:"parent_transition"`
	ParentTransitionAgeMs        int64                             `json:"parent_transition_age_ms,omitempty"`
	LastBroadcastAgeMs           int64                             `json:"last_broadcast_age_ms,omitempty"`
	MaxCurrentJobAgeMs           int64                             `json:"max_current_job_age_ms,omitempty"`
	AcceptedJobs                 poolAcceptedJobsHealth            `json:"accepted_jobs"`
	ExpiredJob                   poolExpiredJobHealth              `json:"expired_job"`
	SubmitStall                  poolSubmitStallHealth             `json:"submit_stall"`
	TemplateConversionStall      poolTemplateConversionStallHealth `json:"template_conversion_stall"`
	Clients                      []poolClientJobHealth             `json:"clients,omitempty"`
	Router                       *poolRouterHealth                 `json:"router,omitempty"`
}

type poolAcceptedJobsHealth struct {
	Total       int `json:"total"`
	Stale       int `json:"stale"`
	Invalidated int `json:"invalidated"`
	Submitted   int `json:"submitted"`
	Expired     int `json:"expired"`
}

type poolClientJobHealth struct {
	Address              string  `json:"address"`
	RemoteHost           string  `json:"remote_host,omitempty"`
	ASICMAC              string  `json:"asic_mac,omitempty"`
	LaneID               string  `json:"lane_id"`
	LaneIdentitySource   string  `json:"lane_identity_source"`
	Authorized           bool    `json:"authorized"`
	AuthorizedWorker     string  `json:"authorized_worker,omitempty"`
	Subscribed           bool    `json:"subscribed"`
	ExtranonceSubscribed bool    `json:"extranonce_subscribed"`
	Ready                bool    `json:"ready"`
	ReasonCode           string  `json:"reason_code"`
	CurrentJobID         string  `json:"current_job_id,omitempty"`
	CurrentJobAgeMs      int64   `json:"current_job_age_ms,omitempty"`
	TemplateSeq          uint64  `json:"template_seq,omitempty"`
	Backend              string  `json:"backend,omitempty"`
	Parent               string  `json:"parent,omitempty"`
	Pdiff                float64 `json:"pdiff,omitempty"`
	ExpiredRejects       int     `json:"expired_rejects"`
	ExpiredWindowRejects int     `json:"expired_window_rejects"`
	LowDiffRejects       int     `json:"low_diff_rejects"`
}

type poolExpiredJobHealth struct {
	Enabled                      bool  `json:"enabled"`
	WindowMs                     int64 `json:"window_ms"`
	WindowAgeMs                  int64 `json:"window_age_ms,omitempty"`
	WindowRejects                int   `json:"window_rejects"`
	WindowClients                int   `json:"window_clients"`
	ClientResendThreshold        int   `json:"client_resend_threshold"`
	ClientReconnectThreshold     int   `json:"client_reconnect_threshold"`
	ClientReconnectWindowMs      int64 `json:"client_reconnect_window_ms"`
	ClientReconnectCooldownMs    int64 `json:"client_reconnect_cooldown_ms"`
	GlobalCleanRefreshThreshold  int   `json:"global_clean_refresh_threshold"`
	GlobalCleanRefreshMinClients int   `json:"global_clean_refresh_min_clients"`
	CooldownMs                   int64 `json:"cooldown_ms"`
	LastRecoveryAgeMs            int64 `json:"last_recovery_age_ms,omitempty"`
	CooldownActive               bool  `json:"cooldown_active"`
}

type poolSubmitStallHealth struct {
	Enabled            bool   `json:"enabled"`
	WindowMs           int64  `json:"window_ms"`
	WindowAgeMs        int64  `json:"window_age_ms,omitempty"`
	MinAgeMs           int64  `json:"min_age_ms"`
	Failures           int    `json:"failures"`
	StaleFailures      int    `json:"stale_failures"`
	DuplicateFailures  int    `json:"duplicate_failures"`
	FailureThreshold   int    `json:"failure_threshold"`
	StaleThreshold     int    `json:"stale_threshold"`
	DuplicateThreshold int    `json:"duplicate_threshold"`
	CooldownMs         int64  `json:"cooldown_ms"`
	LastRecoveryAgeMs  int64  `json:"last_recovery_age_ms,omitempty"`
	CooldownActive     bool   `json:"cooldown_active"`
	ReasonCode         string `json:"reason_code"`
}

type poolTemplateConversionStallHealth struct {
	Enabled               bool    `json:"enabled"`
	WindowMs              int64   `json:"window_ms"`
	WindowAgeMs           int64   `json:"window_age_ms,omitempty"`
	MinAgeMs              int64   `json:"min_age_ms"`
	Accepted              int     `json:"accepted"`
	Failures              int     `json:"failures"`
	StaleFailures         int     `json:"stale_failures"`
	DuplicateFailures     int     `json:"duplicate_failures"`
	TotalCandidates       int     `json:"total_candidates"`
	FailureRatio          float64 `json:"failure_ratio"`
	FailureRatioThreshold float64 `json:"failure_ratio_threshold"`
	MinCandidates         int     `json:"min_candidates"`
	ActiveMiners          int     `json:"active_miners"`
	MinActiveMiners       int     `json:"min_active_miners"`
	CooldownMs            int64   `json:"cooldown_ms"`
	LastRecoveryAgeMs     int64   `json:"last_recovery_age_ms,omitempty"`
	CooldownActive        bool    `json:"cooldown_active"`
	LastReason            string  `json:"last_reason,omitempty"`
	ReasonCode            string  `json:"reason_code"`
}

type poolRouterHealth struct {
	Enabled  bool                   `json:"enabled"`
	NodeName string                 `json:"node_name,omitempty"`
	Node     *poolRouterBackendInfo `json:"node,omitempty"`
}

type poolRouterBackendInfo struct {
	Name               string  `json:"name"`
	Selected           bool    `json:"selected"`
	Healthy            bool    `json:"healthy"`
	Reason             string  `json:"reason,omitempty"`
	Score              float64 `json:"score"`
	LastTemplateAgeMs  int64   `json:"last_template_age_ms,omitempty"`
	LastTemplateHeight int64   `json:"last_template_height,omitempty"`
	LastTemplateSeq    uint64  `json:"last_template_seq,omitempty"`
	WSConnected        bool    `json:"ws_connected"`
	RecentSubmitErrors uint64  `json:"recent_submit_errors"`
	LastSubmitAgeMs    int64   `json:"last_submit_age_ms,omitempty"`
	LastSubmitError    string  `json:"last_submit_error,omitempty"`
}

func handlePoolJobHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshotPoolJobHealth(time.Now(), true))
}

func snapshotPoolJobHealth(now time.Time, includeClients bool) poolJobHealthSnapshot {
	if now.IsZero() {
		now = time.Now()
	}

	jobHealthPublicationMu.RLock()
	snapshot := poolJobHealthSnapshot{
		GeneratedAtUnixMs:  now.UnixMilli(),
		Status:             "degraded",
		ReasonCode:         poolJobHealthNoActiveMiners,
		CurrentTemplateSeq: miningWork.currentTemplateSeq(),
		CurrentParent:      CurrentParent(),
	}
	snapshot.PublishedParent = miningWork.publishedParent()
	if snapshot.PublishedParent == "" {
		snapshot.PublishedParent = normalizeHex(snapshot.CurrentParent)
	}
	snapshot.ReadinessParent = normalizeHex(snapshot.CurrentParent)
	if staleCurrentJobParent(snapshot.PublishedParent, snapshot.CurrentParent) {
		snapshot.ParentTransition = true
		if changedAt, ok := miningWork.currentParentChangedAt(); ok {
			age := now.Sub(changedAt)
			if age < 0 {
				age = 0
			}
			snapshot.ParentTransitionAgeMs = age.Milliseconds()
			if age <= poolJobParentTransitionGrace {
				snapshot.ReadinessParent = snapshot.PublishedParent
			}
		}
	}
	if last, ok := miningWork.lastBroadcastAt(); ok {
		snapshot.LastBroadcastAgeMs = now.Sub(last).Milliseconds()
		if snapshot.LastBroadcastAgeMs < 0 {
			snapshot.LastBroadcastAgeMs = 0
		}
	}

	clients := snapshotClients()
	snapshot.ActiveConnections = len(clients)
	if includeClients {
		snapshot.Clients = make([]poolClientJobHealth, 0, len(clients))
	}
	macConnections := map[string]int{}
	readyMACs := map[string]struct{}{}
	for _, c := range clients {
		clientSnapshot := snapshotClientJobHealth(c, now, snapshot.ReadinessParent)
		if mac := normalizeASICMAC(clientSnapshot.ASICMAC); mac != "" {
			macConnections[mac]++
			if clientSnapshot.Ready {
				readyMACs[mac] = struct{}{}
			}
		} else {
			snapshot.UnknownIdentityConnections++
		}
		if clientSnapshot.Authorized {
			snapshot.AuthorizedConnections++
		}
		if clientSnapshot.Subscribed {
			snapshot.SubscribedConnections++
		}
		if clientSnapshot.Ready {
			snapshot.ReadyConnections++
		}
		switch clientSnapshot.ReasonCode {
		case poolJobHealthMinersWithoutCurrentJob:
			snapshot.ConnectionsWithoutCurrentJob++
		case poolJobHealthStaleCurrentJob:
			snapshot.StaleCurrentJobConnections++
		case poolJobHealthInvalidatedCurrentJob:
			snapshot.InvalidCurrentJobConnections++
		case poolJobHealthExpiredCurrentJob:
			snapshot.ExpiredCurrentJobConnections++
		}
		if clientSnapshot.CurrentJobAgeMs > snapshot.MaxCurrentJobAgeMs {
			snapshot.MaxCurrentJobAgeMs = clientSnapshot.CurrentJobAgeMs
		}
		if includeClients {
			snapshot.Clients = append(snapshot.Clients, clientSnapshot)
		}
	}
	jobHealthPublicationMu.RUnlock()

	snapshot.AcceptedJobs = snapshotAcceptedJobsHealth(now)
	snapshot.ExpiredJob = snapshotExpiredJobHealth(now)
	snapshot.SubmitStall = snapshotSubmitStallHealth(now)
	snapshot.TemplateConversionStall = snapshotTemplateConversionStallHealth(now)
	snapshot.Router = snapshotPoolRouterHealth(now)
	snapshot.PhysicalMiners = len(macConnections)
	snapshot.ReadyPhysicalMiners = len(readyMACs)
	for _, connections := range macConnections {
		if connections > 1 {
			snapshot.DuplicateMACs++
			snapshot.DuplicateMACLanes += connections - 1
		}
	}
	if includeClients {
		sort.Slice(snapshot.Clients, func(i, j int) bool {
			if snapshot.Clients[i].LaneID != snapshot.Clients[j].LaneID {
				return snapshot.Clients[i].LaneID < snapshot.Clients[j].LaneID
			}
			return snapshot.Clients[i].Address < snapshot.Clients[j].Address
		})
	}

	snapshot.Status, snapshot.ReasonCode = poolJobHealthVerdict(snapshot)
	snapshot.IdentityStatus, snapshot.IdentityReasonCode = poolJobIdentityVerdict(snapshot)
	return snapshot
}

func snapshotClientJobHealth(c *client, now time.Time, currentParent string) poolClientJobHealth {
	if now.IsZero() {
		now = time.Now()
	}
	snapshot := poolClientJobHealth{ReasonCode: poolJobHealthOK}
	if c == nil {
		snapshot.ReasonCode = poolJobHealthMinersWithoutCurrentJob
		return snapshot
	}

	c.mu.Lock()
	snapshot.Address = c.addr
	snapshot.RemoteHost = c.remoteHost
	if snapshot.RemoteHost == "" {
		snapshot.RemoteHost = remoteHostFromAddr(c.addr)
	}
	snapshot.ASICMAC = normalizeASICMAC(c.asicMAC)
	snapshot.LaneID = c.laneIdentity()
	snapshot.LaneIdentitySource = clientLaneIdentitySource(snapshot.LaneID)
	snapshot.Authorized = c.authorized
	if snapshot.Authorized {
		snapshot.AuthorizedWorker = c.username
	}
	snapshot.Subscribed = c.sessionID != ""
	snapshot.ExtranonceSubscribed = c.extranonceSubscribed
	c.mu.Unlock()

	c.diffMu.Lock()
	snapshot.Pdiff = c.pdiff
	snapshot.ExpiredRejects = c.expiredJobRejects
	snapshot.ExpiredWindowRejects = c.expiredJobWindowRejects
	snapshot.LowDiffRejects = c.lowDiffRejects
	c.diffMu.Unlock()

	if !snapshot.Authorized {
		snapshot.ReasonCode = poolJobHealthNoAuthorizedMiners
		return snapshot
	}
	if !snapshot.Subscribed {
		snapshot.ReasonCode = poolJobHealthMinersWithoutCurrentJob
		return snapshot
	}

	current, _ := c.currentJob.Load().(*clientCurrentJob)
	if current == nil || current.job == nil {
		snapshot.ReasonCode = poolJobHealthMinersWithoutCurrentJob
		return snapshot
	}

	job := current.job
	snapshot.CurrentJobID = current.id
	snapshot.TemplateSeq = job.TemplateSeq
	snapshot.Backend = job.BackendName
	snapshot.Parent = job.ParentHash
	if !job.CreatedAt.IsZero() {
		snapshot.CurrentJobAgeMs = now.Sub(job.CreatedAt).Milliseconds()
		if snapshot.CurrentJobAgeMs < 0 {
			snapshot.CurrentJobAgeMs = 0
		}
	}

	switch {
	case jobInvalidatedByEpoch(job) || job.invalidated.Load():
		snapshot.ReasonCode = poolJobHealthInvalidatedCurrentJob
	case job.stale.Load() || staleCurrentJobParent(job.ParentHash, currentParent):
		snapshot.ReasonCode = poolJobHealthStaleCurrentJob
	case jobTTL > 0 && !job.CreatedAt.IsZero() && now.Sub(job.CreatedAt) > jobTTL:
		snapshot.ReasonCode = poolJobHealthExpiredCurrentJob
	default:
		snapshot.Ready = true
		snapshot.ReasonCode = poolJobHealthOK
	}
	return snapshot
}

func staleCurrentJobParent(jobParent, currentParent string) bool {
	jobParent = normalizeHex(jobParent)
	currentParent = normalizeHex(currentParent)
	return jobParent != "" && currentParent != "" && jobParent != currentParent
}

func snapshotAcceptedJobsHealth(now time.Time) poolAcceptedJobsHealth {
	if now.IsZero() {
		now = time.Now()
	}
	var snapshot poolAcceptedJobsHealth
	miningWork.rangeAcceptedJobs(func(_ string, job *JobData) bool {
		if job == nil {
			return true
		}
		snapshot.Total++
		if job.stale.Load() {
			snapshot.Stale++
		}
		if jobInvalidatedByEpoch(job) || job.invalidated.Load() {
			snapshot.Invalidated++
		}
		if job.blockSubmitted.Load() {
			snapshot.Submitted++
		}
		if jobTTL > 0 && !job.CreatedAt.IsZero() && now.Sub(job.CreatedAt) > jobTTL {
			snapshot.Expired++
		}
		return true
	})
	return snapshot
}

func snapshotExpiredJobHealth(now time.Time) poolExpiredJobHealth {
	if now.IsZero() {
		now = time.Now()
	}
	globalExpiredJobRecovery.mu.Lock()
	defer globalExpiredJobRecovery.mu.Unlock()

	cfg := globalExpiredJobRecovery.config
	snapshot := poolExpiredJobHealth{
		Enabled:                      cfg.enabled,
		WindowMs:                     cfg.window.Milliseconds(),
		WindowRejects:                globalExpiredJobRecovery.rejects,
		WindowClients:                len(globalExpiredJobRecovery.clientKeys),
		ClientResendThreshold:        cfg.clientResendThreshold,
		ClientReconnectThreshold:     cfg.clientReconnectThreshold,
		ClientReconnectWindowMs:      cfg.clientReconnectWindow.Milliseconds(),
		ClientReconnectCooldownMs:    cfg.clientReconnectCooldown.Milliseconds(),
		GlobalCleanRefreshThreshold:  cfg.globalCleanRefreshThreshold,
		GlobalCleanRefreshMinClients: cfg.globalCleanRefreshMinClients,
		CooldownMs:                   cfg.cooldown.Milliseconds(),
	}
	if !globalExpiredJobRecovery.windowStart.IsZero() {
		snapshot.WindowAgeMs = now.Sub(globalExpiredJobRecovery.windowStart).Milliseconds()
		if snapshot.WindowAgeMs < 0 {
			snapshot.WindowAgeMs = 0
		}
	}
	if !globalExpiredJobRecovery.lastRecovery.IsZero() {
		snapshot.LastRecoveryAgeMs = now.Sub(globalExpiredJobRecovery.lastRecovery).Milliseconds()
		if snapshot.LastRecoveryAgeMs < 0 {
			snapshot.LastRecoveryAgeMs = 0
		}
		snapshot.CooldownActive = now.Sub(globalExpiredJobRecovery.lastRecovery) < cfg.cooldown
	}
	return snapshot
}

func snapshotSubmitStallHealth(now time.Time) poolSubmitStallHealth {
	if now.IsZero() {
		now = time.Now()
	}
	globalSubmitStallRecovery.mu.Lock()
	defer globalSubmitStallRecovery.mu.Unlock()

	cfg := globalSubmitStallRecovery.config
	snapshot := poolSubmitStallHealth{
		Enabled:            cfg.enabled,
		WindowMs:           cfg.window.Milliseconds(),
		MinAgeMs:           cfg.minAge.Milliseconds(),
		Failures:           globalSubmitStallRecovery.failures,
		StaleFailures:      globalSubmitStallRecovery.staleFailures,
		DuplicateFailures:  globalSubmitStallRecovery.duplicateFailures,
		FailureThreshold:   cfg.failureThreshold,
		StaleThreshold:     cfg.staleThreshold,
		DuplicateThreshold: cfg.duplicateThreshold,
		CooldownMs:         cfg.cooldown.Milliseconds(),
		ReasonCode:         poolJobHealthOK,
	}
	if !globalSubmitStallRecovery.windowStart.IsZero() {
		snapshot.WindowAgeMs = now.Sub(globalSubmitStallRecovery.windowStart).Milliseconds()
		if snapshot.WindowAgeMs < 0 {
			snapshot.WindowAgeMs = 0
		}
	}
	if !globalSubmitStallRecovery.lastRecovery.IsZero() {
		snapshot.LastRecoveryAgeMs = now.Sub(globalSubmitStallRecovery.lastRecovery).Milliseconds()
		if snapshot.LastRecoveryAgeMs < 0 {
			snapshot.LastRecoveryAgeMs = 0
		}
		snapshot.CooldownActive = now.Sub(globalSubmitStallRecovery.lastRecovery) < cfg.cooldown
	}
	if snapshot.Failures >= snapshot.FailureThreshold {
		snapshot.ReasonCode = "failure_threshold"
	}
	if snapshot.StaleFailures >= snapshot.StaleThreshold {
		snapshot.ReasonCode = "stale_threshold"
	}
	if snapshot.DuplicateFailures >= snapshot.DuplicateThreshold {
		snapshot.ReasonCode = "duplicate_threshold"
	}
	return snapshot
}

func snapshotTemplateConversionStallHealth(now time.Time) poolTemplateConversionStallHealth {
	if now.IsZero() {
		now = time.Now()
	}
	globalTemplateConversionStallRecovery.mu.Lock()
	defer globalTemplateConversionStallRecovery.mu.Unlock()

	cfg := globalTemplateConversionStallRecovery.config
	snapshot := poolTemplateConversionStallHealth{
		Enabled:               cfg.enabled,
		WindowMs:              cfg.window.Milliseconds(),
		MinAgeMs:              cfg.minAge.Milliseconds(),
		Accepted:              globalTemplateConversionStallRecovery.accepted,
		Failures:              globalTemplateConversionStallRecovery.failures,
		StaleFailures:         globalTemplateConversionStallRecovery.staleFailures,
		DuplicateFailures:     globalTemplateConversionStallRecovery.duplicateFailures,
		TotalCandidates:       globalTemplateConversionStallRecovery.totalCandidates,
		FailureRatio:          globalTemplateConversionStallRecovery.failureRatioLocked(),
		FailureRatioThreshold: cfg.failureRatioThreshold,
		MinCandidates:         cfg.minCandidates,
		ActiveMiners:          globalTemplateConversionStallRecovery.activeMiners,
		MinActiveMiners:       cfg.minActiveMiners,
		CooldownMs:            cfg.cooldown.Milliseconds(),
		LastReason:            globalTemplateConversionStallRecovery.lastReason,
		ReasonCode:            poolJobHealthOK,
	}
	if !globalTemplateConversionStallRecovery.windowStart.IsZero() {
		snapshot.WindowAgeMs = now.Sub(globalTemplateConversionStallRecovery.windowStart).Milliseconds()
		if snapshot.WindowAgeMs < 0 {
			snapshot.WindowAgeMs = 0
		}
	}
	if !globalTemplateConversionStallRecovery.lastRecovery.IsZero() {
		snapshot.LastRecoveryAgeMs = now.Sub(globalTemplateConversionStallRecovery.lastRecovery).Milliseconds()
		if snapshot.LastRecoveryAgeMs < 0 {
			snapshot.LastRecoveryAgeMs = 0
		}
		snapshot.CooldownActive = now.Sub(globalTemplateConversionStallRecovery.lastRecovery) < cfg.cooldown
	}
	switch {
	case snapshot.ActiveMiners < snapshot.MinActiveMiners:
		snapshot.ReasonCode = "insufficient_active_miners"
	case snapshot.TotalCandidates < snapshot.MinCandidates:
		snapshot.ReasonCode = "warming_up"
	case snapshot.WindowAgeMs < snapshot.MinAgeMs:
		snapshot.ReasonCode = "minimum_age"
	case snapshot.FailureRatio >= snapshot.FailureRatioThreshold:
		snapshot.ReasonCode = "failure_ratio_threshold"
	}
	return snapshot
}

func snapshotPoolRouterHealth(now time.Time) *poolRouterHealth {
	router := activeRPCRouter
	if router == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	snapshot := &poolRouterHealth{
		Enabled: true,
	}
	if backend := router.selectedBackend(); backend != nil {
		snapshot.NodeName = backend.name
		state := backend.snapshotState()
		info := poolRouterBackendInfo{
			Name:               backend.name,
			Selected:           true,
			Healthy:            state.healthy,
			Reason:             state.reason,
			Score:              state.score,
			LastTemplateHeight: state.lastTemplateHeight,
			LastTemplateSeq:    state.lastTemplateSeq,
			WSConnected:        state.wsConnected,
			RecentSubmitErrors: state.recentSubmitErrors,
			LastSubmitError:    state.lastSubmitError,
		}
		if !state.lastTemplateAt.IsZero() {
			info.LastTemplateAgeMs = now.Sub(state.lastTemplateAt).Milliseconds()
			if info.LastTemplateAgeMs < 0 {
				info.LastTemplateAgeMs = 0
			}
		} else if state.lastTemplateAge > 0 {
			info.LastTemplateAgeMs = state.lastTemplateAge.Milliseconds()
		}
		if !state.lastSubmitAt.IsZero() {
			info.LastSubmitAgeMs = now.Sub(state.lastSubmitAt).Milliseconds()
			if info.LastSubmitAgeMs < 0 {
				info.LastSubmitAgeMs = 0
			}
		}
		snapshot.Node = &info
	}
	return snapshot
}

func poolJobHealthVerdict(snapshot poolJobHealthSnapshot) (string, string) {
	switch {
	case snapshot.ActiveConnections == 0:
		return "degraded", poolJobHealthNoActiveMiners
	case snapshot.AuthorizedConnections == 0:
		return "degraded", poolJobHealthNoAuthorizedMiners
	case snapshot.ConnectionsWithoutCurrentJob > 0:
		return "degraded", poolJobHealthMinersWithoutCurrentJob
	case snapshot.InvalidCurrentJobConnections > 0:
		return "degraded", poolJobHealthInvalidatedCurrentJob
	case snapshot.StaleCurrentJobConnections > 0:
		return "degraded", poolJobHealthStaleCurrentJob
	case snapshot.ExpiredCurrentJobConnections > 0:
		return "degraded", poolJobHealthExpiredCurrentJob
	default:
		return "ok", poolJobHealthOK
	}
}

func poolJobIdentityVerdict(snapshot poolJobHealthSnapshot) (string, string) {
	switch {
	case snapshot.ActiveConnections == 0:
		return "waiting", poolJobHealthNoActiveMiners
	case snapshot.PhysicalMiners == 0:
		return "warning", poolJobIdentityNoPhysicalMiners
	case snapshot.DuplicateMACLanes > 0:
		return "warning", poolJobIdentityDuplicateMACLanes
	case snapshot.UnknownIdentityConnections > 0:
		return "warning", poolJobIdentityUnknownConnections
	default:
		return "ok", poolJobHealthOK
	}
}

func publishPoolJobHealthMetrics(snapshot poolJobHealthSnapshot) {
	if poolJobHealthOKGauge == nil {
		return
	}
	ok := 0.0
	if snapshot.Status == "ok" {
		ok = 1
	}
	identityOK := 0.0
	if snapshot.IdentityStatus == "ok" {
		identityOK = 1
	}
	poolJobHealthOKGauge.Set(ok)
	poolJobHealthIdentityOKGauge.Set(identityOK)
	poolJobHealthAuthorizedMiners.Set(float64(snapshot.AuthorizedConnections))
	poolJobHealthReadyMiners.Set(float64(snapshot.ReadyConnections))
	poolJobHealthMinersWithoutCurrentJobGauge.Set(float64(snapshot.ConnectionsWithoutCurrentJob))
	poolJobHealthStaleCurrentJobMiners.Set(float64(snapshot.StaleCurrentJobConnections))
	poolJobHealthInvalidatedCurrentJobMiners.Set(float64(snapshot.InvalidCurrentJobConnections))
	poolJobHealthExpiredCurrentJobMiners.Set(float64(snapshot.ExpiredCurrentJobConnections))
	poolJobHealthPhysicalMiners.Set(float64(snapshot.PhysicalMiners))
	poolJobHealthReadyPhysicalMiners.Set(float64(snapshot.ReadyPhysicalMiners))
	poolJobHealthUnknownIdentityConnections.Set(float64(snapshot.UnknownIdentityConnections))
	poolJobHealthDuplicateMACs.Set(float64(snapshot.DuplicateMACs))
	poolJobHealthDuplicateMACLanes.Set(float64(snapshot.DuplicateMACLanes))
	poolJobHealthMaxCurrentJobAge.Set(float64(snapshot.MaxCurrentJobAgeMs) / 1000.0)
	poolJobHealthAcceptedJobs.WithLabelValues("total").Set(float64(snapshot.AcceptedJobs.Total))
	poolJobHealthAcceptedJobs.WithLabelValues("stale").Set(float64(snapshot.AcceptedJobs.Stale))
	poolJobHealthAcceptedJobs.WithLabelValues("invalidated").Set(float64(snapshot.AcceptedJobs.Invalidated))
	poolJobHealthAcceptedJobs.WithLabelValues("submitted").Set(float64(snapshot.AcceptedJobs.Submitted))
	poolJobHealthAcceptedJobs.WithLabelValues("expired").Set(float64(snapshot.AcceptedJobs.Expired))
	poolExpiredJobGlobalWindowRejects.Set(float64(snapshot.ExpiredJob.WindowRejects))
	poolExpiredJobGlobalWindowClients.Set(float64(snapshot.ExpiredJob.WindowClients))
	poolSubmitStallWindowFailures.WithLabelValues("all").Set(float64(snapshot.SubmitStall.Failures))
	poolSubmitStallWindowFailures.WithLabelValues("stale").Set(float64(snapshot.SubmitStall.StaleFailures))
	poolSubmitStallWindowFailures.WithLabelValues("duplicate").Set(float64(snapshot.SubmitStall.DuplicateFailures))
	poolTemplateConversionStallWindowCandidates.WithLabelValues("accepted").Set(float64(snapshot.TemplateConversionStall.Accepted))
	poolTemplateConversionStallWindowCandidates.WithLabelValues("failed").Set(float64(snapshot.TemplateConversionStall.Failures))
	poolTemplateConversionStallWindowCandidates.WithLabelValues("stale").Set(float64(snapshot.TemplateConversionStall.StaleFailures))
	poolTemplateConversionStallWindowCandidates.WithLabelValues("duplicate").Set(float64(snapshot.TemplateConversionStall.DuplicateFailures))
	poolTemplateConversionStallWindowCandidates.WithLabelValues("total").Set(float64(snapshot.TemplateConversionStall.TotalCandidates))
	poolTemplateConversionStallFailureRatio.Set(snapshot.TemplateConversionStall.FailureRatio)
	poolTemplateConversionStallActiveMiners.Set(float64(snapshot.TemplateConversionStall.ActiveMiners))
}
