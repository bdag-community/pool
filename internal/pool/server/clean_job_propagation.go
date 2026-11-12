package server

import (
	"log"
	"strings"
	"time"
)

const (
	defaultCleanJobPropagationRepairCooldown         = time.Second
	defaultCleanJobPropagationRepairMinAge           = 250 * time.Millisecond
	defaultCleanJobPropagationReconnectThreshold     = 3
	defaultCleanJobPropagationReconnectCooldown      = 60 * time.Second
	cleanJobPropagationRepairActionResend            = "resend"
	cleanJobPropagationRepairActionReconnect         = "reconnect"
	cleanJobPropagationRepairActionSuppressed        = "suppressed"
	cleanJobPropagationRepairReasonRepairCooldown    = "repair-cooldown"
	cleanJobPropagationRepairReasonPropagationGrace  = "propagation-grace"
	cleanJobPropagationRepairReasonCurrentNotReady   = "current-job-not-ready"
	cleanJobPropagationRepairReasonReconnectCooldown = "reconnect-cooldown"
)

type cleanJobPropagationEvent struct {
	Lane            string
	MAC             string
	Address         string
	Worker          string
	ExpectedJobID   string
	SubmittedJobID  string
	ResolvedJobID   string
	Result          string
	Reason          string
	ExpectedSeq     uint64
	SubmittedSeq    uint64
	SeqLag          uint64
	ExpectedParent  string
	SubmittedParent string
	Age             time.Duration
	Current         bool
}

type cleanJobPropagationRepairConfig struct {
	enabled            bool
	reconnectEnabled   bool
	repairCooldown     time.Duration
	minRepairAge       time.Duration
	reconnectThreshold int
	reconnectCooldown  time.Duration
}

type cleanJobPropagationRepairDecision struct {
	Action string
	Reason string
	Lane   string
	MAC    string
	Streak int
}

func rememberClientCleanJob(c *client, jobID string, job *JobData) {
	if c == nil || job == nil || !job.Clean {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastCleanJobID = jobID
	c.lastCleanTemplateSeq = job.TemplateSeq
	c.lastCleanParent = normalizeHex(job.ParentHash)
	c.lastCleanSentAt = job.SentAt
	if c.lastCleanSentAt.IsZero() {
		c.lastCleanSentAt = time.Now()
	}
	c.lastCleanSubmitObserved = false
}

func observeCleanJobPropagation(c *client, worker string, resolution submitJobResolution, now time.Time) cleanJobPropagationRepairDecision {
	event, ok := cleanJobPropagationObservation(c, worker, resolution, now)
	if !ok {
		return cleanJobPropagationRepairDecision{}
	}
	if cleanJobSubmitObservations != nil {
		cleanJobSubmitObservations.WithLabelValues(event.Result, event.Reason).Inc()
	}
	if cleanJobSubmitSeqLag != nil {
		cleanJobSubmitSeqLag.WithLabelValues(event.Result).Observe(float64(event.SeqLag))
	}
	log.Printf("[CLEAN JOB] submit observed result=%s reason=%s worker=%s lane=%s mac=%s addr=%s expected_job=%s submitted_job=%s resolved_job=%s current=%t expected_seq=%d submitted_seq=%d seq_lag=%d expected_parent=%s submitted_parent=%s clean_age=%s",
		event.Result,
		event.Reason,
		emptyAsUnknown(event.Worker),
		emptyAsUnknown(event.Lane),
		emptyAsUnknown(event.MAC),
		emptyAsUnknown(event.Address),
		emptyAsUnknown(event.ExpectedJobID),
		emptyAsUnknown(event.SubmittedJobID),
		emptyAsUnknown(event.ResolvedJobID),
		event.Current,
		event.ExpectedSeq,
		event.SubmittedSeq,
		event.SeqLag,
		shortHash(event.ExpectedParent),
		shortHash(event.SubmittedParent),
		event.Age.Round(time.Millisecond),
	)
	return cleanJobPropagationRepairDecisionForEvent(c, event, now)
}

func cleanJobPropagationObservation(c *client, worker string, resolution submitJobResolution, now time.Time) (cleanJobPropagationEvent, bool) {
	if c == nil {
		return cleanJobPropagationEvent{}, false
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.mu.Lock()
	if c.lastCleanJobID == "" || c.lastCleanSubmitObserved {
		c.mu.Unlock()
		return cleanJobPropagationEvent{}, false
	}
	event := cleanJobPropagationEvent{
		Lane:           c.laneIdentity(),
		MAC:            normalizeASICMAC(c.asicMAC),
		Address:        c.addr,
		Worker:         strings.TrimSpace(worker),
		ExpectedJobID:  c.lastCleanJobID,
		ExpectedSeq:    c.lastCleanTemplateSeq,
		ExpectedParent: normalizeHex(c.lastCleanParent),
		Age:            now.Sub(c.lastCleanSentAt),
	}
	if event.Age < 0 {
		event.Age = 0
	}
	c.lastCleanSubmitObserved = true
	c.mu.Unlock()

	event.SubmittedJobID = strings.TrimSpace(resolution.submittedJobID)
	event.ResolvedJobID = strings.TrimSpace(resolution.jobID)
	event.Current = resolution.current
	event.Reason = "first-submit-after-clean"
	if resolution.reject != nil && resolution.reject.lifecycleReason != "" {
		event.Reason = cleanJobPropagationLabel(resolution.reject.lifecycleReason)
	}
	if resolution.job != nil {
		event.SubmittedSeq = resolution.job.TemplateSeq
		event.SubmittedParent = normalizeHex(resolution.job.ParentHash)
	}
	event.Result = classifyCleanJobPropagation(event, resolution)
	if event.ExpectedSeq > event.SubmittedSeq {
		event.SeqLag = event.ExpectedSeq - event.SubmittedSeq
	}
	return event, true
}

func cleanJobPropagationRepairDecisionForEvent(c *client, event cleanJobPropagationEvent, now time.Time) cleanJobPropagationRepairDecision {
	if c == nil {
		return cleanJobPropagationRepairDecision{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !cleanJobPropagationRepairEligible(event.Result) {
		resetCleanJobPropagationRepairState(c)
		return cleanJobPropagationRepairDecision{}
	}

	cfg := cleanJobPropagationRepairConfigForClient(c)
	if !cfg.enabled {
		return cleanJobPropagationRepairDecision{}
	}
	if cfg.minRepairAge > 0 && event.Age > 0 && event.Age < cfg.minRepairAge {
		deferCleanJobPropagationObservation(c, event)
		return cleanJobPropagationRepairDecision{
			Action: cleanJobPropagationRepairActionSuppressed,
			Reason: cleanJobPropagationRepairReasonPropagationGrace,
			Lane:   event.Lane,
			MAC:    event.MAC,
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanJobStaleSubmitStreak++
	decision := cleanJobPropagationRepairDecision{
		Lane:   event.Lane,
		MAC:    event.MAC,
		Reason: event.Result,
		Streak: c.cleanJobStaleSubmitStreak,
	}

	if cfg.reconnectThreshold > 0 && c.cleanJobStaleSubmitStreak >= cfg.reconnectThreshold {
		if c.lastCleanJobReconnect.IsZero() || now.Sub(c.lastCleanJobReconnect) >= cfg.reconnectCooldown {
			c.lastCleanJobReconnect = now
			c.lastCleanJobRepair = now
			c.cleanJobStaleSubmitStreak = 0
			decision.Action = cleanJobPropagationRepairActionReconnect
			return decision
		}
		decision.Action = cleanJobPropagationRepairActionSuppressed
		decision.Reason = cleanJobPropagationRepairReasonReconnectCooldown
		return decision
	}

	if c.lastCleanJobRepair.IsZero() || now.Sub(c.lastCleanJobRepair) >= cfg.repairCooldown {
		c.lastCleanJobRepair = now
		decision.Action = cleanJobPropagationRepairActionResend
		return decision
	}

	decision.Action = cleanJobPropagationRepairActionSuppressed
	decision.Reason = cleanJobPropagationRepairReasonRepairCooldown
	return decision
}

func applyCleanJobPropagationRepair(c *client, decision cleanJobPropagationRepairDecision) {
	if c == nil || decision.Action == "" {
		return
	}

	switch decision.Action {
	case cleanJobPropagationRepairActionResend:
		if !currentJobReadyForResend(c) {
			incCleanJobPropagationRepair(cleanJobPropagationRepairActionSuppressed, cleanJobPropagationRepairReasonCurrentNotReady)
			return
		}
		log.Printf("[CLEAN JOB] resending current clean job after stale clean-job propagation lane=%s mac=%s streak=%d reason=%s",
			emptyAsUnknown(decision.Lane), emptyAsUnknown(decision.MAC), decision.Streak, decision.Reason)
		incCleanJobPropagationRepair(cleanJobPropagationRepairActionResend, decision.Reason)
		go ResendJobToClientClean(c, true)
	case cleanJobPropagationRepairActionReconnect:
		log.Printf("[CLEAN JOB] reconnecting Stratum lane after repeated stale clean-job propagation lane=%s mac=%s streak=%d reason=%s",
			emptyAsUnknown(decision.Lane), emptyAsUnknown(decision.MAC), decision.Streak, decision.Reason)
		incCleanJobPropagationRepair(cleanJobPropagationRepairActionReconnect, decision.Reason)
		closeClientConnection(c)
	case cleanJobPropagationRepairActionSuppressed:
		incCleanJobPropagationRepair(cleanJobPropagationRepairActionSuppressed, decision.Reason)
	}
}

func cleanJobPropagationRepairEligible(result string) bool {
	switch result {
	case "old-after-clean", "missing-after-clean":
		return true
	default:
		return false
	}
}

func resetCleanJobPropagationRepairState(c *client) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.cleanJobStaleSubmitStreak = 0
	c.mu.Unlock()
}

func deferCleanJobPropagationObservation(c *client, event cleanJobPropagationEvent) {
	if c == nil || event.ExpectedJobID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastCleanJobID != event.ExpectedJobID || c.lastCleanTemplateSeq != event.ExpectedSeq {
		return
	}
	c.lastCleanSubmitObserved = false
}

func cleanJobPropagationRepairConfigForClient(c *client) cleanJobPropagationRepairConfig {
	if c == nil || c.cfg == nil {
		return defaultCleanJobPropagationRepairConfig()
	}
	return cleanJobPropagationRepairConfigFromPoolConfig(*c.cfg)
}

func defaultCleanJobPropagationRepairConfig() cleanJobPropagationRepairConfig {
	return cleanJobPropagationRepairConfig{
		repairCooldown:    defaultCleanJobPropagationRepairCooldown,
		minRepairAge:      defaultCleanJobPropagationRepairMinAge,
		reconnectCooldown: defaultCleanJobPropagationReconnectCooldown,
	}
}

func cleanJobPropagationRepairConfigFromPoolConfig(cfg Config) cleanJobPropagationRepairConfig {
	repairCfg := defaultCleanJobPropagationRepairConfig()
	repairCfg.enabled = cfg.EnableCleanJobPropagationRepair
	repairCfg.reconnectEnabled = cfg.EnableCleanJobPropagationReconnect
	if cfg.CleanJobPropagationRepairCooldownMS > 0 {
		repairCfg.repairCooldown = time.Duration(cfg.CleanJobPropagationRepairCooldownMS) * time.Millisecond
	}
	if cfg.CleanJobPropagationRepairMinAgeMS > 0 {
		repairCfg.minRepairAge = time.Duration(cfg.CleanJobPropagationRepairMinAgeMS) * time.Millisecond
	}
	if repairCfg.reconnectEnabled {
		repairCfg.reconnectThreshold = defaultCleanJobPropagationReconnectThreshold
		if cfg.CleanJobPropagationReconnectThreshold > 0 {
			repairCfg.reconnectThreshold = cfg.CleanJobPropagationReconnectThreshold
		}
	}
	if cfg.CleanJobPropagationReconnectCooldownSeconds > 0 {
		repairCfg.reconnectCooldown = time.Duration(cfg.CleanJobPropagationReconnectCooldownSeconds) * time.Second
	}
	return repairCfg
}

func incCleanJobPropagationRepair(action, reason string) {
	if cleanJobPropagationRepairs == nil {
		return
	}
	cleanJobPropagationRepairs.WithLabelValues(cleanJobPropagationLabel(action), cleanJobPropagationLabel(reason)).Inc()
}

func classifyCleanJobPropagation(event cleanJobPropagationEvent, resolution submitJobResolution) string {
	if resolution.job == nil {
		return "missing-after-clean"
	}
	if event.ResolvedJobID == event.ExpectedJobID && resolution.current {
		return "current-clean"
	}
	if cleanJobPropagationParentsDiffer(event.ExpectedParent, event.SubmittedParent) {
		return "old-after-clean"
	}
	if event.ExpectedSeq > 0 && event.SubmittedSeq >= event.ExpectedSeq {
		return "current-or-newer"
	}
	if event.ExpectedSeq > 0 && event.SubmittedSeq < event.ExpectedSeq {
		return "old-after-clean"
	}
	return "unknown"
}

func cleanJobPropagationParentsDiffer(expected, submitted string) bool {
	expected = normalizeHex(expected)
	submitted = normalizeHex(submitted)
	return expected != "" && submitted != "" && expected != submitted
}

func cleanJobPropagationLabel(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(" ", "_", "-", "_", ".", "_", ":", "_", "/", "_")
	value = replacer.Replace(value)
	if len(value) > 64 {
		return value[:64]
	}
	return value
}
