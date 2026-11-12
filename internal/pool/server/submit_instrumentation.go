package server

import "time"

const (
	submitLifecycleNoJob                 = "no_job"
	submitLifecycleCurrent               = "current"
	submitLifecycleRetained              = "retained"
	submitLifecycleRetainedNonCleanGrace = "retained_non_clean_grace"
	submitLifecycleRetainedStaleGrace    = "retained_stale_share_grace"
	submitLifecycleInvalidated           = "invalidated"
	submitLifecycleJobNotFound           = "job_not_found"
)

func submitRequestReceivedAt(req *stratumRequest, fallback time.Time) time.Time {
	if req != nil && !req.ReceivedAt.IsZero() {
		return req.ReceivedAt
	}
	return fallback
}

func observeSubmitReadToHandleLatency(req *stratumRequest, handlerStart time.Time) {
	if submitReadToHandleLatency == nil || req == nil || req.Method != "mining.submit" {
		return
	}
	receivedAt := submitRequestReceivedAt(req, handlerStart)
	latency := handlerStart.Sub(receivedAt)
	if latency < 0 {
		latency = 0
	}
	submitReadToHandleLatency.Observe(latency.Seconds())
}

func submitJobClassification(c *client, jobID string, job *JobData, current bool, now time.Time) string {
	if job == nil {
		if jobID == "" {
			return submitLifecycleNoJob
		}
		return submitLifecycleJobNotFound
	}
	if current {
		return submitLifecycleCurrent
	}
	if canAcceptRetainedJobForClient(c, job, now) {
		return submitLifecycleRetainedNonCleanGrace
	}
	if c != nil && canEvaluateStaleOrdinaryShareGrace(c.cfg, job, now) {
		return submitLifecycleRetainedStaleGrace
	}
	if jobInvalidatedByEpoch(job) || job.stale.Load() || job.invalidated.Load() {
		return submitLifecycleInvalidated
	}
	return submitLifecycleRetained
}

func observeSubmitJobLifecycle(job *JobData, classification, outcome, reason string, now time.Time) {
	classification = normalizeSubmitLifecycleLabel(classification, submitLifecycleNoJob)
	outcome = normalizeSubmitLifecycleLabel(outcome, "unknown")
	reason = normalizeSubmitLifecycleLabel(reason, "unknown")
	if now.IsZero() {
		now = time.Now()
	}

	if submitJobLifecycleEvents != nil {
		submitJobLifecycleEvents.WithLabelValues(classification, outcome, reason).Inc()
	}
	if job == nil {
		return
	}

	issuedAt := job.SentAt
	if issuedAt.IsZero() {
		issuedAt = job.CreatedAt
	}
	if submitJobNotifyToSubmitAge != nil && !issuedAt.IsZero() {
		age := now.Sub(issuedAt)
		if age < 0 {
			age = 0
		}
		submitJobNotifyToSubmitAge.WithLabelValues(classification, outcome, reason).Observe(age.Seconds())
	}

	if submitJobLifecycleSeqDistance == nil || job.TemplateSeq == 0 {
		return
	}
	currentSeq := miningWork.currentTemplateSeq()
	if currentSeq == 0 {
		return
	}
	distance := uint64(0)
	if currentSeq > job.TemplateSeq {
		distance = currentSeq - job.TemplateSeq
	}
	submitJobLifecycleSeqDistance.WithLabelValues(classification, outcome, reason).Observe(float64(distance))
}

func normalizeSubmitLifecycleLabel(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
