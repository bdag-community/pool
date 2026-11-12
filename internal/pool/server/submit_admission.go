package server

import (
	"log"
	"time"
)

type submitJobResolution struct {
	submittedJobID          string
	jobID                   string
	job                     *JobData
	current                 bool
	lifecycleClassification string
	reject                  *submitJobReject
}

type submitJobReject struct {
	lifecycleJob            *JobData
	lifecycleClassification string
	lifecycleOutcome        string
	lifecycleReason         string
	shareRejectLabel        string
	stratumCode             int
	stratumMessage          string
	expiredJobReject        bool
	staleTemplateRaceReason string
}

func resolveSubmitJob(c *client, submittedJobID, extranonce2 string, now time.Time) submitJobResolution {
	jobID := submittedJobID
	if canonicalJobID, aliasKind, ok := canonicalSubmittedJobID(c, submittedJobID, extranonce2); ok {
		jobID = canonicalJobID
		if submitJobIDAliases != nil {
			submitJobIDAliases.WithLabelValues(aliasKind).Inc()
		}
	}

	jobData, current := currentJobForClient(c, jobID)
	resolution := submitJobResolution{
		submittedJobID:          submittedJobID,
		jobID:                   jobID,
		job:                     jobData,
		current:                 current,
		lifecycleClassification: submitJobClassification(c, jobID, jobData, current, now),
	}
	if current {
		return resolution
	}

	tplVal, loaded := miningWork.loadAcceptedJobValue(jobID)
	if !loaded {
		if submittedJobID != jobID {
			log.Printf("⚠️ [Submit Error] Job ID %s canonicalized to %s not found in acceptedJobs (Expired)", submittedJobID, jobID)
		} else {
			log.Printf("⚠️ [Submit Error] Job ID %s not found in acceptedJobs (Expired)", jobID)
		}
		resolution.reject = &submitJobReject{
			lifecycleClassification: submitLifecycleJobNotFound,
			lifecycleOutcome:        "rejected",
			lifecycleReason:         "job_not_found",
			shareRejectLabel:        "job_not_found",
			stratumCode:             21,
			stratumMessage:          "job not found",
			expiredJobReject:        true,
		}
		return resolution
	}

	storedJob, storedOK := tplVal.(*JobData)
	if !storedOK || storedJob == nil {
		log.Printf("⚠️ [Submit Error] Invalid stored template for job %s", jobID)
		resolution.reject = &submitJobReject{
			lifecycleClassification: submitLifecycleJobNotFound,
			lifecycleOutcome:        "rejected",
			lifecycleReason:         "invalid_stored_template",
			stratumCode:             -32602,
			stratumMessage:          "invalid stored template",
		}
		return resolution
	}

	resolution.job = storedJob
	resolution.lifecycleClassification = submitJobClassification(c, jobID, storedJob, false, now)
	if shouldRequireCurrentJobSubmit(c.cfg) && !canEvaluateRetainedJobForClient(c, storedJob, now) {
		resolution.reject = &submitJobReject{
			lifecycleJob:            storedJob,
			lifecycleClassification: resolution.lifecycleClassification,
			lifecycleOutcome:        "rejected",
			lifecycleReason:         "non_current_job",
			shareRejectLabel:        "non_current_job",
			stratumCode:             21,
			stratumMessage:          "stale job",
			staleTemplateRaceReason: "non-current-job",
		}
	}
	return resolution
}

func applySubmitJobReject(c *client, req *stratumRequest, jobID string, reject *submitJobReject, setLifecycle func(*JobData, string, string, string)) {
	if reject == nil {
		return
	}
	if setLifecycle != nil {
		setLifecycle(reject.lifecycleJob, reject.lifecycleClassification, reject.lifecycleOutcome, reject.lifecycleReason)
	}
	if reject.shareRejectLabel != "" && sharesRejected != nil {
		sharesRejected.WithLabelValues(reject.shareRejectLabel).Inc()
	}
	if reject.staleTemplateRaceReason != "" {
		handleStaleTemplateRace(c, jobID, reject.staleTemplateRaceReason)
	}
	sendError(c, req.ID, reject.stratumCode, reject.stratumMessage)
	if reject.expiredJobReject {
		handleExpiredJobReject(c, jobID)
	}
}
