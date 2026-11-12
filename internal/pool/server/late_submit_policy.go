package server

import "time"

type lateBlockCandidateAction string

const (
	lateBlockCandidateSubmit      lateBlockCandidateAction = "submit"
	lateBlockCandidateSubmitStale lateBlockCandidateAction = "submit-stale"
	lateBlockCandidateRejectLocal lateBlockCandidateAction = "reject-local"
)

type lateBlockCandidateInput struct {
	cfg               *Config
	job               *JobData
	blockKey          string
	now               time.Time
	currentTip        string
	jobStaleReason    string
	checkDuplicate    bool
	durableSubmission bool
}

type lateBlockCandidateDecision struct {
	action                    lateBlockCandidateAction
	reason                    string
	rejectContext             blockCandidateRejectContext
	recordAgeFeedback         bool
	recordTemplatePressure    bool
	recordSubmitRecovery      bool
	recordLocalRejectAge      bool
	incrementStaleLocalDrop   bool
	incrementDuplicateDrop    bool
	blockSubmitOutcomeReason  string
	shareRejectLabel          string
	forceCleanRefreshReason   string
	handleStaleTemplateRace   bool
	duplicate                 bool
	duplicateBlockReplayCheck bool
}

func evaluateLateBlockCandidateSubmit(input lateBlockCandidateInput) lateBlockCandidateDecision {
	now := input.now
	if now.IsZero() {
		now = time.Now()
	}

	if input.jobStaleReason != "" {
		ctx := newBlockCandidateRejectContext(input.job, now, input.currentTip)
		if !shouldSubmitStaleBlockCandidate(input.cfg) && !shouldSubmitRecentStaleBlockCandidate(input.cfg, input.job, input.jobStaleReason, ctx, now) {
			return lateBlockCandidateDecision{
				action:                   lateBlockCandidateRejectLocal,
				reason:                   input.jobStaleReason,
				rejectContext:            ctx,
				recordAgeFeedback:        true,
				recordTemplatePressure:   true,
				recordSubmitRecovery:     true,
				recordLocalRejectAge:     true,
				incrementStaleLocalDrop:  true,
				blockSubmitOutcomeReason: input.jobStaleReason,
				shareRejectLabel:         "stale_block_candidate",
				forceCleanRefreshReason:  "stale block candidate: " + input.jobStaleReason,
				handleStaleTemplateRace:  true,
			}
		}
		return lateBlockCandidateDecision{
			action:                  lateBlockCandidateSubmitStale,
			reason:                  input.jobStaleReason,
			recordTemplatePressure:  true,
			forceCleanRefreshReason: "stale block candidate submit: " + input.jobStaleReason,
		}
	}

	if input.checkDuplicate {
		if duplicateReason := markBlockCandidateForSubmit(input.cfg, input.job, input.blockKey, now); duplicateReason != "" {
			label := "duplicate_block_global"
			if duplicateReason == "duplicate-block-job" {
				label = "duplicate_block"
			}
			return lateBlockCandidateDecision{
				action:                    lateBlockCandidateRejectLocal,
				reason:                    duplicateReason,
				rejectContext:             newBlockCandidateRejectContext(input.job, now, input.currentTip),
				recordSubmitRecovery:      true,
				recordLocalRejectAge:      true,
				incrementDuplicateDrop:    true,
				blockSubmitOutcomeReason:  "duplicate-block",
				shareRejectLabel:          label,
				duplicate:                 true,
				duplicateBlockReplayCheck: true,
			}
		}
	}

	return lateBlockCandidateDecision{action: lateBlockCandidateSubmit}
}

func evaluateDuplicateBlockCandidateSubmit(input lateBlockCandidateInput) lateBlockCandidateDecision {
	input.jobStaleReason = ""
	input.checkDuplicate = !input.durableSubmission
	return evaluateLateBlockCandidateSubmit(input)
}

func evaluateStaleBlockCandidateSubmit(input lateBlockCandidateInput) lateBlockCandidateDecision {
	input.blockKey = ""
	input.checkDuplicate = false
	return evaluateLateBlockCandidateSubmit(input)
}
