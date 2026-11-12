package server

import "time"

type submitPipeline struct {
	client    *client
	request   *stratumRequest
	start     time.Time
	lifecycle submitPipelineLifecycle
}

type submitPipelineLifecycle struct {
	job            *JobData
	classification string
	outcome        string
	reason         string
}

type submitPipelineParams struct {
	worker      string
	jobID       string
	extranonce2 string
	ntime       string
	nonce       string
}

func newSubmitPipeline(c *client, req *stratumRequest, start time.Time) *submitPipeline {
	return &submitPipeline{
		client:  c,
		request: req,
		start:   start,
		lifecycle: submitPipelineLifecycle{
			classification: submitLifecycleNoJob,
			outcome:        "rejected",
			reason:         "unknown",
		},
	}
}

func (p *submitPipeline) setLifecycle(job *JobData, classification, outcome, reason string) {
	if p == nil {
		return
	}
	p.lifecycle.job = job
	p.lifecycle.classification = classification
	p.lifecycle.outcome = outcome
	p.lifecycle.reason = reason
}

func (p *submitPipeline) noteResolvedJob(job *JobData, classification string) {
	if p == nil {
		return
	}
	p.lifecycle.job = job
	p.lifecycle.classification = classification
}

func (p *submitPipeline) params() (submitPipelineParams, bool) {
	if p == nil || p.request == nil {
		return submitPipelineParams{}, false
	}
	params := rawParamsToStrings(p.request.Params)
	if len(params) < 5 {
		return submitPipelineParams{}, false
	}
	return submitPipelineParams{
		worker:      params[0],
		jobID:       params[1],
		extranonce2: params[2],
		ntime:       params[3],
		nonce:       params[4],
	}, true
}

func (p *submitPipeline) observeLifecycle() {
	if p == nil {
		return
	}
	observeSubmitJobLifecycle(p.lifecycle.job, p.lifecycle.classification, p.lifecycle.outcome, p.lifecycle.reason, p.start)
}
