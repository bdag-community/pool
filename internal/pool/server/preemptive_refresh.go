package server

import (
	"log"
	"strings"
	"sync"
	"time"
)

const (
	defaultPreemptiveBlockRefreshDelay    = 250 * time.Millisecond
	defaultPreemptiveBlockRefreshInterval = defaultGBTMinInterval
	defaultPreemptiveBlockRefreshTimeout  = 1500 * time.Millisecond
	minPreemptiveBlockRefreshInterval     = defaultGBTMinInterval
	maxPreemptiveBlockRefreshTimeout      = 3 * time.Second
)

type preemptiveBlockRefreshRequest struct {
	seq          uint64
	oldParent    string
	backendName  string
	templateSeq  uint64
	cleanReissue bool
	delay        time.Duration
	interval     time.Duration
	timeout      time.Duration
}

var (
	preemptiveBlockRefreshMu      sync.Mutex
	preemptiveBlockRefreshLatest  preemptiveBlockRefreshRequest
	preemptiveBlockRefreshActive  bool
	preemptiveBlockRefreshSeq     uint64
	preemptiveBlockRefreshWorkers sync.WaitGroup
)

func maybeStartPreemptiveBlockCandidateRefresh(cfg *Config, job *JobData) bool {
	if !preemptiveBlockCandidateRefreshEnabled(cfg) || job == nil {
		return false
	}
	oldParent := normalizeHex(job.ParentHash)
	if oldParent == "" {
		return false
	}
	req := preemptiveBlockRefreshRequest{
		oldParent:    oldParent,
		backendName:  job.BackendName,
		templateSeq:  job.TemplateSeq,
		cleanReissue: preemptiveBlockCandidateCleanReissueEnabled(cfg),
		delay:        preemptiveBlockCandidateRefreshDelay(cfg),
		interval:     preemptiveBlockCandidateRefreshInterval(cfg),
		timeout:      preemptiveBlockCandidateRefreshTimeout(cfg),
	}
	requestPreemptiveBlockRefresh(req)
	return true
}

func preemptiveBlockCandidateRefreshEnabled(cfg *Config) bool {
	return cfg != nil && cfg.PreemptiveBlockCandidateRefreshEnabled
}

func preemptiveBlockCandidateCleanReissueEnabled(cfg *Config) bool {
	return cfg != nil && cfg.PreemptiveBlockCandidateCleanReissueEnabled
}

func preemptiveBlockCandidateRefreshDelay(cfg *Config) time.Duration {
	if cfg != nil && cfg.PreemptiveBlockCandidateRefreshDelayMS >= 0 {
		return time.Duration(cfg.PreemptiveBlockCandidateRefreshDelayMS) * time.Millisecond
	}
	return defaultPreemptiveBlockRefreshDelay
}

func preemptiveBlockCandidateRefreshInterval(cfg *Config) time.Duration {
	interval := defaultPreemptiveBlockRefreshInterval
	if cfg != nil && cfg.PreemptiveBlockCandidateRefreshIntervalMS > 0 {
		interval = time.Duration(cfg.PreemptiveBlockCandidateRefreshIntervalMS) * time.Millisecond
	}
	if interval < minPreemptiveBlockRefreshInterval {
		return minPreemptiveBlockRefreshInterval
	}
	return interval
}

func preemptiveBlockCandidateRefreshTimeout(cfg *Config) time.Duration {
	timeout := defaultPreemptiveBlockRefreshTimeout
	if cfg != nil && cfg.PreemptiveBlockCandidateRefreshTimeoutMS > 0 {
		timeout = time.Duration(cfg.PreemptiveBlockCandidateRefreshTimeoutMS) * time.Millisecond
	}
	if timeout > maxPreemptiveBlockRefreshTimeout {
		return maxPreemptiveBlockRefreshTimeout
	}
	return timeout
}

func requestPreemptiveBlockRefresh(req preemptiveBlockRefreshRequest) {
	preemptiveBlockRefreshMu.Lock()
	preemptiveBlockRefreshSeq++
	req.seq = preemptiveBlockRefreshSeq
	preemptiveBlockRefreshLatest = req
	if preemptiveBlockRefreshActive {
		preemptiveBlockRefreshMu.Unlock()
		return
	}
	preemptiveBlockRefreshActive = true
	preemptiveBlockRefreshMu.Unlock()

	preemptiveBlockRefreshWorkers.Add(1)
	go func() {
		defer preemptiveBlockRefreshWorkers.Done()
		runPreemptiveBlockRefreshWorker()
	}()
}

func runPreemptiveBlockRefreshWorker() {
	for {
		req := latestPreemptiveBlockRefreshRequest()
		runPreemptiveBlockRefreshAttempt(req)
		if finishPreemptiveBlockRefreshAttempt(req.seq) {
			return
		}
	}
}

func runPreemptiveBlockRefreshAttempt(req preemptiveBlockRefreshRequest) {
	if req.oldParent == "" {
		return
	}
	if req.delay > 0 {
		time.Sleep(req.delay)
	}
	deadline := time.Now().Add(req.timeout)
	cleanReissued := false
	for {
		if isPreemptiveBlockRefreshObsolete(req.seq) {
			return
		}
		if !preemptiveBlockRefreshStillOnOldParent(req.oldParent) {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		tpl, err := getRefreshBlockTemplate()
		if err != nil {
			logTemplateFetchError("preemptive template fetch error", err)
		} else if preemptiveTemplateParentAdvanced(tpl, req.oldParent) {
			if isPreemptiveBlockRefreshObsolete(req.seq) || !preemptiveBlockRefreshStillOnOldParent(req.oldParent) {
				return
			}
			parent := normalizeHex(tpl.PreviousHash)
			reason := "block candidate in-flight parent advance"
			invalidated := maybeBroadcastCleanTemplate(tpl, reason, req.backendName)
			log.Printf("[REFRESH] preemptive parent advance old_parent=%s new_parent=%s template_seq=%d invalidated_jobs=%d",
				shortHash(req.oldParent), shortHash(parent), req.templateSeq, invalidated)
			return
		} else if req.cleanReissue && !cleanReissued {
			if isPreemptiveBlockRefreshObsolete(req.seq) || !preemptiveBlockRefreshStillOnOldParent(req.oldParent) {
				return
			}
			invalidated := maybeBroadcastCleanTemplate(tpl, "block candidate in-flight clean reissue", req.backendName)
			log.Printf("[REFRESH] preemptive clean reissue parent=%s template_seq=%d invalidated_jobs=%d",
				shortHash(req.oldParent), req.templateSeq, invalidated)
			cleanReissued = true
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(req.interval)
	}
}

func preemptiveBlockRefreshStillOnOldParent(oldParent string) bool {
	currentParent := normalizeHex(CurrentParent())
	return currentParent != "" && oldParent != "" && strings.EqualFold(currentParent, oldParent)
}

func preemptiveTemplateParentAdvanced(tpl *BlockTemplate, oldParent string) bool {
	if tpl == nil {
		return false
	}
	parent := normalizeHex(tpl.PreviousHash)
	return parent != "" && oldParent != "" && !strings.EqualFold(parent, oldParent)
}

func latestPreemptiveBlockRefreshRequest() preemptiveBlockRefreshRequest {
	preemptiveBlockRefreshMu.Lock()
	defer preemptiveBlockRefreshMu.Unlock()
	return preemptiveBlockRefreshLatest
}

func isPreemptiveBlockRefreshObsolete(seq uint64) bool {
	preemptiveBlockRefreshMu.Lock()
	defer preemptiveBlockRefreshMu.Unlock()
	return preemptiveBlockRefreshLatest.seq != seq
}

func finishPreemptiveBlockRefreshAttempt(seq uint64) bool {
	preemptiveBlockRefreshMu.Lock()
	defer preemptiveBlockRefreshMu.Unlock()
	if preemptiveBlockRefreshLatest.seq != seq {
		return false
	}
	preemptiveBlockRefreshActive = false
	return true
}

func shortHash(hash string) string {
	hash = strings.TrimPrefix(normalizeHex(hash), "0x")
	if len(hash) <= 12 {
		return hash
	}
	return hash[:8] + "..." + hash[len(hash)-4:]
}
