package server

import (
	"sync"
	"sync/atomic"
	"time"
)

var miningWork = miningWorkState{}

// miningWorkState owns the pool's current and retained mining work. Its
// interface is intentionally small so submit, broadcast, health, and recovery
// code stop depending on the layout of the underlying maps and atomics.
type miningWorkState struct{}

var (
	acceptedJobs        sync.Map // jobID(string) -> *JobData
	submittedBlockMarks sync.Map // block hash(string) -> time.Time

	// Job invalidation is on the ASIC profitability path. A six-X100 pool on a
	// ~1s DAG can accept a block every few seconds, so invalidating by scanning
	// every retained job on each block turns old bookkeeping into fresh-work
	// latency. These epochs make clean-job invalidation O(1); individual jobs
	// compare their captured epoch against the current lane epoch at submit time.
	acceptedJobInvalidationEpoch atomic.Uint64
	backendJobInvalidationEpochs sync.Map // backend name(string) -> *atomic.Uint64
	lastJobInvalidationUnixNano  atomic.Int64
	lastBlockMarkCleanupUnix     atomic.Int64
	templateSeqCounter           atomic.Uint64
	// jobHealthPublicationMu makes a template publication atomic from the
	// health endpoint's point of view. A clean publication invalidates the old
	// jobs before replacing each client's current job; without this boundary a
	// health scrape can observe the intentionally mixed intermediate state and
	// report every productive ASIC as stale or invalidated.
	jobHealthPublicationMu sync.RWMutex

	parentMu   sync.Mutex
	lastParent string

	currentTemplateKey atomic.Value
	lastBroadcastTime  atomic.Value

	// Store copy of the latest broadcast info so we can resend to individual clients.
	lastSeq           atomic.Uint64
	lastBaseJobVal    atomic.Value // *JobData
	lastBaseNotifyVal atomic.Value // []interface{}
	currentParent     atomic.Value // holds string of current parent hash (normalized)
	// Store this before publishing a changed currentParent so readers that see
	// the new parent also see a conservative transition start time.
	currentParentChangedUnixNano atomic.Int64
)

func (miningWorkState) loadAcceptedJob(jobID string) (*JobData, bool) {
	value, ok := acceptedJobs.Load(jobID)
	if !ok {
		return nil, false
	}
	job, ok := value.(*JobData)
	return job, ok && job != nil
}

func (miningWorkState) loadAcceptedJobValue(jobID string) (interface{}, bool) {
	if jobID == "" {
		return nil, false
	}
	return acceptedJobs.Load(jobID)
}

func (miningWorkState) acceptedJobExists(jobID string) bool {
	_, ok := acceptedJobs.Load(jobID)
	return ok
}

func (miningWorkState) storeAcceptedJob(jobID string, job *JobData) {
	if jobID == "" || job == nil {
		return
	}
	acceptedJobs.Store(jobID, job)
}

func (miningWorkState) deleteAcceptedJob(jobID string) {
	if jobID == "" {
		return
	}
	acceptedJobs.Delete(jobID)
}

func (miningWorkState) rangeAcceptedJobs(fn func(key string, job *JobData) bool) {
	if fn == nil {
		return
	}
	acceptedJobs.Range(func(key, value interface{}) bool {
		jobID, _ := key.(string)
		job, _ := value.(*JobData)
		return fn(jobID, job)
	})
}

func (miningWorkState) rememberLatestBroadcast(baseNotify []interface{}, baseJob *JobData, seq uint64) {
	lastSeq.Store(seq)
	lastBaseJobVal.Store(baseJob)
	lastBaseNotifyVal.Store(baseNotify)
}

func (miningWorkState) latestBroadcast() ([]interface{}, *JobData, uint64) {
	baseNotify, _ := lastBaseNotifyVal.Load().([]interface{})
	baseJob, _ := lastBaseJobVal.Load().(*JobData)
	return baseNotify, baseJob, lastSeq.Load()
}

func (miningWorkState) currentTemplateSeq() uint64 {
	return lastSeq.Load()
}

func (miningWorkState) nextTemplateSeq() uint64 {
	return templateSeqCounter.Add(1)
}

func (miningWorkState) currentTemplateKey() string {
	key, _ := currentTemplateKey.Load().(string)
	return key
}

func (miningWorkState) rememberTemplateKey(key string) {
	currentTemplateKey.Store(key)
}

func (miningWorkState) lastBroadcastAt() (time.Time, bool) {
	last, ok := lastBroadcastTime.Load().(time.Time)
	return last, ok && !last.IsZero()
}

func (miningWorkState) recordBroadcastAt(at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	lastBroadcastTime.Store(at)
}

func (miningWorkState) rememberParent(parent string) bool {
	if parent == "" {
		return false
	}
	parentMu.Lock()
	changed := parent != lastParent
	if changed {
		lastParent = parent
		currentParentChangedUnixNano.Store(time.Now().UnixNano())
		currentParent.Store(parent)
	}
	parentMu.Unlock()
	return changed
}

func (miningWorkState) currentParent() string {
	s, _ := currentParent.Load().(string)
	return s
}

func (miningWorkState) currentParentChangedAt() (time.Time, bool) {
	unixNano := currentParentChangedUnixNano.Load()
	if unixNano <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, unixNano), true
}

func (s miningWorkState) publishedParent() string {
	_, job, _ := s.latestBroadcast()
	if job == nil {
		return ""
	}
	return normalizeHex(job.ParentHash)
}

func (miningWorkState) currentInvalidationEpoch() uint64 {
	return acceptedJobInvalidationEpoch.Load()
}

func (miningWorkState) backendInvalidationEpoch(name string) uint64 {
	if name == "" {
		return 0
	}
	value, ok := backendJobInvalidationEpochs.Load(name)
	if !ok {
		return 0
	}
	counter, ok := value.(*atomic.Uint64)
	if !ok || counter == nil {
		return 0
	}
	return counter.Load()
}

func (miningWorkState) incrementInvalidationEpoch(backendName string) {
	if backendName == "" {
		acceptedJobInvalidationEpoch.Add(1)
		return
	}
	value, _ := backendJobInvalidationEpochs.LoadOrStore(backendName, &atomic.Uint64{})
	counter, ok := value.(*atomic.Uint64)
	if !ok || counter == nil {
		return
	}
	counter.Add(1)
}

func (m miningWorkState) jobInvalidatedByEpoch(job *JobData) bool {
	if job == nil {
		return false
	}
	if job.InvalidationEpoch < m.currentInvalidationEpoch() {
		return true
	}
	return job.BackendName != "" && job.BackendInvalidationEpoch < m.backendInvalidationEpoch(job.BackendName)
}

func (miningWorkState) recordInvalidationAt(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	lastJobInvalidationUnixNano.Store(now.UnixNano())
}

func (miningWorkState) lastInvalidationAt() time.Time {
	ns := lastJobInvalidationUnixNano.Load()
	if ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (miningWorkState) markBlockCandidate(key string, now time.Time, ttl time.Duration) bool {
	if key == "" {
		return true
	}
	if now.IsZero() {
		now = time.Now()
	}
	existing, loaded := submittedBlockMarks.LoadOrStore(key, now)
	if loaded {
		ts, ok := existing.(time.Time)
		if ok && now.Sub(ts) < ttl {
			return false
		}
		submittedBlockMarks.Store(key, now)
	}
	return true
}

func (miningWorkState) maybeCleanupBlockCandidateMarks(now time.Time, interval, ttl time.Duration) bool {
	if now.IsZero() {
		now = time.Now()
	}
	nowUnix := now.Unix()
	lastUnix := lastBlockMarkCleanupUnix.Load()
	if lastUnix > 0 && now.Sub(time.Unix(lastUnix, 0)) < interval {
		return false
	}
	if !lastBlockMarkCleanupUnix.CompareAndSwap(lastUnix, nowUnix) {
		return false
	}
	submittedBlockMarks.Range(func(key, value interface{}) bool {
		ts, ok := value.(time.Time)
		if !ok || now.Sub(ts) >= ttl {
			submittedBlockMarks.Delete(key)
		}
		return true
	})
	return true
}

func (miningWorkState) cleanupBlockCandidateMarks(now time.Time, ttl time.Duration) {
	if now.IsZero() {
		now = time.Now()
	}
	submittedBlockMarks.Range(func(key, value interface{}) bool {
		ts, ok := value.(time.Time)
		if !ok || now.Sub(ts) >= ttl {
			submittedBlockMarks.Delete(key)
		}
		return true
	})
}
