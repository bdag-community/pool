package server

import "time"

func backendInvalidationEpoch(name string) uint64 {
	return miningWork.backendInvalidationEpoch(name)
}

func incrementBackendInvalidationEpoch(name string) uint64 {
	if name == "" {
		return 0
	}
	before := miningWork.backendInvalidationEpoch(name)
	miningWork.incrementInvalidationEpoch(name)
	return before + 1
}

func jobInvalidatedByEpoch(job *JobData) bool {
	return miningWork.jobInvalidatedByEpoch(job)
}

func markJobInvalidated(job *JobData, now time.Time) {
	if job == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	job.stale.Store(true)
	job.invalidated.Store(true)
	job.invalidatedAt.Store(now.UnixNano())
}

func jobInvalidatedAt(job *JobData) time.Time {
	if job == nil {
		return time.Time{}
	}
	ns := job.invalidatedAt.Load()
	if ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
