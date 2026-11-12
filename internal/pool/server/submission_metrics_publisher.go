package server

import (
	"context"
	"sync"
	"time"

	"asicPool/internal/pool/storage"
)

// Reporting is not an admission/accounting gate. Share a single publisher among
// the submission, reconciliation and accounting paths so solved-block bursts do
// not compete with durable transactions for full-ledger aggregate scans.
// ReadSubmissionMetrics itself remains uncached for drain/recovery callers.
type submissionMetricsPublisherV1 struct {
	mu         sync.Mutex
	refreshing bool
	next       time.Time
	now        func() time.Time
}

func newSubmissionMetricsPublisherV1() *submissionMetricsPublisherV1 {
	return &submissionMetricsPublisherV1{now: time.Now}
}

func (p *submissionMetricsPublisherV1) update(ctx context.Context, journal storage.SubmissionJournal) error {
	if p == nil {
		return updateSubmissionMetricsV1(ctx, journal)
	}
	p.mu.Lock()
	if p.refreshing || p.now().Before(p.next) {
		p.mu.Unlock()
		return nil
	}
	p.refreshing = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.refreshing = false
		p.next = p.now().Add(time.Second)
		p.mu.Unlock()
	}()
	return updateSubmissionMetricsV1(ctx, journal)
}
