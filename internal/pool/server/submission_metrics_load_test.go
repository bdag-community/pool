package server

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"asicPool/internal/pool/storage"
)

type metricsLoadJournal struct {
	coordinatorJournal
	reads   atomic.Int64
	readErr atomic.Bool
}

func (j *metricsLoadJournal) ReadSubmissionMetrics(ctx context.Context) (storage.SubmissionMetricsV1, error) {
	j.reads.Add(1)
	if j.readErr.Load() {
		return storage.SubmissionMetricsV1{}, errors.New("metrics read failed")
	}
	return j.coordinatorJournal.ReadSubmissionMetrics(ctx)
}

func TestMetricsPublisherSharesRefreshButNotAccountingDecisions(t *testing.T) {
	ensureSubmissionTestMetrics()
	j := &metricsLoadJournal{}
	j.candidate.CandidateID = [32]byte{1}
	j.view.State = storage.SubmissionStateAcceptedFinal
	c := newSubmissionCoordinator(j, big.NewInt(1043), "node", nil)
	now := time.Unix(1, 0)
	c.metricsPublisher.now = func() time.Time { return now }
	consumer := newAcceptedAccountingConsumer(j, time.Second, nil, c.metricsPublisher)
	reconciler := newSubmissionReconciler(j, c, time.Second, time.Second, nil)
	if err := c.applyAcceptedAccounting(context.Background(), j.candidate.CandidateID); err != nil {
		t.Fatal(err)
	}
	if err := consumer.reconcileOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if j.reads.Load() != 1 {
		t.Fatal("runtime paths did not share one metrics refresh")
	}
	// A changed accounting decision must fail now, not after the reporting interval.
	j.view.State = storage.SubmissionStatePending
	if err := c.applyAcceptedAccounting(context.Background(), j.candidate.CandidateID); err == nil {
		t.Fatal("accounting decision incorrectly cached")
	}
	now = now.Add(time.Second)
	if err := c.metricsPublisher.update(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if j.reads.Load() != 2 {
		t.Fatal("metrics did not refresh at the one-second boundary")
	}
	// Drain/recovery callers use direct reads: never a cached gauge or snapshot.
	for n := 0; n < 2; n++ {
		if _, err := j.ReadSubmissionMetrics(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if j.reads.Load() != 4 {
		t.Fatal("authoritative direct reads were coalesced")
	}
}

func TestMetricsPublisherRetainsErrorsAndRetriesNextInterval(t *testing.T) {
	ensureSubmissionTestMetrics()
	j := &metricsLoadJournal{}
	p := newSubmissionMetricsPublisherV1()
	now := time.Unix(1, 0)
	p.now = func() time.Time { return now }
	j.readErr.Store(true)
	if err := p.update(context.Background(), j); err == nil {
		t.Fatal("reporting failure suppressed")
	}
	for n := 0; n < 10; n++ {
		_ = p.update(context.Background(), j)
	}
	if j.reads.Load() != 1 {
		t.Fatal("failed reporting flooded the database")
	}
	j.readErr.Store(false)
	now = now.Add(time.Second)
	if err := p.update(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if j.reads.Load() != 2 {
		t.Fatal("reporting did not retry")
	}
}

// Repeated/idempotent accepted-work materialization must not make an expensive
// full-ledger metrics scan for each solved share. This only bounds reporting;
// the real accounting methods and their integrity gates still run every time.
func TestAcceptedAccountingMetricsAreBounded(t *testing.T) {
	ensureSubmissionTestMetrics()
	j := &metricsLoadJournal{}
	j.candidate.CandidateID = [32]byte{1}
	j.view.State = storage.SubmissionStateAcceptedFinal
	c := newSubmissionCoordinator(j, big.NewInt(1043), "node", nil)
	for n := 0; n < 20; n++ {
		if err := c.applyAcceptedAccounting(context.Background(), j.candidate.CandidateID); err != nil {
			t.Fatal(err)
		}
	}
	if got := j.reads.Load(); got != 0 {
		t.Fatalf("accepted-path full-ledger metrics reads = %d, want 0 (background loops publish)", got)
	}
}

func TestConcurrentAcceptedAccountingMetricsAreBounded(t *testing.T) {
	ensureSubmissionTestMetrics()
	j := &metricsLoadJournal{}
	j.candidate.CandidateID = [32]byte{1}
	j.view.State = storage.SubmissionStateAcceptedFinal
	c := newSubmissionCoordinator(j, big.NewInt(1043), "node", nil)
	var wg sync.WaitGroup
	for n := 0; n < 100; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.applyAcceptedAccounting(context.Background(), j.candidate.CandidateID); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := j.reads.Load(); got != 0 {
		t.Fatalf("concurrent accepted-path full-ledger metrics reads = %d, want 0", got)
	}
}
