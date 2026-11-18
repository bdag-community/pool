package server

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"asicPool/internal/pool/storage"
)

const acceptedAccountingPassTimeout = 30 * time.Second

// The primary journal interface remains V1-compatible for its existing test
// doubles and recovery callers. Postgres advertises this additive V2 surface.
type wideAcceptedAccountingJournal interface {
	VerifyAcceptedAccountingV2(context.Context) error
	ApplyAcceptedAccountingV2(context.Context, [32]byte) (storage.AcceptedAccountingApplyResultV2, error)
	ReconcileAcceptedAccountingV2(context.Context) (storage.AcceptedAccountingReconcileResultV2, error)
}

type acceptedAccountingConsumer struct {
	journal           storage.SubmissionJournal
	interval          time.Duration
	stop              <-chan struct{}
	wg                sync.WaitGroup
	metricsPublisher  *submissionMetricsPublisherV1
	reconcileOverride func(context.Context) (storage.AcceptedAccountingReconcileResultV2, error)
}

func newAcceptedAccountingConsumer(journal storage.SubmissionJournal, interval time.Duration, stop <-chan struct{}, publisher ...*submissionMetricsPublisherV1) *acceptedAccountingConsumer {
	if interval <= 0 {
		interval = time.Second
	}
	metrics := newSubmissionMetricsPublisherV1()
	if len(publisher) > 0 && publisher[0] != nil {
		metrics = publisher[0]
	}
	return &acceptedAccountingConsumer{journal: journal, interval: interval, stop: stop, metricsPublisher: metrics}
}

func (c *acceptedAccountingConsumer) reconcileOne(ctx context.Context) error {
	if c == nil || c.journal == nil {
		return errors.New("accepted accounting consumer is unavailable")
	}
	if c.reconcileOverride != nil {
		result, err := c.reconcileOverride(ctx)
		if err != nil {
			return err
		}
		if result.Applied != 0 {
			log.Printf("accepted accounting reconciliation applied=%d", result.Applied)
		}
		return c.metricsPublisher.update(ctx, c.journal)
	}
	if wide, ok := c.journal.(wideAcceptedAccountingJournal); ok {
		result, err := wide.ReconcileAcceptedAccountingV2(ctx)
		if err != nil {
			return err
		}
		if result.Applied != 0 {
			log.Printf("accepted accounting reconciliation applied=%d", result.Applied)
		}
		return c.metricsPublisher.update(ctx, c.journal)
	}
	return errors.New("durable submission journal lacks required wide accounting v2 implementation")
}

func (c *acceptedAccountingConsumer) start() {
	if c == nil || c.journal == nil {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), acceptedAccountingPassTimeout)
				err := c.reconcileOne(ctx)
				cancel()
				if err != nil {
					log.Printf("accepted accounting reconciliation failed: %v", err)
				}
			}
		}
	}()
}

func (c *acceptedAccountingConsumer) wait() {
	if c != nil {
		c.wg.Wait()
	}
}

func (c *submissionCoordinator) applyAcceptedAccounting(ctx context.Context, candidateID [32]byte) error {
	if c == nil || c.journal == nil {
		return errors.New("accepted accounting consumer is unavailable")
	}
	var result storage.AcceptedAccountingApplyResultV1
	var err error
	if wide, ok := c.journal.(wideAcceptedAccountingJournal); ok {
		result, err = wide.ApplyAcceptedAccountingV2(ctx, candidateID)
	} else {
		return errors.New("durable submission journal lacks required wide accounting v2 implementation")
	}
	if err != nil {
		return err
	}
	if result.Applied {
		log.Printf("accepted accounting materialized candidate=%x credit_lines=%d", candidateID, result.CreditLines)
	}
	// Reporting is already published by both background reconciliation paths.
	// Do not scan the full ledger before refreshing the accepted parent: durable
	// receipt/effect validation above remains synchronous and authoritative.
	return nil
}

func reconcileAcceptedAccountingAtStartup(ctx context.Context, journal storage.SubmissionJournal) error {
	if journal == nil {
		return errors.New("accepted accounting journal is unavailable")
	}
	wide, isWide := journal.(wideAcceptedAccountingJournal)
	if isWide {
		if err := wide.VerifyAcceptedAccountingV2(ctx); err != nil {
			return err
		}
		result, err := wide.ReconcileAcceptedAccountingV2(ctx)
		if err != nil {
			return err
		}
		if result.Applied != 0 {
			log.Printf("accepted accounting startup reconciliation applied=%d", result.Applied)
		}
		return wide.VerifyAcceptedAccountingV2(ctx)
	}
	return errors.New("durable submission journal lacks required wide accounting v2 implementation")
}
