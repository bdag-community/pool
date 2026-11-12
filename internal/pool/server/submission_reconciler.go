package server

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"time"

	"asicPool/internal/pool/storage"
)

type submissionReconciler struct {
	journal     storage.SubmissionJournal
	coordinator *submissionCoordinator
	lease       time.Duration
	interval    time.Duration
	stop        <-chan struct{}
	wg          sync.WaitGroup
}

func newSubmissionReconciler(journal storage.SubmissionJournal, coordinator *submissionCoordinator, lease, interval time.Duration, stop <-chan struct{}) *submissionReconciler {
	if lease <= 0 {
		lease = defaultSubmissionLease
	}
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	return &submissionReconciler{journal: journal, coordinator: coordinator, lease: lease, interval: interval, stop: stop}
}

func (r *submissionReconciler) start() {
	if r == nil || r.journal == nil {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ticker.C:
				_ = r.reconcileOne(context.Background())
			}
		}
	}()
}

func (r *submissionReconciler) wait() {
	if r != nil {
		r.wg.Wait()
	}
}

func (r *submissionReconciler) reconcileOne(ctx context.Context) error {
	if r == nil || r.journal == nil {
		return errors.New("submission reconciler is unavailable")
	}
	defer func() {
		if r.coordinator != nil {
			_ = r.coordinator.metricsPublisher.update(context.Background(), r.journal)
		} else {
			_ = updateSubmissionMetricsV1(context.Background(), r.journal)
		}
	}()
	if _, err := r.journal.ReclaimExpiredLeases(ctx); err != nil {
		return err
	}
	var owner [16]byte
	if r.coordinator != nil {
		owner = r.coordinator.owner
	}
	leased, err := r.journal.ClaimNextDueCandidate(ctx, owner, r.lease)
	if err != nil {
		return err
	}
	if leased == nil {
		return nil
	}
	method := SubmitMethod(storage.SubmitMethodV2)
	if r.coordinator != nil && r.coordinator.legacyOnly.Load() && len(leased.Candidate.LastAttemptID) == 0 {
		method = SubmitMethod(storage.SubmitMethodLegacy)
	}
	prepared, err := r.journal.PrepareAttempt(ctx, leased.Lease, method)
	if err != nil {
		return err
	}
	sendCtx := ctx
	cancel := func() {}
	if r.coordinator != nil && r.coordinator.timeout > 0 {
		sendCtx, cancel = context.WithTimeout(ctx, r.coordinator.timeout)
	} else {
		sendCtx, cancel = context.WithTimeout(ctx, defaultSubmitRPCTimeout)
	}
	defer cancel()
	observation, sendErr := sendPreparedAttemptV1(sendCtx, prepared)
	decision := classifySubmitDecisionV1(leased.Candidate.Candidate, observation)
	if sendErr != nil && strictMethodNotFoundObservationV1(observation) {
		decision = storage.SubmitDecisionMethodNotFound
		if r.coordinator != nil {
			r.coordinator.legacyOnly.Store(true)
		}
	} else if sendErr != nil && decision != storage.SubmitDecisionConflict {
		decision = storage.SubmitDecisionPending
	}
	transition, err := r.journal.CompleteAttempt(ctx, leased.Lease, prepared.AttemptID, observation, decision)
	if err != nil {
		recordSubmissionAttemptMetricV1("reconciler", "completion_error", observation)
		return err
	}
	recordSubmissionAttemptMetricV1("reconciler", string(decision), observation)
	if transition.FirstAccepted && r.coordinator != nil {
		r.coordinator.applyFirstAcceptedEffects(leased.Candidate.Candidate, observation)
	}
	return nil
}

func submissionDurations(cfg *Config) (time.Duration, time.Duration, error) {
	if cfg == nil {
		return defaultSubmissionLease, defaultReconcileInterval, nil
	}
	timeout := configuredSubmitRPCTimeout(cfg.SubmitRPCTimeoutMS)
	lease := defaultSubmissionLease
	if cfg.SubmissionLeaseMS > 0 {
		lease = time.Duration(cfg.SubmissionLeaseMS) * time.Millisecond
	}
	if lease < timeout+minimumSubmissionLease {
		return 0, 0, errors.New("submission lease must be at least RPC timeout plus 5000ms")
	}
	if lease > 120*time.Second {
		return 0, 0, errors.New("submission lease exceeds 120 seconds")
	}
	interval := defaultReconcileInterval
	if cfg.SubmissionReconcileIntervalMS > 0 {
		interval = time.Duration(cfg.SubmissionReconcileIntervalMS) * time.Millisecond
	}
	if interval < 100*time.Millisecond || interval > 10*time.Second {
		return 0, 0, errors.New("invalid submission reconcile interval")
	}
	return lease, interval, nil
}

func parseSubmissionChainIDV1(raw string) (*big.Int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("POOL_SUBMISSION_CHAIN_ID is required")
	}
	if strings.HasPrefix(raw, "+") || strings.HasPrefix(raw, "-") {
		return nil, errors.New("submission chain id must be unsigned decimal")
	}
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return nil, errors.New("submission chain id must be unsigned decimal")
		}
	}
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok || n.Sign() < 0 {
		return nil, errors.New("invalid submission chain id")
	}
	return n, nil
}
