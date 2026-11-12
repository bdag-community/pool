package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"asicPool/internal/pool/storage"
)

const (
	submissionDrainSchema       = "blockdag-pool-submission-drain/v1"
	defaultSubmissionDrainWait  = 2 * time.Minute
	minimumSubmissionDrainWait  = 100 * time.Millisecond
	maximumSubmissionDrainWait  = 10 * time.Minute
	submissionDrainPollInterval = 100 * time.Millisecond
)

var (
	errSubmissionDrainActive = errors.New("candidate submissions are paused for dataset activation")
	drainOperationIDPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	activeSubmissionDrain    = newSubmissionDrainGate()
)

type submissionDrainGate struct {
	mu                 sync.Mutex
	paused             bool
	operationID        string
	pausedAt           time.Time
	activeSubmissions  int64
	activePublications int64
	idle               chan struct{}
}

type submissionDrainGateSnapshot struct {
	Paused             bool
	OperationID        string
	PausedAt           time.Time
	ActiveSubmissions  int64
	ActivePublications int64
}

type submissionDrainReport struct {
	Schema                       string    `json:"schema"`
	OperationID                  string    `json:"operation_id,omitempty"`
	ChainID                      string    `json:"chain_id,omitempty"`
	BackendID                    string    `json:"backend_id,omitempty"`
	CoinbaseAddress              string    `json:"coinbase_address,omitempty"`
	PoolFeePercent               float64   `json:"pool_fee_percent"`
	Paused                       bool      `json:"paused"`
	Ready                        bool      `json:"ready"`
	Classification               string    `json:"classification"`
	PausedAt                     time.Time `json:"paused_at,omitempty"`
	ActiveSubmitRequests         int64     `json:"active_submit_requests"`
	ActiveJobPublications        int64     `json:"active_job_publications"`
	PendingCandidates            int64     `json:"pending_candidates"`
	InFlightCandidates           int64     `json:"in_flight_candidates"`
	AccountingPending            int64     `json:"accounting_pending"`
	SubmissionConflicts          int64     `json:"submission_conflicts"`
	RecoveredSubmissionConflicts int64     `json:"recovered_submission_conflicts"`
	AccountingConflicts          int64     `json:"accounting_conflicts"`
	RecoveredAccountingConflicts int64     `json:"recovered_accounting_conflicts"`
	OldestPendingAgeSeconds      float64   `json:"oldest_pending_age_seconds"`
	Message                      string    `json:"message,omitempty"`
}

func newSubmissionDrainGate() *submissionDrainGate {
	return &submissionDrainGate{}
}

func (g *submissionDrainGate) enterSubmission() (func(), error) {
	return g.enter(true)
}

func (g *submissionDrainGate) enterPublication() (func(), error) {
	return g.enter(false)
}

func (g *submissionDrainGate) enter(candidate bool) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	g.mu.Lock()
	if g.paused {
		g.mu.Unlock()
		return nil, errSubmissionDrainActive
	}
	if g.activeSubmissions+g.activePublications == 0 {
		g.idle = make(chan struct{})
	}
	if candidate {
		g.activeSubmissions++
	} else {
		g.activePublications++
	}
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { g.leave(candidate) })
	}, nil
}

func (g *submissionDrainGate) leave(candidate bool) {
	if g == nil {
		return
	}
	g.mu.Lock()
	if candidate {
		if g.activeSubmissions > 0 {
			g.activeSubmissions--
		}
	} else if g.activePublications > 0 {
		g.activePublications--
	}
	if g.activeSubmissions+g.activePublications == 0 && g.idle != nil {
		close(g.idle)
		g.idle = nil
	}
	g.mu.Unlock()
}

func (g *submissionDrainGate) begin(ctx context.Context, operationID string) error {
	if g == nil {
		return errors.New("submission drain gate is unavailable")
	}
	if !drainOperationIDPattern.MatchString(operationID) {
		return errors.New("operation_id must be 8-128 safe identifier characters")
	}
	g.mu.Lock()
	if g.paused && g.operationID != operationID {
		current := g.operationID
		g.mu.Unlock()
		return fmt.Errorf("submission drain is already owned by operation %q", current)
	}
	if !g.paused {
		g.paused = true
		g.operationID = operationID
		g.pausedAt = time.Now().UTC()
	}
	idle := g.idle
	g.mu.Unlock()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *submissionDrainGate) resume(operationID string) error {
	if g == nil {
		return errors.New("submission drain gate is unavailable")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.paused {
		return nil
	}
	if operationID == "" || operationID != g.operationID {
		return errors.New("resume operation_id does not own the active submission drain")
	}
	g.paused = false
	g.operationID = ""
	g.pausedAt = time.Time{}
	return nil
}

func (g *submissionDrainGate) snapshot() submissionDrainGateSnapshot {
	if g == nil {
		return submissionDrainGateSnapshot{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return submissionDrainGateSnapshot{
		Paused:             g.paused,
		OperationID:        g.operationID,
		PausedAt:           g.pausedAt,
		ActiveSubmissions:  g.activeSubmissions,
		ActivePublications: g.activePublications,
	}
}

func submissionDrainStatus(ctx context.Context, journal storage.SubmissionJournal) (submissionDrainReport, error) {
	snapshot := activeSubmissionDrain.snapshot()
	report := submissionDrainReport{
		Schema:                submissionDrainSchema,
		OperationID:           snapshot.OperationID,
		BackendID:             activeSubmissionBackendID,
		Paused:                snapshot.Paused,
		PausedAt:              snapshot.PausedAt,
		ActiveSubmitRequests:  snapshot.ActiveSubmissions,
		ActiveJobPublications: snapshot.ActivePublications,
		Classification:        "not_paused",
	}
	if activeSubmissionChainID != nil {
		report.ChainID = activeSubmissionChainID.String()
	}
	if _, coinbase, err := configuredPoolCoinbaseAddress(); err == nil {
		report.CoinbaseAddress = coinbase
	}
	if activeSubmissionCoordinator != nil {
		report.PoolFeePercent = activeSubmissionCoordinator.poolFeePercent
	}
	if journal == nil {
		report.Classification = "submission_journal_unavailable"
		report.Message = "durable submission journal is unavailable"
		return report, errors.New(report.Message)
	}
	metrics, err := journal.ReadSubmissionMetrics(ctx)
	if err != nil {
		report.Classification = "submission_journal_unavailable"
		report.Message = "cannot read durable submission state"
		return report, err
	}
	report.PendingCandidates = metrics.Pending
	report.InFlightCandidates = metrics.InFlight
	report.AccountingPending = metrics.AccountingPendingCount
	report.SubmissionConflicts = metrics.Conflict
	report.RecoveredSubmissionConflicts = metrics.RecoveredConflict
	report.AccountingConflicts = metrics.AccountingConflictCount
	report.RecoveredAccountingConflicts = metrics.RecoveredAccountingConflict
	report.OldestPendingAgeSeconds = metrics.OldestPendingAge.Seconds()
	if metrics.Conflict != metrics.RecoveredConflict || metrics.AccountingConflictCount != metrics.RecoveredAccountingConflict {
		report.Classification = "submission_conflict"
		report.Message = "durable submission or accounting conflicts require repair"
		return report, errors.New(report.Message)
	}
	if snapshot.Paused && snapshot.ActiveSubmissions == 0 && snapshot.ActivePublications == 0 && metrics.Pending == 0 && metrics.InFlight == 0 && metrics.AccountingPendingCount == 0 {
		report.Ready = true
		report.Classification = "drained"
		return report, nil
	}
	if snapshot.Paused {
		report.Classification = "draining"
	} else {
		report.Message = "pause the pool before switching its node dataset"
	}
	return report, nil
}

func waitSubmissionDrain(ctx context.Context, journal storage.SubmissionJournal, operationID string) (submissionDrainReport, error) {
	if err := activeSubmissionDrain.begin(ctx, operationID); err != nil {
		report, _ := submissionDrainStatus(context.Background(), journal)
		report.Classification = "candidate_drain_timeout"
		report.Message = err.Error()
		return report, err
	}
	ticker := time.NewTicker(submissionDrainPollInterval)
	defer ticker.Stop()
	for {
		report, err := submissionDrainStatus(ctx, journal)
		if err != nil || report.Ready {
			return report, err
		}
		select {
		case <-ctx.Done():
			report.Classification = "candidate_drain_timeout"
			report.Message = ctx.Err().Error()
			return report, ctx.Err()
		case <-ticker.C:
		}
	}
}
