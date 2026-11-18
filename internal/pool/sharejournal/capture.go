package sharejournal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Capture struct {
	Sequence int64
	Created  bool
	Work     map[string]uint64
	Total    uint64
}

// CaptureWork freezes a candidate's share boundary and explicit allocation
// policy together. If share is nil, it captures the latest prior work without
// crediting a discovery share (for a network-valid share below pool target).
// A retry must match the original policy and cannot include later shares.
// Fee calculation, submission and payout remain separate, unintegrated gates.
func (j *Journal) CaptureWork(ctx context.Context, candidate [32]byte, share *Share, target uint64, policy BoundaryPolicy) (Capture, error) {
	if j == nil || j.pool == nil {
		return Capture{}, errors.New("share journal database is required")
	}
	if _, err := validateCapture(ctx, candidate, share, target, policy); err != nil {
		return Capture{}, err
	}
	tx, err := j.pool.Begin(ctx)
	if err != nil {
		return Capture{}, err
	}
	defer rollback(tx)
	outer, ok := tx.(*pgxpool.Tx)
	if !ok {
		return Capture{}, errors.New("share capture requires a top-level pool transaction")
	}
	result, err := CaptureWorkTx(ctx, outer, candidate, share, target, policy)
	if err != nil {
		return Capture{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Capture{}, fmt.Errorf("candidate work and policy commit unproven: %w", err)
	}
	return result, nil
}

func validateCapture(ctx context.Context, candidate [32]byte, share *Share, target uint64, policy BoundaryPolicy) (Share, error) {
	if !bounded(ctx) || candidate == ([32]byte{}) || target == 0 || (policy != WholeShare && policy != PartialShare) {
		return Share{}, errors.New("invalid candidate or explicit allocation policy")
	}
	if share == nil {
		return Share{}, nil
	}
	normalized, err := normalize(*share)
	if err != nil {
		return Share{}, errors.New("invalid discovery share")
	}
	return normalized, nil
}

// CaptureWorkTx operates in a top-level pool transaction. The caller must use
// the same verified database for candidate, journal and accounting records,
// and commit all of them together. Success remains provisional until that
// commit; this function does not insert or submit the candidate. Any error
// aborts the entire transaction, so no caller may commit partial capture work.
func CaptureWorkTx(ctx context.Context, outer *pgxpool.Tx, candidate [32]byte, share *Share, target uint64, policy BoundaryPolicy) (result Capture, err error) {
	return captureWorkTx(ctx, outer, candidate, share, target, policy, false)
}

// CaptureNewDiscoveryTx captures the latest work boundary for a newly inserted
// candidate. A valid share may already have been credited for a different
// candidate with the same proof; it must not be credited twice or move this
// new candidate's boundary back to the earlier discovery. The caller must
// establish that the candidate is new in this same transaction. Existing
// candidates must instead verify their stored discovery identity and replay
// their frozen cut without appending a share.
func CaptureNewDiscoveryTx(ctx context.Context, outer *pgxpool.Tx, candidate [32]byte, share *Share, target uint64, policy BoundaryPolicy) (result Capture, err error) {
	return captureWorkTx(ctx, outer, candidate, share, target, policy, true)
}

func captureWorkTx(ctx context.Context, outer *pgxpool.Tx, candidate [32]byte, share *Share, target uint64, policy BoundaryPolicy, latestCut bool) (result Capture, err error) {
	if outer == nil {
		return result, errors.New("top-level pool transaction is required")
	}
	defer func() {
		if err == nil {
			return
		}
		result = Capture{}
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if rollbackErr := outer.Rollback(rollbackCtx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			// pgx closes the underlying connection when a real transaction's
			// rollback fails. The result remains unproven and must not be retried
			// as if a commit or rollback had been acknowledged.
			err = errors.Join(err, fmt.Errorf("capture transaction rollback unproven: %w", rollbackErr))
		}
	}()
	normalized, err := validateCapture(ctx, candidate, share, target, policy)
	if err != nil {
		return result, err
	}
	tx := outer
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, appendLock); err != nil {
		return result, err
	}
	if latestCut {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_share_journal_v1.cuts WHERE candidate_id=$1)`, candidate[:]).Scan(&exists); err != nil {
			return result, err
		}
		if exists {
			return result, errors.New("new discovery already has a frozen cut")
		}
	}
	var requested *int64
	if share != nil {
		sequence, created, err := appendTx(ctx, tx, normalized)
		if err != nil {
			return result, err
		}
		result.Created = created
		if !latestCut {
			requested = &sequence
		}
	}
	result.Sequence, err = bindCutTx(ctx, tx, candidate, requested)
	if err != nil {
		return Capture{}, err
	}
	if err = bindPolicyTx(ctx, tx, candidate, target, policy); err != nil {
		return Capture{}, err
	}
	if result.Sequence == 0 {
		result.Work = map[string]uint64{}
	} else {
		window, err := windowTx(ctx, tx, result.Sequence, target)
		if err != nil {
			return Capture{}, fmt.Errorf("candidate work unavailable within transaction: %w", err)
		}
		result.Work, result.Total, err = AllocateWindow(window, target, policy)
		if err != nil {
			return Capture{}, err
		}
	}
	return result, nil
}

func bindPolicyTx(ctx context.Context, tx pgx.Tx, candidate [32]byte, target uint64, policy BoundaryPolicy) error {
	want := strconv.FormatUint(target, 10)
	_, err := tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.allocation_policies(candidate_id,boundary_policy,target_work)
 VALUES($1,$2,$3::numeric) ON CONFLICT(candidate_id) DO NOTHING`, candidate[:], string(policy), want)
	if err != nil {
		return err
	}
	var gotPolicy, gotTarget string
	if err = tx.QueryRow(ctx, `SELECT boundary_policy,target_work::text FROM pool_share_journal_v1.allocation_policies WHERE candidate_id=$1`, candidate[:]).Scan(&gotPolicy, &gotTarget); err != nil {
		return err
	}
	if gotPolicy != string(policy) || gotTarget != want {
		return errors.New("candidate allocation policy differs from committed discovery policy")
	}
	return nil
}
