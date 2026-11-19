package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

const maxUnpricedVerificationRows = 100000

type unpricedVerificationRow struct {
	id, shareID, digest        []byte
	fee, total, target, policy string
	cut                        int64
}

// VerifyUnpricedWorkV1 checks a bounded, consistent snapshot of unpriced work
// and the reviewed journal catalog. It is deliberately not a replacement for
// legacy accounting verification, native finality, wallet funding, payer
// fencing or payment authorization. No record is priced or modified here.
// The expected chain, backend and wallet must come from independently verified
// operator configuration, never from the candidate rows being checked.
func (s *PostgresStorage) VerifyUnpricedWorkV1(ctx context.Context, chainID *big.Int, backendID string, wallet common.Address) error {
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil || wallet == (common.Address{}) {
		return errors.New("unpriced verification inputs unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("unpriced verification requires a deadline")
	}
	if err := validateJournalIdentity(chainID, backendID); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollbackUnpricedTx(tx)
	if err = s.verifyUnpricedWorkTxV1(ctx, tx, chainID, backendID, wallet); err != nil {
		return err
	}
	if err = tx.Rollback(ctx); err != nil {
		return fmt.Errorf("unpriced verification snapshot close: %w", err)
	}
	return nil
}

// VerifyAccountingWithUnpricedV1 checks the unpriced work and every existing
// legacy accounting intent/effect in one repeatable-read snapshot. It does not
// itself establish submission-journal readiness, canonical reward evidence,
// finality or payment authority, and is not wired into production startup.
func (s *PostgresStorage) VerifyAccountingWithUnpricedV1(ctx context.Context, chainID *big.Int, backendID string, wallet common.Address) error {
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil || wallet == (common.Address{}) {
		return errors.New("combined accounting verification inputs unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("combined accounting verification requires a deadline")
	}
	if err := validateJournalIdentity(chainID, backendID); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollbackUnpricedTx(tx)
	if err = s.verifyUnpricedWorkTxV1(ctx, tx, chainID, backendID, wallet); err != nil {
		return err
	}
	// The legacy catalog verifier uses current_schema() and unqualified table
	// names. PostgreSQL searches pg_catalog implicitly before this path.
	if _, err = tx.Exec(ctx, `SET LOCAL search_path=public`); err != nil {
		return err
	}
	if err = s.verifyAcceptedAccountingV2Tx(ctx, tx, nil, true); err != nil {
		return err
	}
	if err = tx.Rollback(ctx); err != nil {
		return fmt.Errorf("combined verification snapshot close: %w", err)
	}
	return nil
}

func (s *PostgresStorage) verifyUnpricedWorkTxV1(ctx context.Context, tx pgx.Tx, chainID *big.Int, backendID string, wallet common.Address) error {
	return s.verifyUnpricedWorkCandidateTxV1(ctx, tx, chainID, backendID, wallet, nil)
}

// A payment verifies its own immutable work, while still checking global
// catalog and conflict invariants. Full audits pass nil to verify every cut.
func (s *PostgresStorage) verifyUnpricedWorkCandidateTxV1(ctx context.Context, tx pgx.Tx, chainID *big.Int, backendID string, wallet common.Address, candidateID []byte) error {
	allowHeldDuplicate := s.unpricedSubmissionHold.Load() != nil
	var scope []byte
	if s.bestEffortAccounting.Load() {
		scope = candidateID
	}
	if err := s.verifyUnpricedStructureScopedV1(ctx, tx, allowHeldDuplicate, scope, false); err != nil {
		return err
	}
	return s.verifyUnpricedRowsTxV1(ctx, tx, chainID, backendID, wallet, candidateID)
}

func (s *PostgresStorage) verifyUnpricedStructureTxV1(ctx context.Context, tx pgx.Tx) error {
	return s.verifyUnpricedStructureWithScopedHoldV1(ctx, tx, false)
}

// Only the exact qualified duplicate is exempted; every work cut is still
// verified and any other conflict continues to fail closed.
func (s *PostgresStorage) verifyUnpricedStructureWithScopedHoldV1(ctx context.Context, tx pgx.Tx, allowSubmissionHold bool) error {
	return s.verifyUnpricedStructureScopedV1(ctx, tx, allowSubmissionHold, nil, false)
}

func (s *PostgresStorage) verifyUnpricedStructureScopedV1(ctx context.Context, tx pgx.Tx, allowSubmissionHold bool, candidateID []byte, ignoreHistorical bool) error {
	var err error
	if _, err = tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE public.block_submission_candidates_v1, public.block_submission_state_v1, public.block_submission_conflicts_v1, public.accepted_block_accounting_intents_v1, public.accepted_block_accounting_credit_lines_v1, public.accepted_block_accounting_effects_v1, public.accepted_block_accounting_conflicts_v1, public.blocks, public.credits, pool_share_journal_v1.shares, pool_share_journal_v1.cuts, pool_share_journal_v1.allocation_policies, pool_share_journal_v1.unpriced_discoveries_v1, pool_share_journal_v1.unpriced_conflicts_v1 IN ACCESS SHARE MODE`); err != nil {
		return fmt.Errorf("unpriced verification table lock: %w", err)
	}
	if err = sharejournal.VerifyCatalog(ctx, tx); err != nil {
		return fmt.Errorf("unpriced catalog: %w", err)
	}
	var conflicts, orphans, missing, priced int64
	err = tx.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM pool_share_journal_v1.unpriced_conflicts_v1 WHERE ($1::bytea IS NULL OR candidate_id=$1)),
 (SELECT count(*) FROM pool_share_journal_v1.cuts c LEFT JOIN pool_share_journal_v1.unpriced_discoveries_v1 d USING(candidate_id) WHERE d.candidate_id IS NULL AND ($1::bytea IS NULL OR c.candidate_id=$1)),
 (SELECT count(*) FROM public.block_submission_candidates_v1 c LEFT JOIN public.accepted_block_accounting_intents_v1 i USING(candidate_id) LEFT JOIN pool_share_journal_v1.unpriced_discoveries_v1 d USING(candidate_id) WHERE i.candidate_id IS NULL AND d.candidate_id IS NULL AND ($1::bytea IS NULL OR c.candidate_id=$1)),
 (SELECT count(*) FROM pool_share_journal_v1.unpriced_discoveries_v1 d LEFT JOIN public.accepted_block_accounting_intents_v1 i USING(candidate_id) LEFT JOIN public.accepted_block_accounting_effects_v1 e USING(candidate_id) WHERE (i.candidate_id IS NOT NULL OR e.candidate_id IS NOT NULL) AND ($1::bytea IS NULL OR d.candidate_id=$1))`, candidateID).Scan(&conflicts, &orphans, &missing, &priced)
	if err != nil {
		return err
	}
	if s.bestEffortAccounting.Load() && candidateID != nil && conflicts > 0 {
		// Independently classify each exact, receipted duplicate. An
		// unrelated dispute must not prevent preserving a known owner's work.
		query := `SELECT count(*) FROM pool_share_journal_v1.unpriced_conflicts_v1 uc WHERE uc.candidate_id=$1 AND NOT (` + unpricedSubmissionEvidenceExprSQLV1("uc.conflict_id", true) + `)`
		if err := tx.QueryRow(ctx, query, candidateID).Scan(&conflicts); err != nil {
			return err
		}
	}
	if s.bestEffortAccounting.Load() && candidateID != nil {
		// A submission conflict for this same candidate that has no matching
		// classified work dispute is still unknown attribution.
		var other int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.block_submission_conflicts_v1 f WHERE f.candidate_id=$1 AND NOT EXISTS(SELECT 1 FROM pool_share_journal_v1.unpriced_conflicts_v1 u WHERE u.conflict_id=f.conflict_id)`, candidateID).Scan(&other); err != nil {
			return err
		}
		conflicts += other
	}
	if allowSubmissionHold && conflicts == 1 {
		if hold := s.unpricedSubmissionHold.Load(); hold != nil {
			var qualified bool
			query := `SELECT ` + unpricedSubmissionEvidenceSQLV1(1, s.bestEffortAccounting.Load() && candidateID != nil)
			args := []any{hold[:]}
			if candidateID != nil {
				query += ` AND EXISTS(SELECT 1 FROM public.block_submission_conflicts_v1 WHERE conflict_id=$1 AND candidate_id=$2)`
				args = append(args, candidateID)
			}
			if err := tx.QueryRow(ctx, query, args...).Scan(&qualified); err != nil {
				return err
			} else if qualified {
				conflicts = 0
			}
		}
	}
	if !ignoreHistorical && (conflicts != 0 || orphans != 0 || missing != 0 || priced != 0) {
		return fmt.Errorf("unpriced verification failed conflicts=%d orphan_cuts=%d missing_work=%d unexpected_pricing=%d", conflicts, orphans, missing, priced)
	}
	return nil
}

func (s *PostgresStorage) verifyUnpricedRowsTxV1(ctx context.Context, tx pgx.Tx, chainID *big.Int, backendID string, wallet common.Address, candidateID []byte) error {
	rows, err := tx.Query(ctx, `SELECT d.candidate_id,d.discovery_share_id,d.work_digest,d.fee_float_bits::text,d.total_work::text,p.target_work::text,p.boundary_policy,COALESCE(c.share_sequence,0) FROM pool_share_journal_v1.unpriced_discoveries_v1 d JOIN pool_share_journal_v1.allocation_policies p USING(candidate_id) JOIN pool_share_journal_v1.cuts c USING(candidate_id) WHERE ($2::bytea IS NULL OR d.candidate_id=$2) ORDER BY d.candidate_id LIMIT $1`, maxUnpricedVerificationRows+1, candidateID)
	if err != nil {
		return err
	}
	var entries []unpricedVerificationRow
	for rows.Next() {
		var row unpricedVerificationRow
		if err = rows.Scan(&row.id, &row.shareID, &row.digest, &row.fee, &row.total, &row.target, &row.policy, &row.cut); err != nil {
			break
		}
		entries = append(entries, row)
		if len(entries) > maxUnpricedVerificationRows {
			err = errors.New("unpriced verification row limit exceeded")
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if candidateID != nil && len(entries) != 1 {
		return errors.New("unpriced verification candidate unavailable")
	}
	for _, row := range entries {
		if len(row.id) != 32 || len(row.digest) != 32 || (len(row.shareID) != 0 && len(row.shareID) != 32) || row.cut < 0 {
			return errors.New("invalid unpriced row identity")
		}
		var candidateID [32]byte
		copy(candidateID[:], row.id)
		view, err := scanCandidateView(ctx, tx, candidateID)
		if err != nil {
			return fmt.Errorf("unpriced candidate unavailable: %w", err)
		}
		if view.Candidate.ChainID.Cmp(chainID) != 0 || view.Candidate.BackendID != backendID || !bytes.Equal(view.Candidate.HeaderBytes[148:168], wallet[:]) {
			return errors.New("unpriced candidate chain, backend, or reward wallet differs")
		}
		fee, feeErr := strconv.ParseUint(row.fee, 10, 64)
		total, totalErr := strconv.ParseUint(row.total, 10, 64)
		target, targetErr := strconv.ParseUint(row.target, 10, 64)
		percent := math.Float64frombits(fee)
		policy := sharejournal.BoundaryPolicy(row.policy)
		if feeErr != nil || totalErr != nil || targetErr != nil || math.IsNaN(percent) || math.IsInf(percent, 0) || percent < 0 || percent > 100 || target == 0 || (policy != sharejournal.WholeShare && policy != sharejournal.PartialShare) {
			return errors.New("unpriced candidate policy is invalid")
		}
		if len(row.shareID) != 0 {
			if !bytes.Equal(row.shareID, view.Candidate.SharePowHash[:]) || row.cut == 0 {
				return errors.New("unpriced discovery share does not bind candidate")
			}
			var sequence int64
			if err = tx.QueryRow(ctx, `SELECT sequence FROM pool_share_journal_v1.shares WHERE share_id=$1`, row.shareID).Scan(&sequence); err != nil || sequence > row.cut {
				return errors.New("unpriced discovery share does not bind cut")
			}
		}
		work := map[string]uint64{}
		var actual uint64
		if row.cut != 0 {
			window, err := sharejournal.ReadWindowTx(ctx, tx, row.cut, target)
			if err != nil {
				return fmt.Errorf("unpriced work window: %w", err)
			}
			work, actual, err = sharejournal.AllocateWindow(window, target, policy)
			if err != nil {
				return err
			}
		}
		digest, err := unpricedWorkDigest(candidateID, row.shareID, fee, target, policy, row.cut, work, actual)
		if err != nil || actual != total || !bytes.Equal(digest[:], row.digest) {
			return errors.New("unpriced candidate work digest or total differs")
		}
	}
	return nil
}
