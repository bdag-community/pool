package storage

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// ReconcileAccountingWithUnpricedV1 applies only pre-existing accounting
// intents. It leaves accepted unpriced discoveries untouched until a separate
// reward and finality protocol has established their monetary value. This is
// not payout authorization and must not be wired into production yet.
func (s *PostgresStorage) ReconcileAccountingWithUnpricedV1(ctx context.Context, chainID *big.Int, backendID string, wallet common.Address) (AcceptedAccountingReconcileResultV2, error) {
	var result AcceptedAccountingReconcileResultV2
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil || wallet == (common.Address{}) {
		return result, errors.New("combined accounting reconciliation inputs unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("combined accounting reconciliation requires a deadline")
	}
	if err := validateJournalIdentity(chainID, backendID); err != nil {
		return result, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer rollbackUnpricedTx(tx)
	if err = s.verifyUnpricedWorkTxV1(ctx, tx, chainID, backendID, wallet); err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `SET LOCAL search_path=public`); err != nil {
		return result, err
	}
	if err = s.verifyAcceptedAccountingV2Tx(ctx, tx, nil, true); err != nil {
		return result, err
	}
	rows, err := tx.Query(ctx, `SELECT r.candidate_id FROM accepted_block_submission_receipts_v1 r JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) LEFT JOIN accepted_block_accounting_effects_v1 e USING(candidate_id) WHERE e.candidate_id IS NULL ORDER BY r.accepted_at,r.candidate_id LIMIT $1`, maxAccountingReconcileV1+1)
	if err != nil {
		return result, err
	}
	var pending [][32]byte
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			break
		}
		if len(raw) != 32 {
			err = errors.New("invalid accounting candidate")
			break
		}
		var id [32]byte
		copy(id[:], raw)
		pending = append(pending, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return result, err
	}
	// Do not change any monetary row if the snapshot exceeds our bound.
	if len(pending) > maxAccountingReconcileV1 {
		return result, errors.New("accepted accounting reconciliation bound exceeded")
	}
	if err = tx.Rollback(ctx); err != nil {
		return result, err
	}
	for _, id := range pending {
		applied, err := s.ApplyAcceptedAccountingV2(ctx, id)
		if err != nil {
			return result, err
		}
		if applied.Applied {
			result.Applied++
		}
	}
	return result, nil
}
