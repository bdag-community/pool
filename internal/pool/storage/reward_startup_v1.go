package storage

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// PrepareRewardStartupV1 checks global structural and accounting invariants.
// Each reward's immutable work and wallet are independently revalidated before
// allocation and every payout. Replaying every unrelated work window on every
// pool restart delays mining without strengthening that candidate's payout.
// If older accounting effects need reconciliation, retain the full audit and
// reconciliation path before applying them.
func (s *PostgresStorage) PrepareRewardStartupV1(ctx context.Context, chainID *big.Int, backendID string, wallet common.Address) error {
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil || wallet == (common.Address{}) {
		return errors.New("reward startup inputs unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("reward startup requires a deadline")
	}
	if err := validateJournalIdentity(chainID, backendID); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollbackUnpricedTx(tx)
	if err = s.verifyUnpricedStructureScopedV1(ctx, tx, true, nil, s.bestEffortAccounting.Load()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SET LOCAL search_path=public`); err != nil {
		return err
	}
	if !s.bestEffortAccounting.Load() {
		if err = s.verifyAcceptedAccountingV2Tx(ctx, tx, nil, true); err != nil {
			return err
		}
	}
	var pending bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM accepted_block_submission_receipts_v1 r JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) LEFT JOIN accepted_block_accounting_effects_v1 e USING(candidate_id) WHERE e.candidate_id IS NULL)`).Scan(&pending)
	if err != nil {
		return err
	}
	if err = tx.Rollback(ctx); err != nil {
		return err
	}
	if pending && !s.bestEffortAccounting.Load() {
		_, err = s.ReconcileAccountingWithUnpricedV1(ctx, chainID, backendID, wallet)
	}
	return err
}
