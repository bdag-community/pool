package payments

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
)

const ZeroCreditBatchLimit = 64

// SatisfyZeroCredits never sends funds, creates an intent, or fabricates a
// receipt. A zero liability is satisfied locally with append-only provenance.
// A nonzero/unknown value or prior zero audit cannot be silently rewritten.
func (f *postgresFence) SatisfyZeroCredits(ctx context.Context, max uint64) (int, error) {
	if err := f.live(); err != nil {
		return 0, err
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var unknown bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash WHERE c.amount=0 AND c.is_paid IS NULL AND b.status='MATURE' AND b.height>=0 AND b.height<=$1::numeric)`, strconv.FormatUint(max, 10)).Scan(&unknown); err != nil {
		return 0, err
	}
	if unknown {
		return 0, errors.New("zero credit has unknown paid flag; review required")
	}
	// Same native maturity coordinate as positive credits. Block locks serialize
	// final completion against positive confirmation, even across wallet fences.
	rows, err := tx.Query(ctx, `SELECT c.id,c.block_hash,c.miner_address,c.amount::text
 FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash
 WHERE c.is_paid=FALSE AND c.amount=0 AND b.status='MATURE' AND b.height>=0 AND b.height<=$1::numeric
 ORDER BY b.height,c.id LIMIT $2 FOR UPDATE OF c,b`, strconv.FormatUint(max, 10), ZeroCreditBatchLimit)
	if err != nil {
		return 0, err
	}
	credits := make([]Credit, 0, ZeroCreditBatchLimit)
	for rows.Next() {
		var c Credit
		var recipient, amount string
		if err = rows.Scan(&c.ID, &c.BlockHash, &recipient, &amount); err != nil {
			rows.Close()
			return 0, err
		}
		block, decodeErr := hex.DecodeString(c.BlockHash)
		if decodeErr != nil || len(block) != 32 || c.ID <= 0 || !common.IsHexAddress(recipient) || common.HexToAddress(recipient) == (common.Address{}) || amount != "0" {
			rows.Close()
			return 0, errors.New("zero-credit identity or amount rejected")
		}
		c.Recipient, c.Amount = common.HexToAddress(recipient), big.NewInt(0)
		credits = append(credits, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, c := range credits {
		var prior bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_payments_v1.intents WHERE credit_id=$1) OR EXISTS(SELECT 1 FROM pool_payments_v1.events WHERE credit_id=$1 AND kind IN ('zero_satisfied','confirmed','prepared'))`, c.ID).Scan(&prior); err != nil {
			return 0, err
		}
		if prior {
			return 0, errors.New("zero credit has payment or prior satisfaction evidence; review required")
		}
		changed, err := tx.Exec(ctx, `UPDATE public.credits SET is_paid=TRUE WHERE id=$1 AND is_paid=FALSE AND amount=0`, c.ID)
		if err != nil || changed.RowsAffected() != 1 {
			return 0, errors.New("zero-credit binding changed during satisfaction")
		}
		if f.journal.beforeZeroAudit != nil {
			if err = f.journal.beforeZeroAudit(); err != nil {
				return 0, err
			}
		}
		detail, err := json.Marshal(map[string]any{"schema": "bdag.zero-credit-satisfied.v1",
			"credit_id": c.ID, "block_hash": c.BlockHash, "recipient": c.Recipient.Hex(),
			"amount_wei": "0", "chain_id": f.wallet.ChainID, "sender": f.wallet.Sender.Hex(),
			"max_eligible_native_height": strconv.FormatUint(max, 10), "transaction_created": false})
		if err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES($1,'zero_satisfied',$2)`, c.ID, string(detail)); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, `UPDATE public.blocks SET status='PAID' WHERE hash=$1 AND status='MATURE' AND NOT EXISTS(SELECT 1 FROM public.credits WHERE block_hash=$1 AND is_paid IS DISTINCT FROM TRUE)`, c.BlockHash); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		// Ambiguous commit is observed through paid+audit on the next Step,
		// never permission to create a transaction or duplicate audit.
		return 0, err
	}
	return len(credits), nil
}
