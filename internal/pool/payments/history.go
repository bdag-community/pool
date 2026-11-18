package payments

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const HistoryBatchLimit = 64

var ErrHistoryIntegrity = errors.New("confirmed payout history no longer matches canonical chain")

// Cursor is a committed confirmation event ID, not a credit ID or a wall clock.
// Credits may mature out of ID order and clocks can move backwards.
type HistoryCheckpoint struct {
	Cursor int64
	Number uint64
	Hash   common.Hash
}

type ConfirmedPayment struct {
	EventID int64
	Intent  Intent
	Number  uint64
	Hash    common.Hash
}

type HistoryBatch struct {
	Checkpoint *HistoryCheckpoint
	Payments   []ConfirmedPayment
	More       bool
}

func (e *Engine) historyFault(ctx context.Context, f Fence, reason string) error {
	if err := f.HoldWallet(ctx, reason); err != nil {
		return fmt.Errorf("history hold commit unproven: %w", err)
	}
	return fmt.Errorf("%w: %s", ErrHistoryIntegrity, reason)
}

func (e *Engine) recheckHistoryAnchor(ctx context.Context, f Fence, c *HistoryCheckpoint) error {
	if c == nil {
		return nil
	}
	if c.Cursor <= 0 || c.Hash == (common.Hash{}) {
		return errors.New("invalid payment history checkpoint")
	}
	h, err := e.Network.Header(ctx, new(big.Int).SetUint64(c.Number))
	if err != nil || h == nil || h.Number == nil || !h.Number.IsUint64() || h.Number.Uint64() != c.Number {
		return errors.New("canonical payout history block temporarily unavailable")
	}
	if h.Hash() != c.Hash {
		return e.historyFault(ctx, f, "confirmed payment checkpoint was reorganised; no automatic repayment")
	}
	return nil
}

// auditHistory does constant-size work per Step. The old canonical anchor is
// always checked, even after the cursor has caught up. This detects a reorg
// anywhere in its ancestry without rereading an ever-growing payment ledger.
// New immutable confirmations are checked in event order before advancing the
// checkpoint. A lost commit acknowledgement never authorizes a spend.
func (e *Engine) auditHistory(ctx context.Context, f Fence) (*HistoryCheckpoint, bool, error) {
	b, err := f.History(ctx)
	if err != nil {
		return nil, false, err
	}
	if len(b.Payments) > HistoryBatchLimit || (b.More && len(b.Payments) == 0) {
		return nil, false, errors.New("invalid bounded payment history batch")
	}
	if b.Checkpoint == nil && len(b.Payments) == 0 {
		return nil, true, nil
	}
	if err := e.recheckHistoryAnchor(ctx, f, b.Checkpoint); err != nil {
		return nil, false, err
	}
	head, err := e.Network.Header(ctx, nil)
	if err != nil || head == nil || head.Number == nil || !head.Number.IsUint64() {
		return nil, false, errors.New("canonical payment history head unavailable")
	}
	var next HistoryCheckpoint
	if b.Checkpoint != nil {
		next = *b.Checkpoint
	}
	for _, p := range b.Payments {
		if p.EventID <= next.Cursor || p.Intent.Wallet != e.Wallet || p.Hash == (common.Hash{}) {
			return nil, false, errors.New("payment history event ordering or wallet drift")
		}
		if err := ValidateIntent(p.Intent); err != nil {
			return nil, false, err
		}
		r, err := e.Network.Receipt(ctx, p.Intent.Hash)
		if err != nil || r == nil {
			return nil, false, errors.New("confirmed payment receipt temporarily unavailable; no repayment")
		}
		if r.TxHash != p.Intent.Hash || r.Status != types.ReceiptStatusSuccessful || r.BlockNumber == nil || !r.BlockNumber.IsUint64() || r.BlockNumber.Uint64() != p.Number || r.BlockHash != p.Hash {
			return nil, false, e.historyFault(ctx, f, "confirmed payment receipt changed; preserve ledger for review")
		}
		point := HistoryCheckpoint{Cursor: p.EventID, Number: p.Number, Hash: p.Hash}
		if err := e.recheckHistoryAnchor(ctx, f, &point); err != nil {
			return nil, false, err
		}
		if head.Number.Uint64() < p.Number || head.Number.Uint64()-p.Number < MinimumConfirmations-1 {
			return nil, false, errors.New("confirmed payment history depth regressed")
		}
		if next.Cursor == 0 || p.Number > next.Number {
			next.Number = p.Number
			next.Hash = p.Hash
		}
		next.Cursor = p.EventID
	}
	if head.Number.Uint64() < next.Number || head.Number.Uint64()-next.Number < MinimumConfirmations-1 {
		return nil, false, errors.New("payment history checkpoint depth regressed")
	}
	// Check both anchors after receipt RPCs. Never replace an orphaned old
	// checkpoint with a healthy-looking block on a different history.
	if err := e.recheckHistoryAnchor(ctx, f, b.Checkpoint); err != nil {
		return nil, false, err
	}
	if err := e.recheckHistoryAnchor(ctx, f, &next); err != nil {
		return nil, false, err
	}
	if len(b.Payments) > 0 {
		if err := f.AdvanceHistory(ctx, b.Checkpoint, next); err != nil {
			return nil, false, fmt.Errorf("history checkpoint commit unproven: %w", err)
		}
	}
	return &next, !b.More, nil
}
