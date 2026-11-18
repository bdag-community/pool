package payments

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func historyFixture(t *testing.T) fixture {
	t.Helper()
	f := newFixture(t)
	if _, err := f.engine.Step(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	f.accepted(MinimumConfirmations)
	p := *cloneIntent(f.store.active)
	f.store.history = HistoryBatch{Payments: []ConfirmedPayment{{EventID: 42, Intent: p, Number: 20, Hash: f.net.canonical.Hash()}}}
	f.store.active = nil
	f.store.paid = true
	return f
}

func TestHistoryReorgHoldsWithoutRepayingOrSigning(t *testing.T) {
	f := historyFixture(t)
	ctx := context.Background()
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if f.store.history.Checkpoint == nil {
		t.Fatal("no durable checkpoint")
	}
	// There are no new audit rows. The persisted anchor must still be checked.
	f.net.canonical.Extra = []byte("replacement chain")
	f.store.paid = false
	f.store.credit.ID = 8
	r, err := f.engine.Step(ctx, 100)
	if !errors.Is(err, ErrHistoryIntegrity) || r.State != "awaiting_history_audit" || !f.store.walletHeld {
		t.Fatalf("reorg not held: %+v %v", r, err)
	}
	_, _ = f.engine.Step(ctx, 100)
	if len(f.net.broadcasts) != 1 || f.signer.calls != 1 || f.store.confirms != 0 {
		t.Fatal("history reorg sent, resigned or rewrote payment")
	}
}

func TestHistoryPersistedAnchorAlwaysCheckedBeforeSigning(t *testing.T) {
	f := historyFixture(t)
	f.store.history = HistoryBatch{Checkpoint: &HistoryCheckpoint{Cursor: 42, Number: 20, Hash: f.net.canonical.Hash()}}
	f.net.canonical.Extra = []byte("persisted anchor reorganised")
	f.store.paid = false
	f.store.credit.ID = 8
	f.net.pending = 1
	f.net.mined = 1
	r, err := f.engine.Step(context.Background(), 100)
	if !errors.Is(err, ErrHistoryIntegrity) || !f.store.walletHeld || f.signer.calls != 1 || len(f.net.broadcasts) != 1 {
		t.Fatalf("persisted reorg anchor failed to prevent signing: %+v %v signs=%d broadcasts=%d", r, err, f.signer.calls, len(f.net.broadcasts))
	}
}

func TestHistoryMissingReceiptAndRPCDepthCanSelfHeal(t *testing.T) {
	for _, mode := range []string{"missing-receipt", "head-regressed"} {
		t.Run(mode, func(t *testing.T) {
			f := historyFixture(t)
			receipt := f.net.receipt
			if mode == "missing-receipt" {
				f.net.receipt = nil
			} else {
				f.net.head.Number = big.NewInt(20)
			}
			if _, err := f.engine.Step(context.Background(), 100); err == nil {
				t.Fatal("missing history evidence allowed work")
			}
			if f.store.walletHeld || f.store.history.Checkpoint != nil {
				t.Fatal("transient failure changed permanent state")
			}
			f.net.receipt = receipt
			f.net.head.Number = big.NewInt(31)
			if _, err := f.engine.Step(context.Background(), 100); err != nil {
				t.Fatal(err)
			}
			if f.store.history.Checkpoint == nil || f.store.walletHeld || len(f.net.broadcasts) != 1 {
				t.Fatal("history did not safely resume")
			}
		})
	}
}

func TestHistoryCheckpointAmbiguityAndBacklogCannotAuthorizeSpend(t *testing.T) {
	for _, mode := range []string{"ambiguous", "backlog"} {
		t.Run(mode, func(t *testing.T) {
			f := historyFixture(t)
			f.store.paid = false
			f.store.credit.ID = 8
			if mode == "ambiguous" {
				f.store.historyCommitAmbiguous = true
			} else {
				f.store.history.More = true
			}
			r, err := f.engine.Step(context.Background(), 100)
			if r.State != "awaiting_history_audit" || (mode == "ambiguous" && err == nil) {
				t.Fatalf("history gate %+v %v", r, err)
			}
			if len(f.net.broadcasts) != 1 || f.signer.calls != 1 {
				t.Fatal("unfinished audit authorized signing")
			}
			// Simulate process restart and a credit not yet eligible; persisted cursor
			// and anchor are sufficient, without scanning or replaying old payments.
			f.store.paid = true
			restarted := *f.engine
			if _, err := restarted.Step(context.Background(), 100); err != nil {
				t.Fatal(err)
			}
			if f.store.history.Checkpoint.Cursor != 42 || len(f.net.broadcasts) != 1 {
				t.Fatal("checkpoint restart lost identity")
			}
		})
	}
}

func TestHistoryChangedReceiptIsDurablyHeld(t *testing.T) {
	f := historyFixture(t)
	f.net.receipt.BlockHash = common.HexToHash("0xff")
	if _, err := f.engine.Step(context.Background(), 100); !errors.Is(err, ErrHistoryIntegrity) || !f.store.walletHeld {
		t.Fatal("changed history receipt not held", err)
	}
}
