package payments

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type baselineFake struct {
	*fakeNetwork
	tx *types.Transaction
}

func (n *baselineFake) Transaction(context.Context, common.Hash) (*types.Transaction, error) {
	return n.tx, nil
}

func baselineFixture(t *testing.T) (LegacyBaseline, *baselineFake, fixture) {
	t.Helper()
	f := newFixture(t)
	raw, err := f.signer.sign(f.store.credit, 9, big.NewInt(1))
	if err != nil {
		t.Fatal(err)
	}
	var tx types.Transaction
	if err = tx.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	f.net.pending, f.net.mined = 10, 10
	f.net.canonical = &types.Header{Number: big.NewInt(20)}
	f.net.head = &types.Header{Number: big.NewInt(31)}
	f.net.receipt = &types.Receipt{TxHash: tx.Hash(), Status: 1, BlockNumber: big.NewInt(20), BlockHash: f.net.canonical.Hash()}
	b := LegacyBaseline{Wallet: f.engine.Wallet, NonceFloor: 10, Hash: tx.Hash(),
		Recipient: f.store.credit.Recipient, Amount: new(big.Int).Set(f.store.credit.Amount),
		BlockNumber: 20, BlockHash: f.net.canonical.Hash()}
	return b, &baselineFake{f.net, &tx}, f
}

func TestLegacyBaselineRequiresCanonicalReviewedNonceAnchor(t *testing.T) {
	for _, mode := range []string{"valid", "wrong-chain", "wrong-wallet", "wrong-floor", "wrong-amount", "missing-tx", "missing-receipt", "reverted", "wrong-block", "immature", "reorg-between-reads", "mined-regressed", "pending-regressed", "no-deadline", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			b, net, _ := baselineFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch mode {
			case "wrong-chain":
				net.chain = big.NewInt(1)
			case "wrong-wallet":
				b.Wallet.Sender = b.Recipient
			case "wrong-floor":
				b.NonceFloor = 9
			case "wrong-amount":
				b.Amount.Add(b.Amount, big.NewInt(1))
			case "missing-tx":
				net.tx = nil
			case "missing-receipt":
				net.receipt = nil
			case "reverted":
				net.receipt.Status = 0
			case "wrong-block":
				net.canonical.Extra = []byte("different")
			case "immature":
				net.head.Number = big.NewInt(30)
			case "reorg-between-reads":
				net.orphanOnRecheck = true
			case "mined-regressed":
				net.mined = 9
			case "pending-regressed":
				net.pending = 9
			case "no-deadline":
				ctx = context.Background()
			case "canceled":
				cancel()
			}
			err := b.Check(ctx, net)
			if (err == nil) != (mode == "valid") {
				t.Fatalf("baseline verdict %s: %v", mode, err)
			}
			if len(net.broadcasts) != 0 {
				t.Fatal("baseline verification transmitted funds")
			}
		})
	}
}

func TestNonceFloorEnforcedAtAllocationAndBroadcastNotOnlyStartup(t *testing.T) {
	b, net, f := baselineFixture(t)
	guarded := NonceFloorNetwork{Network: net, Floor: b.NonceFloor}
	ctx := context.Background()
	if n, err := guarded.PendingNonce(ctx, b.Wallet.Sender); n != 10 || err != nil {
		t.Fatal(n, err)
	}
	net.pending, net.mined = 9, 9
	if _, err := guarded.PendingNonce(ctx, b.Wallet.Sender); err == nil {
		t.Fatal("regressed pending nonce accepted")
	}
	if _, err := guarded.MinedNonce(ctx, b.Wallet.Sender); err == nil {
		t.Fatal("regressed mined nonce accepted")
	}
	for _, nonce := range []uint64{9, 10} {
		raw, err := f.signer.sign(f.store.credit, nonce, big.NewInt(1))
		if err != nil {
			t.Fatal(err)
		}
		err = guarded.Broadcast(ctx, raw)
		if (err == nil) != (nonce == 10) {
			t.Fatal("incorrect broadcast floor verdict", nonce, err)
		}
	}
	if len(net.broadcasts) != 1 {
		t.Fatal("old nonce was transmitted")
	}
	guarded.Floor = 0
	if err := guarded.Broadcast(ctx, net.broadcasts[0]); err == nil {
		t.Fatal("unqualified zero floor accepted")
	}
}

func TestBaselineWorkerRefusesKeyLoadingOnCanonicalRegression(t *testing.T) {
	b, net, f := baselineFixture(t)
	gate, _ := gateFixture(f)
	loads := 0
	w := Worker{Wallet: b.Wallet, Journal: reviewedFake{fakeStore: f.store}, Network: net,
		Gate: gate, Exclusive: func(ctx context.Context) error { return b.Check(ctx, net) },
		SignerFactory: func(context.Context) (Signer, error) { loads++; return f.signer, nil }}
	net.mined = 9
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := w.Step(ctx); err == nil || loads != 0 {
		t.Fatal("key loaded with regressed canonical legacy nonce")
	}
	if _, err := w.Step(context.Background()); err == nil || loads != 0 {
		t.Fatal("unbounded baseline worker loaded key")
	}
	if len(net.broadcasts) != 0 || f.store.saves != 0 {
		t.Fatal("regressed baseline changed payment state")
	}
	if ctx.Err() != nil {
		t.Fatal("unexpected test context expiry")
	}
}
