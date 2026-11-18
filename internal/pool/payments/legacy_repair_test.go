package payments

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type legacyTestNetwork struct {
	*fakeNetwork
	replacement *types.Transaction
}

func (n legacyTestNetwork) Transaction(_ context.Context, h common.Hash) (*types.Transaction, error) {
	if n.replacement.Hash() == h {
		return n.replacement, nil
	}
	return nil, nil
}

func legacyFixture(t *testing.T) (sqlFixture, LegacyRepair, legacyTestNetwork, *NativeGate) {
	t.Helper()
	f := newSQLFixture(t)
	ctx := context.Background()
	missing := Credit{ID: 39239, BlockHash: strings.Repeat("d5", 32), Recipient: f.store.credit.Recipient, Amount: big.NewInt(100)}
	replacement := Credit{ID: 313, BlockHash: strings.Repeat("77", 32), Recipient: f.store.credit.Recipient, Amount: big.NewInt(101)}
	key, err := crypto.HexToECDSA(strings.Repeat("0", 63) + "1")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := types.SignTx(types.NewTransaction(6, replacement.Recipient, replacement.Amount, 21000, big.NewInt(1000007), nil), types.LatestSignerForChainID(big.NewInt(1404)), key)
	if err != nil {
		t.Fatal(err)
	}
	q := LegacyRepair{Wallet: f.engine.Wallet, Missing: missing, MissingHash: common.HexToHash("0xdca43095e5db1eeaae36b6c94d27605a40a723523218ded4c02d6a1f25dad251"), MissingPayoutID: 7, Nonce: 6, Replacement: replacement, ReplacementHash: tx.Hash(), ReplacementPayoutID: 8, OfflineProofDigest: strings.Repeat("ab", 32)}
	for _, row := range []struct {
		c    Credit
		hash common.Hash
		pid  int64
		xid  *string
	}{{missing, q.MissingHash, 7, &q.MissingXID}, {replacement, q.ReplacementHash, 8, &q.ReplacementXID}} {
		txn, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = txn.Exec(ctx, `INSERT INTO public.blocks(hash,height,reward,fees,status) VALUES($1,10,100,0,'PAID')`, row.c.BlockHash); err != nil {
			t.Fatal(err)
		}
		if err = txn.QueryRow(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) VALUES($1,$2,$3,$4::numeric,TRUE) RETURNING xmin::text`, row.c.ID, row.c.BlockHash, row.c.Recipient.Hex(), row.c.Amount.String()).Scan(row.xid); err != nil {
			t.Fatal(err)
		}
		if _, err = txn.Exec(ctx, `INSERT INTO public.payouts(id,tx_hash,amount) VALUES($1,$2,$3::numeric)`, row.pid, row.hash.Hex(), row.c.Amount.String()); err != nil {
			t.Fatal(err)
		}
		if err = txn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	f.net.canonical = &types.Header{Number: big.NewInt(20)}
	f.net.head = &types.Header{Number: big.NewInt(31)}
	f.net.receipt = &types.Receipt{TxHash: tx.Hash(), Status: types.ReceiptStatusSuccessful, BlockNumber: big.NewInt(20), BlockHash: f.net.canonical.Hash()}
	f.net.mined = 10
	f.net.pending = 10
	g, _ := gateFixture(f.fixture)
	return f, q, legacyTestNetwork{fakeNetwork: f.net, replacement: tx}, g
}

func TestPostgresLegacyRepairAtomicReplayNeverUnpaysLaterPayment(t *testing.T) {
	f, q, n, g := legacyFixture(t)
	ctx := context.Background()
	changed, err := f.journal.RepairLegacyCredit(ctx, q, n, g.Check)
	if err != nil || !changed {
		t.Fatal(changed, err)
	}
	var paid bool
	var status string
	var amount string
	var payouts, audits int
	if err = f.pool.QueryRow(ctx, `SELECT c.is_paid,b.status,c.amount::text,(SELECT count(*) FROM public.payouts),(SELECT count(*) FROM pool_payments_v1.events WHERE credit_id=39239 AND kind='held') FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash WHERE c.id=39239`).Scan(&paid, &status, &amount, &payouts, &audits); err != nil || paid || status != "MATURE" || amount != "100" || payouts != 2 || audits != 1 {
		t.Fatal("repair not exact/atomic", err)
	}
	// A later legitimate worker changes this flag to true. Replaying the OLD
	// repair must never undo that later payment or append another audit record.
	if _, err = f.pool.Exec(ctx, `UPDATE public.credits SET is_paid=TRUE WHERE id=39239`); err != nil {
		t.Fatal(err)
	}
	changed, err = f.journal.RepairLegacyCredit(ctx, q, n, g.Check)
	if err != nil || changed {
		t.Fatal("repair replay not idempotent", changed, err)
	}
	if err = f.pool.QueryRow(ctx, `SELECT is_paid FROM public.credits WHERE id=39239`).Scan(&paid); err != nil || !paid {
		t.Fatal("old repair unpaid a later payment", err)
	}
	if len(n.broadcasts) != 0 {
		t.Fatal("legacy repair broadcast money")
	}
}

func TestPostgresLegacyRepairPreflightExercisesSameChecksWithoutMoneyWrites(t *testing.T) {
	f, q, n, g := legacyFixture(t)
	ctx := context.Background()
	if err := f.journal.PreflightLegacyCredit(ctx, q, n, g.Check); err != nil {
		t.Fatal(err)
	}
	var paid bool
	var status string
	var audits int
	if err := f.pool.QueryRow(ctx, `SELECT c.is_paid,b.status,(SELECT count(*) FROM pool_payments_v1.events WHERE credit_id=39239) FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash WHERE c.id=39239`).Scan(&paid, &status, &audits); err != nil || !paid || status != "PAID" || audits != 0 {
		t.Fatal("preflight modified cash state", err)
	}
	q.MissingXID = "1"
	if err := f.journal.PreflightLegacyCredit(ctx, q, n, g.Check); err == nil {
		t.Fatal("preflight skipped real binding check")
	}
}

func TestPostgresLegacyRepairFailureRollsBackFlagStatusAndAudit(t *testing.T) {
	f, q, n, g := legacyFixture(t)
	ctx := context.Background()
	f.journal.beforeLegacyAudit = func() error { return errors.New("injected failure after credit and block updates") }
	if _, err := f.journal.RepairLegacyCredit(ctx, q, n, g.Check); err == nil {
		t.Fatal("partial commit failure hidden")
	}
	var paid bool
	var status string
	var audits int
	if err := f.pool.QueryRow(ctx, `SELECT c.is_paid,b.status,(SELECT count(*) FROM pool_payments_v1.events WHERE credit_id=39239 AND kind='held') FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash WHERE c.id=39239`).Scan(&paid, &status, &audits); err != nil || !paid || status != "PAID" || audits != 0 {
		t.Fatal("partial legacy repair committed", err)
	}
	f.journal.beforeLegacyAudit = nil
	if changed, err := f.journal.RepairLegacyCredit(ctx, q, n, g.Check); err != nil || !changed {
		t.Fatal("exact retry did not recover", err)
	}
}

func TestPostgresLegacyRepairRejectsDriftAndUnprovenCanonicalEvidence(t *testing.T) {
	for _, mode := range []string{"credit-drift", "xid-drift", "revert", "reorg", "shallow", "nonce-not-consumed", "original-receipt-found", "native-unready"} {
		t.Run(mode, func(t *testing.T) {
			f, q, n, g := legacyFixture(t)
			ctx := context.Background()
			switch mode {
			case "credit-drift":
				if _, err := f.pool.Exec(ctx, `UPDATE public.credits SET amount=102 WHERE id=39239`); err != nil {
					t.Fatal(err)
				}
			case "xid-drift":
				q.MissingXID = "1"
			case "revert":
				n.receipt.Status = 0
			case "reorg":
				n.receipt.BlockHash = common.HexToHash("0xff")
			case "shallow":
				n.head.Number = big.NewInt(30)
			case "nonce-not-consumed":
				n.mined = 6
			case "original-receipt-found":
				n.receipts = map[common.Hash]*types.Receipt{q.MissingHash: {TxHash: q.MissingHash}}
			case "native-unready":
				g.Native = nil
			}
			if _, err := f.journal.RepairLegacyCredit(ctx, q, n, g.Check); err == nil {
				t.Fatal("unsafe legacy correction accepted")
			}
			var paid bool
			if err := f.pool.QueryRow(ctx, `SELECT is_paid FROM public.credits WHERE id=39239`).Scan(&paid); err != nil || !paid {
				t.Fatal("unsafe correction changed paid flag", err)
			}
		})
	}
}
