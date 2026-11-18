package payments

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgconn"
)

func confirmedSQLFixture(t *testing.T) sqlFixture {
	t.Helper()
	f := newSQLFixture(t)
	ctx := context.Background()
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	f.accepted(t)
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestPostgresHistoryCheckpointRestartAndReorgHold(t *testing.T) {
	f := confirmedSQLFixture(t)
	ctx := context.Background()
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	lease, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	b, err := lease.History(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b.Checkpoint == nil || b.Checkpoint.Cursor <= 0 || b.Checkpoint.Number != 20 || b.Checkpoint.Hash != f.net.canonical.Hash() || len(b.Payments) != 0 {
		t.Fatal("checkpoint not persisted", b)
	}
	// No-op replay must neither append audit events nor advance a cursor.
	if err = lease.AdvanceHistory(ctx, b.Checkpoint, *b.Checkpoint); err == nil {
		t.Fatal("checkpoint regression/replay accepted")
	}
	_ = lease.Close(ctx)
	var before int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM pool_payments_v1.events WHERE kind='checkpoint'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	restarted := *f.engine
	if _, err = restarted.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	var after int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM pool_payments_v1.events WHERE kind='checkpoint'`).Scan(&after); err != nil || before != 1 || after != before {
		t.Fatal("restart duplicated checkpoint", err)
	}
	f.net.canonical.Extra = []byte("reorg")
	if _, err = restarted.Step(ctx, 100); !errors.Is(err, ErrHistoryIntegrity) {
		t.Fatal("reorg not held", err)
	}
	if lease, err = f.journal.Acquire(ctx, f.engine.Wallet); err == nil {
		_ = lease.Close(ctx)
		t.Fatal("durable wallet hold lost")
	}
	paid, state, count := f.state(t)
	if !paid || state != "confirmed" || count != 1 || len(f.net.broadcasts) != 1 {
		t.Fatal("reorg rewrote payment or resent money")
	}
	var held int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM pool_payments_v1.events WHERE kind='wallet_held'`).Scan(&held); err != nil || held != 1 {
		t.Fatal("hold audit not atomic", err)
	}
}

func TestPostgresConfirmationAndCursorEventMustCommitTogether(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	// These are full-shape negative controls: neither the old shape constraint
	// nor a compiler error can be the reason the missing link is rejected.
	for _, sql := range []string{`UPDATE pool_payments_v1.intents SET state='confirmed',confirmed_number=20,confirmed_hash=decode(repeat('01',32),'hex'),confirmed_at=clock_timestamp() WHERE credit_id=7`, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES(7,'confirmed','unproven')`} {
		_, err := f.pool.Exec(ctx, sql)
		var p *pgconn.PgError
		if !errors.As(err, &p) || p.Code != "P0001" || !strings.Contains(p.Message, "confirmed payment and immutable event") {
			t.Fatal("missing-link negative did not fail at deferred constraint", err)
		}
	}
	paid, state, count := f.state(t)
	if paid || state != "pending" || count != 0 {
		t.Fatal("failed deferred link check changed money")
	}
	f.accepted(t)
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES(7,'confirmed','duplicate')`); err == nil {
		t.Fatal("duplicate confirmation event accepted")
	}
}

func TestPostgresHistoryCheckpointFailureRollsBackCursorAndEvent(t *testing.T) {
	f := confirmedSQLFixture(t)
	ctx := context.Background()
	f.journal.beforeHistoryCommit = func() error { return errors.New("injected checkpoint transaction failure") }
	if _, err := f.engine.Step(ctx, 100); err == nil {
		t.Fatal("checkpoint commit failure hidden")
	}
	var cursor int64
	var events int
	if err := f.pool.QueryRow(ctx, `SELECT checkpoint_cursor,(SELECT count(*) FROM pool_payments_v1.events WHERE kind='checkpoint') FROM pool_payments_v1.wallet_state`).Scan(&cursor, &events); err != nil || cursor != 0 || events != 0 {
		t.Fatal("partial checkpoint survived rollback", cursor, events, err)
	}
	f.journal.beforeHistoryCommit = nil
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if len(f.net.broadcasts) != 1 {
		t.Fatal("checkpoint retry resent payment")
	}
}

func TestPostgresHistoryRefusesChangedLegacyPayment(t *testing.T) {
	f := confirmedSQLFixture(t)
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `UPDATE public.payouts SET amount=101`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.engine.Step(ctx, 100); err == nil {
		t.Fatal("legacy payment drift passed history audit")
	}
	var cursor int64
	if err := f.pool.QueryRow(ctx, `SELECT checkpoint_cursor FROM pool_payments_v1.wallet_state`).Scan(&cursor); err != nil || cursor != 0 {
		t.Fatal("bad evidence checkpointed", err)
	}
	if len(f.net.broadcasts) != 1 {
		t.Fatal("drift caused resend")
	}
}

func TestPostgresHistoryBacklogIsBoundedAndResumesByEventNotCredit(t *testing.T) {
	f := confirmedSQLFixture(t)
	ctx := context.Background()
	// Exact signed, confirmed fixtures in the disposable DB only. A single
	// transaction preserves real fsync durability without 64 cold DDL cycles.
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	f.net.receipts = map[common.Hash]*types.Receipt{}
	for id := int64(8); id < 8+HistoryBatchLimit; id++ {
		c := copyCredit(f.store.credit)
		c.ID = id
		raw, err := f.signer.Sign(ctx, c, uint64(id), big.NewInt(1))
		if err != nil {
			t.Fatal(err)
		}
		var signed types.Transaction
		if err = signed.UnmarshalBinary(raw); err != nil {
			t.Fatal(err)
		}
		hash := signed.Hash()
		block := f.net.canonical.Hash()
		if _, err = tx.Exec(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) VALUES($1,$2,$3,100,TRUE)`, id, c.BlockHash, c.Recipient.Hex()); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO public.payouts(tx_hash,amount) VALUES($1,100)`, hash.Hex()); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.intents(credit_id,chain_id,sender,credit_block_hash,recipient,amount,nonce,gas_price,raw_tx,tx_hash,state,confirmed_number,confirmed_hash,confirmed_at) VALUES($1::integer,1404,$2,$3,$4,100,$1::numeric,1,$5,$6,'confirmed',20,$7,clock_timestamp())`, id, f.engine.Wallet.Sender[:], c.BlockHash, c.Recipient[:], raw, hash[:], block[:]); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES($1,'confirmed',$2)`, id, block.Hex()); err != nil {
			t.Fatal(err)
		}
		f.net.receipts[hash] = &types.Receipt{TxHash: hash, Status: types.ReceiptStatusSuccessful, BlockNumber: big.NewInt(20), BlockHash: block}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	signs := f.signer.calls
	r, err := f.engine.Step(ctx, 100)
	if err != nil || r.State != "awaiting_history_audit" {
		t.Fatal("backlog did not gate spends", r, err)
	}
	lease, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	b, err := lease.History(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Close(ctx)
	if b.Checkpoint == nil || len(b.Payments) != 1 || b.More {
		t.Fatal("batch limit/cursor not preserved", b)
	}
	restarted := *f.engine
	r, err = restarted.Step(ctx, 100)
	if err != nil || r.State != "no_eligible_credit" {
		t.Fatal("backlog restart did not finish", r, err)
	}
	if f.signer.calls != signs || len(f.net.broadcasts) != 1 {
		t.Fatal("history replay sent new money")
	}
	var checkpoints int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM pool_payments_v1.events WHERE kind='checkpoint'`).Scan(&checkpoints); err != nil || checkpoints != 2 {
		t.Fatal("expected two bounded commits", checkpoints, err)
	}
}
