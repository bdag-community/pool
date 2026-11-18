package payments

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sqlFixture struct {
	fixture
	pool        *pgxpool.Pool
	journal     *PostgresJournal
	fingerprint string
}

func TestPostgresPendingIntentsSortNumerically(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	second := copyCredit(f.store.credit)
	second.ID++
	if _, err := f.pool.Exec(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) VALUES($1,$2,$3,$4::numeric,FALSE)`, second.ID, second.BlockHash, second.Recipient.Hex(), second.Amount.String()); err != nil {
		t.Fatal(err)
	}
	fence, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close(ctx)
	for _, nonce := range []uint64{999, 1000} {
		credit, err := fence.NextCredit(ctx, 100)
		if err != nil || credit == nil {
			t.Fatalf("credit unavailable: %v", err)
		}
		raw, err := f.signer.sign(*credit, nonce, big.NewInt(1))
		if err != nil {
			t.Fatal(err)
		}
		var tx types.Transaction
		if err := tx.UnmarshalBinary(raw); err != nil {
			t.Fatal(err)
		}
		intent := Intent{Wallet: f.engine.Wallet, Credit: *credit, Nonce: nonce, GasPrice: big.NewInt(1), Raw: raw, Hash: tx.Hash(), CreatedAt: time.Now()}
		if _, err := fence.Save(ctx, intent); err != nil {
			t.Fatal(err)
		}
	}
	active, err := fence.Active(ctx)
	if err != nil || active == nil || active.Nonce != 999 {
		t.Fatalf("oldest active nonce is not 999: %+v %v", active, err)
	}
	window, err := fence.(*postgresFence).PendingWindowV1(ctx)
	if err != nil || len(window) != 2 || window[0].Nonce != 999 || window[1].Nonce != 1000 {
		t.Fatalf("pending nonce order is not [999, 1000]: %+v %v", window, err)
	}
}

func TestPostgresPipelineMigrationPreservesAndOrdersIntents(t *testing.T) {
	f := newSQLFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Reconstruct the old one-active-intent catalog, then prepare a real
	// signed intent before exercising the additive index migration.
	if _, err := f.pool.Exec(ctx, `DROP INDEX pool_payments_v1.intents_active_wallet_nonce; CREATE UNIQUE INDEX intents_one_active_wallet ON pool_payments_v1.intents(chain_id,sender) WHERE state<>'confirmed'`); err != nil {
		t.Fatal(err)
	}
	legacyFingerprint, err := CatalogFingerprint(ctx, f.pool)
	if err != nil || legacyFingerprint != LegacyReviewedSchemaFingerprint {
		t.Fatalf("old payout catalog differs: %s %v", legacyFingerprint, err)
	}
	f.journal.fingerprint = legacyFingerprint
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatalf("prepare old-schema intent: %v", err)
	}
	first := f.active(t)
	if first == nil {
		t.Fatal("old intent missing")
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "20261118_payment_pipeline_v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(ctx, string(data)); err != nil {
		t.Fatalf("pipeline migration: %v", err)
	}
	fingerprint, err := CatalogFingerprint(ctx, f.pool)
	if err != nil || fingerprint != f.fingerprint {
		t.Fatalf("migrated catalog differs from fresh catalog: %s %v", fingerprint, err)
	}
	f.journal.fingerprint = f.fingerprint
	if got := f.active(t); got == nil || got.Hash != first.Hash || !bytes.Equal(got.Raw, first.Raw) {
		t.Fatal("migration changed existing signed intent")
	}
	second := copyCredit(f.store.credit)
	second.ID++
	if _, err = f.pool.Exec(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) VALUES($1,$2,$3,$4::numeric,FALSE)`, second.ID, second.BlockHash, second.Recipient.Hex(), second.Amount.String()); err != nil {
		t.Fatal(err)
	}
	fence, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close(context.Background())
	selected, err := fence.NextCredit(ctx, 100)
	if err != nil || selected == nil || !sameCredit(*selected, second) {
		t.Fatalf("second credit unavailable: %+v %v", selected, err)
	}
	raw, err := f.signer.sign(second, first.Nonce+1, big.NewInt(1))
	if err != nil {
		t.Fatal(err)
	}
	var signed types.Transaction
	if err = signed.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	proposal := Intent{Wallet: f.engine.Wallet, Credit: second, Nonce: first.Nonce + 1, GasPrice: big.NewInt(1), Raw: raw, Hash: signed.Hash(), CreatedAt: time.Now()}
	if _, err = fence.Save(ctx, proposal); err != nil {
		t.Fatalf("second durable intent rejected: %v", err)
	}
	window, err := fence.(*postgresFence).PendingWindowV1(ctx)
	if err != nil || len(window) != 2 || window[0].Hash != first.Hash || window[1].Hash != proposal.Hash {
		t.Fatalf("pending intents not ordered: %v", err)
	}
	block := &types.Header{Number: big.NewInt(20)}
	head := &types.Header{Number: big.NewInt(31)}
	receipt := &types.Receipt{TxHash: proposal.Hash, BlockNumber: big.NewInt(20), BlockHash: block.Hash(), Status: types.ReceiptStatusSuccessful}
	if _, err = fence.Confirm(ctx, proposal, receipt, head); err == nil {
		t.Fatal("later intent confirmed before the earlier intent")
	}
	receipt.TxHash = first.Hash
	if ok, err := fence.Confirm(ctx, *first, receipt, head); err != nil || !ok {
		t.Fatalf("first payout confirmation failed: %t %v", ok, err)
	}
	receipt.TxHash = proposal.Hash
	if ok, err := fence.Confirm(ctx, proposal, receipt, head); err != nil || !ok {
		t.Fatalf("second payout confirmation failed: %t %v", ok, err)
	}
}

func TestPostgresPipelinePaysTwoCreditsWithoutEarlyConfirmation(t *testing.T) {
	f := newSQLFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	second := copyCredit(f.store.credit)
	second.ID++
	if _, err := f.pool.Exec(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) VALUES($1,$2,$3,$4::numeric,FALSE)`, second.ID, second.BlockHash, second.Recipient.Hex(), second.Amount.String()); err != nil {
		t.Fatal(err)
	}
	network := &pipelineNetworkTest{fakeNetwork: f.net}
	network.canonical = &types.Header{Number: big.NewInt(20)}
	network.head = &types.Header{Number: big.NewInt(20)}
	network.receipts = make(map[common.Hash]*types.Receipt)
	f.engine.Network, f.engine.EnablePipeline, f.engine.RequireFreshWalletHistory = network, true, true
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatalf("first intent: %v", err)
	}
	first := f.active(t)
	if first == nil {
		t.Fatal("first intent missing")
	}
	pipelineMine(t, network, *first)
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatalf("second intent: %v", err)
	}
	fence, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	window, err := fence.(*postgresFence).PendingWindowV1(ctx)
	if err != nil || len(window) != 2 {
		t.Fatalf("two durable intents missing: %d %v", len(window), err)
	}
	if err = fence.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var paid int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM public.credits WHERE is_paid`).Scan(&paid); err != nil || paid != 0 {
		t.Fatalf("payout marked paid before finality: %d %v", paid, err)
	}
	pipelineMine(t, network, window[1])
	network.head = &types.Header{Number: big.NewInt(31)}
	if _, err = f.engine.Step(ctx, 100); err != nil {
		t.Fatalf("confirm first intent: %v", err)
	}
	if _, err = f.engine.Step(ctx, 100); err != nil {
		t.Fatalf("confirm second intent: %v", err)
	}
	var payouts int
	if err = f.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM public.credits WHERE is_paid),(SELECT count(*) FROM public.payouts)`).Scan(&paid, &payouts); err != nil || paid != 2 || payouts != 2 {
		t.Fatalf("confirmed payout results differ: paid=%d payouts=%d %v", paid, payouts, err)
	}
}

func newSQLFixture(t *testing.T) sqlFixture {
	t.Helper()
	raw := os.Getenv("POOL_PAYOUT_OUTBOX_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated payout test database not configured")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Port() == "55432" || !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "pool_payout_outbox_test_") {
		t.Fatal("refusing non-isolated payout test database")
	}
	// Cold, fsync-enabled DDL can take tens of seconds on the live recovery
	// host. Keep the real durability path and bound setup separately from tests.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 6
	config.ConnConfig.RuntimeParams["application_name"] = "payout-outbox-isolated-tests"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var database string
	if err = pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		t.Fatal(err)
	}
	if database != strings.TrimPrefix(u.Path, "/") {
		t.Fatal("isolated database alias mismatch")
	}
	// Exact disposable namespace/tables only, after both endpoint and server DB
	// identity guards. These commands are never part of production startup.
	_, err = pool.Exec(ctx, `DROP SCHEMA IF EXISTS pool_payments_v1 CASCADE;
 DROP TABLE IF EXISTS public.payouts,public.credits,public.blocks CASCADE;
 CREATE TABLE public.blocks(hash TEXT PRIMARY KEY,height BIGINT NOT NULL,reward NUMERIC(30,0) NOT NULL,fees NUMERIC(30,0) NOT NULL,status TEXT DEFAULT 'PENDING',created_at TIMESTAMP DEFAULT now());
 CREATE TABLE public.credits(id SERIAL PRIMARY KEY,block_hash TEXT REFERENCES public.blocks(hash),miner_address TEXT NOT NULL,amount NUMERIC(30,0) NOT NULL,is_paid BOOLEAN DEFAULT FALSE,created_at TIMESTAMP DEFAULT now());
 CREATE TABLE public.payouts(id SERIAL PRIMARY KEY,tx_hash TEXT UNIQUE NOT NULL,amount NUMERIC(30,0) NOT NULL,created_at TIMESTAMP DEFAULT now());`)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := ApplyMigration(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	j, err := NewPostgresJournal(pool, fp)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	f.engine.Journal = j
	f.engine.Now = time.Now
	c := f.store.credit
	if _, err = pool.Exec(ctx, `INSERT INTO public.blocks(hash,height,reward,fees,status) VALUES($1,10,100,0,'MATURE');`, c.BlockHash); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) VALUES($1,$2,$3,$4::numeric,FALSE)`, c.ID, c.BlockHash, c.Recipient.Hex(), c.Amount.String()); err != nil {
		t.Fatal(err)
	}
	t.Logf("PAYMENT_CATALOG_FINGERPRINT=%s", fp)
	return sqlFixture{fixture: f, pool: pool, journal: j, fingerprint: fp}
}

func (f sqlFixture) active(t *testing.T) *Intent {
	t.Helper()
	ctx := context.Background()
	lease, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close(ctx)
	p, err := lease.Active(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (f sqlFixture) accepted(t *testing.T) {
	t.Helper()
	p := f.active(t)
	if p == nil {
		t.Fatal("durable intent missing")
	}
	f.net.canonical = &types.Header{Number: big.NewInt(20)}
	f.net.receipt = &types.Receipt{TxHash: p.Hash, BlockNumber: big.NewInt(20), BlockHash: f.net.canonical.Hash(), Status: types.ReceiptStatusSuccessful}
	f.net.head = &types.Header{Number: big.NewInt(31)}
}
func (f sqlFixture) state(t *testing.T) (bool, string, int) {
	t.Helper()
	var paid bool
	var state string
	var count int
	if err := f.pool.QueryRow(context.Background(), `SELECT c.is_paid,p.state,(SELECT count(*) FROM public.payouts) FROM public.credits c JOIN pool_payments_v1.intents p ON p.credit_id=c.id WHERE c.id=7`).Scan(&paid, &state, &count); err != nil {
		t.Fatal(err)
	}
	return paid, state, count
}

func TestPostgresPaymentSurvivesSessionCrashWithoutResigning(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	before := f.active(t)
	paid, state, count := f.state(t)
	if paid || state != "pending" || count != 0 {
		t.Fatal("broadcast marked SQL credit paid")
	}
	lease, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	pg := lease.(*postgresFence)
	pid := pg.conn.Conn().PgConn().PID()
	var signalled bool
	if err = f.pool.QueryRow(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND datname=current_database() AND application_name='payout-outbox-isolated-tests'`, pid).Scan(&signalled); err != nil || !signalled {
		t.Fatalf("isolated session termination: %v", err)
	}
	_ = lease.Close(ctx)
	restarted := *f.engine
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err = restarted.Step(ctx, 100)
		if !errors.Is(err, ErrWalletBusy) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("wallet fence not released after session crash")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	after := f.active(t)
	if after.Hash != before.Hash || after.Nonce != before.Nonce || !bytes.Equal(after.Raw, before.Raw) || f.signer.calls != 1 || len(f.net.broadcasts) != 2 {
		t.Fatal("SQL restart generated another spend")
	}
	f.accepted(t)
	r, err := restarted.Step(ctx, 100)
	if err != nil || !r.NewlyConfirmed {
		t.Fatalf("canonical confirmation: %+v %v", r, err)
	}
	paid, state, count = f.state(t)
	if !paid || state != "confirmed" || count != 1 {
		t.Fatal("confirmation not atomic")
	}
	lease, err = f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	newly, err := lease.Confirm(ctx, *before, f.net.receipt, f.net.head)
	_ = lease.Close(ctx)
	if err != nil || newly {
		t.Fatal("confirmed SQL payment replay was not idempotent")
	}
	if err = f.journal.Verify(ctx); err != nil {
		t.Fatalf("data/sequence contents changed schema fingerprint: %v", err)
	}
}

func TestPostgresFreshWalletRejectsMissingHistoryBeforeIntent(t *testing.T) {
	f := newSQLFixture(t)
	f.engine.RequireFreshWalletHistory = true
	f.net.mined, f.net.pending = 3, 3
	ctx := context.Background()
	result, err := f.engine.Step(ctx, 100)
	if err == nil || result.State != "awaiting_nonce_evidence" || f.signer.calls != 0 || len(f.net.broadcasts) != 0 {
		t.Fatalf("missing journal history was not held: result=%+v err=%v", result, err)
	}
	var intents int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM pool_payments_v1.intents`).Scan(&intents); err != nil || intents != 0 {
		t.Fatalf("unexpected durable payout intent: count=%d err=%v", intents, err)
	}
	f.net.mined, f.net.pending = 0, 0
	result, err = f.engine.Step(ctx, 100)
	if err != nil || result.State != "awaiting_confirmation" || f.signer.calls != 1 || len(f.net.broadcasts) != 1 {
		t.Fatalf("fresh wallet did not persist and send one intent: result=%+v err=%v", result, err)
	}
}

func TestPostgresPaymentConfirmationFailureRollsBackAllMoneyRows(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	f.accepted(t)
	f.journal.beforePayoutInsert = func() error { return errors.New("injected crash after credit update") }
	if _, err := f.engine.Step(ctx, 100); err == nil {
		t.Fatal("fault not exercised")
	}
	paid, state, count := f.state(t)
	if paid || state != "pending" || count != 0 {
		t.Fatal("partial monetary commit survived rollback")
	}
	f.journal.beforePayoutInsert = nil
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	paid, state, count = f.state(t)
	if !paid || state != "confirmed" || count != 1 {
		t.Fatal("exact retry did not recover")
	}
}

func TestPostgresWalletFenceAndImmutableIntentAndEvents(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	lease, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := f.journal.Acquire(ctx, f.engine.Wallet); !errors.Is(err, ErrWalletBusy) {
		if other != nil {
			_ = other.Close(ctx)
		}
		t.Fatal("competing wallet session was not fenced")
	}
	if err = lease.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`UPDATE pool_payments_v1.intents SET amount=101 WHERE credit_id=7`,
		`UPDATE pool_payments_v1.intents SET raw_tx=decode('01','hex') WHERE credit_id=7`,
		`DELETE FROM pool_payments_v1.intents WHERE credit_id=7`,
		`UPDATE pool_payments_v1.events SET detail='erased'`,
		`DELETE FROM pool_payments_v1.events`,
		`UPDATE pool_payments_v1.intents SET state='confirmed' WHERE credit_id=7`,
		`UPDATE pool_payments_v1.wallet_state SET checkpoint_hash=decode(repeat('01',32),'hex')`,
	} {
		if _, err = f.pool.Exec(ctx, sql); err == nil {
			t.Fatalf("immutable/shape constraint bypassed: %s", sql)
		}
	}
	p := f.active(t)
	lease, err = f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Hold(ctx, p.Hash, "receipt missing"); err != nil {
		t.Fatal(err)
	}
	if err = lease.Hold(ctx, p.Hash, "receipt missing"); err != nil {
		t.Fatal(err)
	}
	_ = lease.Close(ctx)
	var heldEvents int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM pool_payments_v1.events WHERE kind='held'`).Scan(&heldEvents); err != nil || heldEvents != 1 {
		t.Fatal("hold retry duplicated audit events")
	}
	if _, err = f.pool.Exec(ctx, `UPDATE pool_payments_v1.intents SET state='pending',hold_reason=NULL WHERE credit_id=7`); err == nil {
		t.Fatal("held intent resumed without canonical proof")
	}
}

func TestPostgresRejectsReusedLegacyHashAndOriginalCreditDrift(t *testing.T) {
	for _, mode := range []string{"legacy-hash", "credit-drift", "paid-drift", "maturity"} {
		t.Run(mode, func(t *testing.T) {
			f := newSQLFixture(t)
			ctx := context.Background()
			if mode == "maturity" {
				if _, err := f.pool.Exec(ctx, `UPDATE public.blocks SET status='PENDING'`); err != nil {
					t.Fatal(err)
				}
				r, err := f.engine.Step(ctx, 100)
				if err != nil || r.State != "no_eligible_credit" || len(f.net.broadcasts) != 0 {
					t.Fatal("immature credit was payable")
				}
				return
			}
			if mode == "legacy-hash" {
				raw, err := f.signer.Sign(ctx, f.store.credit, 0, big.NewInt(1))
				if err != nil {
					t.Fatal(err)
				}
				var tx types.Transaction
				if err = tx.UnmarshalBinary(raw); err != nil {
					t.Fatal(err)
				}
				if _, err = f.pool.Exec(ctx, `INSERT INTO public.payouts(tx_hash,amount) VALUES($1,100)`, tx.Hash().Hex()); err != nil {
					t.Fatal(err)
				}
				if _, err = f.engine.Step(ctx, 100); err == nil || len(f.net.broadcasts) != 0 {
					t.Fatal("existing legacy payment adopted for a new credit")
				}
				return
			}
			if _, err := f.engine.Step(ctx, 100); err != nil {
				t.Fatal(err)
			}
			f.accepted(t)
			change := `UPDATE public.credits SET amount=101 WHERE id=7`
			if mode == "paid-drift" {
				change = `UPDATE public.credits SET is_paid=TRUE WHERE id=7`
			}
			if _, err := f.pool.Exec(ctx, change); err != nil {
				t.Fatal(err)
			}
			if _, err := f.engine.Step(ctx, 100); err == nil {
				t.Fatal("changed original credit was marked confirmed")
			}
			var count int
			if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM public.payouts`).Scan(&count); err != nil || count != 0 {
				t.Fatal("drift inserted a payout")
			}
		})
	}
}

func TestPostgresCatalogFingerprintRejectsStructuralDrift(t *testing.T) {
	for _, sql := range []string{
		`DROP INDEX pool_payments_v1.intents_active_wallet_nonce`,
		`ALTER TABLE pool_payments_v1.intents DISABLE TRIGGER protect_intent`,
		`ALTER TABLE pool_payments_v1.intents ADD COLUMN unreviewed TEXT`,
		`CREATE INDEX unreviewed ON pool_payments_v1.intents(nonce)`,
		`ALTER SEQUENCE pool_payments_v1.events_event_id_seq CACHE 3`,
	} {
		t.Run(fmt.Sprintf("drift-%d", len(sql)), func(t *testing.T) {
			f := newSQLFixture(t)
			ctx := context.Background()
			if _, err := f.pool.Exec(ctx, sql); err != nil {
				t.Fatal(err)
			}
			if err := f.journal.Verify(ctx); err == nil {
				t.Fatal("structural drift accepted")
			}
			if lease, err := f.journal.Acquire(ctx, f.engine.Wallet); err == nil {
				_ = lease.Close(ctx)
				t.Fatal("payout acquired with catalog drift")
			}
		})
	}
}

func TestPostgresMigrationNeverSilentlyOverwritesNamespace(t *testing.T) {
	f := newSQLFixture(t)
	if _, err := ApplyMigration(context.Background(), f.pool); err == nil {
		t.Fatal("existing namespace overwritten/adopted")
	}
	if _, err := NewPostgresJournal(f.pool, ""); err == nil {
		t.Fatal("missing reviewed fingerprint accepted")
	}
}

func TestPostgresNonceReuseIsDatabaseConstraint(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) VALUES(8,$1,$2,101,FALSE)`, f.store.credit.BlockHash, f.store.credit.Recipient.Hex()); err != nil {
		t.Fatal(err)
	}
	for _, confirmed := range []bool{false, true} {
		if confirmed {
			f.accepted(t)
			if _, err := f.engine.Step(ctx, 100); err != nil {
				t.Fatal(err)
			}
		}
		lease, err := f.journal.Acquire(ctx, f.engine.Wallet)
		if err != nil {
			t.Fatal(err)
		}
		c, err := lease.NextCredit(ctx, 100)
		if err != nil || c == nil || c.ID != 8 {
			t.Fatalf("next credit: %v %v", c, err)
		}
		nonce := uint64(0)
		raw, err := f.signer.Sign(ctx, *c, nonce, big.NewInt(1))
		if err != nil {
			t.Fatal(err)
		}
		var tx types.Transaction
		if err = tx.UnmarshalBinary(raw); err != nil {
			t.Fatal(err)
		}
		p := Intent{Wallet: f.engine.Wallet, Credit: *c, Nonce: nonce, GasPrice: big.NewInt(1), Raw: raw, Hash: tx.Hash(), CreatedAt: time.Now()}
		_, err = lease.Save(ctx, p)
		var pgError *pgconn.PgError
		expectedConstraint := "intents_chain_id_sender_nonce_key"
		if !errors.As(err, &pgError) || pgError.Code != "23505" || pgError.ConstraintName != expectedConstraint {
			t.Fatalf("wrong failure for active/nonce constraint: %v", err)
		}
		_ = lease.Close(ctx)
	}
}

func TestPostgresFingerprintDoesNotDependOnOrLeakSearchPath(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	c, err := f.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if _, err = c.Exec(ctx, `SET search_path=public,pool_payments_v1`); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(ctx, `RESET search_path`)
	var before, after string
	if err = c.QueryRow(ctx, `SHOW search_path`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	fp, err := CatalogFingerprint(ctx, c)
	if err != nil || fp != f.fingerprint {
		t.Fatalf("search-path-dependent catalog: %v", err)
	}
	if err = c.QueryRow(ctx, `SHOW search_path`).Scan(&after); err != nil || after != before {
		t.Fatal("fingerprint leaked session settings")
	}
}
