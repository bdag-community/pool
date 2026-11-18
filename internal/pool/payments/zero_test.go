package payments

import (
	"context"
	"errors"
	"testing"
)

func TestZeroCreditsAreBoundedAndNeverSignedOrCountedAsCanonicalPayments(t *testing.T) {
	f := newFixture(t)
	f.store.zeroCount = ZeroCreditBatchLimit + 1
	for _, count := range []int{ZeroCreditBatchLimit, 1} {
		result, err := f.engine.Step(context.Background(), 100)
		if err != nil || result.State != "zero_credits_satisfied" || result.ZeroSatisfied != count || result.NewlyConfirmed || f.signer.calls != 0 || f.store.paid || len(f.net.broadcasts) != 0 || f.store.saves != 0 {
			t.Fatal("zero liability was paid by transaction, not audited locally", result, err)
		}
	}
	if result, err := f.engine.Step(context.Background(), 100); err != nil || result.State != "awaiting_confirmation" || f.signer.calls != 1 {
		t.Fatal("positive credit cannot progress after bounded zero batches", result, err)
	}
}

func TestZeroIntegrityFailureHoldsWithoutSigningAndActiveIntentTakesPrecedence(t *testing.T) {
	f := newFixture(t)
	f.store.zeroErr = errors.New("unreviewed zero identity")
	result, err := f.engine.Step(context.Background(), 100)
	if err == nil || result.State != "credit_integrity_held" || f.signer.calls != 0 || len(f.net.broadcasts) != 0 {
		t.Fatal(result, err)
	}
	f.store.zeroErr = nil
	if _, err := f.engine.Step(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	f.store.zeroCount = 1
	f.store.zeroErr = errors.New("must not run while active intent exists")
	if _, err := f.engine.Step(context.Background(), 100); err != nil || f.store.zeroCount != 1 || f.signer.calls != 1 {
		t.Fatal("zero path preempted durable active intent", err)
	}
}

func TestPostgresZeroSatisfactionAtomicIdempotentWithoutPaymentRows(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `UPDATE public.credits SET amount=0 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	f.journal.beforeZeroAudit = func() error { return errors.New("injected zero audit failure") }
	if result, err := f.engine.Step(ctx, 100); err == nil || result.State != "credit_integrity_held" {
		t.Fatal(result, err)
	}
	var paid bool
	var audits, payouts, intents int
	var status string
	read := func() {
		t.Helper()
		if err := f.pool.QueryRow(ctx, `SELECT c.is_paid,b.status,(SELECT count(*) FROM pool_payments_v1.events WHERE kind='zero_satisfied'),(SELECT count(*) FROM public.payouts),(SELECT count(*) FROM pool_payments_v1.intents) FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash WHERE c.id=7`).Scan(&paid, &status, &audits, &payouts, &intents); err != nil {
			t.Fatal(err)
		}
	}
	read()
	if paid || status != "MATURE" || audits != 0 || payouts != 0 || intents != 0 {
		t.Fatal("zero failure left partial accounting", paid, status, audits, payouts, intents)
	}
	f.journal.beforeZeroAudit = nil
	result, err := f.engine.Step(ctx, 100)
	if err != nil || result.ZeroSatisfied != 1 || result.NewlyConfirmed {
		t.Fatal(result, err)
	}
	read()
	if !paid || status != "PAID" || audits != 1 || payouts != 0 || intents != 0 || f.signer.calls != 0 || len(f.net.broadcasts) != 0 {
		t.Fatal("zero created cash payment or missed completion", paid, status, audits, payouts, intents)
	}
	if _, err = f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	read()
	if audits != 1 {
		t.Fatal("zero replay duplicated audit")
	}
	var detail string
	if err = f.pool.QueryRow(ctx, `SELECT detail FROM pool_payments_v1.events WHERE credit_id=7 AND kind='zero_satisfied'`).Scan(&detail); err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(ctx, `UPDATE pool_payments_v1.events SET detail=$1 WHERE credit_id=7`, detail); err == nil {
		t.Fatal("zero audit mutable")
	}
	// A later false-paid/amount drift must never turn this into another spend.
	if _, err = f.pool.Exec(ctx, `UPDATE public.blocks SET status='MATURE'; UPDATE public.credits SET is_paid=FALSE,amount=1 WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.engine.Step(ctx, 100); err == nil || f.signer.calls != 0 || len(f.net.broadcasts) != 0 {
		t.Fatal("zero-to-positive drift signed or sent funds", err)
	}
}

func TestPostgresZeroMaturityIdentityBatchAndMixedPositiveCredits(t *testing.T) {
	f := newSQLFixture(t)
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `INSERT INTO public.credits(id,block_hash,miner_address,amount,is_paid) SELECT n,$1,$2,0,FALSE FROM generate_series(20,84) n`, f.store.credit.BlockHash, f.store.credit.Recipient.Hex()); err != nil {
		t.Fatal(err)
	}
	// max9 is below native credit height10; no DAG/EVM height substitution.
	lease, err := f.journal.Acquire(ctx, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	n, err := lease.SatisfyZeroCredits(ctx, 9)
	lease.Close(ctx)
	if err != nil || n != 0 {
		t.Fatal("immature zero credit satisfied", n, err)
	}
	for _, want := range []int{64, 1} {
		r, err := f.engine.Step(ctx, 100)
		if err != nil || r.ZeroSatisfied != want {
			t.Fatal(r, err)
		}
	}
	var status string
	var paid bool
	var amount string
	var audits int
	if err = f.pool.QueryRow(ctx, `SELECT b.status,c.is_paid,c.amount::text,(SELECT count(*) FROM pool_payments_v1.events WHERE kind='zero_satisfied') FROM public.blocks b JOIN public.credits c ON c.block_hash=b.hash WHERE c.id=7`).Scan(&status, &paid, &amount, &audits); err != nil {
		t.Fatal(err)
	}
	if status != "MATURE" || paid || amount != "100" || audits != 65 || f.signer.calls != 0 {
		t.Fatal("positive credit changed by zero settlement", status, paid, amount, audits)
	}
	// Pending block and malformed zero owner each fail closed, never normalize.
	if _, err = f.pool.Exec(ctx, `UPDATE public.credits SET amount=0,miner_address='bad-owner' WHERE id=7; UPDATE public.blocks SET status='PENDING'`); err != nil {
		t.Fatal(err)
	}
	if r, err := f.engine.Step(ctx, 100); err != nil || r.State != "no_eligible_credit" {
		t.Fatal(r, err)
	}
	if _, err = f.pool.Exec(ctx, `UPDATE public.blocks SET status='MATURE'`); err != nil {
		t.Fatal(err)
	}
	if r, err := f.engine.Step(ctx, 100); err == nil || r.State != "credit_integrity_held" {
		t.Fatal(r, err)
	}
	if _, err = f.pool.Exec(ctx, `UPDATE public.credits SET is_paid=NULL,miner_address=$1 WHERE id=7`, f.store.credit.Recipient.Hex()); err != nil {
		t.Fatal(err)
	}
	if r, err := f.engine.Step(ctx, 100); err == nil || r.State != "credit_integrity_held" || f.signer.calls != 0 {
		t.Fatal("unknown paid flag silently normalized", r, err)
	}
}
