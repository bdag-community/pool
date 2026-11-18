package main

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFreshPoolSchemaInstallAndRestart(t *testing.T) {
	raw := os.Getenv("POOL_PAYOUT_OUTBOX_TEST_DATABASE_URL")
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Port() == "55432" || !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "pool_payout_outbox_test_") {
		t.Fatal("isolated payout fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var database string
	if err = pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil || database != strings.TrimPrefix(u.Path, "/") {
		t.Fatal("isolated database identity mismatch")
	}
	if _, err = pool.Exec(ctx, `DROP SCHEMA IF EXISTS pool_payments_v1 CASCADE; DROP SCHEMA IF EXISTS pool_reward_observations_v1 CASCADE; DROP SCHEMA IF EXISTS pool_share_journal_v1 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;
CREATE TABLE public.miners(address TEXT PRIMARY KEY,joined_at TIMESTAMP,last_active TIMESTAMP);
CREATE TABLE public.blocks(hash TEXT PRIMARY KEY,height BIGINT NOT NULL,reward NUMERIC(30,0) NOT NULL,fees NUMERIC(30,0) NOT NULL,status TEXT DEFAULT 'PENDING',created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE public.block_submissions(id SERIAL PRIMARY KEY,candidate_hash TEXT NOT NULL,node_block_hash TEXT,height BIGINT,backend TEXT,template_seq BIGINT,accepted BOOLEAN NOT NULL,outcome TEXT NOT NULL,message TEXT,created_at TIMESTAMP);
CREATE TABLE public.credits(id SERIAL PRIMARY KEY,block_hash TEXT REFERENCES public.blocks(hash),miner_address TEXT NOT NULL,amount NUMERIC(30,0) NOT NULL,is_paid BOOLEAN DEFAULT FALSE,created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE public.payouts(id SERIAL PRIMARY KEY,tx_hash TEXT UNIQUE NOT NULL,amount NUMERIC(30,0) NOT NULL,created_at TIMESTAMP DEFAULT NOW());`); err != nil {
		t.Fatal(err)
	}
	if err = run(ctx, raw); err != nil {
		t.Fatalf("fresh install: %v", err)
	}
	if _, err = pool.Exec(ctx, `DROP INDEX pool_payments_v1.intents_active_wallet_nonce; CREATE UNIQUE INDEX intents_one_active_wallet ON pool_payments_v1.intents(chain_id,sender) WHERE state<>'confirmed'`); err != nil {
		t.Fatal(err)
	}
	if err = run(ctx, raw); err != nil {
		t.Fatalf("reviewed existing payout catalog upgrade: %v", err)
	}
	if err = run(ctx, raw); err != nil {
		t.Fatalf("restart verification: %v", err)
	}
	var ready bool
	if err = pool.QueryRow(ctx, `SELECT to_regclass('pool_reward_observations_v1.credit_bindings') IS NOT NULL AND to_regclass('pool_payments_v1.intents') IS NOT NULL`).Scan(&ready); err != nil || !ready {
		t.Fatalf("payout tables unavailable: %t %v", ready, err)
	}
	if _, err = pool.Exec(ctx, `DROP SCHEMA pool_reward_observations_v1 CASCADE; DROP SCHEMA pool_share_journal_v1 CASCADE; INSERT INTO public.blocks(hash,height,reward,fees,status) VALUES('legacy',1,100,0,'MATURE'); INSERT INTO public.credits(block_hash,miner_address,amount,is_paid) VALUES('legacy','0x1111111111111111111111111111111111111111',100,FALSE)`); err != nil {
		t.Fatal(err)
	}
	if err = run(ctx, raw); err == nil {
		t.Fatal("legacy ledger was automatically migrated")
	}
	if err = runWithUpgrade(ctx, raw, true); err != nil {
		t.Fatalf("explicit legacy-preserving migration: %v", err)
	}
	var amount string
	var paid bool
	if err = pool.QueryRow(ctx, `SELECT amount::text,is_paid FROM public.credits WHERE block_hash='legacy'`).Scan(&amount, &paid); err != nil || amount != "100" || paid {
		t.Fatalf("legacy credit changed: %q %t %v", amount, paid, err)
	}
}
