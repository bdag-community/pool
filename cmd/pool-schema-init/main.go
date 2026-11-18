// pool-schema-init installs the payout schema for an empty pool database. It
// refuses to adopt or modify a populated legacy ledger automatically.
package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"asicPool/internal/pool/payments"
	"asicPool/internal/pool/sharejournal"
	"asicPool/internal/pool/storage"
	"asicPool/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func sqlBody(name string) (string, error) {
	data, err := migrations.Files.ReadFile(name)
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(data), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) != "BEGIN;" && strings.TrimSpace(line) != "COMMIT;" {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n"), nil
}

func run(ctx context.Context, url string) error {
	return runWithUpgrade(ctx, url, false)
}

func verifyRewardClaims(ctx context.Context, conn *pgxpool.Conn) error {
	var valid bool
	err := conn.QueryRow(ctx, `SELECT
		EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='pool_reward_observations_v1.reward_evm_claims'::regclass AND contype='p' AND pg_get_constraintdef(oid)='PRIMARY KEY (evm_block_hash)')
		AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='pool_reward_observations_v1.reward_evm_claims'::regclass AND contype='u' AND pg_get_constraintdef(oid)='UNIQUE (candidate_id)')
		AND (SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace
			WHERE NOT t.tgisinternal AND t.tgenabled='O' AND (
			(t.tgrelid='pool_reward_observations_v1.allocations'::regclass AND t.tgname='allocations_claim_evm_reward' AND t.tgtype=5 AND n.nspname='pool_reward_observations_v1' AND p.proname='claim_allocated_evm_reward') OR
			(t.tgrelid='pool_reward_observations_v1.reward_evm_claims'::regclass AND t.tgname='reward_evm_claims_immutable' AND t.tgtype=27 AND n.nspname='pool_share_journal_v1' AND p.proname='immutable_share') OR
			(t.tgrelid='pool_reward_observations_v1.reward_evm_claims'::regclass AND t.tgname='reward_evm_claims_no_truncate' AND t.tgtype=34 AND n.nspname='pool_share_journal_v1' AND p.proname='immutable_share'))) = 3
		AND NOT EXISTS (SELECT 1 FROM pool_reward_observations_v1.allocations a LEFT JOIN pool_reward_observations_v1.reward_evm_claims c USING(candidate_id) WHERE c.candidate_id IS NULL)`).Scan(&valid)
	if err != nil || !valid {
		return errors.New("reward claim schema verification failed")
	}
	return nil
}

func upgradePaymentPipeline(ctx context.Context, conn *pgxpool.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `LOCK TABLE pool_payments_v1.intents IN ACCESS EXCLUSIVE MODE`); err != nil {
		return err
	}
	before, err := payments.CatalogFingerprint(ctx, tx)
	if err != nil || before != payments.LegacyReviewedSchemaFingerprint {
		return errors.New("legacy payout catalog changed before pipeline migration")
	}
	body, err := sqlBody("20261118_payment_pipeline_v1.sql")
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, body); err != nil {
		return fmt.Errorf("payout pipeline migration: %w", err)
	}
	after, err := payments.CatalogFingerprint(ctx, tx)
	if err != nil || after != payments.ReviewedSchemaFingerprint {
		return errors.New("migrated payout catalog is not reviewed")
	}
	return tx.Commit(ctx)
}

func runWithUpgrade(ctx context.Context, url string, allowLegacy bool) error {
	if url == "" {
		return errors.New("PG_URL is required")
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return errors.New("pool database configuration invalid")
	}
	config.MaxConns = 2
	config.ConnConfig.RuntimeParams["search_path"] = "public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return errors.New("pool database unavailable")
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended('BDAG-POOL-SCHEMA-INIT/v1',1))`); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtextextended('BDAG-POOL-SCHEMA-INIT/v1',1))`)
	}()
	var share, reward, payment bool
	if err = conn.QueryRow(ctx, `SELECT to_regnamespace('pool_share_journal_v1') IS NOT NULL,to_regnamespace('pool_reward_observations_v1') IS NOT NULL,to_regnamespace('pool_payments_v1') IS NOT NULL`).Scan(&share, &reward, &payment); err != nil {
		return err
	}
	if share || reward {
		if !share || !reward || !payment {
			return errors.New("partial pool payout schema requires review")
		}
		if err = sharejournal.VerifyCatalog(ctx, conn); err != nil {
			return err
		}
		fingerprint, err := payments.CatalogFingerprint(ctx, conn)
		if err != nil {
			return err
		}
		if fingerprint == payments.LegacyReviewedSchemaFingerprint {
			if err = upgradePaymentPipeline(ctx, conn); err != nil {
				return err
			}
		} else if fingerprint != payments.ReviewedSchemaFingerprint {
			return errors.New("existing payout catalog is not reviewed")
		}
		journal, err := payments.NewReviewedPostgresJournal(pool)
		if err != nil {
			return err
		}
		if err = journal.Verify(ctx); err != nil {
			return err
		}
		var claimsExist bool
		if err = conn.QueryRow(ctx, `SELECT to_regclass('pool_reward_observations_v1.reward_evm_claims') IS NOT NULL`).Scan(&claimsExist); err != nil {
			return err
		}
		if !claimsExist {
			data, err := migrations.Files.ReadFile("20261118_reward_evm_claims_v1.sql")
			if err != nil {
				return err
			}
			if _, err = conn.Exec(ctx, string(data)); err != nil {
				return fmt.Errorf("reward claim migration: %w", err)
			}
		}
		return verifyRewardClaims(ctx, conn)
	}
	var populated bool
	if err = conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.blocks) OR EXISTS(SELECT 1 FROM public.credits) OR EXISTS(SELECT 1 FROM public.payouts)`).Scan(&populated); err != nil {
		return err
	}
	if populated && !allowLegacy {
		return errors.New("existing pool ledger requires explicit migration; nothing was changed")
	}
	if populated {
		// The old journal and its monetary effects must already be coherent.
		// Only new namespaces are added; historical rows are never adopted,
		// rewritten, or assigned to the new reward wallet by this migration.
		old := storage.NewPostgresStorage(url)
		if err = old.Connect(ctx); err != nil {
			return err
		}
		defer old.Close()
		if err = old.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err != nil {
			return err
		}
		if err = old.VerifyAcceptedAccountingV2(ctx); err != nil {
			return err
		}
	} else {
		for _, name := range []string{"20260901_block_submission_journal_v1.sql", "20260902_accepted_receipt_accounting_v1.sql", "20260906_wide_reward_accounting_v2.sql"} {
			data, err := migrations.Files.ReadFile(name)
			if err != nil {
				return err
			}
			if _, err = conn.Exec(ctx, string(data)); err != nil {
				return fmt.Errorf("base pool migration %s: %w", name, err)
			}
		}
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	for _, name := range []string{"20261117_durable_share_journal_v1.sql", "20261117_unpriced_discovery_v1.sql", "20261117_reward_observations_v1.sql", "20261117_reward_credits_v1.sql", "20261118_reward_evm_claims_v1.sql"} {
		body, err := sqlBody(name)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, body); err != nil {
			return fmt.Errorf("pool payout migration %s: %w", name, err)
		}
	}
	if !payment {
		if _, err = tx.Exec(ctx, payments.MigrationSQL); err != nil {
			return err
		}
	}
	if err = sharejournal.VerifyCatalog(ctx, tx); err != nil {
		return err
	}
	fingerprint, err := payments.CatalogFingerprint(ctx, tx)
	if err != nil || fingerprint != payments.ReviewedSchemaFingerprint {
		return errors.New("new payout schema fingerprint rejected")
	}
	return tx.Commit(ctx)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := runWithUpgrade(ctx, os.Getenv("PG_URL"), os.Getenv("POOL_UPGRADE_LEGACY_LEDGER") == "1"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Pool payout schema is ready.")
}
