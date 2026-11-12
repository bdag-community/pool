package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestRefreshedJobRecoveryActualPreservedDump(t *testing.T) {
	dbURL := os.Getenv("POOL_RECOVERY_ACTUAL_DATABASE_URL")
	proofPath := os.Getenv("POOL_RECOVERY_ACTUAL_PROOF_PATH")
	if dbURL == "" || proofPath == "" {
		t.Skip("explicit isolated actual-dump database and proof required")
	}
	u, err := url.Parse(dbURL)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") || u.Port() == "" || !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "pool_submission_journal_test_actual") {
		t.Fatal("refusing non-isolated recovery fixture")
	}
	raw, err := os.ReadFile(proofPath)
	if err != nil {
		t.Fatal(err)
	}
	var proof RefreshedJobRecoveryProofV1
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	s := NewPostgresStorage(dbURL)
	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cidBytes, err := hex.DecodeString(proof.CandidateID)
	if err != nil || len(cidBytes) != 32 {
		t.Fatal("invalid candidate")
	}
	var cid [32]byte
	copy(cid[:], cidBytes)
	immutable := []string{"block_submission_candidates_v1", "block_submission_state_v1", "block_submission_attempts_v1", "block_submission_conflicts_v1", "accepted_block_accounting_intents_v1", "accepted_block_accounting_credit_lines_v1"}
	digests := map[string]string{}
	for _, table := range immutable {
		var digest string
		if err := s.pool.QueryRow(ctx, `SELECT md5(coalesce(string_agg(row_to_json(t)::text,'|' ORDER BY row_to_json(t)::text),'')) FROM `+table+` t`).Scan(&digest); err != nil {
			t.Fatal(err)
		}
		digests[table] = digest
	}
	var beforeReceipts, beforeEffects, beforeBlocks, beforeCredits int
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM accepted_block_submission_receipts_v1),(SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT count(*) FROM blocks),(SELECT count(*) FROM credits)`).Scan(&beforeReceipts, &beforeEffects, &beforeBlocks, &beforeCredits); err != nil {
		t.Fatal(err)
	}
	dry, err := s.RecoverRefreshedJobV1(ctx, proof, false)
	if err != nil || !dry.Prepared || dry.Applied {
		t.Fatalf("actual dry run: %+v %v", dry, err)
	}
	first, err := s.RecoverRefreshedJobV1(ctx, proof, true)
	if err != nil || !first.Applied {
		t.Fatalf("actual recovery: %+v %v", first, err)
	}
	retry, err := s.RecoverRefreshedJobV1(ctx, proof, true)
	if err != nil || !retry.AlreadyApplied {
		t.Fatalf("actual repeat: %+v %v", retry, err)
	}
	account, err := s.ApplyAcceptedAccountingV2(ctx, cid)
	if err != nil || !account.Applied || account.CreditLines != 1 {
		t.Fatalf("actual accounting: %+v %v", account, err)
	}
	again, err := s.ApplyAcceptedAccountingV2(ctx, cid)
	if err != nil || !again.AlreadyApplied {
		t.Fatalf("actual re-accounting: %+v %v", again, err)
	}
	for _, table := range immutable {
		var digest string
		if err := s.pool.QueryRow(ctx, `SELECT md5(coalesce(string_agg(row_to_json(t)::text,'|' ORDER BY row_to_json(t)::text),'')) FROM `+table+` t`).Scan(&digest); err != nil {
			t.Fatal(err)
		}
		if digest != digests[table] {
			t.Fatalf("historical table was changed: %s", table)
		}
	}
	var receipts, effects, blocks, credits, conflicts, recoveries int
	var state, creditAmount string
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM accepted_block_submission_receipts_v1),(SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT count(*) FROM blocks),(SELECT count(*) FROM credits),(SELECT count(*) FROM block_submission_conflicts_v1),(SELECT count(*) FROM block_submission_job_recoveries_v1),(SELECT state FROM block_submission_state_v1 WHERE candidate_id=$1),(SELECT sum(amount)::text FROM credits WHERE block_hash=(SELECT encode(legacy_block_hash,'hex') FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1))`, cid[:]).Scan(&receipts, &effects, &blocks, &credits, &conflicts, &recoveries, &state, &creditAmount); err != nil {
		t.Fatal(err)
	}
	if receipts != beforeReceipts+1 || effects != beforeEffects+1 || blocks != beforeBlocks+1 || credits != beforeCredits+1 || conflicts != 1 || recoveries != 1 || state != "conflict" || creditAmount != "140000000000000000000" {
		t.Fatalf("actual accounting/evidence mismatch: %d/%d/%d/%d conflicts=%d recoveries=%d state=%s credit=%s", receipts, effects, blocks, credits, conflicts, recoveries, state, creditAmount)
	}
	if err := s.VerifySubmissionJournalV1(ctx, bigInt(1043), "node"); err != nil {
		t.Fatalf("actual fully qualified recovery did not verify: %v", err)
	}
	metrics, err := s.ReadSubmissionMetrics(ctx)
	if err != nil || metrics.Conflict != 1 || metrics.RecoveredConflict != 1 || metrics.AccountingConflictCount != 0 {
		t.Fatalf("actual historical/recovered metrics: %+v %v", metrics, err)
	}
	if err := s.VerifyAcceptedAccountingV2(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("ACTUAL_DUMP_QUALIFIED: historical table digests unchanged; receipts %d -> %d; effects %d -> %d; credit added exactly once: %s wei; original conflict retained; exact recovery verified for runtime", beforeReceipts, receipts, beforeEffects, effects, creditAmount)
}
