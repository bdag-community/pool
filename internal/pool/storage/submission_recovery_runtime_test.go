package storage

import (
	"context"
	"crypto/sha256"
	"golang.org/x/crypto/blake2b"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestRecoveredJobRuntimeRetainsHistoryAndAllowsNewWork(t *testing.T) {
	s, c, p := recoveryFixture(t)
	ctx := context.Background()
	if _, err := s.RecoverRefreshedJobV1(ctx, p, true); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err == nil {
		t.Fatal("restart accepted recovery before original accounting materialized")
	}
	if _, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err != nil {
		t.Fatalf("verified additive recovery must allow restart: %v", err)
	}
	// New work, not reopening the original conflicted database row.
	next := c
	next.HeaderBytes[181] ^= 1
	copy(next.NonceBytes[:], next.HeaderBytes[181:185])
	next.SharePowHash[0] ^= 1
	firstHash := blake2b.Sum256(next.HeaderBytes[:])
	next.ExpectedBlockHash = blake2b.Sum256(firstHash[:])
	next.CandidateID = candidateIDForHashV1(next.ExpectedBlockHash)
	next.FrozenPayload, _ = encodeFrozenCandidateJournalV1(next)
	next.FrozenDigest = sha256.Sum256(next.FrozenPayload)
	if _, disposition, err := s.PutCandidateV2(ctx, next, wideAccountingIntentV2(t, next)); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("new work blocked after verified recovery: %s %v", disposition, err)
	}
	if lease, err := s.ClaimCandidate(ctx, next.CandidateID, [16]byte{3}, time.Minute); err != nil || lease == nil {
		t.Fatalf("new work claim: %+v %v", lease, err)
	}
	var rawState string
	if err := s.pool.QueryRow(ctx, `SELECT state FROM block_submission_state_v1 WHERE candidate_id=$1`, c.CandidateID[:]).Scan(&rawState); err != nil {
		t.Fatal(err)
	}
	if rawState != "conflict" {
		t.Fatal("runtime reopened historical conflict")
	}
}

func TestRecoveredJobRestartPreservesUnfinishedLaterSubmission(t *testing.T) {
	s, c, p := recoveryFixture(t)
	ctx := context.Background()
	if _, err := s.RecoverRefreshedJobV1(ctx, p, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err != nil {
		t.Fatal(err)
	}
	next := c
	next.HeaderBytes[181] ^= 1
	copy(next.NonceBytes[:], next.HeaderBytes[181:185])
	next.SharePowHash[0] ^= 1
	h := blake2b.Sum256(next.HeaderBytes[:])
	next.ExpectedBlockHash = blake2b.Sum256(h[:])
	next.CandidateID = candidateIDForHashV1(next.ExpectedBlockHash)
	next.FrozenPayload, _ = encodeFrozenCandidateJournalV1(next)
	next.FrozenDigest = sha256.Sum256(next.FrozenPayload)
	if _, _, err := s.PutCandidateV2(ctx, next, wideAccountingIntentV2(t, next)); err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimCandidate(ctx, next.CandidateID, [16]byte{3}, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err = s.PrepareAttempt(ctx, lease.Lease, SubmitMethodV2); err != nil {
		t.Fatal(err)
	}
	before := recoveryRestartRows(t, s)
	// Fresh storage loses in-memory allowances, exactly like a pool restart.
	restarted := NewPostgresStorage(s.dbURL)
	if err := restarted.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err != nil {
		t.Fatalf("valid historical recovery blocked by unrelated unfinished work: %v", err)
	}
	if after := recoveryRestartRows(t, s); !equalStrings(before, after) {
		t.Fatal("read-only restart verification changed unfinished work or history")
	}
}

func recoveryRestartRows(t *testing.T, s *PostgresStorage) []string {
	t.Helper()
	var out []string
	for _, table := range []string{"block_submission_candidates_v1", "block_submission_state_v1", "block_submission_attempts_v1", "block_submission_conflicts_v1", "block_submission_job_recoveries_v1", "accepted_block_submission_receipts_v1", "accepted_block_accounting_intents_v1", "accepted_block_accounting_credit_lines_v1", "accepted_block_accounting_effects_v1", "credits", "blocks"} {
		var digest string
		if err := s.pool.QueryRow(context.Background(), `SELECT md5(coalesce(string_agg(row_to_json(t)::text,'|' ORDER BY row_to_json(t)::text),'')) FROM `+table+` t`).Scan(&digest); err != nil {
			t.Fatal(err)
		}
		out = append(out, digest)
	}
	return out
}

func TestNewRecoveryStillRequiresDrainedWriters(t *testing.T) {
	ctx := context.Background()
	s, _, proof := recoveryFixtureWithHook(t, func(s *PostgresStorage, c FrozenCandidateV1) {
		next := c
		next.HeaderBytes[181] ^= 1
		copy(next.NonceBytes[:], next.HeaderBytes[181:185])
		next.SharePowHash[0] ^= 1
		h := blake2b.Sum256(next.HeaderBytes[:])
		next.ExpectedBlockHash = blake2b.Sum256(h[:])
		next.CandidateID = candidateIDForHashV1(next.ExpectedBlockHash)
		next.FrozenPayload, _ = encodeFrozenCandidateJournalV1(next)
		next.FrozenDigest = sha256.Sum256(next.FrozenPayload)
		if _, _, err := s.PutCandidateV2(ctx, next, wideAccountingIntentV2(t, next)); err != nil {
			t.Fatal(err)
		}
		lease, err := s.ClaimCandidate(ctx, next.CandidateID, [16]byte{4}, time.Minute)
		if err != nil || lease == nil {
			t.Fatalf("claim: %v", err)
		}
		if _, err := s.PrepareAttempt(ctx, lease.Lease, SubmitMethodV2); err != nil {
			t.Fatal(err)
		}
	})
	for _, apply := range []bool{false, true} {
		if _, err := s.RecoverRefreshedJobV1(ctx, proof, apply); err == nil || !strings.Contains(err.Error(), "drained offline submission writers") {
			t.Fatalf("new recovery without draining accepted (apply=%v): %v", apply, err)
		}
	}
	var receipts int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM accepted_block_submission_receipts_v1`).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("blocked recovery changed receipts: %d %v", receipts, err)
	}
}

func TestRecoveredJobRuntimeRejectsAnyNewConflict(t *testing.T) {
	s, c, p := recoveryFixture(t)
	ctx := context.Background()
	if _, err := s.RecoverRefreshedJobV1(ctx, p, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err != nil {
		t.Fatal(err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertConflict(ctx, tx, c.CandidateID, "submit_observation_conflict", c.FrozenDigest[:], c.FrozenDigest[:], nil, nil, "rpc_observation"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	variant := refreshedJobCandidate(t, c)
	if _, disposition, err := s.PutCandidateV2(ctx, variant, wideAccountingIntentV2(t, variant)); err == nil || disposition != PutDispositionConflict {
		t.Fatal("new unresolved conflict did not stop writes")
	}
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err == nil {
		t.Fatal("new unresolved conflict did not stop restart")
	}
}

func TestRecoveredJobRuntimeRejectsChangedProof(t *testing.T) {
	s, c, p := recoveryFixture(t)
	ctx := context.Background()
	if _, err := s.RecoverRefreshedJobV1(ctx, p, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err != nil {
		t.Fatal(err)
	}
	// Hostile database-admin tamper: even bypassing the immutable audit trigger
	// must not match the exact proof pinned during startup.
	if _, err := s.pool.Exec(ctx, `ALTER TABLE block_submission_job_recoveries_v1 DISABLE TRIGGER block_submission_job_recoveries_v1_immutable; UPDATE block_submission_job_recoveries_v1 SET proof_payload='tampered'::bytea; ALTER TABLE block_submission_job_recoveries_v1 ENABLE TRIGGER block_submission_job_recoveries_v1_immutable`); err != nil {
		t.Fatal(err)
	}
	if _, disposition, err := s.PutCandidateV2(ctx, c, wideAccountingIntentV2(t, c)); err == nil || disposition != PutDispositionConflict {
		t.Fatal("changed recovery proof did not stop writes")
	}
	if err := s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err == nil {
		t.Fatal("changed recovery proof did not stop restart")
	}
}
