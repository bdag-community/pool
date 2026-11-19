package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/crypto/blake2b"
)

func installUnpricedHoldFixture(t *testing.T, s *PostgresStorage, ctx context.Context) {
	t.Helper()
	candidate := secondUnpricedCandidate(t)
	candidate.HeaderBytes[181]++
	copy(candidate.NonceBytes[:], candidate.HeaderBytes[181:185])
	first := blake2b.Sum256(candidate.HeaderBytes[:])
	candidate.ExpectedBlockHash = blake2b.Sum256(first[:])
	candidate.CandidateID = candidateIDForHashV1(candidate.ExpectedBlockHash)
	candidate.SharePowHash[0]++
	var err error
	candidate.FrozenPayload, err = encodeFrozenCandidateJournalV1(candidate)
	if err != nil {
		t.Fatal(err)
	}
	candidate.FrozenDigest = sha256.Sum256(candidate.FrozenPayload)
	share := unpricedShare(candidate)
	original, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimCandidate(ctx, candidate.CandidateID, [16]byte{9}, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	attempt, err := s.PrepareAttempt(ctx, lease.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttempt(ctx, lease.Lease, attempt.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal); err != nil {
		t.Fatal(err)
	}
	view, err := scanCandidateView(ctx, s.pool, candidate.CandidateID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := *share
	changed.Work++
	if _, _, err := recordUnpricedConflict(ctx, tx, view, candidate, "discovery_share", original.WorkDigest[:], &changed, 7, sharejournal.WholeShare, 0); err == nil {
		t.Fatal("fixture conflict absent")
	}
	var conflict []byte
	if err := s.pool.QueryRow(ctx, `SELECT conflict_id FROM block_submission_conflicts_v1`).Scan(&conflict); err != nil {
		t.Fatal(err)
	}
	t.Setenv(unpricedSubmissionHoldEnvV1, hex.EncodeToString(conflict))
	if err := s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err != nil {
		t.Fatalf("qualify hold: %v", err)
	}
}

func TestUnpricedSubmissionHoldPreservesAccountingGate(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	share := unpricedShare(candidate)
	original, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimCandidate(ctx, candidate.CandidateID, [16]byte{9}, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	attempt, err := s.PrepareAttempt(ctx, lease.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttempt(ctx, lease.Lease, attempt.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal); err != nil {
		t.Fatal(err)
	}
	view, err := scanCandidateView(ctx, s.pool, candidate.CandidateID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := *share
	changed.Work++
	if _, _, err := recordUnpricedConflict(ctx, tx, view, candidate, "discovery_share", original.WorkDigest[:], &changed, 7, sharejournal.WholeShare, 0); err == nil {
		t.Fatal("fixture must record the historical conflict")
	}
	var conflict []byte
	if err := s.pool.QueryRow(ctx, `SELECT conflict_id FROM block_submission_conflicts_v1`).Scan(&conflict); err != nil {
		t.Fatal(err)
	}

	if err := s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err == nil {
		t.Fatal("conflict accepted without explicit hold")
	}
	t.Setenv(unpricedSubmissionHoldEnvV1, "00000000000000000000000000000000")
	if err := s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err == nil {
		t.Fatal("unrelated conflict id accepted")
	}
	if _, err := NewRewardCreditGuardV1(s.pool, "node", testMatureRewardVerifier{true}); err == nil {
		t.Fatal("payer accepted an unqualified hold")
	}
	t.Setenv(unpricedSubmissionHoldEnvV1, hex.EncodeToString(conflict))
	if err := s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err != nil {
		t.Fatalf("exact hold: %v", err)
	}
	guard, err := NewRewardCreditGuardV1(s.pool, "node", testMatureRewardVerifier{true})
	if err != nil || guard.hold == nil {
		t.Fatalf("payer did not independently qualify hold: %v", err)
	}
	latch, args := s.conflictLatchSQLV1(1)
	var latched bool
	if err := s.pool.QueryRow(ctx, `SELECT `+latch, args...).Scan(&latched); err != nil || latched {
		t.Fatalf("exact submission hold did not open: %v", err)
	}
	wallet := common.BytesToAddress(candidate.HeaderBytes[148:168])
	if err := s.PrepareRewardStartupV1(ctx, big.NewInt(1404), "node", wallet); err != nil {
		t.Fatalf("held startup: %v", err)
	}
	if err := s.VerifyUnpricedWorkV1(ctx, big.NewInt(1404), "node", wallet); err == nil {
		t.Fatal("hold opened accounting verification")
	}
	other := storageTestCandidate()
	other.HeaderBytes[181]++
	copy(other.NonceBytes[:], other.HeaderBytes[181:185])
	other.SharePowHash[0]++
	first := blake2b.Sum256(other.HeaderBytes[:])
	other.ExpectedBlockHash = blake2b.Sum256(first[:])
	other.CandidateID = candidateIDForHashV1(other.ExpectedBlockHash)
	other.FrozenPayload, err = encodeFrozenCandidateJournalV1(other)
	if err != nil {
		t.Fatal(err)
	}
	other.FrozenDigest = sha256.Sum256(other.FrozenPayload)
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, other, unpricedShare(other), 7, sharejournal.WholeShare, 0); err != nil {
		t.Fatalf("unrelated candidate insertion: %v", err)
	}
	verifyTx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.verifyUnpricedWorkCandidateTxV1(ctx, verifyTx, big.NewInt(1404), "node", wallet, other.CandidateID[:]); err != nil {
		t.Fatalf("unrelated candidate should pass its own work verification: %v", err)
	}
	if err := s.verifyUnpricedWorkCandidateTxV1(ctx, verifyTx, big.NewInt(1404), "node", wallet, candidate.CandidateID[:]); err == nil {
		t.Fatal("disputed candidate must remain held")
	}
	_ = verifyTx.Rollback(ctx)
	// A new unrelated conflict must immediately close the submission gate.
	tx, err = s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertConflict(ctx, tx, candidate.CandidateID, "candidate_identity_mismatch", candidate.FrozenDigest[:], candidate.FrozenDigest[:], candidate.FrozenPayload, nil, "other"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT `+latch, args...).Scan(&latched); err != nil || !latched {
		t.Fatalf("new conflict did not close gate: latched=%t err=%v", latched, err)
	}
}

func TestUnpricedSubmissionHoldRejectsUnreceiptedCandidate(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	share := unpricedShare(candidate)
	original, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimCandidate(ctx, candidate.CandidateID, [16]byte{9}, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	attempt, err := s.PrepareAttempt(ctx, lease.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	view, err := scanCandidateView(ctx, s.pool, candidate.CandidateID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := *share
	changed.Work++
	if _, _, err := recordUnpricedConflict(ctx, tx, view, candidate, "discovery_share", original.WorkDigest[:], &changed, 7, sharejournal.WholeShare, 0); err == nil {
		t.Fatal("fixture conflict absent")
	}
	if _, err := s.CompleteAttempt(ctx, lease.Lease, attempt.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal); err != nil {
		t.Fatal(err)
	}
	var conflict []byte
	if err := s.pool.QueryRow(ctx, `SELECT conflict_id FROM block_submission_conflicts_v1`).Scan(&conflict); err != nil {
		t.Fatal(err)
	}
	t.Setenv(unpricedSubmissionHoldEnvV1, hex.EncodeToString(conflict))
	if err := s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err == nil {
		t.Fatal("unreceipted attempt must not qualify")
	}
}
