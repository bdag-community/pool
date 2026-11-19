package storage

import (
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
)

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
	t.Setenv(unpricedSubmissionHoldEnvV1, hex.EncodeToString(conflict))
	if err := s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err != nil {
		t.Fatalf("exact hold: %v", err)
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
