package storage

import (
	"testing"

	"asicPool/internal/pool/sharejournal"
)

func TestPostgresRewardStartupChecksGlobalStructureAndScopesWork(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	wallet := testRewardWallet(candidate)
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, unpricedShare(candidate), 7, sharejournal.PartialShare, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareRewardStartupV1(ctx, candidate.ChainID, candidate.BackendID, wallet); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 DISABLE TRIGGER immutable_discovery; UPDATE pool_share_journal_v1.unpriced_discoveries_v1 SET work_digest=decode(repeat('00',32),'hex'); ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 ENABLE TRIGGER immutable_discovery`); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareRewardStartupV1(ctx, candidate.ChainID, candidate.BackendID, wallet); err != nil {
		t.Fatalf("unrelated work blocked startup: %v", err)
	}
	if err := s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, wallet); err == nil {
		t.Fatal("full audit missed tampered work")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.verifyUnpricedWorkCandidateTxV1(ctx, tx, candidate.ChainID, candidate.BackendID, wallet, candidate.CandidateID[:]); err == nil {
		t.Fatal("candidate verification accepted tampered work")
	}
	_ = tx.Rollback(ctx)
	journal, err := sharejournal.New(s.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = journal.BindCut(ctx, [32]byte{99}, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.PrepareRewardStartupV1(ctx, candidate.ChainID, candidate.BackendID, wallet); err == nil {
		t.Fatal("global orphan cut accepted")
	}
}

func TestPostgresRewardStartupPreservesFullPendingReconciliation(t *testing.T) {
	s, ctx, _, candidate := acceptedUnpricedFixture(t)
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 DISABLE TRIGGER immutable_discovery; UPDATE pool_share_journal_v1.unpriced_discoveries_v1 SET work_digest=decode(repeat('00',32),'hex'); ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 ENABLE TRIGGER immutable_discovery`); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareRewardStartupV1(ctx, candidate.ChainID, candidate.BackendID, testRewardWallet(candidate)); err == nil {
		t.Fatal("pending reconciliation accepted tampered work")
	}
	var effects int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_effects_v1`).Scan(&effects); err != nil || effects != 0 {
		t.Fatalf("unexpected reconciliation: effects=%d err=%v", effects, err)
	}
}
