package storage

import (
	"context"
	"crypto/sha256"
	"testing"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/crypto/blake2b"
)

func testRewardWallet(c FrozenCandidateV1) common.Address {
	return common.BytesToAddress(c.HeaderBytes[148:168])
}

func TestPostgresScopedUnpricedVerification(t *testing.T) {
	s, ctx := unpricedFixture(t)
	first, second := storageTestCandidate(), secondUnpricedCandidate(t)
	for _, candidate := range []FrozenCandidateV1{first, second} {
		if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, unpricedShare(candidate), 7, sharejournal.PartialShare, 0); err != nil {
			t.Fatal(err)
		}
	}
	check := func(id []byte) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		return s.verifyUnpricedWorkCandidateTxV1(ctx, tx, first.ChainID, first.BackendID, testRewardWallet(first), id)
	}
	if err := check(first.CandidateID[:]); err != nil {
		t.Fatal(err)
	}
	if err := check(make([]byte, 32)); err == nil {
		t.Fatal("missing candidate accepted")
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 DISABLE TRIGGER immutable_discovery`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE pool_share_journal_v1.unpriced_discoveries_v1 SET work_digest=decode(repeat('00',32),'hex') WHERE candidate_id=$1`, second.CandidateID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 ENABLE TRIGGER immutable_discovery`); err != nil {
		t.Fatal(err)
	}
	if err := check(first.CandidateID[:]); err != nil {
		t.Fatalf("unrelated work blocked candidate: %v", err)
	}
	if err := check(second.CandidateID[:]); err == nil {
		t.Fatal("tampered candidate accepted")
	}
	if err := s.VerifyUnpricedWorkV1(ctx, first.ChainID, first.BackendID, testRewardWallet(first)); err == nil {
		t.Fatal("full audit missed tampering")
	}
	j, err := sharejournal.New(s.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.BindCut(ctx, [32]byte{99}, nil); err != nil {
		t.Fatal(err)
	}
	if err = check(first.CandidateID[:]); err == nil {
		t.Fatal("global orphan cut accepted")
	}
}

func TestPostgresVerifyUnpricedWorkSnapshot(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	wallet := testRewardWallet(candidate)
	if err := s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, wallet); err != nil {
		t.Fatalf("empty journal: %v", err)
	}
	if err := s.VerifyUnpricedWorkV1(context.Background(), candidate.ChainID, candidate.BackendID, wallet); err == nil {
		t.Fatal("unbounded verification accepted")
	}
	share := unpricedShare(candidate)
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.PartialShare, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, wallet); err != nil {
		t.Fatalf("matching discovery: %v", err)
	}
	if err := s.VerifyUnpricedWorkV1(ctx, bigInt(1), candidate.BackendID, wallet); err == nil {
		t.Fatal("wrong chain accepted")
	}
	if err := s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, "other", wallet); err == nil {
		t.Fatal("wrong backend accepted")
	}
	if err := s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, common.HexToAddress("0x1234")); err == nil {
		t.Fatal("wrong wallet accepted")
	}
	j, err := sharejournal.New(s.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = j.Append(ctx, sharejournal.Share{ID: [32]byte{8}, Miner: share.Miner, Work: 50}); err != nil {
		t.Fatal(err)
	}
	if err = s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, wallet); err != nil {
		t.Fatalf("later work changed a frozen cut: %v", err)
	}
	if err = s.VerifyAcceptedAccountingV2(ctx); err == nil {
		t.Fatal("legacy startup accepted the unpriced candidate")
	}
}

func TestPostgresVerifyUnpricedWorkRejectsTampering(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"work digest", `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 DISABLE TRIGGER immutable_discovery; UPDATE pool_share_journal_v1.unpriced_discoveries_v1 SET work_digest=decode(repeat('00',32),'hex'); ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 ENABLE TRIGGER immutable_discovery`},
		{"target policy", `ALTER TABLE pool_share_journal_v1.allocation_policies DISABLE TRIGGER immutable_policy; UPDATE pool_share_journal_v1.allocation_policies SET target_work=8; ALTER TABLE pool_share_journal_v1.allocation_policies ENABLE TRIGGER immutable_policy`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := unpricedFixture(t)
			candidate := storageTestCandidate()
			if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, unpricedShare(candidate), 7, sharejournal.PartialShare, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(ctx, tc.sql); err != nil {
				t.Fatal(err)
			}
			if err := sharejournal.VerifyCatalog(ctx, s.pool); err != nil {
				t.Fatalf("fixture did not restore catalog: %v", err)
			}
			if err := s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, testRewardWallet(candidate)); err == nil {
				t.Fatal("tampered work accepted")
			}
		})
	}
}

func TestPostgresVerifyUnpricedWorkRejectsOrphansAndPricing(t *testing.T) {
	t.Run("orphan cut", func(t *testing.T) {
		s, ctx := unpricedFixture(t)
		j, err := sharejournal.New(s.pool)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = j.BindCut(ctx, [32]byte{9}, nil); err != nil {
			t.Fatal(err)
		}
		candidate := storageTestCandidate()
		if err = s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, testRewardWallet(candidate)); err == nil {
			t.Fatal("orphan cut accepted")
		}
	})
	t.Run("unproven price", func(t *testing.T) {
		s, ctx := unpricedFixture(t)
		candidate := storageTestCandidate()
		if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, unpricedShare(candidate), 7, sharejournal.WholeShare, 0); err != nil {
			t.Fatal(err)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = putAcceptedAccountingIntentTxV1(ctx, tx, storageTestAccountingIntent(candidate)); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, testRewardWallet(candidate)); err == nil {
			t.Fatal("unproven price accepted")
		}
	})
}

func TestPostgresVerifyUnpricedWorkAllowsExistingLegacyIntent(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	if _, _, err := s.PutCandidate(ctx, candidate, storageTestAccountingIntent(candidate)); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyUnpricedWorkV1(ctx, candidate.ChainID, candidate.BackendID, testRewardWallet(candidate)); err != nil {
		t.Fatalf("legacy candidate was rejected: %v", err)
	}
}

func secondUnpricedCandidate(t *testing.T) FrozenCandidateV1 {
	t.Helper()
	candidate := storageTestCandidate()
	candidate.HeaderBytes[181]++
	copy(candidate.NonceBytes[:], candidate.HeaderBytes[181:185])
	first := blake2b.Sum256(candidate.HeaderBytes[:])
	candidate.ExpectedBlockHash = blake2b.Sum256(first[:])
	candidate.CandidateID = candidateIDForHashV1(candidate.ExpectedBlockHash)
	var err error
	candidate.FrozenPayload, err = encodeFrozenCandidateJournalV1(candidate)
	if err != nil {
		t.Fatal(err)
	}
	candidate.FrozenDigest = sha256.Sum256(candidate.FrozenPayload)
	return candidate
}

func TestPostgresCombinedAccountingVerifierPreservesLegacyChecks(t *testing.T) {
	s, ctx := unpricedFixture(t)
	legacy := storageTestCandidate()
	unpriced := secondUnpricedCandidate(t)
	if _, _, err := s.PutCandidate(ctx, legacy, storageTestAccountingIntent(legacy)); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyAcceptedAccountingV2(ctx); err != nil {
		t.Fatalf("legacy baseline: %v", err)
	}
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, unpriced, unpricedShare(unpriced), 7, sharejournal.WholeShare, 0); err != nil {
		t.Fatal(err)
	}
	wallet := testRewardWallet(unpriced)
	if err := s.VerifyAcceptedAccountingV2(ctx); err == nil {
		t.Fatal("legacy verifier silently accepted an unpriced candidate")
	}
	if err := s.VerifyAccountingWithUnpricedV1(ctx, unpriced.ChainID, unpriced.BackendID, wallet); err != nil {
		t.Fatalf("combined valid snapshot: %v", err)
	}
	if err := s.VerifyAccountingWithUnpricedV1(ctx, bigInt(1), unpriced.BackendID, wallet); err == nil {
		t.Fatal("wrong chain accepted")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `ALTER TABLE public.accepted_block_accounting_intents_v1 DISABLE TRIGGER accepted_block_accounting_intents_v1_immutable`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE public.accepted_block_accounting_intents_v1 SET intent_digest=decode(repeat('00',32),'hex') WHERE candidate_id=$1`, legacy.CandidateID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `ALTER TABLE public.accepted_block_accounting_intents_v1 ENABLE TRIGGER accepted_block_accounting_intents_v1_immutable`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyUnpricedWorkV1(ctx, unpriced.ChainID, unpriced.BackendID, wallet); err != nil {
		t.Fatalf("unpriced-only check unexpectedly validates legacy money: %v", err)
	}
	if err := s.VerifyAccountingWithUnpricedV1(ctx, unpriced.ChainID, unpriced.BackendID, wallet); err == nil {
		t.Fatal("combined verifier ignored tampered legacy intent")
	}
}

func TestPostgresUnpricedDiscoveryReusedProofFreezesLatestCut(t *testing.T) {
	s, ctx := unpricedFixture(t)
	first, second := storageTestCandidate(), secondUnpricedCandidate(t)
	share := unpricedShare(first)
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, first, share, 20, sharejournal.PartialShare, 0); err != nil {
		t.Fatal(err)
	}
	j, err := sharejournal.New(s.pool)
	if err != nil {
		t.Fatal(err)
	}
	sequence, _, err := j.Append(ctx, sharejournal.Share{ID: [32]byte{8}, Miner: "0x1111111111111111111111111111111111111111", Work: 4})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := s.PutUnpricedDiscoveryV1(ctx, second, share, 20, sharejournal.PartialShare, 0)
	if err != nil || got.Work.Sequence != sequence || got.Work.Total != 10 {
		t.Fatalf("new discovery cut=%d total=%d err=%v", got.Work.Sequence, got.Work.Total, err)
	}
	if _, _, err = j.Append(ctx, sharejournal.Share{ID: [32]byte{9}, Miner: "0x3333333333333333333333333333333333333333", Work: 5}); err != nil {
		t.Fatal(err)
	}
	replayed, _, err := s.PutUnpricedDiscoveryV1(ctx, second, share, 20, sharejournal.PartialShare, 0)
	if err != nil || replayed.Work.Sequence != sequence || replayed.Work.Total != 10 || replayed.WorkDigest != got.WorkDigest {
		t.Fatalf("replayed discovery changed: %+v %v", replayed.Work, err)
	}
	if err = s.VerifyAccountingWithUnpricedV1(ctx, first.ChainID, first.BackendID, testRewardWallet(first)); err != nil {
		t.Fatalf("verify discoveries: %v", err)
	}
}
