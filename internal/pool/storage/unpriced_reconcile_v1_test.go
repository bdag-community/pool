package storage

import (
	"context"
	"testing"
	"time"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
)

func acceptedUnpricedFixture(t *testing.T) (*PostgresStorage, context.Context, FrozenCandidateV1, FrozenCandidateV1) {
	t.Helper()
	s, ctx := unpricedFixture(t)
	legacy, _ := acceptedAccountingReceipt(t, s)
	unpriced := secondUnpricedCandidate(t)
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, unpriced, unpricedShare(unpriced), 7, sharejournal.WholeShare, 0); err != nil {
		t.Fatal(err)
	}
	leased, err := s.ClaimCandidate(ctx, unpriced.CandidateID, [16]byte{3}, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim unpriced: %v", err)
	}
	prepared, err := s.PrepareAttempt(ctx, leased.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := s.CompleteAttempt(ctx, leased.Lease, prepared.AttemptID, acceptedObservation(unpriced), SubmitDecisionAcceptedFinal)
	if err != nil || !transition.FirstAccepted {
		t.Fatalf("accept unpriced: %+v %v", transition, err)
	}
	return s, ctx, legacy, unpriced
}

func TestPostgresCombinedReconcileOnlyMaterializesLegacyIntent(t *testing.T) {
	s, ctx, legacy, unpriced := acceptedUnpricedFixture(t)
	if _, err := s.ReconcileAccountingWithUnpricedV1(context.Background(), unpriced.ChainID, unpriced.BackendID, testRewardWallet(unpriced)); err == nil {
		t.Fatal("unbounded reconciliation accepted")
	}
	if _, err := s.ReconcileAccountingWithUnpricedV1(ctx, unpriced.ChainID, unpriced.BackendID, common.HexToAddress("0x1234")); err == nil {
		t.Fatal("wrong wallet accepted")
	}
	var effects int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_effects_v1`).Scan(&effects); err != nil || effects != 0 {
		t.Fatalf("reconciliation changed accounting on wrong wallet: %d %v", effects, err)
	}
	result, err := s.ReconcileAccountingWithUnpricedV1(ctx, unpriced.ChainID, unpriced.BackendID, testRewardWallet(unpriced))
	if err != nil || result.Applied != 1 {
		t.Fatalf("legacy reconciliation: %+v %v", result, err)
	}
	var legacyEffects, unpricedEffects, unpricedIntents int
	if err = s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1), (SELECT count(*) FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$2), (SELECT count(*) FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$2)`, legacy.CandidateID[:], unpriced.CandidateID[:]).Scan(&legacyEffects, &unpricedEffects, &unpricedIntents); err != nil {
		t.Fatal(err)
	}
	if legacyEffects != 1 || unpricedEffects != 0 || unpricedIntents != 0 {
		t.Fatalf("unexpected accounting: legacy effects=%d unpriced effects=%d intents=%d", legacyEffects, unpricedEffects, unpricedIntents)
	}
	result, err = s.ReconcileAccountingWithUnpricedV1(ctx, unpriced.ChainID, unpriced.BackendID, testRewardWallet(unpriced))
	if err != nil || result.Applied != 0 {
		t.Fatalf("replay: %+v %v", result, err)
	}
	if _, err = s.ReconcileAcceptedAccountingV2(ctx); err == nil {
		t.Fatal("legacy reconciler silently accepted unpriced receipt")
	}
}

func TestPostgresCombinedReconcileFailsClosedOnWorkTamper(t *testing.T) {
	s, ctx, _, unpriced := acceptedUnpricedFixture(t)
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 DISABLE TRIGGER immutable_discovery; UPDATE pool_share_journal_v1.unpriced_discoveries_v1 SET work_digest=decode(repeat('00',32),'hex'); ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 ENABLE TRIGGER immutable_discovery`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReconcileAccountingWithUnpricedV1(ctx, unpriced.ChainID, unpriced.BackendID, testRewardWallet(unpriced)); err == nil {
		t.Fatal("tampered work was accepted")
	}
	var effects int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_effects_v1`).Scan(&effects); err != nil || effects != 0 {
		t.Fatalf("legacy accounting applied after tamper: %d %v", effects, err)
	}
}
