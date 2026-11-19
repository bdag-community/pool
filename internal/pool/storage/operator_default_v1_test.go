package storage

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
)

func TestOperatorDefaultDoesNotTreatTransientOrPricedAsMissing(t *testing.T) {
	if operatorAttributionUnavailableV1(context.DeadlineExceeded) || operatorAttributionUnavailableV1(errors.New("unpriced verification failed conflicts=0 orphan_cuts=0 missing_work=0 unexpected_pricing=1")) {
		t.Fatal("transient failure or existing pricing must not become an operator default")
	}
	if !operatorAttributionUnavailableV1(fmt.Errorf("window: %w", sharejournal.ErrShareCutMissing)) {
		t.Fatal("missing share cut was not recognized")
	}
}

func TestBestEffortUnknownOwnerDefaultsToOperator(t *testing.T) {
	for _, mode := range []string{"no_attributable_work", "conflicting_or_missing_work", "missing_cut"} {
		t.Run(mode, func(t *testing.T) {
			s, ctx := unpricedFixture(t)
			candidate := storageTestCandidate()
			var share *sharejournal.Share
			if mode == "missing_cut" {
				journal, err := sharejournal.New(s.pool)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := journal.Append(ctx, sharejournal.Share{ID: [32]byte{9}, Miner: "0x2222222222222222222222222222222222222222", Work: 4}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "conflicting_or_missing_work" {
				share = unpricedShare(candidate)
			}
			original, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := s.ClaimCandidate(ctx, candidate.CandidateID, [16]byte{7}, time.Minute)
			if err != nil || lease == nil {
				t.Fatalf("claim: %v", err)
			}
			attempt, err := s.PrepareAttempt(ctx, lease.Lease, SubmitMethodV2)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.CompleteAttempt(ctx, lease.Lease, attempt.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal); err != nil {
				t.Fatal(err)
			}
			if mode == "missing_cut" {
				// Simulate a damaged restore without weakening the test catalog.
				if _, err := s.pool.Exec(ctx, `SET session_replication_role=replica; DELETE FROM pool_share_journal_v1.shares; SET session_replication_role=origin`); err != nil {
					t.Fatal(err)
				}
			}
			if share != nil {
				view, err := scanCandidateView(ctx, s.pool, candidate.CandidateID)
				if err != nil {
					t.Fatal(err)
				}
				tx, err := s.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				changed := *share
				changed.Miner = "0x3333333333333333333333333333333333333333"
				if _, _, err = recordUnpricedConflict(ctx, tx, view, candidate, "discovery_share", original.WorkDigest[:], &changed, 7, sharejournal.WholeShare, 0); err == nil {
					t.Fatal("conflict fixture absent")
				}
			}
			for _, name := range []string{"20261117_reward_observations_v1.sql", "20261117_reward_credits_v1.sql", "20261118_reward_evm_claims_v1.sql", "20261119_operator_default_v1.sql"} {
				data, err := os.ReadFile(filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", name))
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.pool.Exec(ctx, string(data)); err != nil {
					t.Fatal(err)
				}
			}
			s.SetBestEffortAccounting(true)
			if err = s.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err != nil {
				t.Fatal(err)
			}
			wallet := testRewardWallet(candidate)
			obs := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "5", SelfPaidFeesWei: "2"}
			if _, err = s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, obs); err != nil {
				t.Fatal(err)
			}
			if _, err = s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), "node", wallet, testMatureRewardVerifier{}); err == nil {
				t.Fatal("unverified reward retained")
			}
			made, err := s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), "node", wallet, testMatureRewardVerifier{true})
			if err != nil || !made {
				t.Fatalf("operator default: %v", err)
			}
			var gotWallet []byte
			var amount, reason string
			var credits, claims int
			err = s.pool.QueryRow(ctx, `SELECT wallet,gross_wei::text,reason,(SELECT count(*) FROM public.credits),(SELECT count(*) FROM pool_reward_observations_v1.reward_evm_claims) FROM pool_reward_observations_v1.operator_defaults_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&gotWallet, &amount, &reason, &credits, &claims)
			wantReason := mode
			if mode == "missing_cut" {
				wantReason = "conflicting_or_missing_work"
			}
			if err != nil || common.BytesToAddress(gotWallet) != wallet || amount != "705" || reason != wantReason || credits != 0 || claims != 1 {
				t.Fatalf("wrong operator retention: %s %s credits=%d claims=%d err=%v", amount, reason, credits, claims, err)
			}
			made, err = s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), "node", wallet, testMatureRewardVerifier{true})
			if err != nil || made {
				t.Fatalf("operator retention not idempotent: %v", err)
			}
			if _, found, err := s.NextMatureRewardV1(ctx, candidate.Height.Uint64()); err != nil || found {
				t.Fatalf("retained reward selected again: %v", err)
			}
			if _, err = s.pool.Exec(ctx, `DELETE FROM pool_reward_observations_v1.operator_defaults_v1`); err == nil {
				t.Fatal("operator retention is mutable")
			}
		})
	}
}
