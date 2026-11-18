package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestPostgresRewardObservationIsDurableAndConflictsHold(t *testing.T) {
	s, ctx, _, candidate := acceptedUnpricedFixture(t)
	path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20261117_reward_observations_v1.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, string(data)); err != nil {
		t.Fatal(err)
	}
	wallet := testRewardWallet(candidate)
	observation := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "5", SelfPaidFeesWei: "2"}
	pending, err := s.NextRewardObservationCandidateV1(ctx)
	if err != nil || pending == nil || pending.CandidateID != candidate.CandidateID {
		t.Fatalf("accepted discovery was not scheduled: %+v %v", pending, err)
	}
	if skipped, err := s.NextRewardObservationCandidateExcludingV1(ctx, [][32]byte{candidate.CandidateID}); err != nil || skipped != nil {
		t.Fatalf("temporarily excluded observation selected: %+v %v", skipped, err)
	}
	created, err := s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, observation)
	if err != nil || !created {
		t.Fatalf("record observation created=%t err=%v", created, err)
	}
	pending, err = s.NextRewardObservationCandidateV1(ctx)
	if err != nil || pending != nil {
		t.Fatalf("recorded discovery remained pending: %+v %v", pending, err)
	}
	created, err = s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, observation)
	if err != nil || created {
		t.Fatalf("replay created=%t err=%v", created, err)
	}
	changed := observation
	changed.SubsidyWei = "701"
	if _, err = s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, changed); err == nil {
		t.Fatal("conflicting reward was accepted")
	}
	var observations, conflicts, newIntents, newEffects int
	if err = s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM pool_reward_observations_v1.observations), (SELECT count(*) FROM pool_reward_observations_v1.conflicts), (SELECT count(*) FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1), (SELECT count(*) FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1)`, candidate.CandidateID[:]).Scan(&observations, &conflicts, &newIntents, &newEffects); err != nil {
		t.Fatal(err)
	}
	if observations != 1 || conflicts != 1 || newIntents != 0 || newEffects != 0 {
		t.Fatalf("unexpected data observations=%d conflicts=%d intents=%d effects=%d", observations, conflicts, newIntents, newEffects)
	}
	if _, err = s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, observation); err == nil {
		t.Fatal("unresolved reward conflict did not hold further observations")
	}
	if _, err = s.NextRewardObservationCandidateV1(ctx); err == nil {
		t.Fatal("unresolved reward conflict did not hold collector")
	}
}
