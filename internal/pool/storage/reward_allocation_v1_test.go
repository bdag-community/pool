package storage

import (
	"context"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"asicPool/internal/pool/payments"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgconn"
)

type testMatureRewardVerifier struct{ allowed bool }

type testScopedRewardGuard struct{ guard *RewardCreditGuardV1 }

func (g testScopedRewardGuard) VerifyScopedCredit(ctx context.Context, wallet payments.WalletID, credit payments.Credit) error {
	return g.guard.Verify(ctx, credit.ID, wallet.Sender)
}

func (v testMatureRewardVerifier) VerifyMatureRewardV1(context.Context, FrozenCandidateV1, RewardObservationV1) error {
	if !v.allowed {
		return errors.New("not mature")
	}
	return nil
}

func TestRewardClaimMigrationBackfillsAllocations(t *testing.T) {
	s, ctx, _, candidate := acceptedUnpricedFixture(t)
	for _, name := range []string{"20261117_reward_observations_v1.sql", "20261117_reward_credits_v1.sql"} {
		data, err := os.ReadFile(filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.pool.Exec(ctx, string(data)); err != nil {
			t.Fatal(err)
		}
	}
	wallet := testRewardWallet(candidate)
	obs := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "0", SelfPaidFeesWei: "0"}
	if _, err := s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, obs); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO pool_reward_observations_v1.allocations(candidate_id,wallet,evidence_digest,work_digest,gross_wei,fee_wei,distributable_wei,total_work,line_count) SELECT candidate_id,wallet,evidence_digest,decode(repeat('01',32),'hex'),700,0,700,1,0 FROM pool_reward_observations_v1.observations WHERE candidate_id=$1`, candidate.CandidateID[:]); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20261118_reward_evm_claims_v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, string(data)); err != nil {
		t.Fatal(err)
	}
	var claimed []byte
	if err = s.pool.QueryRow(ctx, `SELECT evm_block_hash FROM pool_reward_observations_v1.reward_evm_claims WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&claimed); err != nil || common.BytesToHash(claimed) != obs.EVMBlockHash {
		t.Fatalf("backfilled claim differs: %x %v", claimed, err)
	}
}

func TestPostgresRewardAllocationCreatesScopedCreditsOnce(t *testing.T) {
	s, ctx, _, candidate := acceptedUnpricedFixture(t)
	for _, name := range []string{"20261117_reward_observations_v1.sql", "20261117_reward_credits_v1.sql", "20261118_reward_evm_claims_v1.sql", "20261119_operator_default_v1.sql"} {
		path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.pool.Exec(ctx, string(data)); err != nil {
			t.Fatal(err)
		}
	}
	wallet := testRewardWallet(candidate)
	observation := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "5", SelfPaidFeesWei: "2"}
	if _, err := s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, observation); err != nil {
		t.Fatal(err)
	}
	// An unrelated, proven duplicate remains held without blocking this
	// candidate's original allocation or the wallet-scoped payout guard.
	installUnpricedHoldFixture(t, s, ctx)
	s.SetBestEffortAccounting(true)
	changedObservation := observation
	changedObservation.OtherFeesWei = "6"
	if _, err := s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, changedObservation); err == nil {
		t.Fatal("changed observation did not preserve a conflict")
	}
	if _, found, err := s.NextMatureRewardV1(ctx, candidate.Height.Uint64()-1); err != nil || found {
		t.Fatalf("immature discovery selected: %t %v", found, err)
	}
	if id, found, err := s.NextMatureRewardV1(ctx, candidate.Height.Uint64()); err != nil || !found || id != candidate.CandidateID {
		t.Fatalf("mature discovery unavailable: %x %t %v", id, found, err)
	}
	if _, found, err := s.NextMatureRewardExcludingV1(ctx, candidate.Height.Uint64(), [][32]byte{candidate.CandidateID}); err != nil || found {
		t.Fatalf("temporarily held discovery selected: %t %v", found, err)
	}
	legacyHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := s.pool.Exec(ctx, `INSERT INTO public.blocks(hash,height,reward,fees,status) VALUES($1,1,100,0,'MATURE')`, legacyHash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO public.credits(block_hash,miner_address,amount,is_paid) VALUES($1,'0x3333333333333333333333333333333333333333',100,FALSE)`, legacyHash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), candidate.BackendID, wallet, testMatureRewardVerifier{}); err == nil {
		t.Fatal("immature reward allocated")
	}
	created, err := s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), candidate.BackendID, wallet, testMatureRewardVerifier{true})
	if err != nil || !created {
		t.Fatalf("allocation created=%t err=%v", created, err)
	}
	var claimed []byte
	if err = s.pool.QueryRow(ctx, `SELECT evm_block_hash FROM pool_reward_observations_v1.reward_evm_claims WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&claimed); err != nil || common.BytesToHash(claimed) != observation.EVMBlockHash {
		t.Fatalf("reward claim missing or incorrect: %x %v", claimed, err)
	}
	otherCandidate := [32]byte{99}
	if _, err = s.pool.Exec(ctx, `INSERT INTO pool_reward_observations_v1.reward_evm_claims(evm_block_hash,candidate_id) VALUES($1,$2)`, observation.EVMBlockHash[:], otherCandidate[:]); err == nil {
		t.Fatal("duplicate EVM reward claim accepted")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("duplicate claim did not hit unique constraint: %v", err)
		}
	}
	if _, err = s.pool.Exec(ctx, `UPDATE pool_reward_observations_v1.reward_evm_claims SET evm_block_hash=decode(repeat('01',32),'hex') WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("immutable EVM claim was modified")
	}
	if _, found, err := s.NextMatureRewardV1(ctx, candidate.Height.Uint64()); err != nil || found {
		t.Fatalf("allocated discovery still pending: %t %v", found, err)
	}
	var amount, bound string
	var count int
	if err = s.pool.QueryRow(ctx, `SELECT count(*),min(c.amount::text),min(x.amount_wei::text) FROM pool_reward_observations_v1.credit_bindings x JOIN public.credits c ON c.id=x.credit_id WHERE x.candidate_id=$1 AND x.wallet=$2`, candidate.CandidateID[:], wallet[:]).Scan(&count, &amount, &bound); err != nil {
		t.Fatal(err)
	}
	if count != 1 || amount != "705" || bound != "705" {
		t.Fatalf("incorrect credits count=%d amount=%s bound=%s", count, amount, bound)
	}
	created, err = s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), candidate.BackendID, wallet, testMatureRewardVerifier{true})
	if err != nil || created {
		t.Fatalf("allocation replay created=%t err=%v", created, err)
	}
	fingerprint, err := payments.ApplyMigration(ctx, s.pool)
	if err != nil {
		t.Fatal(err)
	}
	baseJournal, err := payments.NewPostgresJournal(s.pool, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewRewardCreditGuardV1(s.pool, candidate.BackendID, testMatureRewardVerifier{true}, true)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := payments.NewWalletScopedJournal(baseJournal, testScopedRewardGuard{guard}, true)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := journal.Acquire(ctx, payments.WalletID{ChainID: 1404, Sender: wallet})
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close(context.Background())
	credit, err := fence.NextCredit(ctx, candidate.Height.Uint64())
	if err != nil || credit == nil || credit.Amount.String() != "705" || credit.Recipient != common.HexToAddress("0x2222222222222222222222222222222222222222") {
		t.Fatalf("payer selected wrong credit: %+v %v", credit, err)
	}
	if err = fence.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE public.credits SET amount=amount+1 WHERE id=(SELECT credit_id FROM pool_reward_observations_v1.credit_bindings LIMIT 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), candidate.BackendID, wallet, testMatureRewardVerifier{true}); err == nil {
		t.Fatal("modified credit was accepted")
	}
}
