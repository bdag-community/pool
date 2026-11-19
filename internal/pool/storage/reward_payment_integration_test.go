package storage

import (
	"context"
	"crypto/sha256"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"asicPool/internal/pool/payments"
	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/blake2b"
)

type syntheticPayoutNetwork struct {
	sent         [][]byte
	receipt      *types.Receipt
	receipts     map[common.Hash]*types.Receipt
	mined        *types.Header
	head         *types.Header
	minedNonce   uint64
	pendingNonce uint64
}

func (*syntheticPayoutNetwork) ChainID(context.Context) (*big.Int, error) {
	return big.NewInt(1404), nil
}
func (*syntheticPayoutNetwork) Balance(context.Context, common.Address) (*big.Int, error) {
	return big.NewInt(1000000), nil
}
func (*syntheticPayoutNetwork) GasPrice(context.Context) (*big.Int, error) { return big.NewInt(1), nil }
func (n *syntheticPayoutNetwork) PendingNonce(context.Context, common.Address) (uint64, error) {
	return n.pendingNonce, nil
}
func (n *syntheticPayoutNetwork) MinedNonce(context.Context, common.Address) (uint64, error) {
	return n.minedNonce, nil
}
func (n *syntheticPayoutNetwork) Receipt(_ context.Context, hash common.Hash) (*types.Receipt, error) {
	if n.receipts != nil {
		return n.receipts[hash], nil
	}
	return n.receipt, nil
}
func (n *syntheticPayoutNetwork) Header(_ context.Context, number *big.Int) (*types.Header, error) {
	if number != nil {
		return n.mined, nil
	}
	if n.head != nil {
		return n.head, nil
	}
	return &types.Header{Number: big.NewInt(100)}, nil
}
func (n *syntheticPayoutNetwork) Broadcast(_ context.Context, raw []byte) error {
	n.sent = append(n.sent, append([]byte(nil), raw...))
	return nil
}
func (n *syntheticPayoutNetwork) PendingTransactionV1(_ context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	for _, raw := range n.sent {
		var tx types.Transaction
		if tx.UnmarshalBinary(raw) == nil && tx.Hash() == hash {
			return &tx, true, nil
		}
	}
	return nil, false, nil
}

func TestSyntheticRewardToWalletScopedPayout(t *testing.T) {
	keyText := strings.Repeat("0", 63) + "1" // public test key, never funded
	key, err := crypto.HexToECDSA(keyText)
	if err != nil {
		t.Fatal(err)
	}
	wallet := crypto.PubkeyToAddress(key.PublicKey)
	s, ctx := unpricedFixture(t)
	candidate := secondUnpricedCandidate(t)
	copy(candidate.HeaderBytes[148:168], wallet[:])
	hash := blake2b.Sum256(candidate.HeaderBytes[:])
	candidate.ExpectedBlockHash = blake2b.Sum256(hash[:])
	candidate.CandidateID = candidateIDForHashV1(candidate.ExpectedBlockHash)
	candidate.FrozenPayload, err = encodeFrozenCandidateJournalV1(candidate)
	if err != nil {
		t.Fatal(err)
	}
	candidate.FrozenDigest = sha256.Sum256(candidate.FrozenPayload)
	journalShares, err := sharejournal.New(s.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = journalShares.Append(ctx, sharejournal.Share{ID: [32]byte{99}, Miner: "0x1111111111111111111111111111111111111111", Work: 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.PutUnpricedDiscoveryV1(ctx, candidate, unpricedShare(candidate), 7, sharejournal.PartialShare, 0); err != nil {
		t.Fatal(err)
	}
	leased, err := s.ClaimCandidate(ctx, candidate.CandidateID, [16]byte{4}, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim: %v", err)
	}
	prepared, err := s.PrepareAttempt(ctx, leased.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteAttempt(ctx, leased.Lease, prepared.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal); err != nil {
		t.Fatal(err)
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
	observation := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "5", SelfPaidFeesWei: "2"}
	if _, err = s.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, observation); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), candidate.BackendID, wallet, testMatureRewardVerifier{true}); err != nil {
		t.Fatal(err)
	}
	var firstAmount, secondAmount string
	if err = s.pool.QueryRow(ctx, `SELECT max(amount_wei::text) FILTER (WHERE miner_address='0x1111111111111111111111111111111111111111'), max(amount_wei::text) FILTER (WHERE miner_address='0x2222222222222222222222222222222222222222') FROM pool_reward_observations_v1.credit_bindings WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&firstAmount, &secondAmount); err != nil || firstAmount != "100" || secondAmount != "604" {
		t.Fatalf("incorrect weighted credits: first=%s second=%s err=%v", firstAmount, secondAmount, err)
	}
	fingerprint, err := payments.ApplyMigration(ctx, s.pool)
	if err != nil {
		t.Fatal(err)
	}
	base, err := payments.NewPostgresJournal(s.pool, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewRewardCreditGuardV1(s.pool, candidate.BackendID, testMatureRewardVerifier{true})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := payments.NewWalletScopedJournal(base, testScopedRewardGuard{guard})
	if err != nil {
		t.Fatal(err)
	}
	identity := payments.WalletID{ChainID: 1404, Sender: wallet}
	signer, err := payments.NewLocalSigner(identity, keyText)
	if err != nil {
		t.Fatal(err)
	}
	defer signer.Close()
	network := &syntheticPayoutNetwork{head: &types.Header{Number: big.NewInt(20)}, receipts: make(map[common.Hash]*types.Receipt)}
	engine := payments.Engine{Wallet: identity, Journal: journal, Network: network, Signer: signer, Ready: func(context.Context) error { return nil }, RequireFreshWalletHistory: true, EnablePipeline: true}
	first, err := engine.Step(ctx, candidate.Height.Uint64())
	if err != nil || len(network.sent) != 1 {
		t.Fatalf("first payout: %+v %v", first, err)
	}
	var transaction types.Transaction
	if err = transaction.UnmarshalBinary(network.sent[0]); err != nil {
		t.Fatal(err)
	}
	if transaction.To() == nil || *transaction.To() != common.HexToAddress("0x1111111111111111111111111111111111111111") || transaction.Value().String() != "100" {
		t.Fatal("signed payout recipient or amount differs")
	}
	network.pendingNonce = 1
	if _, err = engine.Step(ctx, candidate.Height.Uint64()); err != nil || len(network.sent) != 1 {
		t.Fatalf("unmined payout allowed another send: %v", err)
	}
	network.mined = &types.Header{Number: big.NewInt(20)}
	network.receipts[first.Hash] = &types.Receipt{TxHash: first.Hash, BlockNumber: big.NewInt(20), BlockHash: network.mined.Hash(), Status: types.ReceiptStatusSuccessful}
	network.minedNonce, network.pendingNonce = 1, 1
	second, err := engine.Step(ctx, candidate.Height.Uint64())
	if err != nil || len(network.sent) != 2 || second.Hash == first.Hash {
		t.Fatalf("included payout did not permit next credit: %+v %v", second, err)
	}
	var secondTx types.Transaction
	if err = secondTx.UnmarshalBinary(network.sent[1]); err != nil || secondTx.To() == nil || *secondTx.To() != common.HexToAddress("0x2222222222222222222222222222222222222222") || secondTx.Value().String() != "604" {
		t.Fatal("second weighted payout recipient or amount differs")
	}
	network.receipts[second.Hash] = &types.Receipt{TxHash: second.Hash, BlockNumber: big.NewInt(20), BlockHash: network.mined.Hash(), Status: types.ReceiptStatusSuccessful}
	network.minedNonce, network.pendingNonce = 2, 2
	network.head = &types.Header{Number: big.NewInt(31)}
	for _, expected := range []int64{first.CreditID, second.CreditID} {
		confirmed, err := engine.Step(ctx, candidate.Height.Uint64())
		if err != nil || !confirmed.NewlyConfirmed || confirmed.CreditID != expected {
			t.Fatalf("payout confirmation: %+v %v", confirmed, err)
		}
	}
	var paid bool
	if err = s.pool.QueryRow(ctx, `SELECT is_paid FROM public.credits WHERE id=$1`, first.CreditID).Scan(&paid); err != nil || !paid {
		t.Fatalf("credit not confirmed: %t %v", paid, err)
	}
}
