package storage

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestProvenUnobservedRewardRecording(t *testing.T) {
	s, ctx, _, candidate := acceptedUnpricedFixture(t)
	for _, name := range []string{"20261117_reward_observations_v1.sql", "20261117_reward_credits_v1.sql", "20261118_reward_evm_claims_v1.sql"} {
		data, err := os.ReadFile(filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.pool.Exec(ctx, string(data)); err != nil {
			t.Fatal(err)
		}
	}
	wallet := testRewardWallet(candidate)
	beforeRoot, beforeProof := testWalletProof(t, wallet, 1000)
	afterRoot, afterProof := testWalletProof(t, wallet, 700000001000)
	obs := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 5, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x101"), EVMBlockNumber: 9, EVMStateRoot: afterRoot, Wallet: wallet, SubsidyWei: "700000000000", OtherFeesWei: "0", SelfPaidFeesWei: "0"}
	previous := rewardNativeStateV1{Hash: common.HexToHash("0x10"), Order: 4, EVMRoot: beforeRoot, EVMHeight: 8, EVMHead: common.HexToHash("0x100")}
	current := rewardNativeStateV1{Hash: obs.NativeBlockHash, Order: 5, Height: obs.NativeHeight, Valid: true, EVMRoot: afterRoot, EVMHeight: 9, EVMHead: obs.EVMBlockHash}
	parent := &rewardProofBlockV1{Hash: previous.EVMHead, StateRoot: beforeRoot, Number: 8}
	block := &rewardProofBlockV1{Hash: current.EVMHead, ParentHash: parent.Hash, StateRoot: afterRoot, Miner: wallet, Number: 9, Transactions: []common.Hash{}, Extra: []byte{0, 0, 0, 0, 0, 0, 0, 100}, Time: 12}
	native := reorderedRPCStub(func(method string, args ...interface{}) (interface{}, error) {
		switch method {
		case "getMainChainHeight":
			return strconv.FormatUint(obs.NativeHeight+4096, 10), nil
		case "getBlockhash":
			return obs.NativeBlockHash.Hex(), nil
		case "getBlockHeader":
			return map[string]interface{}{"hash": obs.NativeBlockHash, "reward": 100, "time": 12}, nil
		case "getStateRoot":
			if args[0].(int64) == 4 {
				return previous, nil
			}
			return current, nil
		}
		return nil, errors.New("unexpected native RPC")
	})
	evm := reorderedRPCStub(func(method string, args ...interface{}) (interface{}, error) {
		switch method {
		case "eth_chainId":
			return "0x57c", nil
		case "eth_syncing":
			return false, nil
		case "eth_getBlockByNumber":
			if args[0].(string) == "0x8" {
				return parent, nil
			}
			return block, nil
		}
		return nil, errors.New("unexpected EVM RPC")
	})
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	proof := reorderedRewardProofV1{Version: 1, CandidateID: hex.EncodeToString(candidate.CandidateID[:]), NativeOrder: 5, Proofs: [][]string{beforeProof, afterProof}}
	data, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, proof.CandidateID+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	verifier := RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: 4096, RecoveryDirectory: dir}
	bad := obs
	bad.SubsidyWei = "700000000001"
	if _, err = s.RecordProvenUnobservedRewardV1(ctx, candidate.CandidateID, wallet, bad, verifier); err == nil {
		t.Fatal("unproven balance change recorded")
	}
	ready, err := s.ValidateProvenUnobservedRewardV1(ctx, candidate.CandidateID, wallet, obs, verifier)
	if err != nil || !ready {
		t.Fatalf("valid dry run: ready=%v error=%v", ready, err)
	}
	var count int
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM pool_reward_observations_v1.observations`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("dry run persisted an observation: count=%d error=%v", count, err)
	}
	created, err := s.RecordProvenUnobservedRewardV1(ctx, candidate.CandidateID, wallet, obs, verifier)
	if err != nil || !created {
		t.Fatalf("valid proven observation: created=%v error=%v", created, err)
	}
	created, err = s.RecordProvenUnobservedRewardV1(ctx, candidate.CandidateID, wallet, obs, verifier)
	if err != nil || created {
		t.Fatalf("observation replay: created=%v error=%v", created, err)
	}
	if _, err = s.AllocateObservedRewardV1(ctx, candidate.CandidateID, big.NewInt(1404), candidate.BackendID, wallet, verifier); err != nil {
		t.Fatalf("proven observation could not be allocated: %v", err)
	}
}
