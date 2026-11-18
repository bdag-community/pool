package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

type reorderedRPCStub func(string, ...interface{}) (interface{}, error)

func (f reorderedRPCStub) CallContext(_ context.Context, out interface{}, method string, args ...interface{}) error {
	value, err := f(method, args...)
	if err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}
func testWalletProof(t *testing.T, address common.Address, amount uint64) (common.Hash, []string) {
	t.Helper()
	db := triedb.NewDatabase(rawdb.NewMemoryDatabase(), triedb.HashDefaults)
	tree := trie.NewEmpty(db)
	account := types.NewEmptyStateAccount()
	account.Balance.SetUint64(amount)
	encoded, err := rlp.EncodeToBytes(account)
	if err != nil {
		t.Fatal(err)
	}
	key := crypto.Keccak256(address.Bytes())
	if err = tree.Update(key, encoded); err != nil {
		t.Fatal(err)
	}
	proof := memorydb.New()
	if err = tree.Prove(key, proof); err != nil {
		t.Fatal(err)
	}
	it := proof.NewIterator(nil, nil)
	defer it.Release()
	var nodes []string
	for it.Next() {
		nodes = append(nodes, hexutil.Encode(it.Value()))
	}
	if err = it.Error(); err != nil {
		t.Fatal(err)
	}
	return tree.Hash(), nodes
}
func TestReorderedRewardRequiresLocalIdentityAndAccountProofs(t *testing.T) {
	candidate := storageTestCandidate()
	wallet := testRewardWallet(candidate)
	beforeRoot, beforeProof := testWalletProof(t, wallet, 1000)
	afterRoot, afterProof := testWalletProof(t, wallet, 1700)
	old := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "0", SelfPaidFeesWei: "0"}
	prev := rewardNativeStateV1{Hash: common.HexToHash("0x10"), Order: 4, EVMRoot: beforeRoot, EVMHeight: 8, EVMHead: common.HexToHash("0x100")}
	current := rewardNativeStateV1{Hash: old.NativeBlockHash, Order: 5, Height: old.NativeHeight, Valid: true, EVMRoot: afterRoot, EVMHeight: 9, EVMHead: common.HexToHash("0x101")}
	parent := &rewardProofBlockV1{Hash: prev.EVMHead, StateRoot: beforeRoot, Number: 8}
	block := &rewardProofBlockV1{Hash: current.EVMHead, ParentHash: parent.Hash, StateRoot: afterRoot, Miner: wallet, Number: 9, Transactions: []common.Hash{}}
	native := reorderedRPCStub(func(method string, args ...interface{}) (interface{}, error) {
		switch method {
		case "getMainChainHeight":
			return "4102", nil
		case "getBlockhash":
			return common.HexToHash("0x77").Hex(), nil
		case "getStateRoot":
			if args[0].(int64) == 4 {
				return prev, nil
			}
			return current, nil
		}
		return nil, errors.New("unexpected native method")
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
		return nil, errors.New("unexpected EVM method")
	})
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	proof := reorderedRewardProofV1{Version: 1, CandidateID: hex.EncodeToString(candidate.CandidateID[:]), NativeOrder: 5, Proofs: [][]string{beforeProof, afterProof}}
	path := filepath.Join(dir, proof.CandidateID+".json")
	write := func() {
		t.Helper()
		data, e := json.Marshal(proof)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, data, 0600); e != nil {
			t.Fatal(e)
		}
	}
	write()
	verifier := RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: 4096, RecoveryDirectory: dir}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := verifier.verifyReorderedRewardV1(ctx, candidate, old); err != nil {
		info, statErr := os.Stat(dir)
		resolved, resolveErr := filepath.EvalSymlinks(dir)
		t.Fatalf("valid proof detail: %v, directory=%q resolved=%q info=%v stat=%v resolve=%v euid=%d", err, dir, resolved, info, statErr, resolveErr, os.Geteuid())
	}
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, old); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}
	block.Transactions = []common.Hash{common.HexToHash("0x99")}
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, old); !errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("transaction-bearing reward accepted: %v", err)
	}
	block.Transactions = []common.Hash{}
	old.SubsidyWei = "701"
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, old); !errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("wrong amount accepted: %v", err)
	}
	old.SubsidyWei = "700"
	proof.Proofs[1][0] = "0x01"
	write()
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, old); !errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("tampered proof accepted: %v", err)
	}
}
