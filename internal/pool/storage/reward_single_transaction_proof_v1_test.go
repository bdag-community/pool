package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestSingleTransactionRewardProofAttributesFees(t *testing.T) {
	key, err := crypto.HexToECDSA(strings.Repeat("0", 63) + "1") // public, unfunded test key
	if err != nil {
		t.Fatal(err)
	}
	signer := types.LatestSignerForChainID(big.NewInt(1404))
	to := common.HexToAddress("0x7777777777777777777777777777777777777777")
	tx, err := types.SignTx(types.NewTx(&types.LegacyTx{Nonce: 1, GasPrice: big.NewInt(10), Gas: 30000, To: &to, Value: big.NewInt(200)}), signer, key)
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	for _, self := range []bool{false, true} {
		t.Run(map[bool]string{false: "other_sender", true: "pool_sender"}[self], func(t *testing.T) {
			wallet := common.HexToAddress("0x2222222222222222222222222222222222222222")
			if self {
				wallet = sender
			}
			const before = uint64(10_000_000_000)
			const subsidy = uint64(1_000_000_000)
			const gas = uint64(21000)
			const tip = uint64(147000)
			after := before + subsidy + tip
			other, own := "147000", "0"
			if self {
				after -= gas*10 + 200
				other, own = "0", "147000"
			}
			beforeRoot, beforeProof := testWalletProof(t, wallet, before)
			afterRoot, afterProof := testWalletProof(t, wallet, after)
			candidate := FrozenCandidateV1{CandidateID: [32]byte{1}}
			obs := RewardObservationV1{NativeBlockHash: common.HexToHash("0x22"), NativeOrder: 5, NativeHeight: 10, EVMBlockHash: common.HexToHash("0x101"), EVMBlockNumber: 9, EVMStateRoot: afterRoot, Wallet: wallet, SubsidyWei: "1000000000", OtherFeesWei: other, SelfPaidFeesWei: own}
			previous := rewardNativeStateV1{Hash: common.HexToHash("0x10"), Order: 4, EVMRoot: beforeRoot, EVMHeight: 8, EVMHead: common.HexToHash("0x100")}
			current := rewardNativeStateV1{Hash: obs.NativeBlockHash, Order: 5, Height: obs.NativeHeight, Valid: true, EVMRoot: afterRoot, EVMHeight: 9, EVMHead: obs.EVMBlockHash}
			parent := map[string]interface{}{"hash": previous.EVMHead, "stateRoot": beforeRoot, "number": "0x8", "transactions": []common.Hash{}}
			block := map[string]interface{}{"hash": current.EVMHead, "parentHash": previous.EVMHead, "stateRoot": afterRoot, "miner": wallet, "number": "0x9", "transactions": []common.Hash{tx.Hash()}, "baseFeePerGas": "0x3", "gasUsed": hexutil.EncodeUint64(gas)}
			receipt := map[string]interface{}{"blockHash": current.EVMHead, "transactionHash": tx.Hash(), "transactionIndex": "0x0", "gasUsed": hexutil.EncodeUint64(gas), "cumulativeGasUsed": hexutil.EncodeUint64(gas), "effectiveGasPrice": "0xa", "status": "0x1"}
			native := reorderedRPCStub(func(method string, args ...interface{}) (interface{}, error) {
				if method != "getStateRoot" {
					return nil, errors.New("unexpected native RPC")
				}
				if args[0].(int64) == 4 {
					return previous, nil
				}
				return current, nil
			})
			evm := reorderedRPCStub(func(method string, args ...interface{}) (interface{}, error) {
				switch method {
				case "eth_getBlockByNumber":
					if args[0].(string) == "0x8" {
						return parent, nil
					}
					return block, nil
				case "eth_getTransactionByHash":
					return tx, nil
				case "eth_getTransactionReceipt":
					return receipt, nil
				}
				return nil, errors.New("unexpected EVM RPC")
			})
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			proof := reorderedRewardProofV1{Version: 1, CandidateID: hex.EncodeToString(candidate.CandidateID[:]), NativeOrder: 5, Proofs: [][]string{beforeProof, afterProof}}
			encoded, err := json.Marshal(proof)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, proof.CandidateID+".json"), encoded, 0600); err != nil {
				t.Fatal(err)
			}
			verifier := RPCMatureRewardVerifierV1{Native: native, EVM: evm, RecoveryDirectory: dir}
			if got, err := verifier.verifySingleTransactionRewardV1(context.Background(), candidate, obs); err != nil || got != obs.EVMBlockHash {
				t.Fatalf("valid transaction reward rejected: %v", err)
			}
			bad := obs
			bad.OtherFeesWei = "0"
			bad.SelfPaidFeesWei = "0"
			if _, err := verifier.verifySingleTransactionRewardV1(context.Background(), candidate, bad); err == nil {
				t.Fatal("incorrect fee attribution accepted")
			}
			receipt["effectiveGasPrice"] = "0xb"
			if _, err := verifier.verifySingleTransactionRewardV1(context.Background(), candidate, obs); err == nil {
				t.Fatal("receipt price inconsistent with signed transaction accepted")
			}
		})
	}
}
