package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type rewardRPCStub map[string]interface{}

func (s rewardRPCStub) CallContext(_ context.Context, out interface{}, method string, _ ...interface{}) error {
	value, ok := s[method]
	if !ok {
		return errors.New("missing method")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func TestMatureRewardVerifierChecksBothChains(t *testing.T) {
	candidate := storageTestCandidate()
	wallet := testRewardWallet(candidate)
	obs := RewardObservationV1{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "5", SelfPaidFeesWei: "2"}
	native := rewardRPCStub{"getMainChainHeight": "4102", "getBlockhash": obs.NativeBlockHash.Hex()}
	evm := rewardRPCStub{"eth_chainId": "0x57c", "eth_syncing": false, "eth_getBlockByNumber": map[string]interface{}{"hash": obs.EVMBlockHash.Hex(), "number": "0x8", "stateRoot": obs.EVMStateRoot.Hex()}}
	verifier := RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: 4096}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, obs); err != nil {
		t.Fatalf("valid mature reward: %v", err)
	}
	native["getMainChainHeight"] = "4101"
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, obs); err == nil {
		t.Fatal("immature reward accepted")
	}
	native["getMainChainHeight"] = "4102"
	native["getBlockhash"] = common.HexToHash("0x77").Hex()
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, obs); !errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("changed native block not held as stale: %v", err)
	}
	native["getBlockhash"] = "malformed"
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, obs); err == nil || errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("malformed native response misclassified: %v", err)
	}
	native["getBlockhash"] = obs.NativeBlockHash.Hex()
	evm["eth_getBlockByNumber"] = map[string]interface{}{"hash": common.HexToHash("0x77").Hex(), "number": "0x8", "stateRoot": obs.EVMStateRoot.Hex()}
	if err := verifier.VerifyMatureRewardV1(ctx, candidate, obs); !errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("changed EVM block not held as stale: %v", err)
	}
}
