package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestReorderedRewardRejectsPreviouslyAllocatedCurrentAlias(t *testing.T) {
	prior := common.HexToHash("0x1234")
	target := common.HexToHash("0x5678")
	state := rewardNativeStateV1{Hash: prior, Order: 7, Valid: true, EVMHead: common.HexToHash("0x99")}
	rpc := reorderedRPCStub(func(method string, args ...interface{}) (interface{}, error) {
		switch method {
		case "getBlock":
			return map[string]interface{}{"hash": prior.Hex(), "order": 7, "confirmations": 100}, nil
		case "getStateRoot":
			return state, nil
		default:
			return nil, errors.New("unexpected RPC")
		}
	})
	if err := verifyCurrentRewardAliasesV1(context.Background(), rpc, []common.Hash{prior}, target); err != nil {
		t.Fatalf("distinct reward rejected: %v", err)
	}
	state.EVMHead = target
	if err := verifyCurrentRewardAliasesV1(context.Background(), rpc, []common.Hash{prior}, target); !errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("reordered duplicate was accepted: %v", err)
	}
	state.Valid = false
	if err := verifyCurrentRewardAliasesV1(context.Background(), rpc, []common.Hash{prior}, target); !errors.Is(err, ErrRewardEvidenceStale) {
		t.Fatalf("unknown current block accepted: %v", err)
	}
}
