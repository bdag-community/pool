package storage

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// ErrRewardEvidenceStale means a well-formed canonical chain response no
// longer matches this observation. It is not proof that the reward is lost.
var ErrRewardEvidenceStale = errors.New("reward observation is no longer canonical")

type RewardRPCCallerV1 interface {
	CallContext(context.Context, interface{}, string, ...interface{}) error
}

// RPCMatureRewardVerifierV1 checks the exact native ordered block, EVM block,
// state root and main-chain maturity against authenticated local RPCs.
type RPCMatureRewardVerifierV1 struct {
	Native            RewardRPCCallerV1
	EVM               RewardRPCCallerV1
	Confirmations     uint64
	RecoveryDirectory string
}

func (v RPCMatureRewardVerifierV1) VerifyMatureRewardV1(ctx context.Context, candidate FrozenCandidateV1, obs RewardObservationV1) error {
	_, err := v.VerifyMatureRewardIdentityV1(ctx, candidate, obs)
	return err
}

// VerifyMatureRewardIdentityV1 returns the exact canonical EVM reward identity
// so the allocator can bind it uniquely and atomically to one candidate.
func (v RPCMatureRewardVerifierV1) VerifyMatureRewardIdentityV1(ctx context.Context, candidate FrozenCandidateV1, obs RewardObservationV1) (common.Hash, error) {
	if ctx == nil || ctx.Err() != nil || v.Native == nil || v.EVM == nil || v.Confirmations == 0 || obs.NativeOrder > math.MaxInt64 {
		return common.Hash{}, errors.New("mature reward verification unavailable")
	}
	if err := validateRewardObservationV1(candidate, obs.Wallet, obs); err != nil {
		return common.Hash{}, err
	}
	var chain hexutil.Uint64
	if err := v.EVM.CallContext(ctx, &chain, "eth_chainId"); err != nil || uint64(chain) != 1404 {
		return common.Hash{}, errors.New("reward EVM chain identity unavailable")
	}
	var syncing json.RawMessage
	if err := v.EVM.CallContext(ctx, &syncing, "eth_syncing"); err != nil || string(syncing) != "false" {
		return common.Hash{}, errors.New("reward EVM chain is syncing or unknown")
	}
	var rawHeight json.RawMessage
	if err := v.Native.CallContext(ctx, &rawHeight, "getMainChainHeight"); err != nil {
		return common.Hash{}, errors.New("reward native height unavailable")
	}
	heightText := strings.TrimSpace(string(rawHeight))
	if strings.HasPrefix(heightText, `"`) {
		if err := json.Unmarshal(rawHeight, &heightText); err != nil {
			return common.Hash{}, errors.New("reward native height invalid")
		}
	}
	height, err := strconv.ParseUint(heightText, 10, 64)
	if err != nil || strconv.FormatUint(height, 10) != heightText || height < obs.NativeHeight || height-obs.NativeHeight < v.Confirmations-1 {
		return common.Hash{}, errors.New("reward has not reached native maturity")
	}
	var ordered string
	if err = v.Native.CallContext(ctx, &ordered, "getBlockhash", int64(obs.NativeOrder)); err != nil {
		return common.Hash{}, errors.New("reward native ordered block unavailable")
	}
	orderedBytes, err := hexutil.Decode(ordered)
	if err != nil || len(orderedBytes) != 32 {
		return common.Hash{}, errors.New("reward native ordered block is malformed")
	}
	if common.BytesToHash(orderedBytes) != obs.NativeBlockHash {
		if v.RecoveryDirectory != "" {
			if identity, recoveryErr := v.verifyReorderedRewardV1(ctx, candidate, obs); recoveryErr == nil {
				return identity, nil
			}
		}
		return common.Hash{}, ErrRewardEvidenceStale
	}
	var block *struct {
		Hash      common.Hash    `json:"hash"`
		Number    hexutil.Uint64 `json:"number"`
		StateRoot common.Hash    `json:"stateRoot"`
	}
	if err = v.EVM.CallContext(ctx, &block, "eth_getBlockByNumber", hexutil.EncodeUint64(obs.EVMBlockNumber), false); err != nil || block == nil {
		return common.Hash{}, errors.New("reward EVM block unavailable")
	}
	if block.Hash == (common.Hash{}) || block.StateRoot == (common.Hash{}) || uint64(block.Number) == 0 {
		return common.Hash{}, errors.New("reward EVM block is malformed")
	}
	if block.Hash != obs.EVMBlockHash || uint64(block.Number) != obs.EVMBlockNumber || block.StateRoot != obs.EVMStateRoot {
		if v.RecoveryDirectory != "" {
			if identity, recoveryErr := v.verifyReorderedRewardV1(ctx, candidate, obs); recoveryErr == nil {
				return identity, nil
			}
		}
		return common.Hash{}, ErrRewardEvidenceStale
	}
	var orderedAfter string
	if err = v.Native.CallContext(ctx, &orderedAfter, "getBlockhash", int64(obs.NativeOrder)); err != nil || orderedAfter != ordered {
		return common.Hash{}, errors.New("reward native order changed during verification")
	}
	return obs.EVMBlockHash, nil
}
