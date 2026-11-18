package storage

import (
	"context"
	"errors"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

// verifyProvenRewardV1 supports zero or one EVM transaction. It returns the
// current EVM reward identity only after local chain and Merkle proof checks.
func (v RPCMatureRewardVerifierV1) verifyProvenRewardV1(ctx context.Context, candidate FrozenCandidateV1, obs RewardObservationV1) (common.Hash, error) {
	var block *rewardProofBlockV1
	if err := v.EVM.CallContext(ctx, &block, "eth_getBlockByNumber", hexutil.EncodeUint64(obs.EVMBlockNumber), false); err != nil || block == nil || block.Transactions == nil {
		return common.Hash{}, errors.New("reward transaction set unavailable")
	}
	switch len(block.Transactions) {
	case 0:
		return v.verifyReorderedRewardV1(ctx, candidate, obs)
	case 1:
		return v.verifySingleTransactionRewardV1(ctx, candidate, obs)
	default:
		return common.Hash{}, errors.New("multiple transactions require full replay")
	}
}

func (v RPCMatureRewardVerifierV1) verifySingleTransactionRewardV1(ctx context.Context, candidate FrozenCandidateV1, obs RewardObservationV1) (common.Hash, error) {
	proof, err := readReorderedProofV1(v.RecoveryDirectory, candidate.CandidateID)
	if err != nil {
		return common.Hash{}, err
	}
	var previous, current rewardNativeStateV1
	if err = v.Native.CallContext(ctx, &previous, "getStateRoot", int64(proof.NativeOrder-1), true); err != nil {
		return common.Hash{}, err
	}
	if err = v.Native.CallContext(ctx, &current, "getStateRoot", int64(proof.NativeOrder), true); err != nil {
		return common.Hash{}, err
	}
	if current.Hash != obs.NativeBlockHash || current.Order != proof.NativeOrder || !current.Valid || current.Height != obs.NativeHeight || previous.Order+1 != current.Order || previous.EVMHeight == math.MaxUint64 || previous.EVMHeight+1 != current.EVMHeight || previous.EVMHead == (common.Hash{}) || current.EVMHead == (common.Hash{}) || previous.EVMRoot == (common.Hash{}) || current.EVMRoot == (common.Hash{}) {
		return common.Hash{}, errors.New("native reward identity changed")
	}
	var parent, block *struct {
		rewardProofBlockV1
		BaseFee *hexutil.Big   `json:"baseFeePerGas"`
		GasUsed hexutil.Uint64 `json:"gasUsed"`
	}
	if err = v.EVM.CallContext(ctx, &parent, "eth_getBlockByNumber", hexutil.EncodeUint64(previous.EVMHeight), false); err != nil {
		return common.Hash{}, err
	}
	if err = v.EVM.CallContext(ctx, &block, "eth_getBlockByNumber", hexutil.EncodeUint64(current.EVMHeight), false); err != nil {
		return common.Hash{}, err
	}
	if parent == nil || block == nil || parent.Hash != previous.EVMHead || parent.StateRoot != previous.EVMRoot || uint64(parent.Number) != previous.EVMHeight || block.Hash != current.EVMHead || block.StateRoot != current.EVMRoot || uint64(block.Number) != current.EVMHeight || block.ParentHash != parent.Hash || block.Miner != obs.Wallet || len(block.Transactions) != 1 {
		return common.Hash{}, errors.New("canonical single-transaction reward identity differs")
	}
	var transaction *types.Transaction
	if err = v.EVM.CallContext(ctx, &transaction, "eth_getTransactionByHash", block.Transactions[0]); err != nil || transaction == nil || transaction.Hash() != block.Transactions[0] {
		return common.Hash{}, errors.New("reward transaction unavailable")
	}
	sender, err := types.Sender(types.LatestSignerForChainID(big.NewInt(1404)), transaction)
	if err != nil {
		return common.Hash{}, errors.New("reward transaction sender invalid")
	}
	var receipt *struct {
		BlockHash         common.Hash    `json:"blockHash"`
		TransactionHash   common.Hash    `json:"transactionHash"`
		TransactionIndex  hexutil.Uint64 `json:"transactionIndex"`
		GasUsed           hexutil.Uint64 `json:"gasUsed"`
		CumulativeGasUsed hexutil.Uint64 `json:"cumulativeGasUsed"`
		EffectiveGasPrice *hexutil.Big   `json:"effectiveGasPrice"`
		Status            hexutil.Uint64 `json:"status"`
	}
	if err = v.EVM.CallContext(ctx, &receipt, "eth_getTransactionReceipt", transaction.Hash()); err != nil || receipt == nil || receipt.BlockHash != block.Hash || receipt.TransactionHash != transaction.Hash() || receipt.TransactionIndex != 0 || receipt.GasUsed == 0 || receipt.GasUsed != receipt.CumulativeGasUsed || receipt.GasUsed != block.GasUsed || uint64(receipt.GasUsed) > transaction.Gas() || receipt.EffectiveGasPrice == nil || uint64(receipt.Status) > 1 {
		return common.Hash{}, errors.New("reward transaction receipt unavailable")
	}
	price := new(big.Int).Set((*big.Int)(receipt.EffectiveGasPrice))
	base := new(big.Int)
	if block.BaseFee != nil {
		base.Set((*big.Int)(block.BaseFee))
	}
	if price.Sign() < 0 || base.Sign() < 0 || price.Cmp(base) < 0 {
		return common.Hash{}, errors.New("reward transaction gas price invalid")
	}
	perGasTip, err := transaction.EffectiveGasTip(base)
	if err != nil || perGasTip.Sign() < 0 || new(big.Int).Add(base, perGasTip).Cmp(price) != 0 {
		return common.Hash{}, errors.New("reward transaction fee differs from signed terms")
	}
	gas := new(big.Int).SetUint64(uint64(receipt.GasUsed))
	tip := new(big.Int).Mul(gas, perGasTip)
	other, self := new(big.Int), new(big.Int)
	if sender == obs.Wallet {
		self.Set(tip)
	} else {
		other.Set(tip)
	}
	if other.String() != obs.OtherFeesWei || self.String() != obs.SelfPaidFeesWei {
		return common.Hash{}, errors.New("reward transaction fee attribution differs")
	}
	before, err := provenAccountBalanceV1(parent.StateRoot, obs.Wallet, proof.Proofs[0])
	if err != nil {
		return common.Hash{}, err
	}
	after, err := provenAccountBalanceV1(block.StateRoot, obs.Wallet, proof.Proofs[1])
	if err != nil {
		return common.Hash{}, err
	}
	subsidy, ok := new(big.Int).SetString(obs.SubsidyWei, 10)
	if !ok || subsidy.Sign() <= 0 {
		return common.Hash{}, errors.New("reward subsidy invalid")
	}
	expected := new(big.Int).Add(subsidy, tip)
	if sender == obs.Wallet {
		expected.Sub(expected, new(big.Int).Mul(gas, price))
		if receipt.Status == 1 {
			expected.Sub(expected, transaction.Value())
		}
	}
	if new(big.Int).Sub(after, before).Cmp(expected) != 0 {
		return common.Hash{}, errors.New("wallet change differs from reward and transaction")
	}
	var final rewardNativeStateV1
	if err = v.Native.CallContext(ctx, &final, "getStateRoot", int64(proof.NativeOrder), true); err != nil || final != current {
		return common.Hash{}, errors.New("native reward changed during proof verification")
	}
	return current.EVMHead, nil
}
