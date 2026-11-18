package storage

import (
	"errors"
	"math/big"
)

// LiquidRewardArithmeticV1 only calculates exact integer arithmetic for inputs
// already proven against a canonical block and its execution state. It does
// not obtain or authenticate the effective split, subsidy, or transaction fee
// proceeds, and it is never sufficient by itself to authorize a payout.
// SubsidyAtoms uses the native 1e-8 BDAG unit; netFeeWei must be the exact fee
// proceeds actually credited to this reward wallet, not total gas paid.
func LiquidRewardArithmeticV1(subsidyAtoms uint64, effectiveMinerPercent uint64, netFeeWei *big.Int) (*big.Int, error) {
	if effectiveMinerPercent > 100 || netFeeWei == nil || netFeeWei.Sign() < 0 {
		return nil, errors.New("invalid liquid reward arithmetic inputs")
	}
	subsidy := new(big.Int).Mul(new(big.Int).SetUint64(subsidyAtoms), big.NewInt(10_000_000_000))
	liquid := new(big.Int).Mul(subsidy, new(big.Int).SetUint64(effectiveMinerPercent))
	liquid.Quo(liquid, big.NewInt(100))
	liquid.Add(liquid, netFeeWei)
	if len(liquid.String()) > 30 {
		return nil, errors.New("liquid reward exceeds accounting wei range")
	}
	return liquid, nil
}
