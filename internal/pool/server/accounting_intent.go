package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"

	"asicPool/internal/pool/pplns"
	"asicPool/internal/pool/storage"
)

func buildAcceptedAccountingIntentV1(candidate FrozenCandidateV1, job *JobData, snapshot map[string]uint64, totalWork uint64, poolFeePercent float64) (storage.AcceptedAccountingIntentV1, error) {
	var intent storage.AcceptedAccountingIntentV1
	if job == nil || len(job.Reward) != 8 || len(job.BlockFees) != 8 {
		return intent, errors.New("block accounting reward and fees must be exact little-endian uint64 values")
	}
	rewardAtomic := binary.LittleEndian.Uint64(job.Reward)
	blockFees := binary.LittleEndian.Uint64(job.BlockFees)
	const atomicToWei = uint64(10_000_000_000)
	if rewardAtomic > (math.MaxUint64-blockFees)/atomicToWei {
		return intent, errors.New("block accounting gross reward overflows uint64")
	}
	grossReward := rewardAtomic*atomicToWei + blockFees
	if math.IsNaN(poolFeePercent) || math.IsInf(poolFeePercent, 0) || poolFeePercent < 0 || poolFeePercent > 100 {
		return intent, fmt.Errorf("invalid pool fee percentage %.17g", poolFeePercent)
	}
	feeFloat := float64(grossReward) * (poolFeePercent / 100.0)
	if feeFloat < 0 || feeFloat >= math.Ldexp(1, 64) || feeFloat > float64(grossReward) {
		return intent, errors.New("block accounting fee is out of range")
	}
	feeAmount := uint64(feeFloat)
	distributable := grossReward - feeAmount
	credits := pplns.CalculateCredits(snapshot, totalWork, distributable)
	miners := make([]string, 0, len(snapshot))
	for miner := range snapshot {
		miners = append(miners, miner)
	}
	sort.Strings(miners)
	lines := make([]storage.AcceptedAccountingCreditLineV1, 0, len(miners))
	for _, miner := range miners {
		lines = append(lines, storage.AcceptedAccountingCreditLineV1{
			MinerAddress: miner,
			Work:         snapshot[miner],
			Amount:       credits[miner],
		})
	}
	intent = storage.AcceptedAccountingIntentV1{
		CandidateID:      candidate.CandidateID,
		LegacyBlockHash:  candidate.SharePowHash,
		Height:           candidate.Height.Uint64(),
		GrossReward:      grossReward,
		PoolFeeFloatBits: math.Float64bits(poolFeePercent),
		FeeAmount:        feeAmount,
		Distributable:    distributable,
		TotalWork:        totalWork,
		CreditLines:      lines,
	}
	return storage.SealAcceptedAccountingIntentV1(intent)
}

// buildAcceptedAccountingIntentV2 freezes all monetary values as canonical
// Wei strings.  Float64 is retained only as the frozen fee-policy bit pattern;
// its exact IEEE-754 rational value is used for the floor calculation.
func buildAcceptedAccountingIntentV2(candidate FrozenCandidateV1, job *JobData, snapshot map[string]uint64, totalWork uint64, poolFeePercent float64) (storage.AcceptedAccountingIntentV2, error) {
	var intent storage.AcceptedAccountingIntentV2
	if job == nil || len(job.Reward) != 8 || len(job.BlockFees) != 8 {
		return intent, errors.New("block accounting reward and fees must be exact little-endian uint64 values")
	}
	if math.IsNaN(poolFeePercent) || math.IsInf(poolFeePercent, 0) || poolFeePercent < 0 || poolFeePercent > 100 {
		return intent, fmt.Errorf("invalid pool fee percentage %.17g", poolFeePercent)
	}
	rewardAtoms := binary.LittleEndian.Uint64(job.Reward)
	feesWei := binary.LittleEndian.Uint64(job.BlockFees)
	if feesWei > math.MaxInt64 {
		return intent, errors.New("block accounting fees cannot be negative signed Wei")
	}
	gross := new(big.Int).Mul(new(big.Int).SetUint64(rewardAtoms), big.NewInt(10_000_000_000))
	gross.Add(gross, new(big.Int).SetUint64(feesWei))
	if len(gross.String()) > 30 {
		return intent, errors.New("block accounting gross reward exceeds 30-digit Wei limit")
	}
	ratio := new(big.Rat).SetFloat64(poolFeePercent)
	if ratio == nil {
		return intent, errors.New("invalid pool fee percentage")
	}
	fee := new(big.Int).Quo(new(big.Int).Mul(gross, ratio.Num()), new(big.Int).Mul(ratio.Denom(), big.NewInt(100)))
	distributable := new(big.Int).Sub(gross, fee)
	credits := pplns.CalculateCreditsWei(snapshot, totalWork, distributable)
	miners := make([]string, 0, len(snapshot))
	for miner := range snapshot {
		miners = append(miners, miner)
	}
	sort.Strings(miners)
	lines := make([]storage.AcceptedAccountingCreditLineV2, 0, len(miners))
	for _, miner := range miners {
		amount := credits[miner]
		if amount == nil {
			amount = new(big.Int)
		}
		lines = append(lines, storage.AcceptedAccountingCreditLineV2{MinerAddress: miner, Work: snapshot[miner], Amount: amount.String()})
	}
	intent = storage.AcceptedAccountingIntentV2{
		CandidateID: candidate.CandidateID, LegacyBlockHash: candidate.SharePowHash, Height: candidate.Height.Uint64(),
		GrossReward: gross.String(), PoolFeeFloatBits: math.Float64bits(poolFeePercent), FeeAmount: fee.String(),
		Distributable: distributable.String(), TotalWork: totalWork, CreditLines: lines,
	}
	return storage.SealAcceptedAccountingIntentV2(intent)
}
