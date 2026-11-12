package server

import (
	"encoding/binary"
	"math"
	"math/big"
	"testing"

	"asicPool/internal/pool/storage"
)

func TestBuildAcceptedAccountingIntentFreezesSortedFinalCredits(t *testing.T) {
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, activeTestChainID())
	if err != nil {
		t.Fatal(err)
	}
	intent, err := buildAcceptedAccountingIntentV1(candidate, job, map[string]uint64{"miner-b": 3, "miner-a": 1}, 4, 2.5)
	if err != nil {
		t.Fatal(err)
	}
	wantGross := uint64(5*10_000_000_000 + 7)
	wantFee := uint64(float64(wantGross) * 0.025)
	if intent.GrossReward != wantGross || intent.FeeAmount != wantFee || intent.Distributable != wantGross-wantFee || intent.PoolFeeFloatBits != math.Float64bits(2.5) {
		t.Fatalf("frozen reward fields=%+v", intent)
	}
	if len(intent.CreditLines) != 2 || intent.CreditLines[0].MinerAddress != "miner-a" || intent.CreditLines[1].MinerAddress != "miner-b" {
		t.Fatalf("credit lines not canonical: %+v", intent.CreditLines)
	}
	if intent.CreditLines[0].Amount != uint64(float64(intent.Distributable)*0.25) || intent.CreditLines[1].Amount != uint64(float64(intent.Distributable)*0.75) {
		t.Fatalf("final credits differ: %+v", intent.CreditLines)
	}
	replayed, err := buildAcceptedAccountingIntentV1(candidate, job, map[string]uint64{"miner-a": 1, "miner-b": 3}, 4, 2.5)
	if err != nil || replayed.IntentDigest != intent.IntentDigest {
		t.Fatalf("equal replay digest changed: %x/%x err=%v", intent.IntentDigest, replayed.IntentDigest, err)
	}
	variant, err := buildAcceptedAccountingIntentV1(candidate, job, map[string]uint64{"miner-a": 2, "miner-b": 2}, 4, 2.5)
	if err != nil || variant.IntentDigest == intent.IntentDigest {
		t.Fatalf("variant intent did not change digest: %x err=%v", variant.IntentDigest, err)
	}
}

func TestBuildAcceptedAccountingIntentFailsBeforeSubmissionOnUnsafeInputs(t *testing.T) {
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, activeTestChainID())
	if err != nil {
		t.Fatal(err)
	}
	for _, fee := range []float64{-1, 101, math.NaN(), math.Inf(1)} {
		if _, err := buildAcceptedAccountingIntentV1(candidate, job, map[string]uint64{"miner": 1}, 1, fee); err == nil {
			t.Fatalf("unsafe fee %.17g accepted", fee)
		}
	}
	overflow := &JobData{Reward: make([]byte, 8), BlockFees: make([]byte, 8)}
	binary.LittleEndian.PutUint64(overflow.Reward, math.MaxUint64)
	if _, err := buildAcceptedAccountingIntentV1(candidate, overflow, nil, 0, 0); err == nil {
		t.Fatal("overflowing gross reward accepted")
	}
	missing := &JobData{Reward: append([]byte(nil), job.Reward...)}
	if _, err := buildAcceptedAccountingIntentV1(candidate, missing, nil, 0, 0); err == nil {
		t.Fatal("missing exact fee bytes accepted")
	}
}

func TestBuildAcceptedAccountingIntentV2UsesExactWeiAndCanonicalCredits(t *testing.T) {
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, activeTestChainID())
	if err != nil {
		t.Fatal(err)
	}
	// 260 BDAG atoms crosses uint64 after the required 1e10 Wei conversion.
	binary.LittleEndian.PutUint64(job.Reward, 260*100_000_000)
	binary.LittleEndian.PutUint64(job.BlockFees, 19)
	intent, err := buildAcceptedAccountingIntentV2(candidate, job, map[string]uint64{"b": 2, "a": 1}, 3, 2.5)
	if err != nil {
		t.Fatal(err)
	}
	if intent.PolicyVersion != storage.AcceptedAccountingPolicyV2 || intent.GrossReward != "260000000000000000019" || intent.FeeAmount != "6500000000000000000" || intent.Distributable != "253500000000000000019" {
		t.Fatalf("unexpected exact V2 monetary intent: %+v", intent)
	}
	if len(intent.CreditLines) != 2 || intent.CreditLines[0].MinerAddress != "a" || intent.CreditLines[0].Amount != "84500000000000000006" || intent.CreditLines[1].Amount != "169000000000000000012" {
		t.Fatalf("unexpected exact V2 credits: %+v", intent.CreditLines)
	}
	binary.LittleEndian.PutUint64(job.BlockFees, 7)
	oracle, err := buildAcceptedAccountingIntentV2(candidate, job, map[string]uint64{"a": 1, "b": 3}, 4, 2.5)
	if err != nil || oracle.GrossReward != "260000000000000000007" || oracle.FeeAmount != "6500000000000000000" || oracle.CreditLines[0].Amount != "63375000000000000001" || oracle.CreditLines[1].Amount != "190125000000000000005" {
		t.Fatalf("exact 1:3 oracle with one Wei dust: %+v err=%v", oracle, err)
	}
	// This is the observed native RPC reward magnitude, not a synthetic max.
	binary.LittleEndian.PutUint64(job.Reward, 23_395_245_759)
	binary.LittleEndian.PutUint64(job.BlockFees, 0)
	intent, err = buildAcceptedAccountingIntentV2(candidate, job, map[string]uint64{"miner": 1}, 1, 0)
	if err != nil || intent.GrossReward != "233952457590000000000" || intent.CreditLines[0].Amount != intent.GrossReward {
		t.Fatalf("native reward V2 intent=%+v err=%v", intent, err)
	}
}

func TestBuildAcceptedAccountingIntentV2RejectsMalformedEconomics(t *testing.T) {
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, activeTestChainID())
	if err != nil {
		t.Fatal(err)
	}
	for _, fee := range []float64{-1, 101, math.NaN(), math.Inf(1)} {
		if _, err := buildAcceptedAccountingIntentV2(candidate, job, map[string]uint64{"miner": 1}, 1, fee); err == nil {
			t.Fatalf("accepted invalid fee %v", fee)
		}
	}
	binary.LittleEndian.PutUint64(job.BlockFees, math.MaxUint64)
	if _, err := buildAcceptedAccountingIntentV2(candidate, job, map[string]uint64{"miner": 1}, 1, 0); err == nil {
		t.Fatal("accepted a negative signed fee encoded as uint64")
	}
}

func activeTestChainID() *big.Int { return big.NewInt(1404) }
