package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/big"
	"strings"
	"testing"

	"golang.org/x/crypto/blake2b"
)

const rewardEvidenceMiner = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"

func rewardEvidenceFixture(t *testing.T) RewardLedgerEvidenceV1 {
	t.Helper()
	c := storageTestCandidate()
	reward := new(big.Int).SetUint64(binary.LittleEndian.Uint64(c.HeaderBytes[168:176]))
	reward.Mul(reward, big.NewInt(10_000_000_000))
	intent, err := SealAcceptedAccountingIntentV2(AcceptedAccountingIntentV2{
		CandidateID: c.CandidateID, LegacyBlockHash: c.SharePowHash, Height: c.Height.Uint64(),
		GrossReward: reward.String(), PoolFeeFloatBits: math.Float64bits(0), FeeAmount: "0", Distributable: reward.String(), TotalWork: 1,
		CreditLines: []AcceptedAccountingCreditLineV2{{MinerAddress: rewardEvidenceMiner, Work: 1, Amount: reward.String()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wallet [20]byte
	copy(wallet[:], c.HeaderBytes[148:168])
	return RewardLedgerEvidenceV1{Candidate: c, Accounting: intent, ReceiptHash: c.ExpectedBlockHash,
		EffectDigest: intent.IntentDigest, EffectBlockHash: intent.LegacyBlockHash, EffectLineCount: 1,
		CreditID: 7, CreditBlockHash: hex.EncodeToString(c.SharePowHash[:]), CreditAddress: rewardEvidenceMiner,
		CreditAmountWei: reward.String(), RewardWallet: wallet}
}

func TestRewardLedgerEvidenceBindsWalletCandidateAndCredit(t *testing.T) {
	if err := VerifyRewardLedgerEvidenceV1(rewardEvidenceFixture(t)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*RewardLedgerEvidenceV1)
	}{
		{"wrong reward wallet", func(e *RewardLedgerEvidenceV1) { e.RewardWallet[0] ^= 1 }},
		{"zero reward wallet", func(e *RewardLedgerEvidenceV1) { e.RewardWallet = [20]byte{} }},
		{"candidate bytes changed", func(e *RewardLedgerEvidenceV1) { e.Candidate.HeaderBytes[148] ^= 1 }},
		{"wrong chain", func(e *RewardLedgerEvidenceV1) { e.Candidate.ChainID = big.NewInt(1) }},
		{"receipt hash changed", func(e *RewardLedgerEvidenceV1) { e.ReceiptHash[0] ^= 1 }},
		{"effect digest changed", func(e *RewardLedgerEvidenceV1) { e.EffectDigest[0] ^= 1 }},
		{"effect block changed", func(e *RewardLedgerEvidenceV1) { e.EffectBlockHash[0] ^= 1 }},
		{"effect count changed", func(e *RewardLedgerEvidenceV1) { e.EffectLineCount++ }},
		{"accounting line changed", func(e *RewardLedgerEvidenceV1) { e.Accounting.CreditLines[0].Amount = "1" }},
		{"credit block changed", func(e *RewardLedgerEvidenceV1) { e.CreditBlockHash = strings.Repeat("00", 32) }},
		{"credit id invalid", func(e *RewardLedgerEvidenceV1) { e.CreditID = 0 }},
		{"credit address changed", func(e *RewardLedgerEvidenceV1) { e.CreditAddress = "0x1111111111111111111111111111111111111111" }},
		{"zero credit address", func(e *RewardLedgerEvidenceV1) { e.CreditAddress = "0x0000000000000000000000000000000000000000" }},
		{"credit amount changed", func(e *RewardLedgerEvidenceV1) { e.CreditAmountWei = "1" }},
		{"credit amount noncanonical", func(e *RewardLedgerEvidenceV1) { e.CreditAmountWei = "0" + e.CreditAmountWei }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := rewardEvidenceFixture(t)
			tc.change(&e)
			if err := VerifyRewardLedgerEvidenceV1(e); err == nil {
				t.Fatal("inconsistent evidence was accepted")
			}
		})
	}
}

func TestRewardLedgerEvidenceRejectsAmbiguousCredit(t *testing.T) {
	e := rewardEvidenceFixture(t)
	gross, ok := new(big.Int).SetString(e.Accounting.GrossReward, 10)
	if !ok || gross.Bit(0) != 0 {
		t.Fatal("fixture reward is not even")
	}
	half := new(big.Int).Rsh(gross, 1).String()
	e.Accounting.TotalWork = 2
	e.Accounting.CreditLines = []AcceptedAccountingCreditLineV2{
		{MinerAddress: rewardEvidenceMiner, Work: 1, Amount: half},
		{MinerAddress: "0x" + strings.ToUpper(rewardEvidenceMiner[2:]), Work: 1, Amount: half},
	}
	var err error
	e.Accounting, err = SealAcceptedAccountingIntentV2(e.Accounting)
	if err != nil {
		t.Fatal(err)
	}
	e.EffectDigest = e.Accounting.IntentDigest
	e.EffectLineCount = 2
	e.CreditAmountWei = half
	if err := VerifyRewardLedgerEvidenceV1(e); err == nil {
		t.Fatal("ambiguous case-folded recipient was accepted")
	}
}

func TestRewardLedgerEvidenceBindsWalletEvenWhenShareProofSame(t *testing.T) {
	e := rewardEvidenceFixture(t)
	originalShareHash := e.Candidate.SharePowHash
	e.Candidate.HeaderBytes[148] ^= 1
	first := blake2b.Sum256(e.Candidate.HeaderBytes[:])
	e.Candidate.ExpectedBlockHash = blake2b.Sum256(first[:])
	e.Candidate.CandidateID = candidateIDForHashV1(e.Candidate.ExpectedBlockHash)
	var err error
	e.Candidate.FrozenPayload, err = encodeFrozenCandidateJournalV1(e.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	e.Candidate.FrozenDigest = sha256.Sum256(e.Candidate.FrozenPayload)
	e.Accounting.CandidateID = e.Candidate.CandidateID
	e.Accounting, err = SealAcceptedAccountingIntentV2(e.Accounting)
	if err != nil {
		t.Fatal(err)
	}
	e.ReceiptHash = e.Candidate.ExpectedBlockHash
	e.EffectDigest = e.Accounting.IntentDigest
	if e.Candidate.SharePowHash != originalShareHash {
		t.Fatal("fixture unexpectedly changed the share proof")
	}
	if err := VerifyRewardLedgerEvidenceV1(e); err == nil {
		t.Fatal("share proof was accepted for a different reward wallet")
	}
	copy(e.RewardWallet[:], e.Candidate.HeaderBytes[148:168])
	if err := VerifyRewardLedgerEvidenceV1(e); err != nil {
		t.Fatalf("matching reward wallet was rejected: %v", err)
	}
}

func TestRewardLedgerEvidenceAllowsPartialLiquidRewardWithoutProvingAmount(t *testing.T) {
	e := rewardEvidenceFixture(t)
	gross, ok := new(big.Int).SetString(e.Accounting.GrossReward, 10)
	if !ok {
		t.Fatal("invalid fixture gross reward")
	}
	liquid := new(big.Int).Quo(new(big.Int).Mul(gross, big.NewInt(70)), big.NewInt(100)).String()
	e.Accounting.GrossReward = liquid
	e.Accounting.Distributable = liquid
	e.Accounting.CreditLines[0].Amount = liquid
	var err error
	e.Accounting, err = SealAcceptedAccountingIntentV2(e.Accounting)
	if err != nil {
		t.Fatal(err)
	}
	e.EffectDigest = e.Accounting.IntentDigest
	e.CreditAmountWei = liquid
	if err := VerifyRewardLedgerEvidenceV1(e); err != nil {
		t.Fatalf("consistent partial liquid reward was rejected: %v", err)
	}
}
