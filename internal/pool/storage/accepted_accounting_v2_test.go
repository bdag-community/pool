package storage

import (
	"math"
	"testing"
)

func TestAcceptedAccountingV2CanonicalWeiAndDistinctHashDomain(t *testing.T) {
	for _, raw := range []string{"", "00", "01", "-1", "+1", "1000000000000000000000000000000"} {
		if _, err := canonicalWei(raw); err == nil {
			t.Fatalf("accepted malformed wei %q", raw)
		}
	}
	if got, err := canonicalWei("260000000000000000000"); err != nil || got.String() != "260000000000000000000" {
		t.Fatalf("canonical wei=%v err=%v", got, err)
	}
	var candidate, hash [32]byte
	candidate[0], hash[0] = 1, 2
	intent, err := SealAcceptedAccountingIntentV2(AcceptedAccountingIntentV2{CandidateID: candidate, LegacyBlockHash: hash, Height: 1, GrossReward: "260000000000000000000", PoolFeeFloatBits: math.Float64bits(2.5), FeeAmount: "6500000000000000000", Distributable: "253500000000000000000", TotalWork: 3, CreditLines: []AcceptedAccountingCreditLineV2{{MinerAddress: "a", Work: 1, Amount: "84500000000000000000"}, {MinerAddress: "b", Work: 2, Amount: "169000000000000000000"}}})
	if err != nil {
		t.Fatal(err)
	}
	if intent.PolicyVersion != AcceptedAccountingPolicyV2 || intent.IntentDigest == [32]byte{} {
		t.Fatalf("unsealed V2 intent: %+v", intent)
	}
	if err := validateAcceptedAccountingIntentV2(intent); err != nil {
		t.Fatal(err)
	}
	intent.CreditLines[0].Amount = "084500000000000000000"
	if err := validateAcceptedAccountingIntentV2(intent); err == nil {
		t.Fatal("accepted noncanonical credit")
	}
}
