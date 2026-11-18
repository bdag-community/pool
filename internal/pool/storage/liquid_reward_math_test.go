package storage

import (
	"math"
	"math/big"
	"testing"
)

func TestLiquidRewardArithmeticExactAndDoesNotMutateFee(t *testing.T) {
	for _, tc := range []struct {
		name  string
		atoms uint64
		split uint64
		fee   string
		want  string
	}{
		{"preactivation full subsidy", 26000000000, 100, "123", "260000000000000000123"},
		{"postactivation seventy percent", 26000000000, 70, "123", "182000000000000000123"},
		{"no liquid subsidy", 1, 0, "99", "99"},
		{"integer split exact", 1, 33, "0", "3300000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fee, ok := new(big.Int).SetString(tc.fee, 10)
			if !ok {
				t.Fatal("invalid test fee")
			}
			got, err := LiquidRewardArithmeticV1(tc.atoms, tc.split, fee)
			if err != nil || got.String() != tc.want || fee.String() != tc.fee {
				t.Fatalf("got %v, err %v, fee %s", got, err, fee)
			}
		})
	}
}

func TestLiquidRewardArithmeticRejectsUnsafeInputs(t *testing.T) {
	tooLarge, ok := new(big.Int).SetString("1000000000000000000000000000000", 10)
	if !ok {
		t.Fatal("invalid test amount")
	}
	for _, tc := range []struct {
		name         string
		atoms, split uint64
		fee          *big.Int
	}{
		{"split above 100", 1, 101, big.NewInt(0)},
		{"negative fee", 1, 70, big.NewInt(-1)},
		{"missing fee evidence", 1, 70, nil},
		{"accounting overflow", math.MaxUint64, 100, tooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LiquidRewardArithmeticV1(tc.atoms, tc.split, tc.fee); err == nil {
				t.Fatal("unsafe input accepted")
			}
		})
	}
}
