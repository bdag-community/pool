package sharejournal

import (
	"math"
	"math/rand"
	"reflect"
	"testing"

	"asicPool/internal/pool/pplns"
)

func workShare(id byte, sequence int64, miner string, work uint64) Share {
	s := testShare(id, miner, work)
	s.Sequence = sequence
	return s
}

func TestAllocateWindowRequiresExplicitBoundaryAndPreservesOwnerWork(t *testing.T) {
	a := "0x1111111111111111111111111111111111111111"
	b := "0x2222222222222222222222222222222222222222"
	window := []Share{workShare(1, 1, a, 4), workShare(2, 2, b, 6)}
	whole, total, err := AllocateWindow(window, 7, WholeShare)
	if err != nil || total != 6 || whole[a] != 0 || whole[b] != 6 {
		t.Fatalf("whole boundary allocation=%v total=%d err=%v", whole, total, err)
	}
	partial, total, err := AllocateWindow(window, 7, PartialShare)
	if err != nil || total != 7 || partial[a] != 1 || partial[b] != 6 {
		t.Fatalf("partial boundary allocation=%v total=%d err=%v", partial, total, err)
	}
	if _, _, err := AllocateWindow(window, 7, ""); err == nil {
		t.Fatal("missing financial policy was accepted")
	}
}

func TestAllocateWindowHandlesOversizeAndUint64Boundary(t *testing.T) {
	miner := "0x1111111111111111111111111111111111111111"
	window := []Share{workShare(1, 1, miner, math.MaxUint64)}
	whole, total, err := AllocateWindow(window, 10, WholeShare)
	if err != nil || total != 0 || len(whole) != 0 {
		t.Fatal("whole-share policy did not evict oversized share", err)
	}
	partial, total, err := AllocateWindow(window, 10, PartialShare)
	if err != nil || total != 10 || partial[miner] != 10 {
		t.Fatal("partial-share policy lost boundary work", err)
	}
	partial, total, err = AllocateWindow(window, math.MaxUint64, PartialShare)
	if err != nil || total != math.MaxUint64 || partial[miner] != math.MaxUint64 {
		t.Fatal("maximum work overflowed", err)
	}
}

func TestAllocateWindowRejectsDuplicateOrOutOfOrderShares(t *testing.T) {
	miner := "0x1111111111111111111111111111111111111111"
	first := workShare(1, 1, miner, 1)
	duplicate := first
	duplicate.Sequence = 2
	for _, window := range [][]Share{{first, duplicate}, {workShare(2, 2, miner, 1), first}} {
		if _, _, err := AllocateWindow(window, 2, PartialShare); err == nil {
			t.Fatal("invalid share history was accepted")
		}
	}
}

func TestAllocateWindowWholeShareMatchesExistingPoolPolicy(t *testing.T) {
	random := rand.New(rand.NewSource(20261117))
	miners := []string{"0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222"}
	for trial := 0; trial < 500; trial++ {
		target := uint64(random.Intn(20)+1) * pplns.PPLNS_SCALE
		existing := pplns.NewWindow(float64(target) / pplns.PPLNS_SCALE)
		var shares []Share
		for index, count := 0, random.Intn(20)+1; index < count; index++ {
			miner := miners[random.Intn(len(miners))]
			work := uint64(random.Intn(25)+1) * pplns.PPLNS_SCALE
			shares = append(shares, workShare(byte(index+1), int64(index+1), miner, work))
			existing.PushShare(miner, float64(work)/pplns.PPLNS_SCALE)
		}
		want, wantTotal := existing.Snapshot()
		got, gotTotal, err := AllocateWindow(shares, target, WholeShare)
		if err != nil || gotTotal != wantTotal || !reflect.DeepEqual(got, want) {
			t.Fatalf("whole-share policy diverged on trial %d: got=%v/%d want=%v/%d err=%v", trial, got, gotTotal, want, wantTotal, err)
		}
	}
}
