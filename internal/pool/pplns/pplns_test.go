package pplns

import (
	"fmt"
	"sync"
	"testing"
)

func TestPPLNS_Fairness(t *testing.T) {
	// N_WORK = 1000 * SCALE
	// Miner A: diff 16, 10 shares => 160 work
	// Miner B: diff 1, 160 shares => 160 work
	// Total Work: 320
	// Both should have equal work recorded.

	target := 1000.0
	window := NewWindow(target)

	// Miner A
	for i := 0; i < 10; i++ {
		window.PushShare("minerA", 16.0)
	}

	// Miner B
	for i := 0; i < 160; i++ {
		window.PushShare("minerB", 1.0)
	}

	snap, total := window.Snapshot()

	expectedA := uint64(160 * PPLNS_SCALE)
	expectedB := uint64(160 * PPLNS_SCALE)
	expectedTotal := expectedA + expectedB

	if total != expectedTotal {
		t.Errorf("Expected total %d, got %d", expectedTotal, total)
	}

	if snap["minerA"] != expectedA {
		t.Errorf("MinerA work mismatch: got %d, want %d", snap["minerA"], expectedA)
	}

	if snap["minerB"] != expectedB {
		t.Errorf("MinerB work mismatch: got %d, want %d", snap["minerB"], expectedB)
	}
}

func TestPPLNS_Eviction(t *testing.T) {
	// Target = 100 units
	target := 100.0
	window := NewWindow(target)

	// Fill with 10 shares of 10 diff (Total 100)
	for i := 0; i < 10; i++ {
		window.PushShare("minerA", 10.0)
	}

	// Verify full
	_, total := window.Snapshot()
	if total != uint64(100*PPLNS_SCALE) {
		t.Fatalf("Setup failed: total %d", total)
	}

	// Push 1 share of 10 diff for minerB
	// This should evict 1 share of minerA (oldest)
	// New total should be (100 - 10) + 10 = 100
	window.PushShare("minerB", 10.0)

	snap, total2 := window.Snapshot()
	if total2 > uint64(100*PPLNS_SCALE) {
		t.Errorf("Total work exceeded target after push: %d", total2)
	}

	// MinerA should have 90 work (9 shares)
	// MinerB should have 10 work (1 share)
	expectedA := uint64(90 * PPLNS_SCALE)
	expectedB := uint64(10 * PPLNS_SCALE)

	if snap["minerA"] != expectedA {
		t.Errorf("MinerA work incorrect: got %d, want %d", snap["minerA"], expectedA)
	}
	if snap["minerB"] != expectedB {
		t.Errorf("MinerB work incorrect: got %d, want %d", snap["minerB"], expectedB)
	}
}

func TestPPLNS_RampUp(t *testing.T) {
	// Target huge, push small amount
	window := NewWindow(1000.0)
	window.PushShare("minerA", 10.0)

	snap, total := window.Snapshot()
	if total != uint64(10*PPLNS_SCALE) {
		t.Errorf("Total during rampup wrong: %d", total)
	}
	if snap["minerA"] != uint64(10*PPLNS_SCALE) {
		t.Errorf("MinerA work wrong")
	}
}

func TestCalculateCredits(t *testing.T) {
	work := map[string]uint64{
		"A": 50,
		"B": 50,
	}
	total := uint64(100)
	reward := uint64(1000)

	credits := CalculateCredits(work, total, reward)

	if credits["A"] != 500 {
		t.Errorf("A credit wrong: %d", credits["A"])
	}
	if credits["B"] != 500 {
		t.Errorf("B credit wrong: %d", credits["B"])
	}
}

func TestPushShareAndSnapshotIncludesDiscoveryShare(t *testing.T) {
	window := NewWindow(100)
	window.PushShare("older", 10)
	snapshot, total := window.PushShareAndSnapshot("finder", 5)
	if total != 15*PPLNS_SCALE || snapshot["older"] != 10*PPLNS_SCALE || snapshot["finder"] != 5*PPLNS_SCALE {
		t.Fatalf("atomic discovery snapshot=%v total=%d", snapshot, total)
	}
	window.PushShare("later", 1)
	if _, exists := snapshot["later"]; exists {
		t.Fatal("returned discovery snapshot changed after a later push")
	}
}

func TestPushShareAndSnapshotAppliesEvictionBeforeCut(t *testing.T) {
	window := NewWindow(10)
	window.PushShare("older", 10)
	snapshot, total := window.PushShareAndSnapshot("finder", 4)
	if total != 4*PPLNS_SCALE || len(snapshot) != 1 || snapshot["finder"] != 4*PPLNS_SCALE {
		t.Fatalf("evicted discovery snapshot=%v total=%d", snapshot, total)
	}
}

func TestDiscoverySnapshotExcludesUnpushedLowDifficultyBlock(t *testing.T) {
	window := NewWindow(100)
	window.PushShare("older", 10)
	// The submit pipeline deliberately uses Snapshot, without PushShare, when
	// a network-valid block is below the miner's assigned pool difficulty.
	snapshot, total := window.Snapshot()
	if total != 10*PPLNS_SCALE || snapshot["older"] != 10*PPLNS_SCALE {
		t.Fatalf("low-difficulty exclusion snapshot=%v total=%d", snapshot, total)
	}
	if _, exists := snapshot["finder"]; exists {
		t.Fatal("unqualified discovery share entered the PPLNS cut")
	}
}

func TestPushShareAndSnapshotIsRaceSafeWithConcurrentPushes(t *testing.T) {
	window := NewWindow(1000)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			window.PushShare(fmt.Sprintf("miner-%02d", index), 1)
		}(i)
	}
	snapshot, total := window.PushShareAndSnapshot("finder", 2)
	if snapshot["finder"] != 2*PPLNS_SCALE || total < 2*PPLNS_SCALE {
		t.Fatalf("concurrent discovery snapshot lost finder: %v total=%d", snapshot, total)
	}
	wg.Wait()
	_, finalTotal := window.Snapshot()
	if finalTotal != 34*PPLNS_SCALE {
		t.Fatalf("concurrent final total=%d, want %d", finalTotal, 34*PPLNS_SCALE)
	}
}

func TestPPLNS_StatsAfterManyEvictions(t *testing.T) {
	window := NewWindow(100.0)

	for i := 0; i < 1000; i++ {
		window.PushShare(fmt.Sprintf("miner-%02d", i%8), 1.0)
	}

	shares, total, miners := window.Stats()
	if shares != 100 {
		t.Fatalf("share count mismatch after evictions: got %d, want 100", shares)
	}
	if total != uint64(100*PPLNS_SCALE) {
		t.Fatalf("total work mismatch after evictions: got %d", total)
	}
	if miners != 8 {
		t.Fatalf("miner count mismatch after evictions: got %d, want 8", miners)
	}
}

func BenchmarkPPLNSPushShareEvictingWindow(b *testing.B) {
	window := NewWindow(5000.0)
	miners := []string{"ajax", "penelope", "odysseus", "telemachus", "hector", "athena", "achilles", "nestor"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		window.PushShare(miners[i%len(miners)], 1.0)
	}
}
