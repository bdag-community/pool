package server

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
)

type allocatorTestGate struct{}

func (allocatorTestGate) Check(context.Context) (uint64, bool, error) { return 10, true, nil }

type allocatorTestVerifier struct{}

func (allocatorTestVerifier) VerifyMatureRewardV1(context.Context, storage.FrozenCandidateV1, storage.RewardObservationV1) error {
	return nil
}

type allocatorTestStore struct {
	ids      [][32]byte
	fault    map[[32]byte]error
	attempts [][32]byte
}

func (s *allocatorTestStore) NextMatureRewardExcludingV1(_ context.Context, _ uint64, excluded [][32]byte) ([32]byte, bool, error) {
	for _, id := range s.ids {
		held := false
		for _, skip := range excluded {
			if id == skip {
				held = true
				break
			}
		}
		if !held {
			return id, true, nil
		}
	}
	return [32]byte{}, false, nil
}
func (s *allocatorTestStore) AllocateObservedRewardV1(_ context.Context, id [32]byte, _ *big.Int, _ string, _ common.Address, _ storage.MatureRewardVerifierV1) (bool, error) {
	s.attempts = append(s.attempts, id)
	if err := s.fault[id]; err != nil {
		return false, err
	}
	return true, nil
}
func TestAllocatorSkipsOnlyTemporarilyStaleEvidence(t *testing.T) {
	first, second := [32]byte{1}, [32]byte{2}
	store := &allocatorTestStore{ids: [][32]byte{first, second}, fault: map[[32]byte]error{first: storage.ErrRewardEvidenceStale}}
	a := &rewardAllocator{store: store, gate: allocatorTestGate{}, verifier: allocatorTestVerifier{}, wallet: common.HexToAddress("0x1234"), chainID: big.NewInt(1404), backend: "node"}
	if _, err := a.step(context.Background()); !errors.Is(err, storage.ErrRewardEvidenceStale) {
		t.Fatalf("stale observation: %v", err)
	}
	if created, err := a.step(context.Background()); err != nil || !created || store.attempts[len(store.attempts)-1] != second {
		t.Fatalf("later reward not allocated: %t %v", created, err)
	}
	a.held[first] = time.Now().Add(-time.Second)
	if _, err := a.step(context.Background()); !errors.Is(err, storage.ErrRewardEvidenceStale) || store.attempts[len(store.attempts)-1] != first {
		t.Fatalf("stale observation not retried: %v", err)
	}
	delete(a.held, first)
	store.fault[first] = errors.New("RPC unavailable")
	for i := 0; i < 2; i++ {
		if _, err := a.step(context.Background()); err == nil || store.attempts[len(store.attempts)-1] != first {
			t.Fatalf("transient error was skipped: %v", err)
		}
	}
}

func TestBestEffortAllocatorDoesNotLetOneFailureStarveOthers(t *testing.T) {
	first, second := [32]byte{1}, [32]byte{2}
	problem := errors.New("reward work unavailable")
	store := &allocatorTestStore{ids: [][32]byte{first, second}, fault: map[[32]byte]error{first: problem}}
	a := &rewardAllocator{store: store, gate: allocatorTestGate{}, verifier: allocatorTestVerifier{}, wallet: common.HexToAddress("0x1234"), chainID: big.NewInt(1404), backend: "node", bestEffort: true}
	if _, err := a.step(context.Background()); !errors.Is(err, problem) {
		t.Fatalf("failed reward: %v", err)
	}
	if created, err := a.step(context.Background()); err != nil || !created || store.attempts[len(store.attempts)-1] != second {
		t.Fatalf("other reward was starved: %t %v", created, err)
	}
	a.held[first] = time.Now().Add(-time.Second)
	if _, err := a.step(context.Background()); !errors.Is(err, problem) || store.attempts[len(store.attempts)-1] != first {
		t.Fatalf("failed reward was not retried: %v", err)
	}
}

func TestBestEffortAllocatorDoesNotHoldOnCanceledContext(t *testing.T) {
	first := [32]byte{1}
	store := &allocatorTestStore{ids: [][32]byte{first}, fault: map[[32]byte]error{first: context.Canceled}}
	a := &rewardAllocator{store: store, gate: allocatorTestGate{}, verifier: allocatorTestVerifier{}, wallet: common.HexToAddress("0x1234"), chainID: big.NewInt(1404), backend: "node", bestEffort: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.step(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled allocation: %v", err)
	}
	if len(a.held) != 0 {
		t.Fatal("cancellation delayed the reward")
	}
}
