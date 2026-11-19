package server

import (
	"context"
	"errors"
	"log"
	"math/big"
	"time"

	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
)

type rewardAllocationStore interface {
	NextMatureRewardExcludingV1(context.Context, uint64, [][32]byte) ([32]byte, bool, error)
	AllocateObservedRewardV1(context.Context, [32]byte, *big.Int, string, common.Address, storage.MatureRewardVerifierV1) (bool, error)
}

type rewardMaturityGate interface {
	Check(context.Context) (uint64, bool, error)
}

type rewardAllocator struct {
	store      rewardAllocationStore
	gate       rewardMaturityGate
	verifier   storage.MatureRewardVerifierV1
	wallet     common.Address
	chainID    *big.Int
	backend    string
	held       map[[32]byte]time.Time
	bestEffort bool
}

func (a *rewardAllocator) step(ctx context.Context) (bool, error) {
	if a == nil || a.store == nil || a.gate == nil || a.verifier == nil || a.wallet == (common.Address{}) || a.chainID == nil {
		return false, errors.New("reward allocator unavailable")
	}
	maxHeight, ready, err := a.gate.Check(ctx)
	if err != nil || !ready {
		return false, err
	}
	now := time.Now()
	excluded := make([][32]byte, 0, len(a.held))
	for id, until := range a.held {
		if !now.Before(until) {
			delete(a.held, id)
		} else {
			excluded = append(excluded, id)
		}
	}
	id, found, err := a.store.NextMatureRewardExcludingV1(ctx, maxHeight, excluded)
	if err != nil || !found {
		return false, err
	}
	created, err := a.store.AllocateObservedRewardV1(ctx, id, a.chainID, a.backend, a.wallet, a.verifier)
	if err != nil && ctx.Err() == nil && (errors.Is(err, storage.ErrRewardEvidenceStale) || a.bestEffort) {
		if a.held == nil {
			a.held = make(map[[32]byte]time.Time)
		}
		delay := 30 * time.Second
		if errors.Is(err, storage.ErrRewardEvidenceStale) {
			delay = 5 * time.Minute
		}
		a.held[id] = now.Add(delay)
	}
	return created, err
}

func (a *rewardAllocator) run(ctx context.Context) {
	if ctx == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			attempt, cancel := context.WithTimeout(ctx, 20*time.Second)
			_, err := a.step(attempt)
			cancel()
			if err != nil && ctx.Err() == nil {
				log.Printf("pool reward allocation pending: %v", err)
			}
		}
	}
}
