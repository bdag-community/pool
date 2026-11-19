package storage

import (
	"context"
	"errors"
	"math/big"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RewardCreditGuardV1 struct {
	pool       *pgxpool.Pool
	backend    string
	verifier   MatureRewardVerifierV1
	hold       *[16]byte
	bestEffort bool
}

func NewRewardCreditGuardV1(pool *pgxpool.Pool, backend string, verifier MatureRewardVerifierV1, bestEffort ...bool) (*RewardCreditGuardV1, error) {
	if pool == nil || verifier == nil || len(bestEffort) > 1 || validateJournalIdentity(big.NewInt(1404), backend) != nil {
		return nil, errors.New("reward credit guard is incomplete")
	}
	guard := &RewardCreditGuardV1{pool: pool, backend: backend, verifier: verifier}
	if len(bestEffort) == 1 {
		guard.bestEffort = bestEffort[0]
	}
	if os.Getenv(unpricedSubmissionHoldEnvV1) != "" || guard.bestEffort {
		// The payer independently qualifies the exact exception. Every credit
		// is still checked against its own immutable work and both chains.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		borrowed := &PostgresStorage{pool: pool}
		borrowed.SetBestEffortAccounting(guard.bestEffort)
		if err := borrowed.VerifySubmissionJournalV1(ctx, big.NewInt(1404), backend); err != nil {
			return nil, errors.New("reward credit hold verification failed")
		}
		guard.hold = borrowed.unpricedSubmissionHold.Load()
		if guard.hold == nil && !guard.bestEffort {
			return nil, errors.New("reward credit hold is not qualified")
		}
	}
	return guard, nil
}

// Verify checks an existing wallet-bound allocation and both chains without
// creating a credit. The pool remains owned by the caller.
func (g *RewardCreditGuardV1) Verify(ctx context.Context, creditID int64, wallet common.Address) error {
	if g == nil || g.pool == nil || creditID <= 0 || wallet == (common.Address{}) {
		return errors.New("reward credit guard inputs invalid")
	}
	var raw []byte
	if err := g.pool.QueryRow(ctx, `SELECT candidate_id FROM pool_reward_observations_v1.credit_bindings WHERE credit_id=$1 AND wallet=$2`, creditID, wallet[:]).Scan(&raw); err != nil || len(raw) != 32 {
		return errors.New("reward credit has no matching wallet allocation")
	}
	var id [32]byte
	copy(id[:], raw)
	borrowed := &PostgresStorage{pool: g.pool}
	borrowed.unpricedSubmissionHold.Store(g.hold)
	borrowed.SetBestEffortAccounting(g.bestEffort)
	_, err := borrowed.verifyOrAllocateObservedRewardV1(ctx, id, big.NewInt(1404), g.backend, wallet, g.verifier, false)
	return err
}
