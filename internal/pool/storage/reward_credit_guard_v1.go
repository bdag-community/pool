package storage

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RewardCreditGuardV1 struct {
	pool     *pgxpool.Pool
	backend  string
	verifier MatureRewardVerifierV1
}

func NewRewardCreditGuardV1(pool *pgxpool.Pool, backend string, verifier MatureRewardVerifierV1) (*RewardCreditGuardV1, error) {
	if pool == nil || verifier == nil || validateJournalIdentity(big.NewInt(1404), backend) != nil {
		return nil, errors.New("reward credit guard is incomplete")
	}
	return &RewardCreditGuardV1{pool: pool, backend: backend, verifier: verifier}, nil
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
	_, err := borrowed.verifyOrAllocateObservedRewardV1(ctx, id, big.NewInt(1404), g.backend, wallet, g.verifier, false)
	return err
}
