package storage

import (
	"context"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// NextMatureRewardV1 selects one observed, accepted candidate that has not
// yet been allocated. The caller independently verifies canonicality.
func (s *PostgresStorage) NextMatureRewardV1(ctx context.Context, maxHeight uint64) ([32]byte, bool, error) {
	return s.NextMatureRewardExcludingV1(ctx, maxHeight, nil)
}

// NextMatureRewardExcludingV1 skips temporarily held observations without
// mutating them or their credits. The caller must retry them later.
func (s *PostgresStorage) NextMatureRewardExcludingV1(ctx context.Context, maxHeight uint64, excluded [][32]byte) ([32]byte, bool, error) {
	var id [32]byte
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil {
		return id, false, errors.New("reward allocation database unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return id, false, errors.New("reward allocation requires a deadline")
	}
	var conflict bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_reward_observations_v1.conflicts)`).Scan(&conflict); err != nil || conflict {
		return id, false, errors.New("reward observation conflict held")
	}
	var raw []byte
	skipped := make([][]byte, 0, len(excluded))
	for i := range excluded {
		skipped = append(skipped, excluded[i][:])
	}
	err := s.pool.QueryRow(ctx, `SELECT o.candidate_id FROM pool_reward_observations_v1.observations o JOIN block_submission_state_v1 s USING(candidate_id) LEFT JOIN pool_reward_observations_v1.allocations a USING(candidate_id) WHERE s.state='accepted_final' AND a.candidate_id IS NULL AND o.native_height<=$1::numeric AND o.candidate_id<>ALL($2::bytea[]) ORDER BY o.native_height,o.candidate_id LIMIT 1`, strconv.FormatUint(maxHeight, 10), skipped).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return id, false, nil
	}
	if err != nil || len(raw) != 32 {
		return id, false, errors.New("mature reward selection failed")
	}
	copy(id[:], raw)
	return id, true, nil
}
