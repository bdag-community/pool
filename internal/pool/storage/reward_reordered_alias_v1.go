package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// A prior allocation may itself have moved since its original claim was
// recorded. Check its current native-to-EVM mapping before recovering a
// reordered reward; the unique database claim alone cannot detect that case.
func (v RPCMatureRewardVerifierV1) verifyNoReorderedAliasesV1(ctx context.Context, tx pgx.Tx, identity common.Hash) error {
	rows, err := tx.Query(ctx, `SELECT o.native_block_hash FROM pool_reward_observations_v1.allocations a JOIN pool_reward_observations_v1.observations o USING(candidate_id) UNION ALL SELECT o.native_block_hash FROM pool_reward_observations_v1.operator_defaults_v1 d JOIN pool_reward_observations_v1.observations o USING(candidate_id)`)
	if err != nil {
		return err
	}
	var hashes []common.Hash
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil || len(raw) != common.HashLength {
			rows.Close()
			return errors.New("allocated reward identity malformed")
		}
		hashes = append(hashes, common.BytesToHash(raw))
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	return verifyCurrentRewardAliasesV1(ctx, v.Native, hashes, identity)
}

func verifyCurrentRewardAliasesV1(ctx context.Context, native RewardRPCCallerV1, hashes []common.Hash, identity common.Hash) error {
	if native == nil || identity == (common.Hash{}) {
		return errors.New("reward alias verification unavailable")
	}
	for _, hash := range hashes {
		var block *struct {
			Hash          string `json:"hash"`
			Order         int64  `json:"order"`
			Confirmations int64  `json:"confirmations"`
		}
		if err := native.CallContext(ctx, &block, "getBlock", hash.Hex(), true, false, false); err != nil || block == nil || !strings.EqualFold(strings.TrimPrefix(block.Hash, "0x"), strings.TrimPrefix(hash.Hex(), "0x")) || block.Order < 1 || block.Confirmations < 1 {
			return ErrRewardEvidenceStale
		}
		var state rewardNativeStateV1
		if err := native.CallContext(ctx, &state, "getStateRoot", block.Order, true); err != nil || !state.Valid || state.Order != uint64(block.Order) || state.Hash != hash || state.EVMHead == (common.Hash{}) {
			return ErrRewardEvidenceStale
		}
		if state.EVMHead == identity {
			return fmt.Errorf("%w: canonical EVM reward belongs to an existing allocation", ErrRewardEvidenceStale)
		}
	}
	return nil
}
