package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/jackc/pgx/v5"
)

// RecordProvenUnobservedRewardV1 recovers a transaction-free reward whose
// local replay state has been pruned. The account proofs are untrusted input:
// verifyReorderedRewardV1 binds them to the current native and EVM roots.
// This records evidence only; ordinary allocation and payer checks still apply.
func (s *PostgresStorage) RecordProvenUnobservedRewardV1(ctx context.Context, id [32]byte, wallet common.Address, obs RewardObservationV1, verifier RPCMatureRewardVerifierV1) (bool, error) {
	return s.provenUnobservedRewardV1(ctx, id, wallet, obs, verifier, false)
}

// ValidateProvenUnobservedRewardV1 exercises the identical checks and insert
// in a transaction that is rolled back before any observation is visible.
func (s *PostgresStorage) ValidateProvenUnobservedRewardV1(ctx context.Context, id [32]byte, wallet common.Address, obs RewardObservationV1, verifier RPCMatureRewardVerifierV1) (bool, error) {
	return s.provenUnobservedRewardV1(ctx, id, wallet, obs, verifier, true)
}

func (s *PostgresStorage) provenUnobservedRewardV1(ctx context.Context, id [32]byte, wallet common.Address, obs RewardObservationV1, verifier RPCMatureRewardVerifierV1, dryRun bool) (bool, error) {
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil || id == ([32]byte{}) || wallet == (common.Address{}) || verifier.RecoveryDirectory == "" {
		return false, errors.New("unobserved reward recovery unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return false, errors.New("unobserved reward recovery requires a deadline")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer rollbackUnpricedTx(tx)
	// Serialize with new allocations while current aliases are checked. This
	// does not lock the accepted-share journal or interrupt mining.
	if _, err = tx.Exec(ctx, `LOCK TABLE pool_reward_observations_v1.allocations IN SHARE MODE`); err != nil {
		return false, err
	}
	view, err := scanCandidateView(ctx, tx, id)
	if err != nil {
		return false, errors.New("accepted recovery candidate unavailable")
	}
	if accepted, err := s.rewardCandidateAcceptedV1(ctx, tx, view); err != nil || !accepted {
		return false, errors.New("accepted recovery candidate unavailable")
	}
	if err = validateRewardObservationV1(view.Candidate, wallet, obs); err != nil {
		return false, err
	}
	if err = s.verifyUnpricedWorkCandidateTxV1(ctx, tx, big.NewInt(1404), view.Candidate.BackendID, wallet, id[:]); err != nil {
		return false, err
	}
	var present, conflict bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_reward_observations_v1.observations WHERE candidate_id=$1),EXISTS(SELECT 1 FROM pool_reward_observations_v1.conflicts)`, id[:]).Scan(&present, &conflict); err != nil || conflict {
		return false, errors.New("reward observation conflict state unavailable")
	}
	if present {
		return false, nil
	}
	proof, err := readReorderedProofV1(verifier.RecoveryDirectory, id)
	if err != nil || proof.NativeOrder != obs.NativeOrder {
		return false, errors.New("reward recovery proof identity differs")
	}
	identity, err := verifier.verifyProvenRewardV1(ctx, view.Candidate, obs)
	if err != nil || identity != obs.EVMBlockHash {
		return false, errors.New("reward recovery proof rejected")
	}
	// This recovery path deliberately handles only the common 70% liquid
	// subsidy, with no beacon system call or withdrawals. Bind the amount to
	// both native reward metadata and the canonical EVM header. Other policy
	// eras remain held for replay rather than assuming a reward split.
	var nativeHeader *struct {
		Hash   common.Hash `json:"hash"`
		Reward int64       `json:"reward"`
		Time   int64       `json:"time"`
	}
	if err = verifier.Native.CallContext(ctx, &nativeHeader, "getBlockHeader", obs.NativeBlockHash, true); err != nil || nativeHeader == nil || nativeHeader.Hash != obs.NativeBlockHash || nativeHeader.Reward <= 0 || nativeHeader.Time < 0 {
		return false, errors.New("native recovery reward header unavailable")
	}
	var evmHeader *struct {
		Hash        common.Hash    `json:"hash"`
		Extra       hexutil.Bytes  `json:"extraData"`
		Time        hexutil.Uint64 `json:"timestamp"`
		BeaconRoot  *common.Hash   `json:"parentBeaconBlockRoot"`
		Withdrawals []interface{}  `json:"withdrawals"`
	}
	if err = verifier.EVM.CallContext(ctx, &evmHeader, "eth_getBlockByNumber", hexutil.EncodeUint64(obs.EVMBlockNumber), false); err != nil || evmHeader == nil || evmHeader.Hash != identity || len(evmHeader.Extra) != 8 || binary.BigEndian.Uint64(evmHeader.Extra) != uint64(nativeHeader.Reward) || uint64(evmHeader.Time) != uint64(nativeHeader.Time) || evmHeader.BeaconRoot != nil || len(evmHeader.Withdrawals) != 0 {
		return false, errors.New("native and EVM recovery reward headers differ")
	}
	expected := new(big.Int).SetUint64(uint64(nativeHeader.Reward))
	expected.Mul(expected, big.NewInt(10_000_000_000))
	expected.Mul(expected, big.NewInt(70))
	expected.Div(expected, big.NewInt(100))
	if expected.String() != obs.SubsidyWei {
		return false, errors.New("recovery balance differs from 70 percent native subsidy")
	}
	identity, err = verifier.VerifyMatureRewardIdentityV1(ctx, view.Candidate, obs)
	if err != nil || identity != obs.EVMBlockHash {
		return false, errors.New("reward recovery is not currently mature and canonical")
	}
	if err = verifier.verifyNoReorderedAliasesV1(ctx, tx, identity); err != nil {
		return false, err
	}
	again, err := verifier.verifyProvenRewardV1(ctx, view.Candidate, obs)
	if err != nil || again != identity {
		return false, ErrRewardEvidenceStale
	}
	digest, _, err := rewardObservationDigestV1(id, obs)
	if err != nil {
		return false, err
	}
	// An ordinary collector can race this helper. Do not overwrite its result
	// or manufacture a conflict when another observation has already won.
	tag, err := tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.observations(candidate_id,native_block_hash,native_order,native_height,evm_block_hash,evm_block_number,evm_state_root,wallet,subsidy_wei,other_sender_fees_wei,self_paid_fees_wei,evidence_digest) VALUES($1,$2,$3::numeric,$4::numeric,$5,$6::numeric,$7,$8,$9::numeric,$10::numeric,$11::numeric,$12) ON CONFLICT(candidate_id) DO NOTHING`, id[:], obs.NativeBlockHash[:], strconv.FormatUint(obs.NativeOrder, 10), strconv.FormatUint(obs.NativeHeight, 10), obs.EVMBlockHash[:], strconv.FormatUint(obs.EVMBlockNumber, 10), obs.EVMStateRoot[:], wallet[:], obs.SubsidyWei, obs.OtherFeesWei, obs.SelfPaidFeesWei, digest)
	if err != nil {
		return false, err
	}
	if dryRun {
		if err = tx.Rollback(ctx); err != nil {
			return false, errors.New("proven observation dry-run rollback unproven")
		}
		return tag.RowsAffected() == 1, nil
	}
	if err = tx.Commit(ctx); err != nil {
		return false, errors.New("proven observation commit unproven")
	}
	return tag.RowsAffected() == 1, nil
}
