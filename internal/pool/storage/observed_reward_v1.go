package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// RewardObservationV1 describes a node replay, not an authorization to pay.
// Before allocation the caller must establish the authenticity of the node,
// revalidate both canonical chains and maturity, and fence all payment effects.
type RewardObservationV1 struct {
	NativeBlockHash common.Hash    `json:"native_block_hash"`
	NativeOrder     uint64         `json:"native_order"`
	NativeHeight    uint64         `json:"native_height"`
	EVMBlockHash    common.Hash    `json:"evm_block_hash"`
	EVMBlockNumber  uint64         `json:"evm_block_number"`
	EVMStateRoot    common.Hash    `json:"evm_state_root"`
	Wallet          common.Address `json:"wallet"`
	SubsidyWei      string         `json:"subsidy_wei"`
	OtherFeesWei    string         `json:"other_sender_fees_wei"`
	SelfPaidFeesWei string         `json:"self_paid_fees_wei"`
}

func rewardObservationDigestV1(id [32]byte, observation RewardObservationV1) ([]byte, []byte, error) {
	encoded, err := json.Marshal(observation)
	if err != nil {
		return nil, nil, err
	}
	h := sha256.New()
	h.Write([]byte("BDAG-POOL-REWARD-OBSERVATION/v1\x00"))
	h.Write(id[:])
	h.Write(encoded)
	return h.Sum(nil), encoded, nil
}

// NextRewardObservationCandidateV1 prioritizes recent accepted discoveries so
// their EVM parent state is captured before pruning. A conflict holds the
// collector; failed observations remain available for another attempt.
func (s *PostgresStorage) NextRewardObservationCandidateV1(ctx context.Context) (*FrozenCandidateV1, error) {
	return s.NextRewardObservationCandidateExcludingV1(ctx, nil)
}

// NextRewardObservationCandidateExcludingV1 allows a collector to try other
// candidates after an observation failure, without deleting or changing any.
func (s *PostgresStorage) NextRewardObservationCandidateExcludingV1(ctx context.Context, excluded [][32]byte) (*FrozenCandidateV1, error) {
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil {
		return nil, errors.New("reward observation database unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("reward observation requires a deadline")
	}
	var conflicts bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_reward_observations_v1.conflicts)`).Scan(&conflicts); err != nil || (conflicts && !s.bestEffortAccounting.Load()) {
		return nil, errors.New("reward observation conflict state unavailable")
	}
	var raw []byte
	skipped := make([][]byte, 0, len(excluded))
	for i := range excluded {
		skipped = append(skipped, excluded[i][:])
	}
	acceptedSQL, acceptedArgs := s.rewardAcceptedSelectorV1("s", "r.candidate_id", 2)
	args := append([]any{skipped}, acceptedArgs...)
	err := s.pool.QueryRow(ctx, `SELECT r.candidate_id FROM accepted_block_submission_receipts_v1 r JOIN pool_share_journal_v1.unpriced_discoveries_v1 d USING(candidate_id) JOIN block_submission_state_v1 s USING(candidate_id) LEFT JOIN pool_reward_observations_v1.observations o USING(candidate_id) WHERE `+acceptedSQL+` AND NOT EXISTS(SELECT 1 FROM pool_reward_observations_v1.conflicts x WHERE x.candidate_id=r.candidate_id) AND o.candidate_id IS NULL AND r.candidate_id<>ALL($1::bytea[]) ORDER BY r.accepted_at DESC,r.candidate_id LIMIT 1`, args...).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil || len(raw) != 32 {
		return nil, errors.New("unobserved candidate unavailable")
	}
	var id [32]byte
	copy(id[:], raw)
	view, err := scanCandidateView(ctx, s.pool, id)
	if err != nil {
		return nil, errors.New("unobserved candidate changed")
	}
	if accepted, err := s.rewardCandidateAcceptedV1(ctx, s.pool, view); err != nil || !accepted {
		return nil, errors.New("unobserved candidate changed")
	}
	return &view.Candidate, nil
}

func validateRewardObservationV1(candidate FrozenCandidateV1, wallet common.Address, observation RewardObservationV1) error {
	if err := validateFrozenCandidate(candidate); err != nil {
		return err
	}
	if candidate.ChainID == nil || candidate.ChainID.Cmp(big.NewInt(1404)) != 0 || candidate.Height == nil || !candidate.Height.IsUint64() || wallet == (common.Address{}) || !bytes.Equal(candidate.HeaderBytes[148:168], wallet[:]) || observation.Wallet != wallet || observation.NativeBlockHash != common.Hash(candidate.ExpectedBlockHash) || observation.NativeHeight != candidate.Height.Uint64() || observation.NativeOrder == 0 || observation.EVMBlockHash == (common.Hash{}) || observation.EVMStateRoot == (common.Hash{}) || observation.EVMBlockNumber == 0 {
		return errors.New("reward observation identity differs from candidate")
	}
	total := new(big.Int)
	for _, raw := range []string{observation.SubsidyWei, observation.OtherFeesWei, observation.SelfPaidFeesWei} {
		value, ok := new(big.Int).SetString(raw, 10)
		if !ok || value.Sign() < 0 || value.String() != raw || len(raw) > 30 {
			return errors.New("reward observation amount invalid")
		}
		total.Add(total, value)
	}
	if len(total.String()) > 30 {
		return errors.New("reward observation total exceeds accounting range")
	}
	return nil
}

// RecordRewardObservationV1 only preserves the result of a separately
// authenticated node replay for an already accepted, unpriced candidate.
// A differing replay is retained as a durable conflict and returns an error.
// Nothing in this method creates a credit or permits a payout.
func (s *PostgresStorage) RecordRewardObservationV1(ctx context.Context, id [32]byte, wallet common.Address, observation RewardObservationV1) (bool, error) {
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil || id == ([32]byte{}) {
		return false, errors.New("reward observation database unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return false, errors.New("reward observation requires a deadline")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackUnpricedTx(tx)
	view, err := scanCandidateView(ctx, tx, id)
	if err != nil {
		return false, err
	}
	if accepted, err := s.rewardCandidateAcceptedV1(ctx, tx, view); err != nil || !accepted {
		return false, errors.New("reward observation candidate is not accepted")
	}
	if err = validateRewardObservationV1(view.Candidate, wallet, observation); err != nil {
		return false, err
	}
	var ready bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_share_journal_v1.unpriced_discoveries_v1 d JOIN public.accepted_block_submission_receipts_v1 r USING(candidate_id) WHERE d.candidate_id=$1 AND r.block_hash=$2) AND NOT EXISTS(SELECT 1 FROM public.accepted_block_accounting_intents_v1 WHERE candidate_id=$1) AND NOT EXISTS(SELECT 1 FROM pool_reward_observations_v1.conflicts WHERE NOT $3::boolean OR candidate_id=$1)`, id[:], observation.NativeBlockHash[:], s.bestEffortAccounting.Load()).Scan(&ready)
	if err != nil || !ready {
		return false, errors.New("unpriced accepted candidate or conflict state unavailable")
	}
	digest, encoded, err := rewardObservationDigestV1(id, observation)
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.observations(candidate_id,native_block_hash,native_order,native_height,evm_block_hash,evm_block_number,evm_state_root,wallet,subsidy_wei,other_sender_fees_wei,self_paid_fees_wei,evidence_digest) VALUES($1,$2,$3::numeric,$4::numeric,$5,$6::numeric,$7,$8,$9::numeric,$10::numeric,$11::numeric,$12) ON CONFLICT(candidate_id) DO NOTHING`, id[:], observation.NativeBlockHash[:], strconv.FormatUint(observation.NativeOrder, 10), strconv.FormatUint(observation.NativeHeight, 10), observation.EVMBlockHash[:], strconv.FormatUint(observation.EVMBlockNumber, 10), observation.EVMStateRoot[:], observation.Wallet[:], observation.SubsidyWei, observation.OtherFeesWei, observation.SelfPaidFeesWei, digest)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		var existing []byte
		if err = tx.QueryRow(ctx, `SELECT evidence_digest FROM pool_reward_observations_v1.observations WHERE candidate_id=$1`, id[:]).Scan(&existing); err != nil {
			return false, err
		}
		if !bytes.Equal(existing, digest) {
			if _, err = tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.conflicts(candidate_id,original_digest,incoming_digest,incoming_evidence) VALUES($1,$2,$3,$4::jsonb)`, id[:], existing, digest, string(encoded)); err != nil {
				return false, err
			}
			if err = tx.Commit(ctx); err != nil {
				return false, errors.New("reward observation conflict commit unproven")
			}
			return false, errors.New("reward observation conflicts with recorded evidence")
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, errors.New("reward observation commit unproven")
	}
	return tag.RowsAffected() == 1, nil
}
