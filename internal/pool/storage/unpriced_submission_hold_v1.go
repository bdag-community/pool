package storage

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
)

// This opt-in never changes the immutable conflict or its original work cut.
// Every reward retains independent work, chain, maturity, and payment checks.
const unpricedSubmissionHoldEnvV1 = "POOL_UNPRICED_SUBMISSION_HOLD_CONFLICT_ID"

// unpricedSubmissionHoldSQLV1 rechecks the complete evidence in the same SQL
// snapshot as every submission latch check. Other conflicts always block.
func unpricedSubmissionHoldSQLV1(firstParam int) string {
	p := fmt.Sprintf("$%d", firstParam)
	return `((SELECT count(*) FROM public.block_submission_conflicts_v1)=1
 AND (SELECT count(*) FROM pool_share_journal_v1.unpriced_conflicts_v1)=1
 AND EXISTS (
 SELECT 1 FROM public.block_submission_conflicts_v1 f
 JOIN public.block_submission_candidates_v1 c ON c.candidate_id=f.candidate_id
 JOIN public.block_submission_state_v1 state ON state.candidate_id=c.candidate_id
 JOIN pool_share_journal_v1.unpriced_conflicts_v1 u ON u.conflict_id=f.conflict_id AND u.candidate_id=c.candidate_id
 JOIN pool_share_journal_v1.unpriced_discoveries_v1 d ON d.candidate_id=c.candidate_id
 JOIN pool_share_journal_v1.allocation_policies policy ON policy.candidate_id=c.candidate_id
 JOIN pool_share_journal_v1.shares share ON share.share_id=d.discovery_share_id
 JOIN public.accepted_block_submission_receipts_v1 receipt ON receipt.candidate_id=c.candidate_id
 JOIN public.block_submission_attempts_v1 attempt ON attempt.attempt_id=receipt.source_attempt_id AND attempt.candidate_id=c.candidate_id
 WHERE f.conflict_id=` + p + ` AND state.state='conflict'
 AND f.conflict_kind='unpriced_discovery_mismatch' AND f.difference_mask='discovery_share'
 AND f.related_attempt_id IS NULL AND f.existing_frozen_digest=c.frozen_digest
 AND f.incoming_frozen_digest=c.frozen_digest AND f.incoming_frozen_payload=c.frozen_payload
 AND u.reason='discovery_share' AND u.original_work_digest=d.work_digest
 AND u.incoming_share_id=d.discovery_share_id AND share.share_id=c.share_pow_hash
 AND u.incoming_miner_address=share.miner_address AND u.incoming_share_work>0
 AND u.incoming_share_work<>share.work AND share.work>0
 AND u.incoming_fee_bits=d.fee_float_bits AND u.incoming_target_work=policy.target_work
 AND u.incoming_policy=policy.boundary_policy
 AND attempt.phase='completed' AND attempt.decision='accepted_final'
 AND attempt.accepted=true AND attempt.final=true AND attempt.retryable=false
 AND attempt.code='accepted' AND attempt.response_truncated=false
 AND attempt.block_hash=c.expected_block_hash AND attempt.height=c.height
 AND receipt.block_hash=attempt.block_hash AND receipt.chain_id=c.chain_id
 AND receipt.candidate_height=c.height AND receipt.core_height=attempt.height
 AND receipt.core_template_seq=attempt.template_seq AND receipt.core_code='accepted'))`
}

func (s *PostgresStorage) verifyUnpricedSubmissionHoldV1(ctx context.Context, chainID *big.Int, backend string) (*[16]byte, error) {
	raw := os.Getenv(unpricedSubmissionHoldEnvV1)
	if raw == "" {
		return nil, nil
	}
	id, err := canonicalRecoveryHex(raw, 16)
	if err != nil {
		return nil, errors.New("invalid unpriced submission hold conflict id")
	}
	var eligible bool
	if err := s.pool.QueryRow(ctx, `SELECT `+unpricedSubmissionHoldSQLV1(1), id).Scan(&eligible); err != nil || !eligible {
		return nil, errors.New("unpriced submission hold evidence does not qualify")
	}
	var candidate []byte
	if err := s.pool.QueryRow(ctx, `SELECT candidate_id FROM public.block_submission_conflicts_v1 WHERE conflict_id=$1`, id).Scan(&candidate); err != nil || len(candidate) != 32 {
		return nil, errors.New("unpriced submission hold candidate unavailable")
	}
	var cid [32]byte
	copy(cid[:], candidate)
	view, err := scanCandidateView(ctx, s.pool, cid)
	if err != nil || view.Candidate.ChainID.Cmp(chainID) != 0 || view.Candidate.BackendID != backend {
		return nil, errors.New("unpriced submission hold candidate identity differs")
	}
	var hold [16]byte
	copy(hold[:], id)
	return &hold, nil
}

// UnpricedSubmissionHoldActive identifies the explicitly qualified hold.
func (s *PostgresStorage) UnpricedSubmissionHoldActive() bool {
	return s != nil && s.unpricedSubmissionHold.Load() != nil
}

func (s *PostgresStorage) rewardCandidateAcceptedV1(ctx context.Context, q queryRower, view CandidateViewV1) (bool, error) {
	if view.State == SubmissionStateAcceptedFinal {
		return true, nil
	}
	hold := s.unpricedSubmissionHold.Load()
	if hold == nil || view.State != SubmissionStateConflict {
		return false, nil
	}
	var accepted bool
	err := q.QueryRow(ctx, `SELECT `+unpricedSubmissionHoldSQLV1(1)+` AND EXISTS(SELECT 1 FROM public.block_submission_conflicts_v1 WHERE conflict_id=$1 AND candidate_id=$2)`, hold[:], view.Candidate.CandidateID[:]).Scan(&accepted)
	return accepted, err
}

// The aliases are internal constants supplied by the two reward selectors.
func (s *PostgresStorage) rewardAcceptedSelectorV1(stateAlias, candidateExpr string, firstParam int) (string, []any) {
	regular := stateAlias + `.state='accepted_final'`
	hold := s.unpricedSubmissionHold.Load()
	if hold == nil {
		return regular, nil
	}
	p := fmt.Sprintf("$%d", firstParam)
	return `(` + regular + ` OR (` + stateAlias + `.state='conflict' AND EXISTS(SELECT 1 FROM public.block_submission_conflicts_v1 f WHERE f.conflict_id=` + p + ` AND f.candidate_id=` + candidateExpr + `) AND ` + unpricedSubmissionHoldSQLV1(firstParam) + `))`, []any{hold[:]}
}
