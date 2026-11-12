package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// Only a fully verified startup can populate this allowlist. It is scoped to
// one exact immutable conflict/proof/receipt/effect, not a class of conflicts.
// Newly inserted recovery rows are NOT automatically trusted by runtime SQL.
type jobRecoveryAllowanceV1 struct {
	conflictID, candidateID, attemptID, proofPayload, proofDigest              []byte
	frozenDigest, frozenPayload, incomingDigest, incomingPayload, intentDigest []byte
	coreSequence                                                               *big.Int
	acceptedAt                                                                 time.Time
}

func (s *PostgresStorage) verifyJobRecoveryAllowanceV1(ctx context.Context) (*jobRecoveryAllowanceV1, error) {
	if err := verifyRecoveryCatalogV1(ctx, s.pool); err != nil {
		return nil, err
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM block_submission_job_recoveries_v1`).Scan(&count); err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, errors.New("restart requires exactly one explicit verified recovery")
	}
	a := new(jobRecoveryAllowanceV1)
	if err := s.pool.QueryRow(ctx, `SELECT conflict_id,candidate_id,source_attempt_id,proof_payload,proof_sha256 FROM block_submission_job_recoveries_v1`).Scan(&a.conflictID, &a.candidateID, &a.attemptID, &a.proofPayload, &a.proofDigest); err != nil {
		return nil, err
	}
	if _, err := decodeUniqueJSONObjectV1(a.proofPayload); err != nil {
		return nil, err
	}
	var proof RefreshedJobRecoveryProofV1
	if err := json.Unmarshal(a.proofPayload, &proof); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(proof)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(canonical)
	if !bytes.Equal(canonical, a.proofPayload) || !bytes.Equal(digest[:], a.proofDigest) || hex.EncodeToString(a.conflictID) != proof.ConflictID || hex.EncodeToString(a.candidateID) != proof.CandidateID || hex.EncodeToString(a.attemptID) != proof.AttemptID {
		return nil, errors.New("recovery record/proof identity mismatch")
	}
	result, err := s.RecoverRefreshedJobV1(ctx, proof, false)
	if err != nil {
		return nil, err
	}
	if !result.AlreadyApplied {
		return nil, errors.New("recovery is not durably applied")
	}
	var cid [32]byte
	copy(cid[:], a.candidateID)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	intent, err := readAcceptedAccountingIntentTxV2(ctx, tx, cid, false)
	if err != nil {
		return nil, err
	}
	if err := verifyAppliedAcceptedAccountingTxV2(ctx, tx, intent); err != nil {
		return nil, fmt.Errorf("recovered accounting effect not verified: %w", err)
	}
	a.intentDigest = append([]byte(nil), intent.IntentDigest[:]...)
	var sequence string
	if err := tx.QueryRow(ctx, `SELECT c.frozen_digest,c.frozen_payload,f.incoming_frozen_digest,f.incoming_frozen_payload,r.core_template_seq::text,r.accepted_at FROM block_submission_candidates_v1 c JOIN block_submission_conflicts_v1 f USING(candidate_id) JOIN accepted_block_submission_receipts_v1 r USING(candidate_id) WHERE f.conflict_id=$1 AND c.candidate_id=$2`, a.conflictID, a.candidateID).Scan(&a.frozenDigest, &a.frozenPayload, &a.incomingDigest, &a.incomingPayload, &sequence, &a.acceptedAt); err != nil {
		return nil, err
	}
	a.coreSequence, _ = new(big.Int).SetString(sequence, 10)
	if a.coreSequence == nil {
		return nil, errors.New("invalid recovered core sequence")
	}
	return a, nil
}

// SQL remains an atomic latch check at each Put/Claim. Missing/tampered proof,
// missing receipt/effect, changed original intent, ANY other conflict, or ANY
// accounting conflict all keep the gate closed. Values are bind parameters.
func (s *PostgresStorage) conflictLatchSQLV1(firstParam int) (string, []any) {
	a := s.jobRecovery.Load()
	if a == nil {
		q, args := s.accountingConflictLatchSQLV1(firstParam)
		return `(EXISTS(SELECT 1 FROM block_submission_conflicts_v1) OR ` + q + `)`, args
	}
	p := func(n int) string { return fmt.Sprintf("$%d", firstParam+n) }
	query := `(EXISTS(SELECT 1 FROM block_submission_conflicts_v1 WHERE conflict_id<>` + p(0) + `)
 OR NOT EXISTS(SELECT 1 FROM block_submission_job_recoveries_v1 r
 JOIN block_submission_conflicts_v1 f USING(conflict_id)
 JOIN block_submission_candidates_v1 c ON c.candidate_id=r.candidate_id
 JOIN block_submission_attempts_v1 a ON a.attempt_id=r.source_attempt_id
 JOIN accepted_block_submission_receipts_v1 receipt ON receipt.candidate_id=r.candidate_id
 JOIN accepted_block_accounting_intents_v1 i ON i.candidate_id=r.candidate_id
 JOIN accepted_block_accounting_effects_v1 e ON e.candidate_id=r.candidate_id
 WHERE r.conflict_id=` + p(0) + ` AND r.candidate_id=` + p(1) + ` AND r.source_attempt_id=` + p(2) + `
 AND r.proof_payload=` + p(3) + ` AND r.proof_sha256=` + p(4) + `
 AND f.candidate_id=r.candidate_id AND f.conflict_kind='candidate_identity_mismatch'
 AND f.difference_mask='frozen_digest|frozen_payload|template_seq'
 AND f.existing_frozen_digest=` + p(5) + ` AND c.frozen_digest=` + p(5) + ` AND c.frozen_payload=` + p(6) + `
 AND f.incoming_frozen_digest=` + p(7) + ` AND f.incoming_frozen_payload=` + p(8) + `
 AND a.candidate_id=r.candidate_id AND a.phase='completed' AND a.decision='accepted_final'
 AND a.accepted=true AND a.final=true AND a.retryable=false AND a.code='accepted'
 AND a.block_hash=c.expected_block_hash AND a.height=c.height
 AND receipt.source_attempt_id=a.attempt_id AND receipt.block_hash=a.block_hash
 AND receipt.chain_id=c.chain_id AND receipt.candidate_height=c.height
 AND receipt.core_height=a.height AND receipt.core_template_seq=a.template_seq
 AND receipt.core_code='accepted' AND receipt.accepted_at=a.completed_at
 AND i.intent_digest=` + p(9) + ` AND e.intent_digest=i.intent_digest
 AND e.legacy_block_hash=i.legacy_block_hash AND e.credit_line_count=i.credit_line_count))`
	args := []any{a.conflictID, a.candidateID, a.attemptID, a.proofPayload, a.proofDigest, a.frozenDigest, a.frozenPayload, a.incomingDigest, a.incomingPayload, a.intentDigest}
	economic, extra := s.accountingConflictLatchSQLV1(firstParam + len(args))
	return `(` + query + ` OR ` + economic + `)`, append(args, extra...)
}

func (s *PostgresStorage) recoveredCandidateViewV1(v CandidateViewV1) CandidateViewV1 {
	if a := s.discoveryRecovery.Load(); a != nil && v.State == SubmissionStateConflict && bytes.Equal(v.Candidate.CandidateID[:], a.candidateID) {
		return recoveredAcceptedViewV1(v, a.coreSequence, a.acceptedAt)
	}
	a := s.jobRecovery.Load()
	if a == nil || v.State != SubmissionStateConflict || !bytes.Equal(v.Candidate.CandidateID[:], a.candidateID) {
		return v
	}
	return recoveredAcceptedViewV1(v, a.coreSequence, a.acceptedAt)
}

func recoveredAcceptedViewV1(v CandidateViewV1, sequence *big.Int, at time.Time) CandidateViewV1 {
	v.HistoricalState = v.State
	v.Recovered = true
	v.State = SubmissionStateAcceptedFinal
	v.FinalCode = "accepted"
	v.AuthoritativeBlockHash = append([]byte(nil), v.Candidate.ExpectedBlockHash[:]...)
	v.AuthoritativeHeight = new(big.Int).Set(v.Candidate.Height)
	v.AuthoritativeTemplateSeq = new(big.Int).Set(sequence)
	acceptedAt := at
	v.FinalizedAt = &acceptedAt
	return v
}
