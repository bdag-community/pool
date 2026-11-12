package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// This recovery is deliberately limited to the evidenced single-recipient,
// zero-fee repeated-discovery race. It cannot approve changed money/recipients.
func validateDiscoveryConflictV1(ctx context.Context, tx pgx.Tx, c FrozenCandidateV1, fid []byte, p RefreshedJobRecoveryProofV1) error {
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_conflicts_v1`).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return errors.New("discovery recovery requires exactly one economic conflict")
	}
	var kind, detail string
	var existing, incoming []byte
	if err := tx.QueryRow(ctx, `SELECT conflict_kind,existing_intent_digest,incoming_intent_digest,detail FROM accepted_block_accounting_conflicts_v1 WHERE conflict_id=$1 AND candidate_id=$2`, fid, c.CandidateID[:]).Scan(&kind, &existing, &incoming, &detail); err != nil {
		return err
	}
	if len(existing) != 32 || len(incoming) != 32 || kind != "candidate_intent_mismatch" || detail != "stored accounting v2 intent is missing or differs" || hex.EncodeToString(existing) != p.OriginalIntentDigest || hex.EncodeToString(incoming) != p.IncomingIntentDigest {
		return errors.New("discovery proof does not bind exact conflict")
	}
	first, err := readAcceptedAccountingIntentTxV2(ctx, tx, c.CandidateID, true)
	if err != nil {
		return err
	}
	if len(first.CreditLines) != 1 || first.FeeAmount != "0" || first.PoolFeeFloatBits != 0 || first.IntentDigest != *(*[32]byte)(existing) || first.LegacyBlockHash != c.SharePowHash || first.Height != c.Height.Uint64() {
		return errors.New("unsupported discovery allocation")
	}
	reward := new(big.Int).Mul(new(big.Int).SetUint64(binary.LittleEndian.Uint64(c.HeaderBytes[168:176])), big.NewInt(10000000000)).String()
	if first.GrossReward != reward || first.Distributable != reward || first.CreditLines[0].Amount != reward || strings.ToLower(first.CreditLines[0].MinerAddress) != "0x"+hex.EncodeToString(c.HeaderBytes[148:168]) {
		return errors.New("original allocation differs from accepted header reward/recipient")
	}
	delta, err := strconv.ParseUint(p.AdditionalWork, 10, 64)
	if err != nil || delta == 0 || strconv.FormatUint(delta, 10) != p.AdditionalWork || delta > first.TotalWork || math.MaxUint64-first.TotalWork < delta {
		return errors.New("invalid discovery work increment")
	}
	replay := first
	replay.CreditLines = append([]AcceptedAccountingCreditLineV2(nil), first.CreditLines...)
	replay.TotalWork += delta
	replay.CreditLines[0].Work += delta
	replay, err = SealAcceptedAccountingIntentV2(replay)
	if err != nil || !bytes.Equal(replay.IntentDigest[:], incoming) {
		return errors.New("incoming digest is not an exact work-only rediscovery")
	}
	// Any historical job conflict must have its own immutable recovery. It will
	// be revalidated in full before runtime admission; this check is offline only.
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM block_submission_conflicts_v1`).Scan(&count); err != nil {
		return err
	}
	if count > 1 {
		return errors.New("unexpected additional submission conflicts")
	}
	if count == 1 {
		if err := verifyRecoveryCatalogV1(ctx, tx); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM block_submission_conflicts_v1 f JOIN block_submission_job_recoveries_v1 r USING(conflict_id) JOIN accepted_block_submission_receipts_v1 a ON a.candidate_id=r.candidate_id JOIN accepted_block_accounting_effects_v1 e ON e.candidate_id=r.candidate_id WHERE f.candidate_id=r.candidate_id AND a.source_attempt_id=r.source_attempt_id`).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return errors.New("historical submission conflict has no qualified recovery")
		}
	}
	// Never attach new evidence to an unrelated existing legacy credit.
	var receipts, effects, blocks, credits int
	if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1),(SELECT count(*) FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1),(SELECT count(*) FROM blocks WHERE hash=$2),(SELECT count(*) FROM credits WHERE block_hash=$2)`, c.CandidateID[:], hex.EncodeToString(first.LegacyBlockHash[:])).Scan(&receipts, &effects, &blocks, &credits); err != nil {
		return err
	}
	if receipts == 0 && (effects != 0 || blocks != 0 || credits != 0) {
		return errors.New("discovery candidate has pre-existing effects without receipt")
	}
	return nil
}

type discoveryRecoveryAllowanceV1 struct {
	candidateID                                                         []byte
	coreSequence                                                        *big.Int
	acceptedAt                                                          time.Time
	conflictID                                                          []byte
	proof, conflict, candidate, attempt, receipt, intent, lines, effect string
}

func (s *PostgresStorage) verifyDiscoveryRecoveryAllowanceV1(ctx context.Context) (*discoveryRecoveryAllowanceV1, error) {
	var conflicts int
	var relation *string
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_conflicts_v1`).Scan(&conflicts); err != nil {
		return nil, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass('block_submission_discovery_recoveries_v1')::text`).Scan(&relation); err != nil {
		return nil, err
	}
	if relation == nil {
		if conflicts != 0 {
			return nil, errors.New("unresolved economic conflict")
		}
		return nil, nil
	}
	if conflicts != 1 {
		return nil, errors.New("discovery recovery conflict count changed")
	}
	if err := verifyRecoveryCatalogForV1(ctx, s.pool, true); err != nil {
		return nil, err
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM block_submission_discovery_recoveries_v1`).Scan(&count); err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, errors.New("requires exactly one explicit discovery recovery")
	}
	var raw, digest, fid, cid, aid []byte
	if err := s.pool.QueryRow(ctx, `SELECT proof_payload,proof_sha256,conflict_id,candidate_id,source_attempt_id FROM block_submission_discovery_recoveries_v1`).Scan(&raw, &digest, &fid, &cid, &aid); err != nil {
		return nil, err
	}
	if _, err := decodeUniqueJSONObjectV1(raw); err != nil {
		return nil, err
	}
	var p RefreshedJobRecoveryProofV1
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	if !bytes.Equal(raw, canonical) || !bytes.Equal(digest, sum[:]) || p.Version != "discovery-cut-recovery/v1" || p.ConflictID != hex.EncodeToString(fid) || p.CandidateID != hex.EncodeToString(cid) || p.AttemptID != hex.EncodeToString(aid) {
		return nil, errors.New("discovery record/proof identity mismatch")
	}
	result, err := s.RecoverRefreshedJobV1(ctx, p, false)
	if err != nil || !result.AlreadyApplied {
		return nil, fmt.Errorf("unqualified discovery recovery: %v", err)
	}
	a := &discoveryRecoveryAllowanceV1{candidateID: cid, conflictID: fid}
	var seq string
	if err := s.pool.QueryRow(ctx, `SELECT to_jsonb(r)::text,to_jsonb(f)::text,to_jsonb(c)::text,to_jsonb(a)::text,to_jsonb(receipt)::text,to_jsonb(i)::text,(SELECT coalesce(jsonb_agg(to_jsonb(l) ORDER BY line_index),'[]'::jsonb)::text FROM accepted_block_accounting_credit_lines_v1 l WHERE l.candidate_id=r.candidate_id),to_jsonb(e)::text,receipt.core_template_seq::text,receipt.accepted_at FROM block_submission_discovery_recoveries_v1 r JOIN accepted_block_accounting_conflicts_v1 f USING(conflict_id) JOIN block_submission_candidates_v1 c ON c.candidate_id=r.candidate_id JOIN block_submission_attempts_v1 a ON a.attempt_id=r.source_attempt_id JOIN accepted_block_submission_receipts_v1 receipt ON receipt.candidate_id=r.candidate_id JOIN accepted_block_accounting_intents_v1 i ON i.candidate_id=r.candidate_id JOIN accepted_block_accounting_effects_v1 e ON e.candidate_id=r.candidate_id WHERE r.candidate_id=$1`, cid).Scan(&a.proof, &a.conflict, &a.candidate, &a.attempt, &a.receipt, &a.intent, &a.lines, &a.effect, &seq, &a.acceptedAt); err != nil {
		return nil, err
	}
	a.coreSequence, _ = new(big.Int).SetString(seq, 10)
	if a.coreSequence == nil {
		return nil, errors.New("invalid discovery recovered sequence")
	}
	return a, nil
}

// Atomic gate pins every immutable proof/source/effect row. New conflicts or
// post-startup edits fail closed; inserting a new resolution grants no trust.
func (s *PostgresStorage) accountingConflictLatchSQLV1(firstParam int) (string, []any) {
	a := s.discoveryRecovery.Load()
	if a == nil {
		return `EXISTS(SELECT 1 FROM accepted_block_accounting_conflicts_v1)`, nil
	}
	p := func(n int) string { return fmt.Sprintf("$%d", firstParam+n) }
	q := `(EXISTS(SELECT 1 FROM accepted_block_accounting_conflicts_v1 WHERE conflict_id<>` + p(0) + `)
 OR (SELECT count(*) FROM block_submission_discovery_recoveries_v1)<>1
 OR NOT EXISTS(SELECT 1 FROM block_submission_discovery_recoveries_v1 r
 JOIN accepted_block_accounting_conflicts_v1 f USING(conflict_id)
 JOIN block_submission_candidates_v1 c ON c.candidate_id=r.candidate_id
 JOIN block_submission_attempts_v1 a ON a.attempt_id=r.source_attempt_id
 JOIN accepted_block_submission_receipts_v1 receipt ON receipt.candidate_id=r.candidate_id
 JOIN accepted_block_accounting_intents_v1 i ON i.candidate_id=r.candidate_id
 JOIN accepted_block_accounting_effects_v1 e ON e.candidate_id=r.candidate_id
 WHERE r.conflict_id=` + p(0) + ` AND to_jsonb(r)=` + p(1) + `::jsonb AND to_jsonb(f)=` + p(2) + `::jsonb
 AND to_jsonb(c)=` + p(3) + `::jsonb AND to_jsonb(a)=` + p(4) + `::jsonb
 AND to_jsonb(receipt)=` + p(5) + `::jsonb AND to_jsonb(i)=` + p(6) + `::jsonb
 AND (SELECT coalesce(jsonb_agg(to_jsonb(l) ORDER BY line_index),'[]'::jsonb) FROM accepted_block_accounting_credit_lines_v1 l WHERE l.candidate_id=r.candidate_id)=` + p(7) + `::jsonb
 AND to_jsonb(e)=` + p(8) + `::jsonb
 AND EXISTS(SELECT 1 FROM blocks b WHERE b.hash=encode(i.legacy_block_hash,'hex') AND b.height=i.height AND b.reward=i.gross_reward AND b.fees=i.fee_amount)
 AND (SELECT count(*) FROM credits cr WHERE cr.block_hash=encode(i.legacy_block_hash,'hex'))=i.credit_line_count
 AND NOT EXISTS(SELECT 1 FROM credits cr WHERE cr.block_hash=encode(i.legacy_block_hash,'hex') AND NOT EXISTS(SELECT 1 FROM accepted_block_accounting_credit_lines_v1 l WHERE l.candidate_id=i.candidate_id AND l.miner_address=cr.miner_address AND l.amount=cr.amount))
 AND NOT EXISTS(SELECT 1 FROM accepted_block_accounting_credit_lines_v1 l WHERE l.candidate_id=i.candidate_id AND NOT EXISTS(SELECT 1 FROM credits cr WHERE cr.block_hash=encode(i.legacy_block_hash,'hex') AND cr.miner_address=l.miner_address AND cr.amount=l.amount))))`
	return q, []any{a.conflictID, a.proof, a.conflict, a.candidate, a.attempt, a.receipt, a.intent, a.lines, a.effect}
}
