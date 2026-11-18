package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UnpricedDiscoveryV1 is an immutable work boundary, not a payout or reward
// assertion. It must only be consumed after separate native-chain, reward,
// finality, wallet, schema and payer authorization checks.
type UnpricedDiscoveryV1 struct {
	Candidate  CandidateViewV1
	Work       sharejournal.Capture
	WorkDigest [32]byte
}

func unpricedWorkDigest(candidateID [32]byte, shareID []byte, feeBits, target uint64, policy sharejournal.BoundaryPolicy, cut int64, work map[string]uint64, total uint64) ([32]byte, error) {
	var zero [32]byte
	if candidateID == zero || (len(shareID) != 0 && len(shareID) != 32) || target == 0 || cut < 0 || (policy != sharejournal.WholeShare && policy != sharejournal.PartialShare) {
		return zero, errors.New("invalid unpriced discovery digest inputs")
	}
	miners := make([]string, 0, len(work))
	var sum uint64
	for miner, amount := range work {
		if len(miner) != 42 || amount == 0 || math.MaxUint64-sum < amount {
			return zero, errors.New("invalid unpriced work allocation")
		}
		sum += amount
		miners = append(miners, miner)
	}
	if sum != total || total > target || len(miners) > 100000 {
		return zero, errors.New("inconsistent unpriced work total")
	}
	sort.Strings(miners)
	h := sha256.New()
	h.Write([]byte("BDAG-POOL-UNPRICED-DISCOVERY/v1\x00"))
	h.Write(candidateID[:])
	if len(shareID) == 0 {
		h.Write([]byte{0})
	} else {
		h.Write([]byte{1})
		h.Write(shareID)
	}
	writeNum := func(n uint64) { var b [8]byte; binary.BigEndian.PutUint64(b[:], n); h.Write(b[:]) }
	writeNum(feeBits)
	writeNum(target)
	writeNum(uint64(cut))
	writeNum(total)
	writeNum(uint64(len(miners)))
	writeNum(uint64(len(policy)))
	h.Write([]byte(policy))
	for _, miner := range miners {
		writeNum(uint64(len(miner)))
		h.Write([]byte(miner))
		writeNum(work[miner])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}

// PutUnpricedDiscoveryV1 atomically inserts the frozen block candidate, its
// accepted discovery share (if any), immutable share cut and fee policy, and
// pending submission state. It deliberately creates no monetary intent. The
// share and candidate must already have been independently validated by the
// submit pipeline. This experimental method is not wired into startup, submit,
// or payout: existing V2 startup verification correctly rejects its rows.
func (s *PostgresStorage) PutUnpricedDiscoveryV1(ctx context.Context, candidate FrozenCandidateV1, share *sharejournal.Share, target uint64, policy sharejournal.BoundaryPolicy, feePercent float64) (UnpricedDiscoveryV1, PutDisposition, error) {
	var empty UnpricedDiscoveryV1
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil {
		return empty, "", errors.New("unpriced discovery database or context unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return empty, "", errors.New("unpriced discovery requires a deadline")
	}
	if err := validateFrozenCandidate(candidate); err != nil {
		return empty, "", err
	}
	if math.IsNaN(feePercent) || math.IsInf(feePercent, 0) || feePercent < 0 || feePercent > 100 || target == 0 || (policy != sharejournal.WholeShare && policy != sharejournal.PartialShare) || bytes.Equal(candidate.HeaderBytes[148:168], make([]byte, 20)) {
		return empty, "", errors.New("unpriced discovery policy or reward wallet invalid")
	}
	var shareID []byte
	if share != nil {
		if share.ID != candidate.SharePowHash || share.ID == ([32]byte{}) || share.Sequence != 0 || share.Work == 0 || !common.IsHexAddress(share.Miner) || common.HexToAddress(share.Miner) == (common.Address{}) {
			return empty, "", errors.New("discovery share does not match candidate proof")
		}
		shareID = append([]byte(nil), share.ID[:]...)
	}
	feeBits := math.Float64bits(feePercent)
	raw, err := s.pool.Begin(ctx)
	if err != nil {
		return empty, "", err
	}
	defer rollbackUnpricedTx(raw)
	tx, ok := raw.(*pgxpool.Tx)
	if !ok {
		return empty, "", errors.New("unpriced discovery requires a top-level pool transaction")
	}
	latchSQL, latchArgs := s.conflictLatchSQLV1(1)
	var latched bool
	if err = tx.QueryRow(ctx, `SELECT `+latchSQL, latchArgs...).Scan(&latched); err != nil {
		return empty, "", err
	}
	if latched {
		return empty, PutDispositionConflict, errors.New("submission conflict latch is set")
	}
	if err = submissionFaultV1("unpriced_candidate_insert", false); err != nil {
		return empty, "", err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO block_submission_candidates_v1 (candidate_id,frozen_digest,frozen_payload,chain_id,expected_block_hash,header_bytes,extra_nonce2,nonce_bytes,share_pow_hash,parent_hash,height,template_seq,backend_id,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,COALESCE($14,clock_timestamp())) ON CONFLICT DO NOTHING`, candidate.CandidateID[:], candidate.FrozenDigest[:], candidate.FrozenPayload, candidate.ChainID.String(), candidate.ExpectedBlockHash[:], candidate.HeaderBytes[:], candidate.ExtraNonce2[:], candidate.NonceBytes[:], candidate.SharePowHash[:], candidate.ParentHash[:], candidate.Height.String(), candidate.TemplateSeq.String(), candidate.BackendID, nullableTime(candidate.CreatedAt))
	if err != nil {
		return empty, "", err
	}
	if err = submissionFaultV1("unpriced_candidate_insert", true); err != nil {
		return empty, "", err
	}
	inserted := tag.RowsAffected() == 1
	if !inserted {
		view, lookupErr := scanCandidateView(ctx, tx, candidate.CandidateID)
		if lookupErr != nil {
			return empty, "", fmt.Errorf("existing candidate is unavailable or collided: %w", lookupErr)
		}
		var storedShare, digest []byte
		var feeRaw, totalRaw, policyRaw, targetRaw string
		var storedMiner, storedWork *string
		if err = tx.QueryRow(ctx, `SELECT d.discovery_share_id,d.fee_float_bits::text,d.total_work::text,d.work_digest,p.boundary_policy,p.target_work::text,s.miner_address,s.work::text FROM pool_share_journal_v1.unpriced_discoveries_v1 d JOIN pool_share_journal_v1.allocation_policies p USING(candidate_id) LEFT JOIN pool_share_journal_v1.shares s ON s.share_id=d.discovery_share_id WHERE d.candidate_id=$1`, candidate.CandidateID[:]).Scan(&storedShare, &feeRaw, &totalRaw, &digest, &policyRaw, &targetRaw, &storedMiner, &storedWork); err != nil {
			return empty, "", fmt.Errorf("candidate lacks an unpriced discovery: %w", err)
		}
		storedFee, feeErr := strconv.ParseUint(feeRaw, 10, 64)
		storedTotal, totalErr := strconv.ParseUint(totalRaw, 10, 64)
		storedTarget, targetErr := strconv.ParseUint(targetRaw, 10, 64)
		if feeErr != nil || totalErr != nil || targetErr != nil || len(digest) != 32 {
			return empty, "", errors.New("stored unpriced discovery is malformed")
		}
		if !candidateMatchesRefreshedJob(view.Candidate, candidate) {
			return recordUnpricedConflict(ctx, tx, view, candidate, "candidate_identity", digest, share, target, policy, feeBits)
		}
		if storedFee != feeBits || storedTarget != target || policyRaw != string(policy) || !bytes.Equal(storedShare, shareID) {
			return recordUnpricedConflict(ctx, tx, view, candidate, "discovery_policy", digest, share, target, policy, feeBits)
		}
		if share != nil {
			if !common.IsHexAddress(share.Miner) || share.Sequence != 0 || share.Work == 0 || storedMiner == nil || storedWork == nil || common.HexToAddress(share.Miner) == (common.Address{}) || common.HexToAddress(share.Miner).Hex() != *storedMiner || strconv.FormatUint(share.Work, 10) != *storedWork {
				return recordUnpricedConflict(ctx, tx, view, candidate, "discovery_share", digest, share, target, policy, feeBits)
			}
		}
		// The stored share identity and work were checked above. Reuse the
		// original candidate cut without appending or moving it on a retry.
		capture, captureErr := sharejournal.CaptureWorkTx(ctx, tx, candidate.CandidateID, nil, target, policy)
		if captureErr != nil {
			return empty, "", captureErr
		}
		got, digestErr := unpricedWorkDigest(candidate.CandidateID, shareID, feeBits, target, policy, capture.Sequence, capture.Work, capture.Total)
		if digestErr != nil || capture.Total != storedTotal || !bytes.Equal(got[:], digest) {
			return recordUnpricedConflict(ctx, tx, view, candidate, "discovery_allocation", digest, share, target, policy, feeBits)
		}
		if err = commitSubmissionTxV1(ctx, tx, "unpriced_existing_commit"); err != nil {
			return empty, "", fmt.Errorf("existing discovery commit unproven: %w", err)
		}
		return UnpricedDiscoveryV1{Candidate: view, Work: capture, WorkDigest: got}, PutDispositionExisting, nil
	}
	capture, err := sharejournal.CaptureNewDiscoveryTx(ctx, tx, candidate.CandidateID, share, target, policy)
	if err != nil {
		return empty, "", err
	}
	digest, err := unpricedWorkDigest(candidate.CandidateID, shareID, feeBits, target, policy, capture.Sequence, capture.Work, capture.Total)
	if err != nil {
		return empty, "", err
	}
	if err = submissionFaultV1("unpriced_marker_insert", false); err != nil {
		return empty, "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.unpriced_discoveries_v1 (candidate_id,discovery_share_id,fee_float_bits,total_work,work_digest) VALUES ($1,$2,$3::numeric,$4::numeric,$5)`, candidate.CandidateID[:], shareID, strconv.FormatUint(feeBits, 10), strconv.FormatUint(capture.Total, 10), digest[:]); err != nil {
		return empty, "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO block_submission_state_v1(candidate_id,state,state_version,attempt_count,next_attempt_at,updated_at) VALUES ($1,'pending',1,0,clock_timestamp(),clock_timestamp())`, candidate.CandidateID[:]); err != nil {
		return empty, "", err
	}
	view, err := scanCandidateView(ctx, tx, candidate.CandidateID)
	if err != nil {
		return empty, "", err
	}
	if err = commitSubmissionTxV1(ctx, tx, "unpriced_discovery_commit"); err != nil {
		return empty, "", fmt.Errorf("unpriced discovery commit unproven: %w", err)
	}
	return UnpricedDiscoveryV1{Candidate: view, Work: capture, WorkDigest: digest}, PutDispositionInserted, nil
}

func recordUnpricedConflict(ctx context.Context, tx pgx.Tx, view CandidateViewV1, incoming FrozenCandidateV1, reason string, originalDigest []byte, share *sharejournal.Share, target uint64, policy sharejournal.BoundaryPolicy, feeBits uint64) (UnpricedDiscoveryV1, PutDisposition, error) {
	var empty UnpricedDiscoveryV1
	if len(originalDigest) != 32 {
		return empty, "", errors.New("unpriced conflict lacks original work digest")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return empty, "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO block_submission_conflicts_v1 (conflict_id,candidate_id,conflict_kind,existing_frozen_digest,incoming_frozen_digest,incoming_frozen_payload,difference_mask,observed_at) VALUES ($1,$2,'unpriced_discovery_mismatch',$3,$4,$5,$6,clock_timestamp())`, id[:], view.Candidate.CandidateID[:], view.Candidate.FrozenDigest[:], incoming.FrozenDigest[:], incoming.FrozenPayload, reason); err != nil {
		return empty, "", err
	}
	var shareID, miner, work any
	if share != nil {
		shareID = share.ID[:]
		miner = common.HexToAddress(share.Miner).Hex()
		work = strconv.FormatUint(share.Work, 10)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.unpriced_conflicts_v1 (conflict_id,candidate_id,reason,original_work_digest,incoming_fee_bits,incoming_target_work,incoming_policy,incoming_share_id,incoming_miner_address,incoming_share_work) VALUES ($1,$2,$3,$4,$5::numeric,$6::numeric,$7,$8,$9,$10::numeric)`, id[:], view.Candidate.CandidateID[:], reason, originalDigest, strconv.FormatUint(feeBits, 10), strconv.FormatUint(target, 10), string(policy), shareID, miner, work); err != nil {
		return empty, "", err
	}
	if err := latchCandidateConflict(ctx, tx, view.Candidate.CandidateID); err != nil {
		return empty, "", err
	}
	if err := commitSubmissionTxV1(ctx, tx, "unpriced_conflict_commit"); err != nil {
		return empty, "", fmt.Errorf("unpriced conflict commit unproven: %w", err)
	}
	view.State = SubmissionStateConflict
	return UnpricedDiscoveryV1{Candidate: view}, PutDispositionConflict, errors.New("unpriced discovery conflict: " + reason)
}

func rollbackUnpricedTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
