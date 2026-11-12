package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

const (
	AcceptedAccountingPolicyV1 = "PPLNS-DISCOVERY-CUT/v1"
	maxAccountingCreditLinesV1 = 100000
	maxAccountingMinerBytesV1  = 512
	maxAccountingReconcileV1   = 1000
)

var acceptedAccountingPrefixV1 = append([]byte("BDAG-POOL-ACCEPTED-ACCOUNTING-INTENT/v1"), 0)

// AcceptedAccountingCreditLineV1 is a final discovery-time credit allocation.
// Lines are persisted and encoded in strictly increasing miner-address order.
type AcceptedAccountingCreditLineV1 struct {
	MinerAddress string
	Work         uint64
	Amount       uint64
}

// AcceptedAccountingIntentV1 contains every economic input and final credit
// output needed to materialize legacy block accounting after process restart.
// No current JobData, configuration, or PPLNS state is consulted on replay.
type AcceptedAccountingIntentV1 struct {
	CandidateID      [32]byte
	IntentDigest     [32]byte
	PolicyVersion    string
	LegacyBlockHash  [32]byte
	Height           uint64
	GrossReward      uint64
	PoolFeeFloatBits uint64
	FeeAmount        uint64
	Distributable    uint64
	TotalWork        uint64
	CreditLines      []AcceptedAccountingCreditLineV1
}

type AcceptedAccountingApplyResultV1 struct {
	CandidateID    [32]byte
	Applied        bool
	AlreadyApplied bool
	CreditLines    int64
}

type AcceptedAccountingReconcileResultV1 struct {
	Applied int64
}

// Installed only by storage-package fault tests. Production always leaves it
// nil. The after-commit hook deliberately models commit acknowledgement loss.
var acceptedAccountingFaultHookV1 func(point string, after bool) error

func acceptedAccountingFaultV1(point string, after bool) error {
	if acceptedAccountingFaultHookV1 == nil {
		return nil
	}
	return acceptedAccountingFaultHookV1(point, after)
}

func commitAcceptedAccountingTxV1(ctx context.Context, tx pgx.Tx, point string) error {
	if err := acceptedAccountingFaultV1(point, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return acceptedAccountingFaultV1(point, true)
}

func SealAcceptedAccountingIntentV1(intent AcceptedAccountingIntentV1) (AcceptedAccountingIntentV1, error) {
	intent.PolicyVersion = AcceptedAccountingPolicyV1
	intent.CreditLines = append([]AcceptedAccountingCreditLineV1(nil), intent.CreditLines...)
	sort.Slice(intent.CreditLines, func(i, j int) bool {
		return intent.CreditLines[i].MinerAddress < intent.CreditLines[j].MinerAddress
	})
	encoded, err := encodeAcceptedAccountingIntentV1(intent)
	if err != nil {
		return AcceptedAccountingIntentV1{}, err
	}
	intent.IntentDigest = sha256.Sum256(encoded)
	return intent, nil
}

func validateAcceptedAccountingIntentV1(intent AcceptedAccountingIntentV1) error {
	encoded, err := encodeAcceptedAccountingIntentV1(intent)
	if err != nil {
		return err
	}
	if sha256.Sum256(encoded) != intent.IntentDigest {
		return errors.New("accepted accounting intent digest mismatch")
	}
	return nil
}

func encodeAcceptedAccountingIntentV1(intent AcceptedAccountingIntentV1) ([]byte, error) {
	if intent.CandidateID == [32]byte{} || intent.LegacyBlockHash == [32]byte{} {
		return nil, errors.New("accepted accounting identity is empty")
	}
	if intent.PolicyVersion != AcceptedAccountingPolicyV1 {
		return nil, fmt.Errorf("unsupported accepted accounting policy %q", intent.PolicyVersion)
	}
	if intent.Height > math.MaxInt64 {
		return nil, errors.New("accepted accounting height exceeds PostgreSQL bigint")
	}
	feePercent := math.Float64frombits(intent.PoolFeeFloatBits)
	if math.IsNaN(feePercent) || math.IsInf(feePercent, 0) || feePercent < 0 || feePercent > 100 {
		return nil, errors.New("accepted accounting pool fee is invalid")
	}
	feeFloat := float64(intent.GrossReward) * (feePercent / 100.0)
	if feeFloat < 0 || feeFloat >= math.Ldexp(1, 64) || feeFloat > float64(intent.GrossReward) || uint64(feeFloat) != intent.FeeAmount {
		return nil, errors.New("accepted accounting fee differs from frozen policy")
	}
	if intent.FeeAmount > intent.GrossReward || intent.Distributable != intent.GrossReward-intent.FeeAmount {
		return nil, errors.New("accepted accounting reward arithmetic is inconsistent")
	}
	if len(intent.CreditLines) > maxAccountingCreditLinesV1 {
		return nil, errors.New("accepted accounting credit-line bound exceeded")
	}
	if intent.TotalWork == 0 && len(intent.CreditLines) != 0 {
		return nil, errors.New("zero-work accepted accounting intent has credit lines")
	}
	var workSum, creditSum uint64
	previous := ""
	for index, line := range intent.CreditLines {
		if !utf8.ValidString(line.MinerAddress) || len(line.MinerAddress) == 0 || len(line.MinerAddress) > maxAccountingMinerBytesV1 || strings.IndexByte(line.MinerAddress, 0) >= 0 {
			return nil, fmt.Errorf("accepted accounting miner address %d is invalid", index)
		}
		if index > 0 && line.MinerAddress <= previous {
			return nil, errors.New("accepted accounting credit lines are not strictly sorted")
		}
		if line.Work == 0 {
			return nil, errors.New("accepted accounting credit line has zero work")
		}
		if math.MaxUint64-workSum < line.Work || math.MaxUint64-creditSum < line.Amount {
			return nil, errors.New("accepted accounting totals overflow")
		}
		workSum += line.Work
		creditSum += line.Amount
		if intent.TotalWork > 0 {
			creditFloat := float64(intent.Distributable) * (float64(line.Work) / float64(intent.TotalWork))
			if creditFloat < 0 || creditFloat >= math.Ldexp(1, 64) {
				return nil, errors.New("accepted accounting credit conversion is out of range")
			}
			want := uint64(creditFloat)
			if line.Amount != want {
				return nil, fmt.Errorf("accepted accounting credit differs for miner %q", line.MinerAddress)
			}
		}
		previous = line.MinerAddress
	}
	if workSum != intent.TotalWork {
		return nil, errors.New("accepted accounting line work differs from total work")
	}
	if creditSum > intent.Distributable {
		return nil, errors.New("accepted accounting credits exceed distributable reward")
	}

	encoded := append([]byte(nil), acceptedAccountingPrefixV1...)
	encoded = appendAccountingBytesV1(encoded, []byte(intent.PolicyVersion))
	encoded = append(encoded, intent.CandidateID[:]...)
	encoded = append(encoded, intent.LegacyBlockHash[:]...)
	for _, value := range []uint64{
		intent.Height,
		intent.GrossReward,
		intent.PoolFeeFloatBits,
		intent.FeeAmount,
		intent.Distributable,
		intent.TotalWork,
		uint64(len(intent.CreditLines)),
	} {
		var fixed [8]byte
		binary.BigEndian.PutUint64(fixed[:], value)
		encoded = append(encoded, fixed[:]...)
	}
	for _, line := range intent.CreditLines {
		encoded = appendAccountingBytesV1(encoded, []byte(line.MinerAddress))
		var fixed [16]byte
		binary.BigEndian.PutUint64(fixed[:8], line.Work)
		binary.BigEndian.PutUint64(fixed[8:], line.Amount)
		encoded = append(encoded, fixed[:]...)
	}
	return encoded, nil
}

func appendAccountingBytesV1(dst, value []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	dst = append(dst, length[:]...)
	return append(dst, value...)
}

func acceptedAccountingIntentEqualV1(left, right AcceptedAccountingIntentV1) bool {
	leftBytes, leftErr := encodeAcceptedAccountingIntentV1(left)
	rightBytes, rightErr := encodeAcceptedAccountingIntentV1(right)
	return leftErr == nil && rightErr == nil && left.IntentDigest == right.IntentDigest && bytes.Equal(leftBytes, rightBytes)
}

func putAcceptedAccountingIntentTxV1(ctx context.Context, tx pgx.Tx, intent AcceptedAccountingIntentV1) error {
	if err := validateAcceptedAccountingIntentV1(intent); err != nil {
		return err
	}
	if err := acceptedAccountingFaultV1("accounting_intent_insert", false); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO accepted_block_accounting_intents_v1
		(candidate_id,intent_digest,policy_version,legacy_block_hash,height,gross_reward,pool_fee_float_bits,fee_amount,distributable,total_work,credit_line_count,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,clock_timestamp())`,
		intent.CandidateID[:], intent.IntentDigest[:], intent.PolicyVersion, intent.LegacyBlockHash[:],
		strconv.FormatUint(intent.Height, 10), strconv.FormatUint(intent.GrossReward, 10), strconv.FormatUint(intent.PoolFeeFloatBits, 10),
		strconv.FormatUint(intent.FeeAmount, 10), strconv.FormatUint(intent.Distributable, 10), strconv.FormatUint(intent.TotalWork, 10), len(intent.CreditLines)); err != nil {
		return fmt.Errorf("insert accepted accounting intent: %w", err)
	}
	if err := acceptedAccountingFaultV1("accounting_intent_insert", true); err != nil {
		return err
	}
	for index, line := range intent.CreditLines {
		if err := acceptedAccountingFaultV1("accounting_credit_line_insert", false); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO accepted_block_accounting_credit_lines_v1
			(candidate_id,line_index,miner_address,work,amount) VALUES ($1,$2,$3,$4,$5)`,
			intent.CandidateID[:], index, line.MinerAddress, strconv.FormatUint(line.Work, 10), strconv.FormatUint(line.Amount, 10)); err != nil {
			return fmt.Errorf("insert accepted accounting credit line %d: %w", index, err)
		}
		if err := acceptedAccountingFaultV1("accounting_credit_line_insert", true); err != nil {
			return err
		}
	}
	return nil
}

func readAcceptedAccountingIntentTxV1(ctx context.Context, tx pgx.Tx, candidateID [32]byte, lock bool) (AcceptedAccountingIntentV1, error) {
	var intent AcceptedAccountingIntentV1
	var candidateBytes, digest, legacyHash []byte
	var height, gross, feeBits, fee, distributable, totalWork string
	var lineCount int
	query := `SELECT candidate_id,intent_digest,policy_version,legacy_block_hash,height::text,gross_reward::text,pool_fee_float_bits::text,fee_amount::text,distributable::text,total_work::text,credit_line_count FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	if err := tx.QueryRow(ctx, query, candidateID[:]).Scan(&candidateBytes, &digest, &intent.PolicyVersion, &legacyHash, &height, &gross, &feeBits, &fee, &distributable, &totalWork, &lineCount); err != nil {
		return intent, err
	}
	if len(candidateBytes) != 32 || len(digest) != 32 || len(legacyHash) != 32 || lineCount < 0 || lineCount > maxAccountingCreditLinesV1 {
		return intent, errors.New("accepted accounting intent fixed fields are invalid")
	}
	copy(intent.CandidateID[:], candidateBytes)
	copy(intent.IntentDigest[:], digest)
	copy(intent.LegacyBlockHash[:], legacyHash)
	values := []*uint64{&intent.Height, &intent.GrossReward, &intent.PoolFeeFloatBits, &intent.FeeAmount, &intent.Distributable, &intent.TotalWork}
	for index, raw := range []string{height, gross, feeBits, fee, distributable, totalWork} {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return AcceptedAccountingIntentV1{}, fmt.Errorf("accepted accounting numeric field %d is invalid", index)
		}
		*values[index] = parsed
	}
	rows, err := tx.Query(ctx, `SELECT line_index,miner_address,work::text,amount::text FROM accepted_block_accounting_credit_lines_v1 WHERE candidate_id=$1 ORDER BY line_index`, candidateID[:])
	if err != nil {
		return AcceptedAccountingIntentV1{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var line AcceptedAccountingCreditLineV1
		var lineIndex int
		var work, amount string
		if err := rows.Scan(&lineIndex, &line.MinerAddress, &work, &amount); err != nil {
			return AcceptedAccountingIntentV1{}, err
		}
		if lineIndex != len(intent.CreditLines) {
			return AcceptedAccountingIntentV1{}, errors.New("accepted accounting credit-line index drift")
		}
		line.Work, err = strconv.ParseUint(work, 10, 64)
		if err != nil {
			return AcceptedAccountingIntentV1{}, err
		}
		line.Amount, err = strconv.ParseUint(amount, 10, 64)
		if err != nil {
			return AcceptedAccountingIntentV1{}, err
		}
		intent.CreditLines = append(intent.CreditLines, line)
	}
	if err := rows.Err(); err != nil {
		return AcceptedAccountingIntentV1{}, err
	}
	if len(intent.CreditLines) != lineCount {
		return AcceptedAccountingIntentV1{}, errors.New("accepted accounting credit-line count mismatch")
	}
	if err := validateAcceptedAccountingIntentV1(intent); err != nil {
		return AcceptedAccountingIntentV1{}, err
	}
	return intent, nil
}

func insertAcceptedAccountingConflictTxV1(ctx context.Context, tx pgx.Tx, candidateID [32]byte, kind string, existing, incoming []byte, detail string) error {
	var conflictID [16]byte
	if _, err := rand.Read(conflictID[:]); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO accepted_block_accounting_conflicts_v1
		(conflict_id,candidate_id,conflict_kind,existing_intent_digest,incoming_intent_digest,detail,observed_at)
		VALUES ($1,$2,$3,$4,$5,$6,clock_timestamp())`, conflictID[:], candidateID[:], kind, nullableBytes(existing), nullableBytes(incoming), detail)
	return err
}

func (s *PostgresStorage) ApplyAcceptedAccountingV1(ctx context.Context, candidateID [32]byte) (AcceptedAccountingApplyResultV1, error) {
	result := AcceptedAccountingApplyResultV1{CandidateID: candidateID}
	if s == nil || s.pool == nil {
		return result, errors.New("accepted accounting database is not connected")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	var receiptHash, expectedHash, shareHash []byte
	var candidateHeight string
	if err := acceptedAccountingFaultV1("accepted_receipt_consume", false); err != nil {
		return result, err
	}
	if err := tx.QueryRow(ctx, `SELECT r.block_hash,c.expected_block_hash,c.share_pow_hash,c.height::text
		FROM accepted_block_submission_receipts_v1 r JOIN block_submission_candidates_v1 c USING(candidate_id)
		WHERE r.candidate_id=$1 FOR UPDATE OF r`, candidateID[:]).Scan(&receiptHash, &expectedHash, &shareHash, &candidateHeight); err != nil {
		return result, fmt.Errorf("lock accepted receipt: %w", err)
	}
	if err := acceptedAccountingFaultV1("accepted_receipt_consume", true); err != nil {
		return result, err
	}
	intent, err := readAcceptedAccountingIntentTxV1(ctx, tx, candidateID, true)
	if err != nil {
		return result, fmt.Errorf("read accepted accounting intent: %w", err)
	}
	height, parseErr := strconv.ParseUint(candidateHeight, 10, 64)
	if parseErr != nil || len(receiptHash) != 32 || len(expectedHash) != 32 || len(shareHash) != 32 || !bytes.Equal(receiptHash, expectedHash) || !bytes.Equal(shareHash, intent.LegacyBlockHash[:]) || height != intent.Height {
		if conflictErr := insertAcceptedAccountingConflictTxV1(ctx, tx, candidateID, "receipt_intent_identity_mismatch", intent.IntentDigest[:], nil, "receipt, candidate, and accounting intent identities differ"); conflictErr != nil {
			return result, conflictErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return result, commitErr
		}
		return result, errors.New("accepted accounting receipt and intent identities differ")
	}
	var existingConflicts int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_conflicts_v1`).Scan(&existingConflicts); err != nil {
		return result, err
	}
	if existingConflicts != 0 {
		return result, fmt.Errorf("accepted accounting conflict latch contains %d rows", existingConflicts)
	}
	var effectDigest, effectHash []byte
	var effectLines int64
	effectErr := tx.QueryRow(ctx, `SELECT intent_digest,legacy_block_hash,credit_line_count FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1`, candidateID[:]).Scan(&effectDigest, &effectHash, &effectLines)
	if effectErr == nil {
		if err := verifyAppliedAcceptedAccountingTxV1(ctx, tx, intent, effectDigest, effectHash, effectLines); err != nil {
			if conflictErr := insertAcceptedAccountingConflictTxV1(ctx, tx, candidateID, "applied_effect_drift", intent.IntentDigest[:], effectDigest, err.Error()); conflictErr != nil {
				return result, conflictErr
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return result, commitErr
			}
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, err
		}
		result.AlreadyApplied = true
		result.CreditLines = effectLines
		return result, nil
	}
	if !errors.Is(effectErr, pgx.ErrNoRows) {
		return result, effectErr
	}
	legacyKey := hex.EncodeToString(intent.LegacyBlockHash[:])
	var legacyBlocks, legacyCredits int64
	if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM blocks WHERE hash=$1),(SELECT count(*) FROM credits WHERE block_hash=$1)`, legacyKey).Scan(&legacyBlocks, &legacyCredits); err != nil {
		return result, err
	}
	if legacyBlocks != 0 || legacyCredits != 0 {
		detail := fmt.Sprintf("pre-existing legacy rows blocks=%d credits=%d", legacyBlocks, legacyCredits)
		if err := insertAcceptedAccountingConflictTxV1(ctx, tx, candidateID, "preexisting_legacy_rows", nil, intent.IntentDigest[:], detail); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, err
		}
		return result, errors.New(detail)
	}
	if err := acceptedAccountingFaultV1("accounting_effect_insert", false); err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO accepted_block_accounting_effects_v1
		(candidate_id,intent_digest,legacy_block_hash,credit_line_count,applied_at) VALUES ($1,$2,$3,$4,clock_timestamp())`,
		candidateID[:], intent.IntentDigest[:], intent.LegacyBlockHash[:], len(intent.CreditLines)); err != nil {
		return result, err
	}
	if err := acceptedAccountingFaultV1("accounting_effect_insert", true); err != nil {
		return result, err
	}
	if err := acceptedAccountingFaultV1("legacy_block_insert", false); err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO blocks (hash,height,reward,fees,status) VALUES ($1,$2,$3,$4,'PENDING')`,
		legacyKey, int64(intent.Height), strconv.FormatUint(intent.GrossReward, 10), strconv.FormatUint(intent.FeeAmount, 10)); err != nil {
		return result, fmt.Errorf("insert accepted legacy block: %w", err)
	}
	if err := acceptedAccountingFaultV1("legacy_block_insert", true); err != nil {
		return result, err
	}
	for _, line := range intent.CreditLines {
		if err := acceptedAccountingFaultV1("legacy_credit_insert", false); err != nil {
			return result, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO credits (block_hash,miner_address,amount) VALUES ($1,$2,$3)`, legacyKey, line.MinerAddress, strconv.FormatUint(line.Amount, 10)); err != nil {
			return result, fmt.Errorf("insert accepted legacy credit: %w", err)
		}
		if err := acceptedAccountingFaultV1("legacy_credit_insert", true); err != nil {
			return result, err
		}
	}
	if err := commitAcceptedAccountingTxV1(ctx, tx, "accounting_effect_commit"); err != nil {
		return result, err
	}
	result.Applied = true
	result.CreditLines = int64(len(intent.CreditLines))
	return result, nil
}

func verifyAppliedAcceptedAccountingTxV1(ctx context.Context, tx pgx.Tx, intent AcceptedAccountingIntentV1, effectDigest, effectHash []byte, effectLines int64) error {
	if len(effectDigest) != 32 || len(effectHash) != 32 || !bytes.Equal(effectDigest, intent.IntentDigest[:]) || !bytes.Equal(effectHash, intent.LegacyBlockHash[:]) || effectLines != int64(len(intent.CreditLines)) {
		return errors.New("accepted accounting effect marker differs from intent")
	}
	legacyKey := hex.EncodeToString(intent.LegacyBlockHash[:])
	var height int64
	var reward, fee string
	if err := tx.QueryRow(ctx, `SELECT height,reward::text,fees::text FROM blocks WHERE hash=$1`, legacyKey).Scan(&height, &reward, &fee); err != nil {
		return fmt.Errorf("read accepted legacy block: %w", err)
	}
	if height != int64(intent.Height) || reward != strconv.FormatUint(intent.GrossReward, 10) || fee != strconv.FormatUint(intent.FeeAmount, 10) {
		return errors.New("accepted legacy block differs from intent")
	}
	rows, err := tx.Query(ctx, `SELECT miner_address,amount::text FROM credits WHERE block_hash=$1 ORDER BY miner_address,id`, legacyKey)
	if err != nil {
		return err
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var miner, amount string
		if err := rows.Scan(&miner, &amount); err != nil {
			return err
		}
		if index >= len(intent.CreditLines) || miner != intent.CreditLines[index].MinerAddress || amount != strconv.FormatUint(intent.CreditLines[index].Amount, 10) {
			return errors.New("accepted legacy credits differ from intent")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if index != len(intent.CreditLines) {
		return errors.New("accepted legacy credit count differs from intent")
	}
	return nil
}

func (s *PostgresStorage) ReconcileAcceptedAccountingV1(ctx context.Context) (AcceptedAccountingReconcileResultV1, error) {
	var result AcceptedAccountingReconcileResultV1
	if s == nil || s.pool == nil {
		return result, errors.New("accepted accounting database is not connected")
	}
	for processed := 0; processed < maxAccountingReconcileV1; processed++ {
		var raw []byte
		err := s.pool.QueryRow(ctx, `SELECT r.candidate_id FROM accepted_block_submission_receipts_v1 r LEFT JOIN accepted_block_accounting_effects_v1 e USING(candidate_id) WHERE e.candidate_id IS NULL ORDER BY r.accepted_at,r.candidate_id LIMIT 1`).Scan(&raw)
		if errors.Is(err, pgx.ErrNoRows) {
			return result, nil
		}
		if err != nil {
			return result, err
		}
		if len(raw) != 32 {
			return result, errors.New("accepted accounting backlog contains invalid candidate id")
		}
		var candidateID [32]byte
		copy(candidateID[:], raw)
		applied, err := s.ApplyAcceptedAccountingV1(ctx, candidateID)
		if err != nil {
			return result, err
		}
		if !applied.Applied && !applied.AlreadyApplied {
			return result, errors.New("accepted accounting reconciliation made no progress")
		}
		if applied.Applied {
			result.Applied++
		}
	}
	return result, errors.New("accepted accounting reconciliation bound exceeded")
}

func (s *PostgresStorage) VerifyAcceptedAccountingV1(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("accepted accounting database is not connected")
	}
	if err := verifyAcceptedAccountingCatalogV1(ctx, s.pool); err != nil {
		return err
	}
	var conflicts, missingIntents, orphanLines, badLineCounts int64
	if err := s.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM accepted_block_accounting_conflicts_v1),
		(SELECT count(*) FROM block_submission_candidates_v1 c LEFT JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) WHERE i.candidate_id IS NULL),
		(SELECT count(*) FROM accepted_block_accounting_credit_lines_v1 l LEFT JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) WHERE i.candidate_id IS NULL),
		(SELECT count(*) FROM accepted_block_accounting_intents_v1 i WHERE i.credit_line_count<>(SELECT count(*) FROM accepted_block_accounting_credit_lines_v1 l WHERE l.candidate_id=i.candidate_id))`).Scan(&conflicts, &missingIntents, &orphanLines, &badLineCounts); err != nil {
		return err
	}
	if conflicts != 0 || missingIntents != 0 || orphanLines != 0 || badLineCounts != 0 {
		return fmt.Errorf("accepted accounting verification failed conflicts=%d missing_intents=%d orphan_lines=%d bad_line_counts=%d", conflicts, missingIntents, orphanLines, badLineCounts)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT candidate_id,intent_digest,legacy_block_hash,credit_line_count FROM accepted_block_accounting_effects_v1 ORDER BY candidate_id`)
	if err != nil {
		return err
	}
	type effect struct {
		id, digest, hash []byte
		lines            int64
	}
	var effects []effect
	for rows.Next() {
		var item effect
		if err := rows.Scan(&item.id, &item.digest, &item.hash, &item.lines); err != nil {
			rows.Close()
			return err
		}
		effects = append(effects, item)
	}
	rows.Close()
	for _, item := range effects {
		if len(item.id) != 32 {
			return errors.New("accepted accounting effect has invalid candidate id")
		}
		var candidateID [32]byte
		copy(candidateID[:], item.id)
		intent, err := readAcceptedAccountingIntentTxV1(ctx, tx, candidateID, false)
		if err != nil {
			return err
		}
		if err := verifyAppliedAcceptedAccountingTxV1(ctx, tx, intent, item.digest, item.hash, item.lines); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

type accountingCatalogQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func verifyAcceptedAccountingCatalogV1(ctx context.Context, q accountingCatalogQueryer) error {
	legacyColumns := []struct {
		table, column, typ string
		notNull            bool
		nullabilityExempt  bool
		defaultTag         string
	}{
		{"blocks", "hash", "text", true, false, ""},
		{"blocks", "height", "bigint", true, false, ""},
		{"blocks", "reward", "numeric(30,0)", true, false, ""},
		{"blocks", "fees", "numeric(30,0)", true, false, ""},
		{"blocks", "status", "text", false, true, "PENDING"},
		{"blocks", "created_at", "timestamp without time zone", false, false, "now()"},
		{"credits", "id", "integer", true, false, "nextval"},
		{"credits", "block_hash", "text", false, true, ""},
		{"credits", "miner_address", "text", true, false, ""},
		{"credits", "amount", "numeric(30,0)", true, false, ""},
		{"credits", "is_paid", "boolean", false, true, "false"},
		{"credits", "created_at", "timestamp without time zone", false, false, "now()"},
	}
	for _, check := range legacyColumns {
		var typ, defaultExpression string
		var notNull bool
		if err := q.QueryRow(ctx, `SELECT format_type(a.atttypid,a.atttypmod),a.attnotnull,COALESCE(pg_get_expr(ad.adbin,ad.adrelid),'') FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum WHERE a.attrelid=to_regclass($1) AND a.attname=$2 AND a.attnum>0 AND NOT a.attisdropped`, check.table, check.column).Scan(&typ, &notNull, &defaultExpression); err != nil {
			return fmt.Errorf("verify legacy accounting target %s.%s: %w", check.table, check.column, err)
		}
		if typ != check.typ || (!check.nullabilityExempt && notNull != check.notNull) {
			return fmt.Errorf("legacy accounting target column drift in %s.%s", check.table, check.column)
		}
		compactDefault := strings.ReplaceAll(defaultExpression, " ", "")
		if check.defaultTag == "" && defaultExpression != "" {
			return fmt.Errorf("unexpected legacy accounting target default in %s.%s", check.table, check.column)
		}
		if check.defaultTag != "" && !strings.Contains(compactDefault, check.defaultTag) {
			return fmt.Errorf("legacy accounting target default drift in %s.%s", check.table, check.column)
		}
	}
	for _, check := range []struct {
		table, kind, definition string
	}{
		{"blocks", "p", "PRIMARY KEY (hash)"},
		{"credits", "p", "PRIMARY KEY (id)"},
		{"credits", "f", "FOREIGN KEY (block_hash) REFERENCES blocks(hash)"},
	} {
		var exists bool
		if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype=$2::"char" AND pg_get_constraintdef(oid,true)=$3)`, check.table, check.kind, check.definition).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("legacy accounting target key drift for %s", check.table)
		}
	}
	checks := []struct {
		name  string
		query string
		want  string
	}{
		{
			name: "column catalog",
			query: `SELECT md5(string_agg(c.relname||'.'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull::text||':'||COALESCE(pg_get_expr(ad.adbin,ad.adrelid),''),'|' ORDER BY c.relname,a.attnum))
				FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
				LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum
				WHERE c.relnamespace=current_schema()::regnamespace AND c.relname IN ('accepted_block_accounting_intents_v1','accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1')`,
			want: "2e91c7ca5ed29b8782156f7d1cd12238",
		},
		{
			name:  "constraint definitions",
			query: `SELECT md5(string_agg(conname||':'||conrelid::regclass::text||':'||pg_get_constraintdef(oid,true),'|' ORDER BY conname)) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid=ANY(ARRAY['accepted_block_accounting_intents_v1','accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1']::regclass[])`,
			want:  "ceeb12a3dbe1b793858f15fbe46e01d5",
		},
		{
			name:  "index definitions",
			query: `SELECT md5(string_agg(indexname||':'||regexp_replace(indexdef,current_schema()||'\.','','g'),'|' ORDER BY indexname)) FROM pg_indexes WHERE schemaname=current_schema() AND tablename IN ('accepted_block_accounting_intents_v1','accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1')`,
			want:  "e79c2e18807f569f5fc7a3dc0c9f8d5d",
		},
		{
			name:  "trigger topology",
			query: `SELECT md5(string_agg(t.tgname||':'||t.tgrelid::regclass::text||':'||p.proname||':'||t.tgtype::text||':'||t.tgenabled::text,'|' ORDER BY t.tgname)) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=ANY(ARRAY['accepted_block_accounting_intents_v1','accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1']::regclass[]) AND NOT t.tgisinternal`,
			want:  "62c2fe9e11f71f55df658a8a6afbe70d",
		},
		{
			name:  "immutable function",
			query: `SELECT md5(prosrc) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() AND p.proname='reject_immutable_submission_row_v1' AND p.pronargs=0`,
			want:  "5e1b176dcfcc518ea0855f912089d4b3",
		},
	}
	for _, check := range checks {
		var got string
		if err := q.QueryRow(ctx, check.query).Scan(&got); err != nil {
			return fmt.Errorf("verify accepted accounting %s: %w", check.name, err)
		}
		if got != check.want {
			return fmt.Errorf("accepted accounting %s drift", check.name)
		}
	}
	return nil
}
