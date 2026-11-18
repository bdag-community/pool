package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// V2 keeps the original tables and effect protocol, but makes monetary values
// canonical decimal Wei.  V1 is deliberately retained verbatim for old rows.
const AcceptedAccountingPolicyV2 = "PPLNS-DISCOVERY-CUT/v2"

var acceptedAccountingPrefixV2 = append([]byte("BDAG-POOL-ACCEPTED-ACCOUNTING-INTENT/v2"), 0)

type AcceptedAccountingCreditLineV2 struct {
	MinerAddress string
	Work         uint64
	Amount       string
}

type AcceptedAccountingIntentV2 struct {
	CandidateID      [32]byte
	IntentDigest     [32]byte
	PolicyVersion    string
	LegacyBlockHash  [32]byte
	Height           uint64
	GrossReward      string
	PoolFeeFloatBits uint64
	FeeAmount        string
	Distributable    string
	TotalWork        uint64
	CreditLines      []AcceptedAccountingCreditLineV2
}

type AcceptedAccountingApplyResultV2 = AcceptedAccountingApplyResultV1
type AcceptedAccountingReconcileResultV2 = AcceptedAccountingReconcileResultV1

func canonicalWei(raw string) (*big.Int, error) {
	if raw == "0" {
		return new(big.Int), nil
	}
	if len(raw) == 0 || len(raw) > 30 || raw[0] == '0' {
		return nil, errors.New("wei is not canonical unsigned decimal")
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return nil, errors.New("wei is not canonical unsigned decimal")
		}
	}
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok || n.Sign() < 0 {
		return nil, errors.New("wei is not canonical unsigned decimal")
	}
	return n, nil
}

func SealAcceptedAccountingIntentV2(intent AcceptedAccountingIntentV2) (AcceptedAccountingIntentV2, error) {
	intent.PolicyVersion = AcceptedAccountingPolicyV2
	intent.CreditLines = append([]AcceptedAccountingCreditLineV2(nil), intent.CreditLines...)
	sort.Slice(intent.CreditLines, func(i, j int) bool { return intent.CreditLines[i].MinerAddress < intent.CreditLines[j].MinerAddress })
	encoded, err := encodeAcceptedAccountingIntentV2(intent)
	if err != nil {
		return AcceptedAccountingIntentV2{}, err
	}
	intent.IntentDigest = sha256.Sum256(encoded)
	return intent, nil
}

func validateAcceptedAccountingIntentV2(intent AcceptedAccountingIntentV2) error {
	encoded, err := encodeAcceptedAccountingIntentV2(intent)
	if err != nil {
		return err
	}
	if sha256.Sum256(encoded) != intent.IntentDigest {
		return errors.New("accepted accounting v2 intent digest mismatch")
	}
	return nil
}

func encodeAcceptedAccountingIntentV2(intent AcceptedAccountingIntentV2) ([]byte, error) {
	if intent.CandidateID == [32]byte{} || intent.LegacyBlockHash == [32]byte{} {
		return nil, errors.New("accepted accounting identity is empty")
	}
	if intent.PolicyVersion != AcceptedAccountingPolicyV2 {
		return nil, fmt.Errorf("unsupported accepted accounting policy %q", intent.PolicyVersion)
	}
	if intent.Height > math.MaxInt64 {
		return nil, errors.New("accepted accounting height exceeds PostgreSQL bigint")
	}
	feePercent := math.Float64frombits(intent.PoolFeeFloatBits)
	if math.IsNaN(feePercent) || math.IsInf(feePercent, 0) || feePercent < 0 || feePercent > 100 {
		return nil, errors.New("accepted accounting pool fee is invalid")
	}
	gross, err := canonicalWei(intent.GrossReward)
	if err != nil {
		return nil, fmt.Errorf("gross reward: %w", err)
	}
	fee, err := canonicalWei(intent.FeeAmount)
	if err != nil {
		return nil, fmt.Errorf("fee amount: %w", err)
	}
	distributable, err := canonicalWei(intent.Distributable)
	if err != nil {
		return nil, fmt.Errorf("distributable: %w", err)
	}
	if fee.Cmp(gross) > 0 || new(big.Int).Sub(gross, fee).Cmp(distributable) != 0 {
		return nil, errors.New("accepted accounting reward arithmetic is inconsistent")
	}
	ratio := new(big.Rat).SetFloat64(feePercent)
	if ratio == nil {
		return nil, errors.New("accepted accounting pool fee is invalid")
	}
	wantFee := new(big.Int).Quo(new(big.Int).Mul(gross, ratio.Num()), new(big.Int).Mul(ratio.Denom(), big.NewInt(100)))
	if wantFee.Cmp(fee) != 0 {
		return nil, errors.New("accepted accounting fee differs from frozen policy")
	}
	if len(intent.CreditLines) > maxAccountingCreditLinesV1 || (intent.TotalWork == 0 && len(intent.CreditLines) != 0) {
		return nil, errors.New("accepted accounting credit-line bound")
	}
	var workSum uint64
	creditSum := new(big.Int)
	previous := ""
	for i, line := range intent.CreditLines {
		if !utf8.ValidString(line.MinerAddress) || len(line.MinerAddress) == 0 || len(line.MinerAddress) > maxAccountingMinerBytesV1 || strings.IndexByte(line.MinerAddress, 0) >= 0 {
			return nil, fmt.Errorf("accepted accounting miner address %d is invalid", i)
		}
		if i > 0 && line.MinerAddress <= previous {
			return nil, errors.New("accepted accounting credit lines are not strictly sorted")
		}
		if line.Work == 0 || math.MaxUint64-workSum < line.Work {
			return nil, errors.New("accepted accounting work is invalid")
		}
		amount, err := canonicalWei(line.Amount)
		if err != nil {
			return nil, fmt.Errorf("credit line %d: %w", i, err)
		}
		workSum += line.Work
		creditSum.Add(creditSum, amount)
		if intent.TotalWork > 0 {
			want := new(big.Int).Quo(new(big.Int).Mul(distributable, new(big.Int).SetUint64(line.Work)), new(big.Int).SetUint64(intent.TotalWork))
			if want.Cmp(amount) != 0 {
				return nil, fmt.Errorf("accepted accounting credit differs for miner %q", line.MinerAddress)
			}
		}
		previous = line.MinerAddress
	}
	if workSum != intent.TotalWork || creditSum.Cmp(distributable) > 0 {
		return nil, errors.New("accepted accounting credit totals are inconsistent")
	}
	encoded := append([]byte(nil), acceptedAccountingPrefixV2...)
	encoded = appendAccountingBytesV1(encoded, []byte(intent.PolicyVersion))
	encoded = append(encoded, intent.CandidateID[:]...)
	encoded = append(encoded, intent.LegacyBlockHash[:]...)
	for _, v := range []uint64{intent.Height, intent.PoolFeeFloatBits, intent.TotalWork, uint64(len(intent.CreditLines))} {
		var fixed [8]byte
		binary.BigEndian.PutUint64(fixed[:], v)
		encoded = append(encoded, fixed[:]...)
	}
	for _, v := range []string{intent.GrossReward, intent.FeeAmount, intent.Distributable} {
		encoded = appendAccountingBytesV1(encoded, []byte(v))
	}
	for _, line := range intent.CreditLines {
		encoded = appendAccountingBytesV1(encoded, []byte(line.MinerAddress))
		var fixed [8]byte
		binary.BigEndian.PutUint64(fixed[:], line.Work)
		encoded = append(encoded, fixed[:]...)
		encoded = appendAccountingBytesV1(encoded, []byte(line.Amount))
	}
	return encoded, nil
}

func acceptedAccountingIntentEqualV2(left, right AcceptedAccountingIntentV2) bool {
	l, e1 := encodeAcceptedAccountingIntentV2(left)
	r, e2 := encodeAcceptedAccountingIntentV2(right)
	return e1 == nil && e2 == nil && left.IntentDigest == right.IntentDigest && bytes.Equal(l, r)
}

// commitAcceptedAccountingConflictV2 makes an integrity failure durable before
// it is reported. A transient caller error must never permit later replay.
func commitAcceptedAccountingConflictV2(ctx context.Context, tx pgx.Tx, candidateID [32]byte, kind string, existing, incoming []byte, detail string) error {
	if err := insertAcceptedAccountingConflictTxV1(ctx, tx, candidateID, kind, existing, incoming, detail); err != nil {
		return fmt.Errorf("record accepted accounting v2 conflict: %w", err)
	}
	if err := latchCandidateConflict(ctx, tx, candidateID); err != nil {
		return fmt.Errorf("latch accepted accounting v2 conflict: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit accepted accounting v2 conflict: %w", err)
	}
	return nil
}

func putAcceptedAccountingIntentTxV2(ctx context.Context, tx pgx.Tx, intent AcceptedAccountingIntentV2) error {
	if err := validateAcceptedAccountingIntentV2(intent); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO accepted_block_accounting_intents_v1 (candidate_id,intent_digest,policy_version,legacy_block_hash,height,gross_reward,pool_fee_float_bits,fee_amount,distributable,total_work,credit_line_count,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,clock_timestamp())`, intent.CandidateID[:], intent.IntentDigest[:], intent.PolicyVersion, intent.LegacyBlockHash[:], strconv.FormatUint(intent.Height, 10), intent.GrossReward, strconv.FormatUint(intent.PoolFeeFloatBits, 10), intent.FeeAmount, intent.Distributable, strconv.FormatUint(intent.TotalWork, 10), len(intent.CreditLines))
	if err != nil {
		return fmt.Errorf("insert accepted accounting v2 intent: %w", err)
	}
	for i, line := range intent.CreditLines {
		if _, err := tx.Exec(ctx, `INSERT INTO accepted_block_accounting_credit_lines_v1 (candidate_id,line_index,miner_address,work,amount) VALUES ($1,$2,$3,$4,$5)`, intent.CandidateID[:], i, line.MinerAddress, strconv.FormatUint(line.Work, 10), line.Amount); err != nil {
			return fmt.Errorf("insert accepted accounting v2 credit %d: %w", i, err)
		}
	}
	return nil
}

func readAcceptedAccountingIntentTxV2(ctx context.Context, tx pgx.Tx, candidateID [32]byte, lock bool) (AcceptedAccountingIntentV2, error) {
	var out AcceptedAccountingIntentV2
	var cid, digest, hash []byte
	var height, bits, work string
	var n int
	q := `SELECT candidate_id,intent_digest,policy_version,legacy_block_hash,height::text,gross_reward::text,pool_fee_float_bits::text,fee_amount::text,distributable::text,total_work::text,credit_line_count FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1`
	if lock {
		q += ` FOR UPDATE`
	}
	if err := tx.QueryRow(ctx, q, candidateID[:]).Scan(&cid, &digest, &out.PolicyVersion, &hash, &height, &out.GrossReward, &bits, &out.FeeAmount, &out.Distributable, &work, &n); err != nil {
		return out, err
	}
	if len(cid) != 32 || len(digest) != 32 || len(hash) != 32 || n < 0 || n > maxAccountingCreditLinesV1 {
		return out, errors.New("accepted accounting v2 fixed fields invalid")
	}
	copy(out.CandidateID[:], cid)
	copy(out.IntentDigest[:], digest)
	copy(out.LegacyBlockHash[:], hash)
	var err error
	if out.Height, err = strconv.ParseUint(height, 10, 64); err != nil {
		return out, err
	}
	if out.PoolFeeFloatBits, err = strconv.ParseUint(bits, 10, 64); err != nil {
		return out, err
	}
	if out.TotalWork, err = strconv.ParseUint(work, 10, 64); err != nil {
		return out, err
	}
	rows, err := tx.Query(ctx, `SELECT line_index,miner_address,work::text,amount::text FROM accepted_block_accounting_credit_lines_v1 WHERE candidate_id=$1 ORDER BY line_index`, candidateID[:])
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var i int
		var line AcceptedAccountingCreditLineV2
		var rawWork string
		if err := rows.Scan(&i, &line.MinerAddress, &rawWork, &line.Amount); err != nil {
			return out, err
		}
		if i != len(out.CreditLines) {
			return out, errors.New("accepted accounting v2 line index drift")
		}
		if line.Work, err = strconv.ParseUint(rawWork, 10, 64); err != nil {
			return out, err
		}
		out.CreditLines = append(out.CreditLines, line)
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if len(out.CreditLines) != n {
		return out, errors.New("accepted accounting v2 line count mismatch")
	}
	if err := validateAcceptedAccountingIntentV2(out); err != nil {
		return out, err
	}
	return out, nil
}

// PutCandidateV2 is intentionally separate from V1: a V2 candidate cannot
// be compared by re-encoding a legacy intent.
func (s *PostgresStorage) PutCandidateV2(ctx context.Context, candidate FrozenCandidateV1, accounting AcceptedAccountingIntentV2) (CandidateViewV1, PutDisposition, error) {
	return s.putCandidateV2(ctx, candidate, accounting, nil)
}

// DiscoverCandidateV2 assigns the immutable reward cut to the transaction
// that wins insertion. A repeated discovery owns no new economic allocation.
// Strict allocation writes must continue to use PutCandidateV2.
func (s *PostgresStorage) DiscoverCandidateV2(ctx context.Context, candidate FrozenCandidateV1, freeze func() (AcceptedAccountingIntentV2, error)) (CandidateViewV1, PutDisposition, error) {
	if freeze == nil {
		return CandidateViewV1{}, "", errors.New("discovery allocation factory is required")
	}
	return s.putCandidateV2(ctx, candidate, AcceptedAccountingIntentV2{}, freeze)
}

func (s *PostgresStorage) putCandidateV2(ctx context.Context, candidate FrozenCandidateV1, accounting AcceptedAccountingIntentV2, freeze func() (AcceptedAccountingIntentV2, error)) (CandidateViewV1, PutDisposition, error) {
	if s == nil || s.pool == nil {
		return CandidateViewV1{}, "", errors.New("submission journal database is not connected")
	}
	if err := validateFrozenCandidate(candidate); err != nil {
		return CandidateViewV1{}, "", err
	}
	validateAllocation := func() error {
		if err := validateAcceptedAccountingIntentV2(accounting); err != nil {
			return err
		}
		if accounting.CandidateID != candidate.CandidateID || accounting.LegacyBlockHash != candidate.SharePowHash || accounting.Height != candidate.Height.Uint64() {
			return errors.New("accepted accounting v2 intent does not bind frozen candidate")
		}
		return nil
	}
	if freeze == nil {
		if err := validateAllocation(); err != nil {
			return CandidateViewV1{}, "", err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CandidateViewV1{}, "", err
	}
	defer tx.Rollback(ctx)
	var latched bool
	latchSQL, latchArgs := s.conflictLatchSQLV1(1)
	if err = tx.QueryRow(ctx, `SELECT `+latchSQL, latchArgs...).Scan(&latched); err != nil {
		return CandidateViewV1{}, "", err
	}
	if latched {
		return CandidateViewV1{}, PutDispositionConflict, errors.New("submission conflict latch is set")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO block_submission_candidates_v1 (candidate_id,frozen_digest,frozen_payload,chain_id,expected_block_hash,header_bytes,extra_nonce2,nonce_bytes,share_pow_hash,parent_hash,height,template_seq,backend_id,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,COALESCE($14,clock_timestamp())) ON CONFLICT DO NOTHING`, candidate.CandidateID[:], candidate.FrozenDigest[:], candidate.FrozenPayload, candidate.ChainID.String(), candidate.ExpectedBlockHash[:], candidate.HeaderBytes[:], candidate.ExtraNonce2[:], candidate.NonceBytes[:], candidate.SharePowHash[:], candidate.ParentHash[:], candidate.Height.String(), candidate.TemplateSeq.String(), candidate.BackendID, nullableTime(candidate.CreatedAt))
	if err != nil {
		return CandidateViewV1{}, "", err
	}
	if tag.RowsAffected() == 1 {
		if freeze != nil {
			accounting, err = freeze()
			if err != nil {
				return CandidateViewV1{}, "", err
			}
			if err = validateAllocation(); err != nil {
				return CandidateViewV1{}, "", err
			}
		}
		if err := putAcceptedAccountingIntentTxV2(ctx, tx, accounting); err != nil {
			return CandidateViewV1{}, "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO block_submission_state_v1(candidate_id,state,state_version,attempt_count,next_attempt_at,updated_at) VALUES ($1,'pending',1,0,clock_timestamp(),clock_timestamp())`, candidate.CandidateID[:]); err != nil {
			return CandidateViewV1{}, "", err
		}
		view, err := scanCandidateView(ctx, tx, candidate.CandidateID)
		if err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return CandidateViewV1{}, "", err
		}
		return view, PutDispositionInserted, nil
	}
	view, err := scanCandidateView(ctx, tx, candidate.CandidateID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return CandidateViewV1{}, "", err
		}
		var existingID, existingDigest []byte
		if err := tx.QueryRow(ctx, `SELECT candidate_id,frozen_digest FROM block_submission_candidates_v1 WHERE expected_block_hash=$1`, candidate.ExpectedBlockHash[:]).Scan(&existingID, &existingDigest); err != nil {
			return CandidateViewV1{}, "", fmt.Errorf("lookup colliding candidate: %w", err)
		}
		if len(existingID) != 32 || len(existingDigest) != 32 {
			return CandidateViewV1{}, "", errors.New("colliding candidate has invalid identity")
		}
		// The incoming candidate was never inserted: it lost the independent
		// expected-block-hash key. Persist against the existing FK-backed row.
		var existingCandidateID [32]byte
		copy(existingCandidateID[:], existingID)
		if err := insertConflict(ctx, tx, existingCandidateID, "candidate_block_hash_collision", existingDigest, candidate.FrozenDigest[:], candidate.FrozenPayload, nil, "expected_block_hash"); err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := latchCandidateConflict(ctx, tx, existingCandidateID); err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return CandidateViewV1{}, "", err
		}
		return CandidateViewV1{}, PutDispositionConflict, nil
	}
	if !candidateMatchesRefreshedJob(view.Candidate, candidate) {
		if err := insertConflict(ctx, tx, candidate.CandidateID, "candidate_identity_mismatch", view.Candidate.FrozenDigest[:], candidate.FrozenDigest[:], candidate.FrozenPayload, nil, differenceMask(view.Candidate, candidate)); err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := latchCandidateConflict(ctx, tx, candidate.CandidateID); err != nil {
			return CandidateViewV1{}, "", err
		}
		view.State = SubmissionStateConflict
		if err := tx.Commit(ctx); err != nil {
			return CandidateViewV1{}, "", err
		}
		return view, PutDispositionConflict, nil
	}
	existing, err := readAcceptedAccountingIntentTxV2(ctx, tx, candidate.CandidateID, true)
	storedBound := existing.CandidateID == candidate.CandidateID && existing.LegacyBlockHash == candidate.SharePowHash && existing.Height == candidate.Height.Uint64()
	if err != nil || !storedBound || (freeze == nil && !acceptedAccountingIntentEqualV2(existing, accounting)) {
		var digest []byte
		if err == nil {
			digest = existing.IntentDigest[:]
		}
		detail := "stored accounting v2 intent is missing or differs"
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			detail = err.Error()
		}
		if err := insertAcceptedAccountingConflictTxV1(ctx, tx, candidate.CandidateID, "candidate_intent_mismatch", digest, accounting.IntentDigest[:], detail); err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := latchCandidateConflict(ctx, tx, candidate.CandidateID); err != nil {
			return CandidateViewV1{}, "", err
		}
		view.State = SubmissionStateConflict
		if err := tx.Commit(ctx); err != nil {
			return CandidateViewV1{}, "", err
		}
		return view, PutDispositionConflict, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return CandidateViewV1{}, "", err
	}
	return s.recoveredCandidateViewV1(view), PutDispositionExisting, nil
}

func (s *PostgresStorage) ApplyAcceptedAccountingV2(ctx context.Context, candidateID [32]byte) (AcceptedAccountingApplyResultV2, error) {
	result := AcceptedAccountingApplyResultV2{CandidateID: candidateID}
	if s == nil || s.pool == nil {
		return result, errors.New("accepted accounting database is not connected")
	}
	// A pending V1 candidate can receive its acceptance after an upgrade.
	// Dispatch on its immutable policy without re-encoding historical money.
	var policy string
	if err := s.pool.QueryRow(ctx, `SELECT policy_version FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1`, candidateID[:]).Scan(&policy); err != nil {
		return result, fmt.Errorf("read accepted accounting policy: %w", err)
	}
	switch policy {
	case AcceptedAccountingPolicyV1:
		return s.ApplyAcceptedAccountingV1(ctx, candidateID)
	case AcceptedAccountingPolicyV2:
	default:
		return result, fmt.Errorf("unsupported accepted accounting policy %q", policy)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	var receipt, expected, share []byte
	var height string
	if err := tx.QueryRow(ctx, `SELECT r.block_hash,c.expected_block_hash,c.share_pow_hash,c.height::text FROM accepted_block_submission_receipts_v1 r JOIN block_submission_candidates_v1 c USING(candidate_id) WHERE r.candidate_id=$1 FOR UPDATE OF r`, candidateID[:]).Scan(&receipt, &expected, &share, &height); err != nil {
		return result, err
	}
	intent, err := readAcceptedAccountingIntentTxV2(ctx, tx, candidateID, true)
	if err != nil {
		return result, err
	}
	h, e := strconv.ParseUint(height, 10, 64)
	if e != nil || !bytes.Equal(receipt, expected) || !bytes.Equal(share, intent.LegacyBlockHash[:]) || h != intent.Height {
		detail := "receipt, candidate, and accounting intent identities differ"
		if conflictErr := commitAcceptedAccountingConflictV2(ctx, tx, candidateID, "receipt_intent_identity_mismatch", intent.IntentDigest[:], nil, detail); conflictErr != nil {
			return result, conflictErr
		}
		return result, errors.New(detail)
	}
	var existingConflicts bool
	latch, args := s.accountingConflictLatchSQLV1(1)
	if err := tx.QueryRow(ctx, `SELECT `+latch, args...).Scan(&existingConflicts); err != nil {
		return result, err
	}
	if existingConflicts {
		return result, errors.New("accepted accounting conflict latch is unresolved")
	}
	var effectDigest []byte
	effectErr := tx.QueryRow(ctx, `SELECT intent_digest FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1`, candidateID[:]).Scan(&effectDigest)
	if effectErr == nil {
		if err := verifyAppliedAcceptedAccountingTxV2(ctx, tx, intent); err != nil {
			if conflictErr := commitAcceptedAccountingConflictV2(ctx, tx, candidateID, "applied_effect_drift", intent.IntentDigest[:], effectDigest, err.Error()); conflictErr != nil {
				return result, conflictErr
			}
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, err
		}
		result.AlreadyApplied = true
		result.CreditLines = int64(len(intent.CreditLines))
		return result, nil
	}
	if !errors.Is(effectErr, pgx.ErrNoRows) {
		return result, effectErr
	}
	if err := materializeAcceptedAccountingTxV2(ctx, tx, intent); err != nil {
		return result, err
	}
	if err = tx.Commit(ctx); err != nil {
		return result, err
	}
	result.Applied = true
	result.CreditLines = int64(len(intent.CreditLines))
	return result, nil
}

func materializeAcceptedAccountingTxV2(ctx context.Context, tx pgx.Tx, intent AcceptedAccountingIntentV2) error {
	candidateID := intent.CandidateID
	legacy := hex.EncodeToString(intent.LegacyBlockHash[:])
	var legacyBlocks, legacyCredits int64
	if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM blocks WHERE hash=$1),(SELECT count(*) FROM credits WHERE block_hash=$1)`, legacy).Scan(&legacyBlocks, &legacyCredits); err != nil {
		return err
	}
	if legacyBlocks != 0 || legacyCredits != 0 {
		detail := fmt.Sprintf("pre-existing legacy rows blocks=%d credits=%d", legacyBlocks, legacyCredits)
		if conflictErr := commitAcceptedAccountingConflictV2(ctx, tx, candidateID, "preexisting_legacy_rows", nil, intent.IntentDigest[:], detail); conflictErr != nil {
			return conflictErr
		}
		return errors.New(detail)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO accepted_block_accounting_effects_v1(candidate_id,intent_digest,legacy_block_hash,credit_line_count,applied_at) VALUES($1,$2,$3,$4,clock_timestamp())`, candidateID[:], intent.IntentDigest[:], intent.LegacyBlockHash[:], len(intent.CreditLines)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO blocks(hash,height,reward,fees,status) VALUES($1,$2,$3,$4,'PENDING')`, legacy, int64(intent.Height), intent.GrossReward, intent.FeeAmount); err != nil {
		return err
	}
	for _, line := range intent.CreditLines {
		if _, err := tx.Exec(ctx, `INSERT INTO credits(block_hash,miner_address,amount) VALUES($1,$2,$3)`, legacy, line.MinerAddress, line.Amount); err != nil {
			return err
		}
	}
	return nil
}

func verifyAppliedAcceptedAccountingTxV2(ctx context.Context, tx pgx.Tx, intent AcceptedAccountingIntentV2) error {
	var digest, hash []byte
	var lines int64
	if err := tx.QueryRow(ctx, `SELECT intent_digest,legacy_block_hash,credit_line_count FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1`, intent.CandidateID[:]).Scan(&digest, &hash, &lines); err != nil {
		return err
	}
	if len(digest) != 32 || len(hash) != 32 || !bytes.Equal(digest, intent.IntentDigest[:]) || !bytes.Equal(hash, intent.LegacyBlockHash[:]) || lines != int64(len(intent.CreditLines)) {
		return errors.New("accepted accounting v2 effect differs from intent")
	}
	legacy := hex.EncodeToString(intent.LegacyBlockHash[:])
	var height int64
	var reward, fee string
	if err := tx.QueryRow(ctx, `SELECT height,reward::text,fees::text FROM blocks WHERE hash=$1`, legacy).Scan(&height, &reward, &fee); err != nil {
		return err
	}
	if height != int64(intent.Height) || reward != intent.GrossReward || fee != intent.FeeAmount {
		return errors.New("accepted accounting v2 legacy block differs from intent")
	}
	rows, err := tx.Query(ctx, `SELECT miner_address,amount::text FROM credits WHERE block_hash=$1 ORDER BY miner_address,id`, legacy)
	if err != nil {
		return err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var miner, amount string
		if err := rows.Scan(&miner, &amount); err != nil {
			return err
		}
		if i >= len(intent.CreditLines) || miner != intent.CreditLines[i].MinerAddress || amount != intent.CreditLines[i].Amount {
			return errors.New("accepted accounting v2 legacy credits differ from intent")
		}
		i++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if i != len(intent.CreditLines) {
		return errors.New("accepted accounting v2 legacy credit count differs from intent")
	}
	return nil
}

func (s *PostgresStorage) ReconcileAcceptedAccountingV2(ctx context.Context) (AcceptedAccountingReconcileResultV2, error) {
	var out AcceptedAccountingReconcileResultV2
	if s == nil || s.pool == nil {
		return out, errors.New("accepted accounting database is not connected")
	}
	for n := 0; n < maxAccountingReconcileV1; n++ {
		var raw []byte
		var policy string
		err := s.pool.QueryRow(ctx, `SELECT r.candidate_id,COALESCE(i.policy_version,'') FROM accepted_block_submission_receipts_v1 r LEFT JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) LEFT JOIN accepted_block_accounting_effects_v1 e USING(candidate_id) WHERE e.candidate_id IS NULL ORDER BY r.accepted_at,r.candidate_id LIMIT 1`).Scan(&raw, &policy)
		if errors.Is(err, pgx.ErrNoRows) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		if len(raw) != 32 {
			return out, errors.New("invalid accounting candidate")
		}
		if policy == "" {
			return out, errors.New("accepted accounting receipt has no intent")
		}
		var id [32]byte
		copy(id[:], raw)
		if policy == AcceptedAccountingPolicyV1 {
			r, e := s.ApplyAcceptedAccountingV1(ctx, id)
			if e != nil {
				return out, e
			}
			if r.Applied {
				out.Applied++
			}
			continue
		}
		if policy != AcceptedAccountingPolicyV2 {
			return out, errors.New("unsupported accounting policy")
		}
		r, e := s.ApplyAcceptedAccountingV2(ctx, id)
		if e != nil {
			return out, e
		}
		if r.Applied {
			out.Applied++
		}
	}
	return out, errors.New("accepted accounting reconciliation bound exceeded")
}

func (s *PostgresStorage) VerifyAcceptedAccountingV2(ctx context.Context) error {
	return s.verifyAcceptedAccountingV2(ctx, nil)
}

// Offline recovery may exclude only its separately validated immutable target;
// all other economic rows/effects/catalog checks remain identical.
func (s *PostgresStorage) verifyAcceptedAccountingV2(ctx context.Context, offlineConflict []byte) error {
	if s == nil || s.pool == nil {
		return errors.New("accepted accounting database is not connected")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollbackUnpricedTx(tx)
	if err = s.verifyAcceptedAccountingV2Tx(ctx, tx, offlineConflict, false); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// The optional unpriced exclusion is used only after every corresponding row
// has been verified in this same transaction and snapshot. Legacy V2 callers
// always pass false and continue to reject every candidate lacking an intent.
func (s *PostgresStorage) verifyAcceptedAccountingV2Tx(ctx context.Context, tx pgx.Tx, offlineConflict []byte, allowVerifiedUnpriced bool) error {
	if err := verifyAcceptedAccountingCatalogV2(ctx, tx); err != nil {
		return err
	}
	var bad int64
	err := tx.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_intents_v1 WHERE policy_version NOT IN ($1,$2) OR gross_reward<0 OR fee_amount<0 OR distributable<0 OR gross_reward>=power(10::numeric,30) OR fee_amount>=power(10::numeric,30) OR distributable>=power(10::numeric,30)`, AcceptedAccountingPolicyV1, AcceptedAccountingPolicyV2).Scan(&bad)
	if err != nil {
		return err
	}
	if bad != 0 {
		return errors.New("accepted accounting v2 catalog/data drift")
	}
	var conflicts, missingIntents, orphanLines, badLineCounts int64
	missingPredicate := `i.candidate_id IS NULL`
	if allowVerifiedUnpriced {
		missingPredicate += ` AND NOT EXISTS (SELECT 1 FROM pool_share_journal_v1.unpriced_discoveries_v1 d WHERE d.candidate_id=c.candidate_id)`
	}
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM accepted_block_accounting_conflicts_v1),
		(SELECT count(*) FROM block_submission_candidates_v1 c LEFT JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) WHERE `+missingPredicate+`),
		(SELECT count(*) FROM accepted_block_accounting_credit_lines_v1 l LEFT JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) WHERE i.candidate_id IS NULL),
		(SELECT count(*) FROM accepted_block_accounting_intents_v1 i WHERE i.credit_line_count<>(SELECT count(*) FROM accepted_block_accounting_credit_lines_v1 l WHERE l.candidate_id=i.candidate_id))`).Scan(&conflicts, &missingIntents, &orphanLines, &badLineCounts); err != nil {
		return err
	}
	if conflicts != 0 {
		if offlineConflict != nil {
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM accepted_block_accounting_conflicts_v1 WHERE conflict_id<>$1`, offlineConflict).Scan(&conflicts); err != nil {
				return err
			}
		} else {
			query, args := s.accountingConflictLatchSQLV1(1)
			var latched bool
			if err := tx.QueryRow(ctx, "SELECT "+query, args...).Scan(&latched); err != nil {
				return err
			}
			if !latched {
				conflicts = 0
			}
		}
	}
	if conflicts != 0 || missingIntents != 0 || orphanLines != 0 || badLineCounts != 0 {
		return fmt.Errorf("accepted accounting v2 verification failed conflicts=%d missing_intents=%d orphan_lines=%d bad_line_counts=%d", conflicts, missingIntents, orphanLines, badLineCounts)
	}
	rows, err := tx.Query(ctx, `SELECT candidate_id,policy_version FROM accepted_block_accounting_intents_v1 ORDER BY candidate_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type policyRef struct {
		id     [32]byte
		policy string
	}
	var intentRefs []policyRef
	for rows.Next() {
		var raw []byte
		var policy string
		if err := rows.Scan(&raw, &policy); err != nil {
			return err
		}
		if len(raw) != 32 {
			return errors.New("accepted accounting intent has invalid candidate id")
		}
		var id [32]byte
		copy(id[:], raw)
		intentRefs = append(intentRefs, policyRef{id: id, policy: policy})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, ref := range intentRefs {
		id, policy := ref.id, ref.policy
		switch policy {
		case AcceptedAccountingPolicyV1:
			if _, err := readAcceptedAccountingIntentTxV1(ctx, tx, id, false); err != nil {
				return err
			}
		case AcceptedAccountingPolicyV2:
			if _, err := readAcceptedAccountingIntentTxV2(ctx, tx, id, false); err != nil {
				return err
			}
		default:
			return errors.New("unsupported accepted accounting policy")
		}
	}
	effects, err := tx.Query(ctx, `SELECT e.candidate_id,i.policy_version FROM accepted_block_accounting_effects_v1 e JOIN accepted_block_accounting_intents_v1 i USING(candidate_id) ORDER BY e.candidate_id`)
	if err != nil {
		return err
	}
	defer effects.Close()
	var effectRefs []policyRef
	for effects.Next() {
		var raw []byte
		var policy string
		if err := effects.Scan(&raw, &policy); err != nil {
			return err
		}
		if len(raw) != 32 {
			return errors.New("accepted accounting effect has invalid candidate id")
		}
		var id [32]byte
		copy(id[:], raw)
		effectRefs = append(effectRefs, policyRef{id: id, policy: policy})
	}
	if err := effects.Err(); err != nil {
		return err
	}
	effects.Close()
	for _, ref := range effectRefs {
		id, policy := ref.id, ref.policy
		if policy == AcceptedAccountingPolicyV1 {
			intent, e := readAcceptedAccountingIntentTxV1(ctx, tx, id, false)
			if e != nil {
				return e
			}
			var d, h []byte
			var n int64
			if e = tx.QueryRow(ctx, `SELECT intent_digest,legacy_block_hash,credit_line_count FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1`, id[:]).Scan(&d, &h, &n); e != nil {
				return e
			}
			if e = verifyAppliedAcceptedAccountingTxV1(ctx, tx, intent, d, h, n); e != nil {
				return e
			}
		} else if policy == AcceptedAccountingPolicyV2 {
			intent, e := readAcceptedAccountingIntentTxV2(ctx, tx, id, false)
			if e != nil {
				return e
			}
			if e = verifyAppliedAcceptedAccountingTxV2(ctx, tx, intent); e != nil {
				return e
			}
		} else {
			return errors.New("unsupported accepted accounting policy")
		}
	}
	return nil
}
