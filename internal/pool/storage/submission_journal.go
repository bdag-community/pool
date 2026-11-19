package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/blake2b"
)

const (
	SubmissionStatePending       = "pending"
	SubmissionStateInFlight      = "in_flight"
	SubmissionStateAcceptedFinal = "accepted_final"
	SubmissionStateRejectedFinal = "rejected_final"
	SubmissionStateConflict      = "conflict"

	PutDispositionInserted = "inserted"
	PutDispositionExisting = "existing"
	PutDispositionConflict = "conflict"

	SubmitMethodV2     = "submitBlockHeaderV2"
	SubmitMethodLegacy = "submitBlockHeader"

	SubmitDecisionPending        = "pending"
	SubmitDecisionAcceptedFinal  = "accepted_final"
	SubmitDecisionRejectedFinal  = "rejected_final"
	SubmitDecisionConflict       = "conflict"
	SubmitDecisionMethodNotFound = "method_not_found"

	journalHeaderBytesV1       = 185
	maxSubmissionResponseBytes = 65536
)

var backendIDPattern = regexp.MustCompile(`^[a-z0-9._-]{1,64}$`)

// Installed only by storage-package fault tests. The nil production value
// keeps the journal path unchanged while allowing tests to fail the real SQL
// boundary before or after it, including commit acknowledgement loss.
var submissionFaultHookV1 func(point string, after bool) error

func submissionFaultV1(point string, after bool) error {
	if submissionFaultHookV1 == nil {
		return nil
	}
	return submissionFaultHookV1(point, after)
}

func commitSubmissionTxV1(ctx context.Context, tx pgx.Tx, point string) error {
	if err := submissionFaultV1(point, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return submissionFaultV1(point, true)
}

// SubmissionJournal is the only durable boundary used by the block-submit
// coordinator. HTTP is deliberately not part of this interface: callers must
// commit a prepared attempt before performing the exchange.
type SubmissionJournal interface {
	VerifySubmissionJournalV1(ctx context.Context, chainID *big.Int, backendID string) error
	PutCandidate(ctx context.Context, candidate FrozenCandidateV1, accounting AcceptedAccountingIntentV1) (CandidateViewV1, PutDisposition, error)
	GetCandidate(ctx context.Context, candidateID [32]byte) (CandidateViewV1, error)
	ReclaimExpiredLeases(ctx context.Context) (int64, error)
	ClaimCandidate(ctx context.Context, candidateID [32]byte, owner [16]byte, lease time.Duration) (*LeasedCandidateV1, error)
	ClaimNextDueCandidate(ctx context.Context, owner [16]byte, lease time.Duration) (*LeasedCandidateV1, error)
	PrepareAttempt(ctx context.Context, lease LeaseV1, initialMethod SubmitMethod) (PreparedAttemptV1, error)
	CompleteAttempt(ctx context.Context, lease LeaseV1, attemptID [16]byte, observation RPCObservationV1, decision SubmitDecisionV1) (TransitionResultV1, error)
	ReadSubmissionMetrics(ctx context.Context) (SubmissionMetricsV1, error)
	VerifyAcceptedAccountingV1(ctx context.Context) error
	ApplyAcceptedAccountingV1(ctx context.Context, candidateID [32]byte) (AcceptedAccountingApplyResultV1, error)
	ReconcileAcceptedAccountingV1(ctx context.Context) (AcceptedAccountingReconcileResultV1, error)
}

type PutDisposition string
type SubmitMethod string
type SubmitDecision string

type SubmitDecisionV1 = SubmitDecision

type FrozenCandidateV1 struct {
	CandidateID       [32]byte
	FrozenDigest      [32]byte
	FrozenPayload     []byte
	ChainID           *big.Int
	ExpectedBlockHash [32]byte
	HeaderBytes       [185]byte
	ExtraNonce2       [4]byte
	NonceBytes        [4]byte
	SharePowHash      [32]byte
	ParentHash        [32]byte
	Height            *big.Int
	TemplateSeq       *big.Int
	BackendID         string
	CreatedAt         time.Time
}

type CandidateViewV1 struct {
	HistoricalState          string
	Recovered                bool
	Candidate                FrozenCandidateV1
	State                    string
	StateVersion             int64
	AttemptCount             int64
	NextAttemptAt            *time.Time
	LastAttemptID            []byte
	LastCode                 string
	LastMessage              string
	FinalCode                string
	AuthoritativeBlockHash   []byte
	AuthoritativeHeight      *big.Int
	AuthoritativeTemplateSeq *big.Int
	FinalizedAt              *time.Time
	UpdatedAt                time.Time
}

type LeaseV1 struct {
	CandidateID [32]byte
	Owner       [16]byte
	Token       [32]byte
	ExpiresAt   time.Time
	AttemptNo   int64
}

type LeasedCandidateV1 struct {
	Candidate CandidateViewV1
	Lease     LeaseV1
}

type PreparedAttemptV1 struct {
	AttemptID     [16]byte
	CandidateID   [32]byte
	AttemptNo     int64
	LeaseToken    [32]byte
	Method        SubmitMethod
	RequestBody   []byte
	RequestSHA256 [32]byte
	PreparedAt    time.Time
}

type RPCObservationV1 struct {
	TransportClass       string
	HTTPStatus           *int
	ResponsePrefix       []byte
	ResponseTruncated    bool
	ResponsePrefixSHA256 [32]byte
	MethodNotFound       bool
	Accepted             *bool
	Final                *bool
	Retryable            *bool
	Code                 *string
	Message              *string
	BlockHash            []byte
	Height               *big.Int
	TemplateSeq          *big.Int
}

type TransitionResultV1 struct {
	CandidateID   [32]byte
	AttemptID     [16]byte
	State         string
	FirstAccepted bool
	Conflict      bool
}

type SubmissionMetricsV1 struct {
	RecoveredConflict           int64
	RecoveredAccountingConflict int64
	Pending                     int64
	InFlight                    int64
	AcceptedFinal               int64
	RejectedFinal               int64
	Conflict                    int64
	AcceptedReceiptCount        int64
	AccountingIntentCount       int64
	AccountingPendingCount      int64
	AccountingEffectCount       int64
	AccountingConflictCount     int64
	OldestPendingAge            time.Duration
	LastAcceptedAt              *time.Time
}

func (s *PostgresStorage) VerifySubmissionJournalV1(ctx context.Context, chainID *big.Int, backendID string) error {
	if s == nil || s.pool == nil {
		return errors.New("submission journal database is not connected")
	}
	if err := validateJournalIdentity(chainID, backendID); err != nil {
		return err
	}
	s.jobRecovery.Store(nil)
	s.discoveryRecovery.Store(nil)
	s.unpricedSubmissionHold.Store(nil)
	verified := false
	defer func() {
		if !verified {
			s.jobRecovery.Store(nil)
			s.discoveryRecovery.Store(nil)
			s.unpricedSubmissionHold.Store(nil)
		}
	}()
	if err := verifyJournalCatalogV1(ctx, s.pool); err != nil {
		return err
	}
	var incompatible int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM block_submission_candidates_v1 WHERE chain_id <> $1::numeric OR backend_id <> $2`, chainID.String(), backendID).Scan(&incompatible); err != nil {
		return fmt.Errorf("verify candidate identity: %w", err)
	}
	if incompatible != 0 {
		return fmt.Errorf("submission journal contains %d incompatible candidate identities", incompatible)
	}
	discovery, err := s.verifyDiscoveryRecoveryAllowanceV1(ctx)
	if err != nil {
		return fmt.Errorf("economic conflict unresolved: %w", err)
	}
	s.discoveryRecovery.Store(discovery)
	var conflicts int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM block_submission_conflicts_v1`).Scan(&conflicts); err != nil {
		return fmt.Errorf("verify conflict latch: %w", err)
	}
	if conflicts != 0 {
		if conflicts != 1 {
			return fmt.Errorf("submission conflict latch contains %d rows", conflicts)
		}
		if hold, err := s.verifyUnpricedSubmissionHoldV1(ctx, chainID, backendID); err != nil {
			return fmt.Errorf("unpriced submission hold verification failed: %w", err)
		} else if hold != nil {
			s.unpricedSubmissionHold.Store(hold)
			verified = true
			return nil
		}
		allowance, err := s.verifyJobRecoveryAllowanceV1(ctx)
		if err != nil {
			return fmt.Errorf("submission conflict latch is unresolved: %w", err)
		}
		s.jobRecovery.Store(allowance)
	}
	verified = true
	return nil
}

func validateJournalIdentity(chainID *big.Int, backendID string) error {
	if chainID == nil || chainID.Sign() < 0 || len(chainID.String()) > 78 {
		return errors.New("invalid submission chain id")
	}
	if !backendIDPattern.MatchString(backendID) {
		return fmt.Errorf("invalid submission backend id %q", backendID)
	}
	return nil
}

func (s *PostgresStorage) PutCandidate(ctx context.Context, candidate FrozenCandidateV1, accounting AcceptedAccountingIntentV1) (CandidateViewV1, PutDisposition, error) {
	if s == nil || s.pool == nil {
		return CandidateViewV1{}, "", errors.New("submission journal database is not connected")
	}
	if err := validateFrozenCandidate(candidate); err != nil {
		return CandidateViewV1{}, "", err
	}
	if err := validateAcceptedAccountingIntentV1(accounting); err != nil {
		return CandidateViewV1{}, "", err
	}
	if accounting.CandidateID != candidate.CandidateID || accounting.LegacyBlockHash != candidate.SharePowHash || accounting.Height != candidate.Height.Uint64() {
		return CandidateViewV1{}, "", errors.New("accepted accounting intent does not bind the frozen candidate")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CandidateViewV1{}, "", err
	}
	defer tx.Rollback(ctx)
	var latched bool
	latchSQL, latchArgs := s.conflictLatchSQLV1(1)
	if err := tx.QueryRow(ctx, `SELECT `+latchSQL, latchArgs...).Scan(&latched); err != nil {
		return CandidateViewV1{}, "", fmt.Errorf("check submission conflict latch: %w", err)
	}
	if latched {
		return CandidateViewV1{}, PutDispositionConflict, errors.New("submission conflict latch is set")
	}
	if err := submissionFaultV1("candidate_insert", false); err != nil {
		return CandidateViewV1{}, "", err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO block_submission_candidates_v1
		(candidate_id,frozen_digest,frozen_payload,chain_id,expected_block_hash,header_bytes,extra_nonce2,nonce_bytes,share_pow_hash,parent_hash,height,template_seq,backend_id,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,COALESCE($14,clock_timestamp()))
		ON CONFLICT DO NOTHING`,
		candidate.CandidateID[:], candidate.FrozenDigest[:], candidate.FrozenPayload, candidate.ChainID.String(), candidate.ExpectedBlockHash[:], candidate.HeaderBytes[:], candidate.ExtraNonce2[:], candidate.NonceBytes[:], candidate.SharePowHash[:], candidate.ParentHash[:], candidate.Height.String(), candidate.TemplateSeq.String(), candidate.BackendID, nullableTime(candidate.CreatedAt))
	if err != nil {
		return CandidateViewV1{}, "", fmt.Errorf("insert candidate: %w", err)
	}
	if err := submissionFaultV1("candidate_insert", true); err != nil {
		return CandidateViewV1{}, "", err
	}
	if tag.RowsAffected() == 1 {
		if err := putAcceptedAccountingIntentTxV1(ctx, tx, accounting); err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := submissionFaultV1("state_insert", false); err != nil {
			return CandidateViewV1{}, "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO block_submission_state_v1 (candidate_id,state,state_version,attempt_count,next_attempt_at,updated_at) VALUES ($1,'pending',1,0,clock_timestamp(),clock_timestamp())`, candidate.CandidateID[:]); err != nil {
			return CandidateViewV1{}, "", fmt.Errorf("insert candidate state: %w", err)
		}
		if err := submissionFaultV1("state_insert", true); err != nil {
			return CandidateViewV1{}, "", err
		}
		view, err := scanCandidateView(ctx, tx, candidate.CandidateID)
		if err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := commitSubmissionTxV1(ctx, tx, "candidate_commit"); err != nil {
			return CandidateViewV1{}, "", fmt.Errorf("commit candidate: %w", err)
		}
		return view, PutDispositionInserted, nil
	}
	view, err := scanCandidateView(ctx, tx, candidate.CandidateID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return CandidateViewV1{}, "", err
		}
		// The candidate id is normally the conflict key. A concurrent variant
		// may instead lose on the independent expected-block-hash key; turn
		// that raw uniqueness error into the same durable global conflict.
		var existingID, existingDigest []byte
		if lookupErr := tx.QueryRow(ctx, `SELECT candidate_id,frozen_digest FROM block_submission_candidates_v1 WHERE expected_block_hash=$1`, candidate.ExpectedBlockHash[:]).Scan(&existingID, &existingDigest); lookupErr != nil {
			return CandidateViewV1{}, "", fmt.Errorf("lookup colliding candidate: %w", lookupErr)
		}
		if len(existingID) != 32 || len(existingDigest) != 32 {
			return CandidateViewV1{}, "", errors.New("colliding candidate has invalid identity")
		}
		if err := insertConflict(ctx, tx, candidate.CandidateID, "candidate_block_hash_collision", existingDigest, candidate.FrozenDigest[:], candidate.FrozenPayload, nil, "expected_block_hash"); err != nil {
			return CandidateViewV1{}, "", err
		}
		if err := commitSubmissionTxV1(ctx, tx, "candidate_commit"); err != nil {
			return CandidateViewV1{}, "", err
		}
		return CandidateViewV1{}, PutDispositionConflict, nil
	}
	if candidateMatchesRefreshedJob(view.Candidate, candidate) {
		existingAccounting, accountingErr := readAcceptedAccountingIntentTxV1(ctx, tx, candidate.CandidateID, true)
		if accountingErr != nil || !acceptedAccountingIntentEqualV1(existingAccounting, accounting) {
			var existingDigest []byte
			if accountingErr == nil {
				existingDigest = existingAccounting.IntentDigest[:]
			}
			detail := "stored accepted accounting intent is missing or differs"
			if accountingErr != nil && !errors.Is(accountingErr, pgx.ErrNoRows) {
				detail = accountingErr.Error()
			}
			if err := insertAcceptedAccountingConflictTxV1(ctx, tx, candidate.CandidateID, "candidate_intent_mismatch", existingDigest, accounting.IntentDigest[:], detail); err != nil {
				return CandidateViewV1{}, "", err
			}
			if err := latchCandidateConflict(ctx, tx, candidate.CandidateID); err != nil {
				return CandidateViewV1{}, "", err
			}
			view.State = SubmissionStateConflict
			if err := commitSubmissionTxV1(ctx, tx, "candidate_commit"); err != nil {
				return CandidateViewV1{}, "", err
			}
			return view, PutDispositionConflict, nil
		}
		if err := commitSubmissionTxV1(ctx, tx, "candidate_commit"); err != nil {
			return CandidateViewV1{}, "", err
		}
		return s.recoveredCandidateViewV1(view), PutDispositionExisting, nil
	}
	if err := insertConflict(ctx, tx, candidate.CandidateID, "candidate_identity_mismatch", view.Candidate.FrozenDigest[:], candidate.FrozenDigest[:], candidate.FrozenPayload, nil, differenceMask(view.Candidate, candidate)); err != nil {
		return CandidateViewV1{}, "", err
	}
	if err := latchCandidateConflict(ctx, tx, candidate.CandidateID); err != nil {
		return CandidateViewV1{}, "", err
	}
	view.State = SubmissionStateConflict
	if err := commitSubmissionTxV1(ctx, tx, "candidate_commit"); err != nil {
		return CandidateViewV1{}, "", err
	}
	return view, PutDispositionConflict, nil
}

func (s *PostgresStorage) GetCandidate(ctx context.Context, candidateID [32]byte) (CandidateViewV1, error) {
	if s == nil || s.pool == nil {
		return CandidateViewV1{}, errors.New("submission journal database is not connected")
	}
	view, err := scanCandidateView(ctx, s.pool, candidateID)
	if err != nil {
		return view, err
	}
	if allowance := s.jobRecovery.Load(); allowance != nil && bytes.Equal(candidateID[:], allowance.candidateID) {
		latchSQL, latchArgs := s.conflictLatchSQLV1(1)
		var latched bool
		if err := s.pool.QueryRow(ctx, `SELECT `+latchSQL, latchArgs...).Scan(&latched); err != nil {
			return view, err
		}
		if !latched {
			view = s.recoveredCandidateViewV1(view)
		}
	}
	return view, nil
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func scanCandidateView(ctx context.Context, q queryRower, candidateID [32]byte) (CandidateViewV1, error) {
	var v CandidateViewV1
	var candidateIDBytes, digest, expected, header, extra, nonce, pow, parent []byte
	var payload []byte
	var chain, height, seq string
	var created time.Time
	var next, finalized *time.Time
	var lastAttempt, authHash []byte
	var authHeight, authSeq *string
	var state string
	var stateVersion, attempts int64
	var lastCode, lastMessage, finalCode *string
	var updated time.Time
	err := q.QueryRow(ctx, `SELECT c.candidate_id,c.frozen_digest,c.frozen_payload,c.chain_id::text,c.expected_block_hash,c.header_bytes,c.extra_nonce2,c.nonce_bytes,c.share_pow_hash,c.parent_hash,c.height::text,c.template_seq::text,c.backend_id,c.created_at,
		s.state,s.state_version,s.attempt_count,s.next_attempt_at,s.last_attempt_id,s.last_code,s.last_message,s.final_code,s.authoritative_block_hash,s.authoritative_height::text,s.authoritative_template_seq::text,s.finalized_at,s.updated_at
		FROM block_submission_candidates_v1 c JOIN block_submission_state_v1 s USING (candidate_id) WHERE c.candidate_id=$1`, candidateID[:]).Scan(
		&candidateIDBytes, &digest, &payload, &chain, &expected, &header, &extra, &nonce, &pow, &parent, &height, &seq, &v.Candidate.BackendID, &created, &state, &stateVersion, &attempts, &next, &lastAttempt, &lastCode, &lastMessage, &finalCode, &authHash, &authHeight, &authSeq, &finalized, &updated)
	if err != nil {
		return CandidateViewV1{}, err
	}
	if len(candidateIDBytes) != 32 || len(digest) != 32 || len(expected) != 32 || len(header) != journalHeaderBytesV1 || len(extra) != 4 || len(nonce) != 4 || len(pow) != 32 || len(parent) != 32 {
		return CandidateViewV1{}, errors.New("journal returned invalid fixed-width candidate")
	}
	copy(v.Candidate.CandidateID[:], candidateIDBytes)
	copy(v.Candidate.FrozenDigest[:], digest)
	v.Candidate.FrozenPayload = append([]byte(nil), payload...)
	copy(v.Candidate.ExpectedBlockHash[:], expected)
	copy(v.Candidate.HeaderBytes[:], header)
	copy(v.Candidate.ExtraNonce2[:], extra)
	copy(v.Candidate.NonceBytes[:], nonce)
	copy(v.Candidate.SharePowHash[:], pow)
	copy(v.Candidate.ParentHash[:], parent)
	var ok bool
	v.Candidate.ChainID, ok = new(big.Int).SetString(chain, 10)
	if !ok {
		return CandidateViewV1{}, errors.New("journal returned invalid chain id")
	}
	v.Candidate.Height, ok = new(big.Int).SetString(height, 10)
	if !ok {
		return CandidateViewV1{}, errors.New("journal returned invalid candidate height")
	}
	v.Candidate.TemplateSeq, ok = new(big.Int).SetString(seq, 10)
	if !ok {
		return CandidateViewV1{}, errors.New("journal returned invalid template sequence")
	}
	v.Candidate.CreatedAt = created
	v.State = state
	v.StateVersion = stateVersion
	v.AttemptCount = attempts
	v.NextAttemptAt = next
	v.LastAttemptID = append([]byte(nil), lastAttempt...)
	if lastCode != nil {
		v.LastCode = *lastCode
	}
	if lastMessage != nil {
		v.LastMessage = *lastMessage
	}
	if finalCode != nil {
		v.FinalCode = *finalCode
	}
	v.AuthoritativeBlockHash = append([]byte(nil), authHash...)
	if authHeight != nil {
		v.AuthoritativeHeight, ok = new(big.Int).SetString(*authHeight, 10)
		if !ok {
			return CandidateViewV1{}, errors.New("journal returned invalid authoritative height")
		}
	}
	if authSeq != nil {
		v.AuthoritativeTemplateSeq, ok = new(big.Int).SetString(*authSeq, 10)
		if !ok {
			return CandidateViewV1{}, errors.New("journal returned invalid authoritative template sequence")
		}
	}
	v.FinalizedAt = finalized
	v.UpdatedAt = updated
	if err := validateFrozenCandidate(v.Candidate); err != nil {
		return CandidateViewV1{}, fmt.Errorf("journal candidate integrity failure: %w", err)
	}
	return v, nil
}

func (s *PostgresStorage) ReclaimExpiredLeases(ctx context.Context) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, errors.New("submission journal database is not connected")
	}
	tag, err := s.pool.Exec(ctx, `WITH expired AS (
		SELECT candidate_id FROM block_submission_state_v1
		WHERE state='in_flight' AND lease_expires_at <= clock_timestamp()
		FOR UPDATE SKIP LOCKED
	) UPDATE block_submission_state_v1 AS s
	SET state='pending',state_version=state_version+1,next_attempt_at=clock_timestamp(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
	FROM expired WHERE s.candidate_id=expired.candidate_id`)
	return tag.RowsAffected(), err
}

func (s *PostgresStorage) ClaimCandidate(ctx context.Context, candidateID [32]byte, owner [16]byte, lease time.Duration) (*LeasedCandidateV1, error) {
	return s.claimCandidate(ctx, &candidateID, owner, lease)
}

func (s *PostgresStorage) ClaimNextDueCandidate(ctx context.Context, owner [16]byte, lease time.Duration) (*LeasedCandidateV1, error) {
	return s.claimCandidate(ctx, nil, owner, lease)
}

func (s *PostgresStorage) claimCandidate(ctx context.Context, candidateID *[32]byte, owner [16]byte, lease time.Duration) (*LeasedCandidateV1, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("submission journal database is not connected")
	}
	if lease <= 0 {
		return nil, errors.New("invalid submission lease")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var id []byte
	var state string
	var args []any
	query := `SELECT s.candidate_id,s.state FROM block_submission_state_v1 s
		WHERE s.state='pending' AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= clock_timestamp())`
	if candidateID != nil {
		query += ` AND s.candidate_id=$1`
		args = append(args, candidateID[:])
	}
	latchSQL, latchArgs := s.conflictLatchSQLV1(len(args) + 1)
	query += ` AND NOT ` + latchSQL
	args = append(args, latchArgs...)
	query += ` ORDER BY s.next_attempt_at NULLS FIRST,s.candidate_id LIMIT 1 FOR UPDATE SKIP LOCKED`
	if err := tx.QueryRow(ctx, query, args...).Scan(&id, &state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if len(id) != 32 || state != SubmissionStatePending {
		return nil, errors.New("journal returned invalid claim candidate")
	}
	var selected [32]byte
	copy(selected[:], id)
	token := [32]byte{}
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	var expires time.Time
	var attemptNo int64
	if err := tx.QueryRow(ctx, `UPDATE block_submission_state_v1
		SET state='in_flight',state_version=state_version+1,attempt_count=attempt_count+1,
		lease_owner=$2,lease_token=$3,lease_expires_at=clock_timestamp()+$4::interval,
		next_attempt_at=NULL,updated_at=clock_timestamp()
		WHERE candidate_id=$1 AND state='pending'
		RETURNING lease_expires_at,attempt_count`, selected[:], owner[:], token[:], lease.String()).Scan(&expires, &attemptNo); err != nil {
		return nil, err
	}
	view, err := scanCandidateView(ctx, tx, selected)
	if err != nil {
		return nil, err
	}
	if err := commitSubmissionTxV1(ctx, tx, "lease_update_commit"); err != nil {
		return nil, err
	}
	return &LeasedCandidateV1{Candidate: view, Lease: LeaseV1{CandidateID: selected, Owner: owner, Token: token, ExpiresAt: expires, AttemptNo: attemptNo}}, nil
}

func (s *PostgresStorage) PrepareAttempt(ctx context.Context, lease LeaseV1, initialMethod SubmitMethod) (PreparedAttemptV1, error) {
	if s == nil || s.pool == nil {
		return PreparedAttemptV1{}, errors.New("submission journal database is not connected")
	}
	if initialMethod != SubmitMethodV2 && initialMethod != SubmitMethodLegacy {
		return PreparedAttemptV1{}, errors.New("unsupported submit method")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PreparedAttemptV1{}, err
	}
	defer tx.Rollback(ctx)
	var state string
	var stateOwner, stateToken []byte
	var attemptNo int64
	var expires time.Time
	if err := tx.QueryRow(ctx, `SELECT s.state,s.lease_owner,s.lease_token,s.attempt_count,s.lease_expires_at
		FROM block_submission_candidates_v1 c JOIN block_submission_state_v1 s USING(candidate_id)
		WHERE c.candidate_id=$1 AND s.state='in_flight' AND s.lease_owner=$2 AND s.lease_token=$3
		AND s.attempt_count=$4 AND s.lease_expires_at > clock_timestamp() FOR UPDATE`, lease.CandidateID[:], lease.Owner[:], lease.Token[:], lease.AttemptNo).Scan(&state, &stateOwner, &stateToken, &attemptNo, &expires); err != nil {
		return PreparedAttemptV1{}, fmt.Errorf("prepare lease is not active: %w", err)
	}
	if state != SubmissionStateInFlight || len(stateOwner) != 16 || len(stateToken) != 32 || !bytes.Equal(stateOwner, lease.Owner[:]) || !bytes.Equal(stateToken, lease.Token[:]) {
		return PreparedAttemptV1{}, errors.New("prepare lease identity is invalid")
	}
	view, err := scanCandidateView(ctx, tx, lease.CandidateID)
	if err != nil {
		return PreparedAttemptV1{}, fmt.Errorf("load candidate for prepared request: %w", err)
	}
	candidate := view.Candidate
	var latest struct {
		AttemptID      []byte
		AttemptNo      int64
		LeaseToken     []byte
		Method         string
		RequestBody    []byte
		RequestSHA256  []byte
		Phase          string
		HTTPStatus     *int
		ResponsePrefix []byte
		ResponseSHA256 []byte
		ResponseTrunc  bool
		Decision       string
	}
	latestErr := tx.QueryRow(ctx, `SELECT attempt_id,attempt_no,lease_token,rpc_method,request_body,request_sha256,phase,http_status,response_prefix,response_prefix_sha256,response_truncated,decision
		FROM block_submission_attempts_v1 WHERE candidate_id=$1 ORDER BY attempt_no DESC LIMIT 1 FOR UPDATE`, lease.CandidateID[:]).Scan(
		&latest.AttemptID, &latest.AttemptNo, &latest.LeaseToken, &latest.Method, &latest.RequestBody, &latest.RequestSHA256, &latest.Phase, &latest.HTTPStatus, &latest.ResponsePrefix, &latest.ResponseSHA256, &latest.ResponseTrunc, &latest.Decision)
	haveLatest := latestErr == nil
	if latestErr != nil && !errors.Is(latestErr, pgx.ErrNoRows) {
		return PreparedAttemptV1{}, fmt.Errorf("load latest prepared attempt: %w", latestErr)
	}
	if haveLatest && latest.AttemptNo >= attemptNo {
		return s.commitPreparationConflict(ctx, tx, lease.CandidateID, "attempt_lineage_mismatch", latest.RequestSHA256, nil, latest.AttemptID, "attempt_no", errors.New("latest persisted attempt is not older than active lease"))
	}
	if haveLatest && (len(latest.AttemptID) != 16 || len(latest.LeaseToken) != 32 || len(latest.RequestSHA256) != sha256.Size) {
		return s.commitPreparationConflict(ctx, tx, lease.CandidateID, "attempt_identity_integrity", latest.RequestSHA256, nil, latest.AttemptID, "attempt_id|lease_token|request_sha256", errors.New("latest persisted attempt has invalid fixed-width identity"))
	}
	method := initialMethod
	var requestBody []byte
	var storedDigest []byte
	if !haveLatest {
		requestBody, err = canonicalSubmitRequestV1(method, candidate.HeaderBytes[:], candidate.ExtraNonce2[:])
		if err != nil {
			return PreparedAttemptV1{}, err
		}
	} else if latest.Method == SubmitMethodV2 && latest.Phase == "completed" && latest.Decision == SubmitDecisionMethodNotFound && bytes.Equal(view.LastAttemptID, latest.AttemptID) && strictPersistedMethodNotFound(latest.HTTPStatus, latest.ResponsePrefix, latest.ResponseSHA256, latest.ResponseTrunc) {
		method = SubmitMethodLegacy
		requestBody, err = canonicalSubmitRequestV1(method, candidate.HeaderBytes[:], candidate.ExtraNonce2[:])
		if err != nil {
			return PreparedAttemptV1{}, err
		}
	} else {
		method = SubmitMethod(latest.Method)
		requestBody = append([]byte(nil), latest.RequestBody...)
		storedDigest = latest.RequestSHA256
	}
	digest, err := validatePreparedRequest(candidate, method, requestBody, storedDigest)
	if err != nil {
		var existing []byte
		if haveLatest {
			existing = latest.RequestSHA256
		}
		return s.commitPreparationConflict(ctx, tx, lease.CandidateID, "prepared_request_integrity", existing, digest[:], latest.AttemptID, "request_body|request_sha256|canonical", err)
	}
	attemptID := [16]byte{}
	if _, err := rand.Read(attemptID[:]); err != nil {
		return PreparedAttemptV1{}, err
	}
	var preparedAt time.Time
	if err := submissionFaultV1("prepared_attempt_insert", false); err != nil {
		return PreparedAttemptV1{}, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO block_submission_attempts_v1
		(attempt_id,candidate_id,attempt_no,lease_token,rpc_method,request_body,request_sha256,phase,prepared_at,decision)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'prepared',clock_timestamp(),'pending') RETURNING prepared_at`,
		attemptID[:], lease.CandidateID[:], attemptNo, lease.Token[:], string(method), requestBody, digest[:]).Scan(&preparedAt); err != nil {
		return PreparedAttemptV1{}, err
	}
	if err := submissionFaultV1("prepared_attempt_insert", true); err != nil {
		return PreparedAttemptV1{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE block_submission_state_v1 SET updated_at=clock_timestamp() WHERE candidate_id=$1 AND state='in_flight' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at > clock_timestamp()`, lease.CandidateID[:], lease.Owner[:], lease.Token[:]); err != nil {
		return PreparedAttemptV1{}, err
	}
	if err := commitSubmissionTxV1(ctx, tx, "prepared_attempt_commit"); err != nil {
		return PreparedAttemptV1{}, err
	}
	_ = expires
	return PreparedAttemptV1{AttemptID: attemptID, CandidateID: lease.CandidateID, AttemptNo: attemptNo, LeaseToken: lease.Token, Method: method, RequestBody: append([]byte(nil), requestBody...), RequestSHA256: digest, PreparedAt: preparedAt}, nil
}

func validatePreparedRequest(candidate FrozenCandidateV1, method SubmitMethod, requestBody, storedDigest []byte) ([32]byte, error) {
	var zero [32]byte
	if method != SubmitMethodV2 && method != SubmitMethodLegacy {
		return zero, errors.New("persisted attempt has unsupported method")
	}
	if len(requestBody) == 0 {
		return zero, errors.New("persisted attempt has empty request")
	}
	digest := sha256.Sum256(requestBody)
	if len(storedDigest) != 0 && !bytes.Equal(storedDigest, digest[:]) {
		return digest, errors.New("persisted attempt request SHA-256 mismatch")
	}
	canonical, err := canonicalSubmitRequestV1(method, candidate.HeaderBytes[:], candidate.ExtraNonce2[:])
	if err != nil {
		return digest, err
	}
	if !bytes.Equal(requestBody, canonical) {
		return digest, errors.New("persisted attempt request is not canonical for frozen candidate")
	}
	return digest, nil
}

func (s *PostgresStorage) commitPreparationConflict(ctx context.Context, tx pgx.Tx, candidateID [32]byte, kind string, existing, incoming, attemptID []byte, mask string, cause error) (PreparedAttemptV1, error) {
	if err := insertConflict(ctx, tx, candidateID, kind, existing, incoming, nil, attemptID, mask); err != nil {
		return PreparedAttemptV1{}, fmt.Errorf("record prepared request conflict: %w", err)
	}
	if err := latchCandidateConflict(ctx, tx, candidateID); err != nil {
		return PreparedAttemptV1{}, fmt.Errorf("latch prepared request conflict: %w", err)
	}
	if err := commitSubmissionTxV1(ctx, tx, "prepared_attempt_conflict_commit"); err != nil {
		return PreparedAttemptV1{}, fmt.Errorf("commit prepared request conflict: %w", err)
	}
	return PreparedAttemptV1{}, fmt.Errorf("prepared request integrity conflict: %w", cause)
}

func (s *PostgresStorage) CompleteAttempt(ctx context.Context, lease LeaseV1, attemptID [16]byte, observation RPCObservationV1, decision SubmitDecisionV1) (TransitionResultV1, error) {
	if s == nil || s.pool == nil {
		return TransitionResultV1{}, errors.New("submission journal database is not connected")
	}
	if err := validateSubmitDecision(decision); err != nil {
		return TransitionResultV1{}, err
	}
	if len(observation.ResponsePrefix) > maxSubmissionResponseBytes {
		observation.ResponsePrefix = append([]byte(nil), observation.ResponsePrefix[:maxSubmissionResponseBytes]...)
		observation.ResponseTruncated = true
	}
	observation.ResponsePrefixSHA256 = sha256.Sum256(observation.ResponsePrefix)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TransitionResultV1{}, err
	}
	defer tx.Rollback(ctx)
	var phase, storedMethod string
	var candidateID []byte
	var attemptNo int64
	var storedToken []byte
	if err := tx.QueryRow(ctx, `SELECT phase,candidate_id,attempt_no,lease_token,rpc_method FROM block_submission_attempts_v1 WHERE attempt_id=$1 FOR UPDATE`, attemptID[:]).Scan(&phase, &candidateID, &attemptNo, &storedToken, &storedMethod); err != nil {
		return TransitionResultV1{}, err
	}
	if phase != "prepared" {
		return TransitionResultV1{}, errors.New("attempt already completed")
	}
	if len(candidateID) != 32 || !bytes.Equal(storedToken, lease.Token[:]) || !bytes.Equal(candidateID, lease.CandidateID[:]) {
		return TransitionResultV1{}, errors.New("attempt lease mismatch")
	}
	var cid [32]byte
	copy(cid[:], candidateID)
	var candidate FrozenCandidateV1
	var state string
	var finalCode *string
	var stateVersion int64
	var existingHash []byte
	var stateOwner, stateToken []byte
	var active bool
	var expectedHeight, expectedSeq, chainID string
	var header, extra, nonce, pow, parent, digest, payload, expected []byte
	var backend string
	var storedCandidateID []byte
	if err := tx.QueryRow(ctx, `SELECT c.candidate_id,c.frozen_digest,c.frozen_payload,c.chain_id::text,c.expected_block_hash,c.header_bytes,c.extra_nonce2,c.nonce_bytes,c.share_pow_hash,c.parent_hash,c.height::text,c.template_seq::text,c.backend_id,
		s.state,s.state_version,s.final_code,s.authoritative_block_hash,s.lease_owner,s.lease_token,
		(s.state='in_flight' AND s.lease_owner=$2 AND s.lease_token=$3 AND s.lease_expires_at > clock_timestamp())
		FROM block_submission_candidates_v1 c JOIN block_submission_state_v1 s USING(candidate_id) WHERE c.candidate_id=$1 FOR UPDATE`, candidateID, lease.Owner[:], lease.Token[:]).Scan(
		&storedCandidateID, &digest, &payload, &chainID, &expected, &header, &extra, &nonce, &pow, &parent, &expectedHeight, &expectedSeq, &backend, &state, &stateVersion, &finalCode, &existingHash, &stateOwner, &stateToken, &active); err != nil {
		return TransitionResultV1{}, err
	}
	if len(storedCandidateID) != 32 || len(digest) != 32 || len(expected) != 32 || len(header) != 185 || len(extra) != 4 || len(nonce) != 4 || len(pow) != 32 || len(parent) != 32 {
		return TransitionResultV1{}, errors.New("journal candidate integrity failure")
	}
	copy(candidate.CandidateID[:], storedCandidateID)
	copy(candidate.FrozenDigest[:], digest)
	candidate.FrozenPayload = append([]byte(nil), payload...)
	candidate.ChainID, _ = new(big.Int).SetString(chainID, 10)
	copy(candidate.ExpectedBlockHash[:], expected)
	copy(candidate.HeaderBytes[:], header)
	copy(candidate.ExtraNonce2[:], extra)
	copy(candidate.NonceBytes[:], nonce)
	copy(candidate.SharePowHash[:], pow)
	copy(candidate.ParentHash[:], parent)
	candidate.Height, _ = new(big.Int).SetString(expectedHeight, 10)
	candidate.TemplateSeq, _ = new(big.Int).SetString(expectedSeq, 10)
	candidate.BackendID = backend
	if err := validateFrozenCandidate(candidate); err != nil {
		return TransitionResultV1{}, fmt.Errorf("journal candidate integrity failure: %w", err)
	}
	result := TransitionResultV1{CandidateID: cid, AttemptID: attemptID, State: state}
	active = active && state == SubmissionStateInFlight && bytes.Equal(stateOwner, lease.Owner[:]) && bytes.Equal(stateToken, lease.Token[:])
	if decision == SubmitDecisionMethodNotFound && (storedMethod != SubmitMethodV2 || !strictPersistedMethodNotFound(observation.HTTPStatus, observation.ResponsePrefix, observation.ResponsePrefixSHA256[:], observation.ResponseTruncated)) {
		decision = SubmitDecisionPending
	}
	if !observationCompatibleWithDecision(candidate, observation, decision) {
		decision = SubmitDecisionConflict
	}
	if err := submissionFaultV1("attempt_completion_update", false); err != nil {
		return TransitionResultV1{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE block_submission_attempts_v1 SET phase='completed',completed_at=clock_timestamp(),transport_class=$2,http_status=$3,response_prefix=$4,response_truncated=$5,response_prefix_sha256=$6,accepted=$7,final=$8,retryable=$9,code=$10,message=$11,block_hash=$12,height=$13,template_seq=$14,decision=$15 WHERE attempt_id=$1 AND phase='prepared'`, attemptID[:], observation.TransportClass, observation.HTTPStatus, nullableBytes(observation.ResponsePrefix), observation.ResponseTruncated, observation.ResponsePrefixSHA256[:], observation.Accepted, observation.Final, observation.Retryable, nullableString(observation.Code), nullableString(observation.Message), nullableBytes(observation.BlockHash), nullableBig(observation.Height), nullableBig(observation.TemplateSeq), string(decision)); err != nil {
		return TransitionResultV1{}, err
	}
	if err := submissionFaultV1("attempt_completion_update", true); err != nil {
		return TransitionResultV1{}, err
	}
	if !active {
		if err := commitSubmissionTxV1(ctx, tx, "completion_commit"); err != nil {
			return TransitionResultV1{}, err
		}
		return result, nil
	}
	if terminalStateContradiction(state, finalCode, existingHash, candidate, observation, decision) {
		if err := insertConflict(ctx, tx, cid, "contradictory_terminal_observation", candidate.FrozenDigest[:], observation.ResponsePrefixSHA256[:], nil, attemptID[:], "terminal_observation"); err != nil {
			return TransitionResultV1{}, err
		}
		if err := latchCandidateConflict(ctx, tx, cid); err != nil {
			return TransitionResultV1{}, err
		}
		result.State, result.Conflict = SubmissionStateConflict, true
		if err := commitSubmissionTxV1(ctx, tx, "completion_commit"); err != nil {
			return TransitionResultV1{}, err
		}
		return result, nil
	}
	if decision == SubmitDecisionConflict {
		if err := insertConflict(ctx, tx, cid, "submit_observation_conflict", candidate.FrozenDigest[:], observation.ResponsePrefixSHA256[:], nil, attemptID[:], "rpc_observation"); err != nil {
			return TransitionResultV1{}, err
		}
		if err := latchCandidateConflict(ctx, tx, cid); err != nil {
			return TransitionResultV1{}, err
		}
		result.State, result.Conflict = SubmissionStateConflict, true
		if err := commitSubmissionTxV1(ctx, tx, "completion_commit"); err != nil {
			return TransitionResultV1{}, err
		}
		return result, nil
	}
	switch decision {
	case SubmitDecisionAcceptedFinal:
		if observation.Code == nil || observation.Height == nil || observation.TemplateSeq == nil {
			return TransitionResultV1{}, errors.New("accepted transition lacks strict outcome fields")
		}
		if err := submissionFaultV1("state_update", false); err != nil {
			return TransitionResultV1{}, err
		}
		updated, err := tx.Exec(ctx, `UPDATE block_submission_state_v1 SET state='accepted_final',state_version=state_version+1,next_attempt_at=NULL,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,last_attempt_id=$2,last_code=$3,last_message=$4,final_code=$3,authoritative_block_hash=$5,authoritative_height=$6,authoritative_template_seq=$7,finalized_at=clock_timestamp(),updated_at=clock_timestamp() WHERE candidate_id=$1 AND state='in_flight' AND lease_owner=$8 AND lease_token=$9 AND lease_expires_at > clock_timestamp()`, cid[:], attemptID[:], nullableString(observation.Code), nullableString(observation.Message), candidate.ExpectedBlockHash[:], nullableBig(observation.Height), nullableBig(observation.TemplateSeq), lease.Owner[:], lease.Token[:])
		if err != nil {
			return TransitionResultV1{}, err
		}
		if err := submissionFaultV1("state_update", true); err != nil {
			return TransitionResultV1{}, err
		}
		if updated.RowsAffected() != 1 {
			if err := commitSubmissionTxV1(ctx, tx, "completion_commit"); err != nil {
				return TransitionResultV1{}, err
			}
			return result, nil
		}
		var inserted int64
		if err := submissionFaultV1("accepted_receipt_insert", false); err != nil {
			return TransitionResultV1{}, err
		}
		insertErr := tx.QueryRow(ctx, `INSERT INTO accepted_block_submission_receipts_v1
			(candidate_id,source_attempt_id,chain_id,block_hash,candidate_height,core_code,core_height,core_template_seq,accepted_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp())
			ON CONFLICT (candidate_id) DO NOTHING RETURNING 1`, cid[:], attemptID[:], candidate.ChainID.String(), candidate.ExpectedBlockHash[:], candidate.Height.String(), *observation.Code, nullableBig(observation.Height), observation.TemplateSeq.String()).Scan(&inserted)
		if insertErr != nil && !errors.Is(insertErr, pgx.ErrNoRows) {
			return TransitionResultV1{}, insertErr
		}
		if err := submissionFaultV1("accepted_receipt_insert", true); err != nil {
			return TransitionResultV1{}, err
		}
		result.FirstAccepted = inserted == 1
		result.State = SubmissionStateAcceptedFinal
	case SubmitDecisionRejectedFinal:
		if err := submissionFaultV1("state_update", false); err != nil {
			return TransitionResultV1{}, err
		}
		updated, err := tx.Exec(ctx, `UPDATE block_submission_state_v1 SET state='rejected_final',state_version=state_version+1,next_attempt_at=NULL,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,last_attempt_id=$2,last_code=$3,last_message=$4,final_code=$3,authoritative_block_hash=NULL,authoritative_height=NULL,authoritative_template_seq=NULL,finalized_at=clock_timestamp(),updated_at=clock_timestamp() WHERE candidate_id=$1 AND state='in_flight' AND lease_owner=$5 AND lease_token=$6 AND lease_expires_at > clock_timestamp()`, cid[:], attemptID[:], nullableString(observation.Code), nullableString(observation.Message), lease.Owner[:], lease.Token[:])
		if err != nil {
			return TransitionResultV1{}, err
		}
		if err := submissionFaultV1("state_update", true); err != nil {
			return TransitionResultV1{}, err
		}
		if updated.RowsAffected() != 1 {
			if err := commitSubmissionTxV1(ctx, tx, "completion_commit"); err != nil {
				return TransitionResultV1{}, err
			}
			return result, nil
		}
		result.State = SubmissionStateRejectedFinal
	default:
		lastCode := nullableString(observation.Code)
		if decision == SubmitDecisionMethodNotFound {
			lastCode = "method-not-found"
		}
		if err := submissionFaultV1("state_update", false); err != nil {
			return TransitionResultV1{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE block_submission_state_v1 SET state='pending',state_version=state_version+1,next_attempt_at=clock_timestamp()+make_submission_retry_delay(attempt_count),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,last_attempt_id=$2,last_code=$3,last_message=$4,updated_at=clock_timestamp() WHERE candidate_id=$1 AND state='in_flight' AND lease_owner=$5 AND lease_token=$6 AND lease_expires_at > clock_timestamp()`, cid[:], attemptID[:], lastCode, nullableString(observation.Message), lease.Owner[:], lease.Token[:]); err != nil {
			return TransitionResultV1{}, err
		}
		if err := submissionFaultV1("state_update", true); err != nil {
			return TransitionResultV1{}, err
		}
		result.State = SubmissionStatePending
	}
	if err := commitSubmissionTxV1(ctx, tx, "completion_commit"); err != nil {
		return TransitionResultV1{}, err
	}
	_ = attemptNo
	_ = stateVersion
	return result, nil
}

func (s *PostgresStorage) ReadSubmissionMetrics(ctx context.Context) (SubmissionMetricsV1, error) {
	if s == nil || s.pool == nil {
		return SubmissionMetricsV1{}, errors.New("submission journal database is not connected")
	}
	var m SubmissionMetricsV1
	var oldestSeconds *float64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE s.state='pending'),count(*) FILTER (WHERE s.state='in_flight'),count(*) FILTER (WHERE s.state='accepted_final'),count(*) FILTER (WHERE s.state='rejected_final'),count(*) FILTER (WHERE s.state='conflict'),(SELECT count(*) FROM accepted_block_submission_receipts_v1),(SELECT count(*) FROM accepted_block_accounting_intents_v1),(SELECT count(*) FROM accepted_block_submission_receipts_v1 r LEFT JOIN accepted_block_accounting_effects_v1 e USING(candidate_id) WHERE e.candidate_id IS NULL),(SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT count(*) FROM accepted_block_accounting_conflicts_v1),EXTRACT(EPOCH FROM (clock_timestamp()-(min(c.created_at) FILTER (WHERE s.state='pending')))),(SELECT max(accepted_at) FROM accepted_block_submission_receipts_v1) FROM block_submission_state_v1 s JOIN block_submission_candidates_v1 c USING(candidate_id)`).Scan(&m.Pending, &m.InFlight, &m.AcceptedFinal, &m.RejectedFinal, &m.Conflict, &m.AcceptedReceiptCount, &m.AccountingIntentCount, &m.AccountingPendingCount, &m.AccountingEffectCount, &m.AccountingConflictCount, &oldestSeconds, &m.LastAcceptedAt); err != nil {
		return m, err
	}
	if oldestSeconds != nil {
		m.OldestPendingAge = time.Duration(*oldestSeconds * float64(time.Second))
	}
	if s.jobRecovery.Load() != nil || s.discoveryRecovery.Load() != nil {
		latchSQL, latchArgs := s.conflictLatchSQLV1(1)
		var latched bool
		if err := s.pool.QueryRow(ctx, `SELECT `+latchSQL, latchArgs...).Scan(&latched); err != nil {
			return m, err
		}
		if !latched {
			if s.jobRecovery.Load() != nil {
				m.RecoveredConflict++
			}
			if s.discoveryRecovery.Load() != nil {
				m.RecoveredConflict++
				m.RecoveredAccountingConflict = 1
			}
		}
	}
	return m, nil
}

func validateFrozenCandidate(c FrozenCandidateV1) error {
	if err := validateJournalIdentity(c.ChainID, c.BackendID); err != nil {
		return err
	}
	if c.Height == nil || c.Height.Sign() < 0 || c.Height.BitLen() > 64 || c.TemplateSeq == nil || c.TemplateSeq.Sign() < 0 || c.TemplateSeq.BitLen() > 64 {
		return errors.New("invalid candidate uint64 identity")
	}
	if len(c.FrozenPayload) == 0 || len(c.HeaderBytes) != journalHeaderBytesV1 || len(c.ExtraNonce2) != 4 || len(c.NonceBytes) != 4 {
		return errors.New("candidate frozen bytes have invalid width")
	}
	if !bytes.Equal(c.ParentHash[:], c.HeaderBytes[4:36]) || !bytes.Equal(c.NonceBytes[:], c.HeaderBytes[181:185]) || c.HeaderBytes[176] != 0x0a || !bytes.Equal(c.HeaderBytes[177:181], []byte{0, 0, 0, 0}) {
		return errors.New("candidate header fields are inconsistent")
	}
	if new(big.Int).SetUint64(binary.LittleEndian.Uint64(c.HeaderBytes[140:148])).Cmp(c.Height) != 0 {
		return errors.New("candidate height does not match header")
	}
	first := blake2b.Sum256(c.HeaderBytes[:])
	hash := blake2b.Sum256(first[:])
	if !bytes.Equal(hash[:], c.ExpectedBlockHash[:]) {
		return errors.New("candidate Core hash does not match frozen header")
	}
	if candidateIDForHashV1(c.ExpectedBlockHash) != c.CandidateID {
		return errors.New("candidate id does not match Core hash")
	}
	payload, err := encodeFrozenCandidateJournalV1(c)
	if err != nil {
		return err
	}
	if !bytes.Equal(payload, c.FrozenPayload) || sha256.Sum256(payload) != c.FrozenDigest {
		return errors.New("candidate frozen payload or digest does not match identity")
	}
	return nil
}

func encodeFrozenCandidateJournalV1(c FrozenCandidateV1) ([]byte, error) {
	chain, err := minimalUnsignedBytes(c.ChainID)
	if err != nil {
		return nil, err
	}
	height, err := minimalUnsignedBytes(c.Height)
	if err != nil {
		return nil, err
	}
	seq, err := minimalUnsignedBytes(c.TemplateSeq)
	if err != nil {
		return nil, err
	}
	if !backendIDPattern.MatchString(c.BackendID) {
		return nil, fmt.Errorf("invalid backend id %q", c.BackendID)
	}
	payload := append([]byte("BDAG-POOL-SUBMISSION-FROZEN/v1"), 0)
	payload = appendLengthPrefixed(payload, chain)
	payload = append(payload, c.ExpectedBlockHash[:]...)
	payload = appendLengthPrefixed(payload, c.HeaderBytes[:])
	payload = append(payload, c.ExtraNonce2[:]...)
	payload = append(payload, c.SharePowHash[:]...)
	payload = append(payload, c.ParentHash[:]...)
	payload = appendLengthPrefixed(payload, height)
	payload = appendLengthPrefixed(payload, seq)
	payload = appendLengthPrefixed(payload, []byte(c.BackendID))
	return payload, nil
}

func minimalUnsignedBytes(value *big.Int) ([]byte, error) {
	if value == nil || value.Sign() < 0 {
		return nil, errors.New("unsigned value is nil or negative")
	}
	if value.Sign() == 0 {
		return []byte{0}, nil
	}
	return value.Bytes(), nil
}

func appendLengthPrefixed(dst, value []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	dst = append(dst, length[:]...)
	return append(dst, value...)
}

func candidateIDForHashV1(hash [32]byte) [32]byte {
	h := sha256.New()
	h.Write([]byte("BDAG-POOL-SUBMISSION-ID/v1"))
	h.Write([]byte{0})
	h.Write(hash[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func canonicalSubmitRequestV1(method SubmitMethod, header, extra []byte) ([]byte, error) {
	if method != SubmitMethodV2 && method != SubmitMethodLegacy {
		return nil, errors.New("unsupported submit method")
	}
	if len(header) != journalHeaderBytesV1 || len(extra) != 4 {
		return nil, errors.New("invalid frozen request inputs")
	}
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"`)
	body = append(body, string(method)...)
	body = append(body, []byte(`","params":["`)...)
	body = append(body, hex.EncodeToString(header)...)
	body = append(body, []byte(`",`)...)
	body = strconvAppendUint(body, uint64(binary.BigEndian.Uint32(extra)))
	body = append(body, []byte(`]}`)...)
	return body, nil
}

func strictPersistedMethodNotFound(httpStatus *int, body, responseSHA256 []byte, truncated bool) bool {
	if httpStatus == nil || *httpStatus != 200 || truncated || len(body) == 0 || len(body) > maxSubmissionResponseBytes || len(responseSHA256) != sha256.Size {
		return false
	}
	digest := sha256.Sum256(body)
	if !bytes.Equal(digest[:], responseSHA256) {
		return false
	}
	top, err := decodeUniqueJSONObjectV1(body)
	if err != nil || len(top) != 3 {
		return false
	}
	if value, ok := top["jsonrpc"]; !ok || string(bytes.TrimSpace(value)) != `"2.0"` {
		return false
	}
	if value, ok := top["id"]; !ok || string(bytes.TrimSpace(value)) != "1" {
		return false
	}
	errorRaw, ok := top["error"]
	if !ok {
		return false
	}
	errorFields, err := decodeUniqueJSONObjectV1(errorRaw)
	if err != nil || len(errorFields) != 2 || string(bytes.TrimSpace(errorFields["code"])) != "-32601" {
		return false
	}
	var message string
	if raw, ok := errorFields["message"]; !ok || json.Unmarshal(raw, &message) != nil {
		return false
	}
	return true
}

func decodeUniqueJSONObjectV1(raw []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil, errors.New("expected JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("object key is not a string")
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate JSON field %q", key)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = append([]byte(nil), value...)
	}
	if token, err := dec.Token(); err != nil || token != json.Delim('}') {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("malformed JSON object")
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("trailing JSON value")
		}
		return nil, err
	}
	return fields, nil
}

func strconvAppendUint(dst []byte, value uint64) []byte {
	if value == 0 {
		return append(dst, '0')
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return append(dst, buf[i:]...)
}

func candidateEqual(a, b FrozenCandidateV1) bool {
	return a.CandidateID == b.CandidateID && a.FrozenDigest == b.FrozenDigest && bytes.Equal(a.FrozenPayload, b.FrozenPayload) && bigEqual(a.ChainID, b.ChainID) && a.ExpectedBlockHash == b.ExpectedBlockHash && a.HeaderBytes == b.HeaderBytes && a.ExtraNonce2 == b.ExtraNonce2 && a.NonceBytes == b.NonceBytes && a.SharePowHash == b.SharePowHash && a.ParentHash == b.ParentHash && bigEqual(a.Height, b.Height) && bigEqual(a.TemplateSeq, b.TemplateSeq) && a.BackendID == b.BackendID
}

// A refreshed local pool job can describe the exact same solved block with
// a different sequence. Validate both complete frozen records first, then
// compare using the first observation's sequence. Never rewrite either record
// or accept differences in work, chain, backend or economic accounting intent.
func candidateMatchesRefreshedJob(first, incoming FrozenCandidateV1) bool {
	if validateFrozenCandidate(first) != nil || validateFrozenCandidate(incoming) != nil {
		return false
	}
	incoming.TemplateSeq = new(big.Int).Set(first.TemplateSeq)
	payload, err := encodeFrozenCandidateJournalV1(incoming)
	if err != nil {
		return false
	}
	incoming.FrozenPayload = payload
	incoming.FrozenDigest = sha256.Sum256(payload)
	return candidateEqual(first, incoming)
}

func bigEqual(a, b *big.Int) bool { return a != nil && b != nil && a.Cmp(b) == 0 }

func differenceMask(a, b FrozenCandidateV1) string {
	var out []string
	if a.CandidateID != b.CandidateID {
		out = append(out, "candidate_id")
	}
	if a.FrozenDigest != b.FrozenDigest {
		out = append(out, "frozen_digest")
	}
	if !bytes.Equal(a.FrozenPayload, b.FrozenPayload) {
		out = append(out, "frozen_payload")
	}
	if !bigEqual(a.ChainID, b.ChainID) {
		out = append(out, "chain_id")
	}
	if a.ExpectedBlockHash != b.ExpectedBlockHash {
		out = append(out, "block_hash")
	}
	if a.HeaderBytes != b.HeaderBytes {
		out = append(out, "header")
	}
	if a.ExtraNonce2 != b.ExtraNonce2 {
		out = append(out, "extra_nonce2")
	}
	if a.NonceBytes != b.NonceBytes {
		out = append(out, "nonce")
	}
	if a.SharePowHash != b.SharePowHash {
		out = append(out, "share_pow_hash")
	}
	if a.ParentHash != b.ParentHash {
		out = append(out, "parent_hash")
	}
	if !bigEqual(a.Height, b.Height) {
		out = append(out, "height")
	}
	if !bigEqual(a.TemplateSeq, b.TemplateSeq) {
		out = append(out, "template_seq")
	}
	if a.BackendID != b.BackendID {
		out = append(out, "backend_id")
	}
	return strings.Join(out, "|")
}

func insertConflict(ctx context.Context, tx pgx.Tx, candidateID [32]byte, kind string, existing, incoming, payload []byte, attemptID []byte, mask string) error {
	id := [16]byte{}
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO block_submission_conflicts_v1 (conflict_id,candidate_id,conflict_kind,existing_frozen_digest,incoming_frozen_digest,incoming_frozen_payload,related_attempt_id,difference_mask,observed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp())`, id[:], candidateID[:], kind, nullableBytes(existing), nullableBytes(incoming), nullableBytes(payload), nullableBytes(attemptID), mask)
	return err
}

func latchCandidateConflict(ctx context.Context, tx pgx.Tx, candidateID [32]byte) error {
	_, err := tx.Exec(ctx, `UPDATE block_submission_state_v1 SET state='conflict',state_version=state_version+1,next_attempt_at=NULL,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,finalized_at=COALESCE(finalized_at,clock_timestamp()),updated_at=clock_timestamp() WHERE candidate_id=$1 AND state <> 'conflict'`, candidateID[:])
	return err
}

func validateSubmitDecision(decision SubmitDecisionV1) error {
	switch decision {
	case SubmitDecisionPending, SubmitDecisionAcceptedFinal, SubmitDecisionRejectedFinal, SubmitDecisionConflict, SubmitDecisionMethodNotFound:
		return nil
	default:
		return fmt.Errorf("invalid submit decision %q", decision)
	}
}

func observationCompatibleWithDecision(candidate FrozenCandidateV1, o RPCObservationV1, d SubmitDecisionV1) bool {
	if len(o.BlockHash) != 0 && (len(o.BlockHash) != 32 || !bytes.Equal(o.BlockHash, candidate.ExpectedBlockHash[:])) {
		return false
	}
	if d == SubmitDecisionConflict || d == SubmitDecisionPending || d == SubmitDecisionMethodNotFound {
		return true
	}
	if o.Accepted == nil || o.Final == nil || o.Retryable == nil || o.Code == nil || o.Message == nil || o.TemplateSeq == nil {
		return false
	}
	code := strings.ToLower(strings.TrimSpace(*o.Code))
	if d == SubmitDecisionAcceptedFinal {
		if (code != "accepted" && code != "already_accepted") || !*o.Accepted || !*o.Final || *o.Retryable || len(o.BlockHash) != 32 || !bytes.Equal(o.BlockHash, candidate.ExpectedBlockHash[:]) || o.Height == nil {
			return false
		}
		// Retain Core's counter as metadata, not Pool publication identity.
		if o.TemplateSeq.Sign() < 0 || o.TemplateSeq.BitLen() > 64 {
			return false
		}
		return code == "already_accepted" || o.Height.Cmp(candidate.Height) == 0
	}
	return d == SubmitDecisionRejectedFinal && !*o.Accepted && *o.Final && !*o.Retryable && finalRejectionCode(code)
}

func finalRejectionCode(code string) bool {
	switch code {
	case "already_rejected", "stale_parent", "tip_overdue", "expired", "invalid_pow", "txroot_mismatch", "governance_rejected":
		return true
	default:
		return false
	}
}

func terminalStateContradiction(state string, finalCode *string, existingHash []byte, candidate FrozenCandidateV1, o RPCObservationV1, d SubmitDecisionV1) bool {
	if state != SubmissionStateAcceptedFinal && state != SubmissionStateRejectedFinal {
		return false
	}
	if d == SubmitDecisionConflict {
		return true
	}
	if state == SubmissionStateAcceptedFinal {
		return d != SubmitDecisionAcceptedFinal || (len(existingHash) == 32 && len(o.BlockHash) == 32 && !bytes.Equal(existingHash, o.BlockHash)) || !observationCompatibleWithDecision(candidate, o, d)
	}
	if d == SubmitDecisionAcceptedFinal {
		return true
	}
	if d == SubmitDecisionRejectedFinal && finalCode != nil && o.Code != nil && strings.ToLower(strings.TrimSpace(*o.Code)) != *finalCode {
		return true
	}
	return false
}

func nullableString(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}
func nullableBytes(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return v
}
func nullableBig(v *big.Int) any {
	if v == nil {
		return nil
	}
	return v.String()
}
func nullableTime(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v
}

// verifyJournalCatalogV1 checks the catalog properties that are safety
// relevant at startup. The migration performs the same checks before it
// commits; keeping the runtime check independent means a drifted database
// cannot be used merely because migration was skipped.
func verifyJournalCatalogV1(ctx context.Context, q queryRower) error {
	tables := map[string][]string{
		"block_submission_candidates_v1":        {"candidate_id", "frozen_digest", "frozen_payload", "chain_id", "expected_block_hash", "header_bytes", "extra_nonce2", "nonce_bytes", "share_pow_hash", "parent_hash", "height", "template_seq", "backend_id", "created_at"},
		"block_submission_state_v1":             {"candidate_id", "state", "state_version", "attempt_count", "next_attempt_at", "lease_owner", "lease_token", "lease_expires_at", "last_attempt_id", "last_code", "last_message", "final_code", "authoritative_block_hash", "authoritative_height", "authoritative_template_seq", "finalized_at", "updated_at"},
		"block_submission_attempts_v1":          {"attempt_id", "candidate_id", "attempt_no", "lease_token", "rpc_method", "request_body", "request_sha256", "phase", "prepared_at", "completed_at", "transport_class", "http_status", "response_prefix", "response_truncated", "response_prefix_sha256", "accepted", "final", "retryable", "code", "message", "block_hash", "height", "template_seq", "decision"},
		"block_submission_conflicts_v1":         {"conflict_id", "candidate_id", "conflict_kind", "existing_frozen_digest", "incoming_frozen_digest", "incoming_frozen_payload", "related_attempt_id", "difference_mask", "observed_at"},
		"accepted_block_submission_receipts_v1": {"candidate_id", "source_attempt_id", "chain_id", "block_hash", "candidate_height", "core_code", "core_height", "core_template_seq", "accepted_at"},
	}
	for table, expected := range tables {
		var relkind []byte
		if err := q.QueryRow(ctx, `SELECT relkind::text FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=$1`, table).Scan(&relkind); err != nil {
			return fmt.Errorf("journal table %s is missing: %w", table, err)
		}
		if string(relkind) != "r" {
			return fmt.Errorf("journal object %s is not a table", table)
		}
		var got string
		if err := q.QueryRow(ctx, `SELECT COALESCE(string_agg(attname, ',' ORDER BY attnum),'') FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped`, table).Scan(&got); err != nil {
			return fmt.Errorf("verify journal columns %s: %w", table, err)
		}
		if got != strings.Join(expected, ",") {
			return fmt.Errorf("journal schema drift in %s: got %s want %s", table, got, strings.Join(expected, ","))
		}
	}
	type columnCheck struct{ table, column, typ, nullable, def string }
	checks := []columnCheck{
		{"block_submission_candidates_v1", "candidate_id", "bytea", "NO", ""}, {"block_submission_candidates_v1", "frozen_digest", "bytea", "NO", ""}, {"block_submission_candidates_v1", "frozen_payload", "bytea", "NO", ""}, {"block_submission_candidates_v1", "chain_id", "numeric", "NO", ""}, {"block_submission_candidates_v1", "expected_block_hash", "bytea", "NO", ""}, {"block_submission_candidates_v1", "header_bytes", "bytea", "NO", ""}, {"block_submission_candidates_v1", "extra_nonce2", "bytea", "NO", ""}, {"block_submission_candidates_v1", "nonce_bytes", "bytea", "NO", ""}, {"block_submission_candidates_v1", "share_pow_hash", "bytea", "NO", ""}, {"block_submission_candidates_v1", "parent_hash", "bytea", "NO", ""}, {"block_submission_candidates_v1", "height", "numeric", "NO", ""}, {"block_submission_candidates_v1", "template_seq", "numeric", "NO", ""}, {"block_submission_candidates_v1", "backend_id", "text", "NO", ""}, {"block_submission_candidates_v1", "created_at", "timestamptz", "NO", "clock_timestamp"},
		{"block_submission_state_v1", "candidate_id", "bytea", "NO", ""}, {"block_submission_state_v1", "state", "text", "NO", ""}, {"block_submission_state_v1", "state_version", "int8", "NO", ""}, {"block_submission_state_v1", "attempt_count", "int8", "NO", ""}, {"block_submission_state_v1", "next_attempt_at", "timestamptz", "YES", ""}, {"block_submission_state_v1", "lease_owner", "bytea", "YES", ""}, {"block_submission_state_v1", "lease_token", "bytea", "YES", ""}, {"block_submission_state_v1", "lease_expires_at", "timestamptz", "YES", ""}, {"block_submission_state_v1", "last_attempt_id", "bytea", "YES", ""}, {"block_submission_state_v1", "last_code", "text", "YES", ""}, {"block_submission_state_v1", "last_message", "text", "YES", ""}, {"block_submission_state_v1", "final_code", "text", "YES", ""}, {"block_submission_state_v1", "authoritative_block_hash", "bytea", "YES", ""}, {"block_submission_state_v1", "authoritative_height", "numeric", "YES", ""}, {"block_submission_state_v1", "authoritative_template_seq", "numeric", "YES", ""}, {"block_submission_state_v1", "finalized_at", "timestamptz", "YES", ""}, {"block_submission_state_v1", "updated_at", "timestamptz", "NO", "clock_timestamp"},
		{"block_submission_attempts_v1", "attempt_id", "bytea", "NO", ""}, {"block_submission_attempts_v1", "candidate_id", "bytea", "NO", ""}, {"block_submission_attempts_v1", "attempt_no", "int8", "NO", ""}, {"block_submission_attempts_v1", "lease_token", "bytea", "NO", ""}, {"block_submission_attempts_v1", "rpc_method", "text", "NO", ""}, {"block_submission_attempts_v1", "request_body", "bytea", "NO", ""}, {"block_submission_attempts_v1", "request_sha256", "bytea", "NO", ""}, {"block_submission_attempts_v1", "phase", "text", "NO", ""}, {"block_submission_attempts_v1", "prepared_at", "timestamptz", "NO", ""}, {"block_submission_attempts_v1", "completed_at", "timestamptz", "YES", ""}, {"block_submission_attempts_v1", "transport_class", "text", "YES", ""}, {"block_submission_attempts_v1", "http_status", "int4", "YES", ""}, {"block_submission_attempts_v1", "response_prefix", "bytea", "YES", ""}, {"block_submission_attempts_v1", "response_truncated", "bool", "NO", "false"}, {"block_submission_attempts_v1", "response_prefix_sha256", "bytea", "NO", "decode"}, {"block_submission_attempts_v1", "accepted", "bool", "YES", ""}, {"block_submission_attempts_v1", "final", "bool", "YES", ""}, {"block_submission_attempts_v1", "retryable", "bool", "YES", ""}, {"block_submission_attempts_v1", "code", "text", "YES", ""}, {"block_submission_attempts_v1", "message", "text", "YES", ""}, {"block_submission_attempts_v1", "block_hash", "bytea", "YES", ""}, {"block_submission_attempts_v1", "height", "numeric", "YES", ""}, {"block_submission_attempts_v1", "template_seq", "numeric", "YES", ""}, {"block_submission_attempts_v1", "decision", "text", "NO", ""},
		{"block_submission_conflicts_v1", "conflict_id", "bytea", "NO", ""}, {"block_submission_conflicts_v1", "candidate_id", "bytea", "NO", ""}, {"block_submission_conflicts_v1", "conflict_kind", "text", "NO", ""}, {"block_submission_conflicts_v1", "existing_frozen_digest", "bytea", "YES", ""}, {"block_submission_conflicts_v1", "incoming_frozen_digest", "bytea", "YES", ""}, {"block_submission_conflicts_v1", "incoming_frozen_payload", "bytea", "YES", ""}, {"block_submission_conflicts_v1", "related_attempt_id", "bytea", "YES", ""}, {"block_submission_conflicts_v1", "difference_mask", "text", "NO", ""}, {"block_submission_conflicts_v1", "observed_at", "timestamptz", "NO", "clock_timestamp"},
		{"accepted_block_submission_receipts_v1", "candidate_id", "bytea", "NO", ""}, {"accepted_block_submission_receipts_v1", "source_attempt_id", "bytea", "NO", ""}, {"accepted_block_submission_receipts_v1", "chain_id", "numeric", "NO", ""}, {"accepted_block_submission_receipts_v1", "block_hash", "bytea", "NO", ""}, {"accepted_block_submission_receipts_v1", "candidate_height", "numeric", "NO", ""}, {"accepted_block_submission_receipts_v1", "core_code", "text", "NO", ""}, {"accepted_block_submission_receipts_v1", "core_height", "numeric", "YES", ""}, {"accepted_block_submission_receipts_v1", "core_template_seq", "numeric", "NO", ""}, {"accepted_block_submission_receipts_v1", "accepted_at", "timestamptz", "NO", "clock_timestamp"},
	}
	for _, check := range checks {
		var dataType, udt, nullable string
		var precision, scale *int32
		var def *string
		if err := q.QueryRow(ctx, `SELECT data_type,udt_name,is_nullable,numeric_precision,numeric_scale,column_default FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2`, check.table, check.column).Scan(&dataType, &udt, &nullable, &precision, &scale, &def); err != nil {
			return fmt.Errorf("verify journal column %s.%s: %w", check.table, check.column, err)
		}
		if check.typ == "numeric" {
			if dataType != "numeric" || precision == nil || scale == nil {
				return fmt.Errorf("journal type drift in %s.%s", check.table, check.column)
			}
			wantPrecision := int32(78)
			if check.column == "height" || check.column == "template_seq" || check.column == "authoritative_height" || check.column == "authoritative_template_seq" || check.column == "candidate_height" || check.column == "core_height" || check.column == "core_template_seq" {
				wantPrecision = 20
			}
			if *precision != wantPrecision || *scale != 0 {
				return fmt.Errorf("journal numeric typmod drift in %s.%s", check.table, check.column)
			}
		} else if udt != check.typ {
			return fmt.Errorf("journal type drift in %s.%s: %s", check.table, check.column, udt)
		}
		if nullable != check.nullable {
			return fmt.Errorf("journal nullability drift in %s.%s", check.table, check.column)
		}
		if check.def == "clock_timestamp" && (def == nil || !strings.Contains(strings.ReplaceAll(*def, " ", ""), "clock_timestamp()")) {
			return fmt.Errorf("journal default drift in %s.%s", check.table, check.column)
		}
		if check.def == "decode" && (def == nil || !strings.Contains(strings.ReplaceAll(*def, " ", ""), "decode(repeat('00'::text,32),'hex'::text)")) {
			return fmt.Errorf("journal default drift in %s.%s", check.table, check.column)
		}
		if check.def == "false" && (def == nil || strings.ReplaceAll(*def, " ", "") != "false") {
			return fmt.Errorf("journal default drift in %s.%s", check.table, check.column)
		}
	}
	indexNames := []string{"block_submission_candidates_v1_pkey", "block_submission_candidates_v1_expected_block_hash_key", "block_submission_state_v1_pkey", "block_submission_attempts_v1_pkey", "block_submission_attempts_candidate_no_v1_idx", "block_submission_state_due_v1_idx", "block_submission_conflicts_v1_candidate_v1_idx", "accepted_block_submission_receipts_v1_pkey", "accepted_block_submission_receipts_v1_source_attempt_id_key", "accepted_block_submission_receipts_v1_block_hash_key", "accepted_block_submission_receipts_time_v1_idx"}
	for _, name := range indexNames {
		var exists bool
		if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=$1 AND relkind='i')`, name).Scan(&exists); err != nil {
			return fmt.Errorf("verify journal index %s: %w", name, err)
		}
		if !exists {
			return fmt.Errorf("journal index %s is missing", name)
		}
	}
	var catalogDigest string
	if err := q.QueryRow(ctx, `SELECT md5(string_agg(conname||':'||pg_get_constraintdef(oid,true),'|' ORDER BY conrelid::regclass::text,conname)) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid::regclass::text IN ('block_submission_candidates_v1','block_submission_state_v1','block_submission_attempts_v1','block_submission_conflicts_v1','accepted_block_submission_receipts_v1')`).Scan(&catalogDigest); err != nil {
		return fmt.Errorf("verify journal constraints: %w", err)
	}
	if catalogDigest != "8d5681e13c7d4197c393d44357f3c9c4" {
		return errors.New("journal constraint definition drift")
	}
	if err := q.QueryRow(ctx, `SELECT md5(string_agg(indexname||':'||indexdef,'|' ORDER BY indexname)) FROM pg_indexes WHERE schemaname=current_schema() AND indexname IN ('block_submission_candidates_v1_pkey','block_submission_candidates_v1_expected_block_hash_key','block_submission_state_v1_pkey','block_submission_attempts_v1_pkey','block_submission_attempts_candidate_no_v1_idx','block_submission_state_due_v1_idx','block_submission_conflicts_v1_pkey','block_submission_conflicts_v1_candidate_v1_idx','accepted_block_submission_receipts_v1_pkey','accepted_block_submission_receipts_v1_source_attempt_id_key','accepted_block_submission_receipts_v1_block_hash_key','accepted_block_submission_receipts_time_v1_idx')`).Scan(&catalogDigest); err != nil {
		return fmt.Errorf("verify journal indexes: %w", err)
	}
	if catalogDigest != "456eddae33659d738af6010b00e2ee5d" {
		return errors.New("journal index definition drift")
	}
	triggerChecks := map[string]string{"block_submission_candidates_v1_immutable": "reject_immutable_submission_row_v1", "block_submission_state_v1_transition": "enforce_submission_state_transition_v1", "block_submission_attempts_v1_immutable": "reject_completed_submission_attempt_update_v1", "block_submission_attempts_v1_no_delete": "reject_immutable_submission_row_v1", "block_submission_conflicts_v1_immutable": "reject_immutable_submission_row_v1", "accepted_block_submission_receipts_v1_immutable": "reject_immutable_submission_row_v1"}
	for trigger, function := range triggerChecks {
		var got string
		if err := q.QueryRow(ctx, `SELECT p.proname FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname=$1 AND NOT t.tgisinternal`, trigger).Scan(&got); err != nil {
			return fmt.Errorf("verify journal trigger %s: %w", trigger, err)
		}
		if got != function {
			return fmt.Errorf("journal trigger %s is bound to %s", trigger, got)
		}
	}
	for _, name := range []string{"reject_immutable_submission_row_v1", "reject_completed_submission_attempt_update_v1", "enforce_submission_state_transition_v1", "make_submission_retry_delay"} {
		var count int
		if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() AND p.proname=$1`, name).Scan(&count); err != nil {
			return fmt.Errorf("verify journal function %s: %w", name, err)
		}
		if count != 1 {
			return fmt.Errorf("journal function %s is missing or duplicated", name)
		}
	}
	if err := q.QueryRow(ctx, `SELECT md5(string_agg(tgname||':'||tgrelid::regclass::text||':'||pg_get_triggerdef(oid,true),'|' ORDER BY tgname)) FROM pg_trigger WHERE NOT tgisinternal AND tgrelid::regclass::text IN ('block_submission_candidates_v1','block_submission_state_v1','block_submission_attempts_v1','block_submission_conflicts_v1','accepted_block_submission_receipts_v1')`).Scan(&catalogDigest); err != nil {
		return fmt.Errorf("verify journal triggers: %w", err)
	}
	if catalogDigest != "6676ae5cf2ffa32a687a9f8894d5d986" {
		return errors.New("journal trigger definition drift")
	}
	functionDigests := map[string]string{
		"reject_immutable_submission_row_v1":            "5e1b176dcfcc518ea0855f912089d4b3",
		"reject_completed_submission_attempt_update_v1": "34f9e3688206d1263773dc207619ba58",
		"enforce_submission_state_transition_v1":        "c51eba4451f2e49b5a097d4262733022",
		"make_submission_retry_delay":                   "4adf74bb20c113a4b8a35f9b6bb8c04c",
	}
	for name, want := range functionDigests {
		var got string
		if err := q.QueryRow(ctx, `SELECT md5(prosrc) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() AND p.proname=$1`, name).Scan(&got); err != nil {
			return fmt.Errorf("verify journal function body %s: %w", name, err)
		}
		if got != want {
			return fmt.Errorf("journal function body drift in %s", name)
		}
	}
	return nil
}

var _ Storage = (*PostgresStorage)(nil)
