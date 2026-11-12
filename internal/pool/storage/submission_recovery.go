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
	"io"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// RefreshedJobRecoveryProofV1 is an offline operator attestation, not a chain
// oracle. Witness reads must be independently collected before invocation.
// No network endpoint may construct or apply this proof on the operator's behalf.
type RefreshedJobRecoveryProofV1 struct {
	Version              string   `json:"version"`
	CandidateID          string   `json:"candidateId"`
	ConflictID           string   `json:"conflictId"`
	AttemptID            string   `json:"attemptId"`
	ChainID              string   `json:"chainId"`
	BackendID            string   `json:"backendId"`
	BlockHash            string   `json:"blockHash"`
	HeaderHex            string   `json:"headerHex"`
	NativeHeight         string   `json:"nativeHeight"`
	CoreTemplateSeq      string   `json:"coreTemplateSeq"`
	CheckpointNumber     string   `json:"checkpointNumber"`
	CheckpointHash       string   `json:"checkpointHash"`
	CheckpointStateRoot  string   `json:"checkpointStateRoot"`
	ObservedAtUTC        string   `json:"observedAtUtc"`
	WitnessPeerIDs       []string `json:"witnessPeerIds"`
	OperatorReference    string   `json:"operatorReference"`
	OriginalIntentDigest string   `json:"originalIntentDigest,omitempty"`
	IncomingIntentDigest string   `json:"incomingIntentDigest,omitempty"`
	AdditionalWork       string   `json:"additionalWork,omitempty"`
}

type RefreshedJobRecoveryResultV1 struct {
	Prepared       bool   `json:"prepared"`
	Applied        bool   `json:"applied"`
	AlreadyApplied bool   `json:"alreadyApplied"`
	CandidateID    string `json:"candidateId"`
}

func (s *PostgresStorage) RecoverRefreshedJobV1(ctx context.Context, proof RefreshedJobRecoveryProofV1, apply bool) (RefreshedJobRecoveryResultV1, error) {
	result := RefreshedJobRecoveryResultV1{CandidateID: proof.CandidateID}
	discovery := proof.Version == "discovery-cut-recovery/v1"
	table := "block_submission_job_recoveries_v1"
	if discovery {
		table = "block_submission_discovery_recoveries_v1"
	}
	cidBytes, err := canonicalRecoveryHex(proof.CandidateID, 32)
	if err != nil {
		return result, err
	}
	var cid [32]byte
	copy(cid[:], cidBytes)
	fid, err := canonicalRecoveryHex(proof.ConflictID, 16)
	if err != nil {
		return result, err
	}
	aid, err := canonicalRecoveryHex(proof.AttemptID, 16)
	if err != nil {
		return result, err
	}
	if (!discovery && proof.Version != "refreshed-job-recovery/v1") || len(proof.OperatorReference) < 8 || len(proof.OperatorReference) > 512 || len(proof.WitnessPeerIDs) != 2 || proof.WitnessPeerIDs[0] == proof.WitnessPeerIDs[1] || len(proof.WitnessPeerIDs[0]) < 8 || len(proof.WitnessPeerIDs[1]) < 8 {
		return result, errors.New("invalid explicit recovery attestation")
	}
	if !discovery && (proof.AdditionalWork != "" || proof.OriginalIntentDigest != "" || proof.IncomingIntentDigest != "") {
		return result, errors.New("unexpected discovery fields in historical job recovery")
	}
	observed, err := time.Parse(time.RFC3339Nano, proof.ObservedAtUTC)
	if err != nil || observed.Location() != time.UTC {
		return result, errors.New("recovery observation must be UTC")
	}
	for _, value := range []string{proof.CheckpointHash, proof.CheckpointStateRoot} {
		if _, err := canonicalRecoveryHex(value, 32); err != nil {
			return result, err
		}
	}
	if n, ok := new(big.Int).SetString(proof.CheckpointNumber, 10); !ok || n.Sign() < 0 || n.String() != proof.CheckpointNumber {
		return result, errors.New("invalid checkpoint number")
	}
	if s == nil || s.pool == nil {
		return result, errors.New("recovery database is not connected")
	}
	if err := verifyJournalCatalogV1(ctx, s.pool); err != nil {
		return result, err
	}
	if !discovery {
		if err := s.VerifyAcceptedAccountingV2(ctx); err != nil {
			return result, err
		}
	} else if err := verifyAcceptedAccountingCatalogV2(ctx, s.pool); err != nil {
		return result, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	// Offline-only: serialize with ALL submission/accounting writers. We do
	// not change the old transition trigger or reopen the conflicted state.
	if _, err := tx.Exec(ctx, `LOCK TABLE block_submission_candidates_v1,block_submission_state_v1,block_submission_attempts_v1,block_submission_conflicts_v1,accepted_block_submission_receipts_v1,accepted_block_accounting_conflicts_v1,accepted_block_accounting_intents_v1,accepted_block_accounting_credit_lines_v1,accepted_block_accounting_effects_v1,blocks,credits IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return result, err
	}
	view, err := scanCandidateView(ctx, tx, cid)
	if err != nil {
		return result, err
	}
	c := view.Candidate
	if view.State != SubmissionStateConflict || c.ChainID.String() != proof.ChainID || c.BackendID != proof.BackendID || c.Height.String() != proof.NativeHeight || hex.EncodeToString(c.ExpectedBlockHash[:]) != proof.BlockHash || hex.EncodeToString(c.HeaderBytes[:]) != proof.HeaderHex {
		return result, errors.New("proof does not match original conflicted candidate")
	}
	var count int
	if discovery {
		if err := validateDiscoveryConflictV1(ctx, tx, c, fid, proof); err != nil {
			return result, err
		}
		if err := s.verifyAcceptedAccountingV2(ctx, fid); err != nil {
			return result, err
		}
	} else {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM block_submission_conflicts_v1`).Scan(&count); err != nil {
			return result, err
		}
		if count != 1 {
			return result, errors.New("recovery requires exactly one submission conflict")
		}
		query, args := s.accountingConflictLatchSQLV1(1)
		var latched bool
		if err := tx.QueryRow(ctx, "SELECT "+query, args...).Scan(&latched); err != nil {
			return result, err
		}
		if latched {
			return result, errors.New("unresolved economic conflict")
		}
	}
	if !discovery {
		var kind, mask string
		var existing, incoming, payload, related []byte
		if err := tx.QueryRow(ctx, `SELECT conflict_kind,difference_mask,existing_frozen_digest,incoming_frozen_digest,incoming_frozen_payload,related_attempt_id FROM block_submission_conflicts_v1 WHERE conflict_id=$1 AND candidate_id=$2`, fid, cid[:]).Scan(&kind, &mask, &existing, &incoming, &payload, &related); err != nil {
			return result, err
		}
		if kind != "candidate_identity_mismatch" || mask != "frozen_digest|frozen_payload|template_seq" || len(related) != 0 || !bytes.Equal(existing, c.FrozenDigest[:]) || len(incoming) != 32 || !bytes.Equal(incoming, hashRecoveryBytes(payload)) {
			return result, errors.New("conflict is not a pure refreshed-job mismatch")
		}
		variant, err := decodeFrozenRecoveryCandidateV1(payload)
		if err != nil {
			return result, err
		}
		if c.TemplateSeq.Cmp(variant.TemplateSeq) == 0 || !candidateMatchesRefreshedJob(c, variant) {
			return result, errors.New("conflict payload differs in work or identity")
		}
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM block_submission_attempts_v1 WHERE candidate_id=$1`, cid[:]).Scan(&count); err != nil {
		return result, err
	}
	if count != 1 {
		return result, errors.New("recovery requires exactly one unambiguous submission attempt")
	}
	var method, phase, decision, transport, code, message string
	var request, requestDigest, response, responseDigest, blockHash []byte
	var height, seq string
	var completed time.Time
	var accepted, final, retryable, truncated bool
	var httpStatus int
	if err := tx.QueryRow(ctx, `SELECT rpc_method,phase,decision,transport_class,http_status,request_body,request_sha256,response_prefix,response_prefix_sha256,response_truncated,accepted,final,retryable,code,message,block_hash,height::text,template_seq::text,completed_at FROM block_submission_attempts_v1 WHERE attempt_id=$1 AND candidate_id=$2`, aid, cid[:]).Scan(&method, &phase, &decision, &transport, &httpStatus, &request, &requestDigest, &response, &responseDigest, &truncated, &accepted, &final, &retryable, &code, &message, &blockHash, &height, &seq, &completed); err != nil {
		return result, err
	}
	if method != SubmitMethodV2 || phase != "completed" || decision != SubmitDecisionAcceptedFinal || transport != "http" || httpStatus != 200 || truncated || !accepted || !final || retryable || code != "accepted" || message != "accepted" || !bytes.Equal(blockHash, c.ExpectedBlockHash[:]) || height != c.Height.String() || seq != proof.CoreTemplateSeq || completed.After(observed) || !bytes.Equal(responseDigest, hashRecoveryBytes(response)) {
		return result, errors.New("attempt is not a strict completed accepted observation")
	}
	if _, err := validatePreparedRequest(c, SubmitMethod(method), request, requestDigest); err != nil {
		return result, err
	}
	if err := validateRecoveryResponseV1(response, c, seq); err != nil {
		return result, err
	}
	// Validate original economic intent. Never replace its recipients/amounts.
	intent, err := readAcceptedAccountingIntentTxV2(ctx, tx, cid, true)
	if err != nil {
		return result, err
	}
	if intent.LegacyBlockHash != c.SharePowHash || intent.Height != c.Height.Uint64() {
		return result, errors.New("original accounting intent does not match candidate")
	}
	proofBytes, err := json.Marshal(proof)
	if err != nil {
		return result, err
	}
	proofDigest := sha256.Sum256(proofBytes)
	var relation *string
	if err := tx.QueryRow(ctx, `SELECT to_regclass($1)::text`, table).Scan(&relation); err != nil {
		return result, err
	}
	if relation != nil {
		if err := verifyRecoveryCatalogForV1(ctx, tx, discovery); err != nil {
			return result, err
		}
		var oldProof, oldDigest, oldAttempt []byte
		err := tx.QueryRow(ctx, `SELECT proof_payload,proof_sha256,source_attempt_id FROM `+table+` WHERE conflict_id=$1 AND candidate_id=$2`, fid, cid[:]).Scan(&oldProof, &oldDigest, &oldAttempt)
		if err == nil {
			if !bytes.Equal(oldProof, proofBytes) || !bytes.Equal(oldDigest, proofDigest[:]) || !bytes.Equal(oldAttempt, aid) {
				return result, errors.New("existing recovery proof differs")
			}
			if err := verifyRecoveredReceiptV1(ctx, tx, c, aid, seq, completed); err != nil {
				return result, err
			}
			if discovery {
				if err := verifyAppliedAcceptedAccountingTxV2(ctx, tx, intent); err != nil {
					return result, err
				}
			}
			result.Prepared, result.AlreadyApplied = true, true
			return result, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return result, err
		}
	}
	// A verified, already-applied recovery above is a read-only historical
	// proof check, not a new repair. Unrelated prepared work must survive a
	// restart so the reconciler can resolve it. ALL proof/receipt/intent/effect
	// and catalog checks remain mandatory. New recovery preparation/application
	// still requires the exact original drained-writer gate under table locks.
	if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_state_v1 WHERE state='in_flight')+(SELECT count(*) FROM block_submission_attempts_v1 a WHERE phase='prepared' AND NOT EXISTS(SELECT 1 FROM accepted_block_submission_receipts_v1 r WHERE r.candidate_id=a.candidate_id))`).Scan(&count); err != nil {
		return result, err
	}
	if count != 0 {
		return result, errors.New("recovery requires drained offline submission writers")
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1`, cid[:]).Scan(&count); err != nil {
		return result, err
	}
	if count != 0 {
		return result, errors.New("receipt already exists without this recovery proof")
	}
	result.Prepared = true
	if !apply {
		return result, nil
	}
	if relation == nil {
		ddl := recoveryDDLv1
		if discovery {
			ddl = strings.ReplaceAll(ddl, "block_submission_job_recoveries_v1", table)
			ddl = strings.ReplaceAll(ddl, "REFERENCES block_submission_conflicts_v1", "REFERENCES accepted_block_accounting_conflicts_v1")
		}
		if _, err := tx.Exec(ctx, ddl); err != nil {
			return result, err
		}
	}
	if err := verifyRecoveryCatalogForV1(ctx, tx, discovery); err != nil {
		return result, err
	}
	if err := submissionFaultV1("recovery_receipt", false); err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO accepted_block_submission_receipts_v1(candidate_id,source_attempt_id,chain_id,block_hash,candidate_height,core_code,core_height,core_template_seq,accepted_at) VALUES($1,$2,$3,$4,$5,'accepted',$5,$6,$7)`, cid[:], aid, c.ChainID.String(), c.ExpectedBlockHash[:], c.Height.String(), seq, completed); err != nil {
		return result, err
	}
	if err := submissionFaultV1("recovery_receipt", true); err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+table+`(conflict_id,candidate_id,source_attempt_id,proof_payload,proof_sha256,operator_reference,recovered_at) VALUES($1,$2,$3,$4,$5,$6,clock_timestamp())`, fid, cid[:], aid, proofBytes, proofDigest[:], proof.OperatorReference); err != nil {
		return result, err
	}
	if discovery {
		if err := materializeAcceptedAccountingTxV2(ctx, tx, intent); err != nil {
			return result, err
		}
	}
	if err := commitSubmissionTxV1(ctx, tx, "recovery_commit"); err != nil {
		return result, err
	}
	result.Applied = true
	return result, nil
}

func canonicalRecoveryHex(raw string, size int) ([]byte, error) {
	value, err := hex.DecodeString(raw)
	if err != nil || len(value) != size || hex.EncodeToString(value) != raw {
		return nil, errors.New("invalid canonical recovery hex")
	}
	return value, nil
}

func hashRecoveryBytes(raw []byte) []byte { sum := sha256.Sum256(raw); return sum[:] }

func decodeFrozenRecoveryCandidateV1(payload []byte) (FrozenCandidateV1, error) {
	var c FrozenCandidateV1
	if len(payload) > 2048 || !bytes.HasPrefix(payload, append([]byte("BDAG-POOL-SUBMISSION-FROZEN/v1"), 0)) {
		return c, errors.New("invalid recovery payload prefix/size")
	}
	r := bytes.NewReader(payload[len("BDAG-POOL-SUBMISSION-FROZEN/v1")+1:])
	field := func() ([]byte, error) {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		if n == 0 || int(n) > r.Len() {
			return nil, errors.New("invalid payload length")
		}
		raw := make([]byte, n)
		_, err := io.ReadFull(r, raw)
		return raw, err
	}
	chain, err := field()
	if err != nil {
		return c, err
	}
	c.ChainID = new(big.Int).SetBytes(chain)
	if _, err := io.ReadFull(r, c.ExpectedBlockHash[:]); err != nil {
		return c, err
	}
	header, err := field()
	if err != nil || len(header) != 185 {
		return c, errors.New("invalid payload header")
	}
	copy(c.HeaderBytes[:], header)
	copy(c.NonceBytes[:], header[181:185])
	for _, fixed := range [][]byte{c.ExtraNonce2[:], c.SharePowHash[:], c.ParentHash[:]} {
		if _, err := io.ReadFull(r, fixed); err != nil {
			return c, err
		}
	}
	height, err := field()
	if err != nil {
		return c, err
	}
	c.Height = new(big.Int).SetBytes(height)
	seq, err := field()
	if err != nil {
		return c, err
	}
	c.TemplateSeq = new(big.Int).SetBytes(seq)
	backend, err := field()
	if err != nil || r.Len() != 0 {
		return c, errors.New("invalid payload backend/trailing data")
	}
	c.BackendID = string(backend)
	c.CandidateID = candidateIDForHashV1(c.ExpectedBlockHash)
	c.FrozenPayload = append([]byte(nil), payload...)
	c.FrozenDigest = sha256.Sum256(payload)
	return c, validateFrozenCandidate(c)
}

func validateRecoveryResponseV1(raw []byte, c FrozenCandidateV1, seq string) error {
	outer, err := decodeUniqueJSONObjectV1(raw)
	if err != nil {
		return err
	}
	if e, ok := outer["error"]; ok && string(e) != "null" {
		return errors.New("response contains RPC error")
	}
	obj, err := decodeUniqueJSONObjectV1(outer["result"])
	if err != nil {
		return err
	}
	checks := map[string]string{"accepted": "true", "final": "true", "retryable": "false", "code": `"accepted"`, "message": `"accepted"`, "block_hash": `"0x` + hex.EncodeToString(c.ExpectedBlockHash[:]) + `"`, "height": c.Height.String(), "template_seq": seq}
	for key, want := range checks {
		if strings.TrimSpace(string(obj[key])) != want {
			return fmt.Errorf("response field does not match strict acceptance: %s", key)
		}
	}
	return nil
}

func verifyRecoveredReceiptV1(ctx context.Context, q queryRower, c FrozenCandidateV1, aid []byte, seq string, acceptedAt time.Time) error {
	var count int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1 AND source_attempt_id=$2 AND chain_id=$3 AND block_hash=$4 AND candidate_height=$5 AND core_code='accepted' AND core_height=$5 AND core_template_seq=$6 AND accepted_at=$7`, c.CandidateID[:], aid, c.ChainID.String(), c.ExpectedBlockHash[:], c.Height.String(), seq, acceptedAt).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return errors.New("recovered receipt differs from immutable attempt")
	}
	return nil
}

const recoveryDDLv1 = `CREATE TABLE block_submission_job_recoveries_v1(
 conflict_id BYTEA PRIMARY KEY REFERENCES block_submission_conflicts_v1(conflict_id) CHECK(octet_length(conflict_id)=16),
 candidate_id BYTEA NOT NULL UNIQUE REFERENCES accepted_block_submission_receipts_v1(candidate_id) CHECK(octet_length(candidate_id)=32),
 source_attempt_id BYTEA NOT NULL UNIQUE REFERENCES block_submission_attempts_v1(attempt_id) CHECK(octet_length(source_attempt_id)=16),
 proof_payload BYTEA NOT NULL CHECK(octet_length(proof_payload)>0),
 proof_sha256 BYTEA NOT NULL CHECK(octet_length(proof_sha256)=32),
 operator_reference TEXT NOT NULL CHECK(length(operator_reference) BETWEEN 8 AND 512),
 recovered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE TRIGGER block_submission_job_recoveries_v1_immutable BEFORE UPDATE OR DELETE ON block_submission_job_recoveries_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();`

func verifyRecoveryCatalogV1(ctx context.Context, q queryRower) error {
	return verifyRecoveryCatalogForV1(ctx, q, false)
}

func verifyRecoveryCatalogForV1(ctx context.Context, q queryRower, discovery bool) error {
	table := "block_submission_job_recoveries_v1"
	if discovery {
		table = "block_submission_discovery_recoveries_v1"
	}
	var columns string
	var constraints, triggers int
	if err := q.QueryRow(ctx, `SELECT string_agg(column_name||':'||data_type||':'||is_nullable,',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1`, table).Scan(&columns); err != nil {
		return err
	}
	if columns != "conflict_id:bytea:NO,candidate_id:bytea:NO,source_attempt_id:bytea:NO,proof_payload:bytea:NO,proof_sha256:bytea:NO,operator_reference:text:NO,recovered_at:timestamp with time zone:NO" {
		return errors.New("recovery catalog column drift")
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE conrelid=$1::regclass AND convalidated`, table).Scan(&constraints); err != nil {
		return err
	}
	if constraints != 12 {
		return errors.New("recovery catalog constraint drift")
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=$1::regclass AND NOT t.tgisinternal AND t.tgname=$2 AND t.tgenabled='O' AND p.proname='reject_immutable_submission_row_v1' AND t.tgtype=27`, table, table+"_immutable").Scan(&triggers); err != nil {
		return err
	}
	if triggers != 1 {
		return errors.New("recovery catalog immutable-trigger drift")
	}
	var digest, defaultExpr string
	if err := q.QueryRow(ctx, `SELECT md5(string_agg(conname||':'||pg_get_constraintdef(oid,true),'|' ORDER BY conname)) FROM pg_constraint WHERE conrelid=$1::regclass`, table).Scan(&digest); err != nil {
		return err
	}
	wantConstraints := "63e139c41618a8c0d29b7f28736651a5"
	if discovery {
		wantConstraints = "fda0f2c71a002da8dfd32bfa0db6866a"
	}
	if digest != wantConstraints {
		return errors.New("recovery constraint definition drift")
	}
	if err := q.QueryRow(ctx, `SELECT md5(string_agg(tgname||':'||pg_get_triggerdef(oid,true),'|' ORDER BY tgname)) FROM pg_trigger WHERE tgrelid=$1::regclass AND NOT tgisinternal`, table).Scan(&digest); err != nil {
		return err
	}
	wantTriggers := "cb60bd9001428d0b922c71fdf3886d3c"
	if discovery {
		wantTriggers = "a618591c8b9a2a3f4d65bdd3d1ae84f2"
	}
	if digest != wantTriggers {
		return errors.New("recovery trigger definition drift")
	}
	if err := q.QueryRow(ctx, `SELECT column_default FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name='recovered_at'`, table).Scan(&defaultExpr); err != nil {
		return err
	}
	if defaultExpr != "clock_timestamp()" {
		return errors.New("recovery timestamp default drift")
	}
	return nil
}
