package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/crypto/blake2b"

	"asicPool/internal/pool/storage"
)

const (
	frozenHeaderBytesV1      = 185
	maxSubmitResponseBytesV1 = 65536
)

// These byte arrays deliberately include an actual NUL byte. Do not replace
// them with a Go string spelling of "\\0": that is two bytes and changes the
// durable v1 identity.
var frozenCandidatePrefixV1 = []byte{
	0x42, 0x44, 0x41, 0x47, 0x2d, 0x50, 0x4f, 0x4f, 0x4c, 0x2d,
	0x53, 0x55, 0x42, 0x4d, 0x49, 0x53, 0x53, 0x49, 0x4f, 0x4e,
	0x2d, 0x46, 0x52, 0x4f, 0x5a, 0x45, 0x4e, 0x2f, 0x76, 0x31,
	0x00,
}

var candidateIDPrefixV1 = []byte{
	0x42, 0x44, 0x41, 0x47, 0x2d, 0x50, 0x4f, 0x4f, 0x4c, 0x2d,
	0x53, 0x55, 0x42, 0x4d, 0x49, 0x53, 0x53, 0x49, 0x4f, 0x4e,
	0x2d, 0x49, 0x44, 0x2f, 0x76, 0x31, 0x00,
}

var submissionBackendIDPatternV1 = regexp.MustCompile(`^[a-z0-9._-]{1,64}$`)

type FrozenCandidateV1 = storage.FrozenCandidateV1
type CandidateViewV1 = storage.CandidateViewV1
type LeaseV1 = storage.LeaseV1
type LeasedCandidateV1 = storage.LeasedCandidateV1
type PreparedAttemptV1 = storage.PreparedAttemptV1
type RPCObservationV1 = storage.RPCObservationV1
type TransitionResultV1 = storage.TransitionResultV1
type SubmitMethod = storage.SubmitMethod
type SubmitDecisionV1 = storage.SubmitDecisionV1

const (
	submitMethodV2     = storage.SubmitMethodV2
	submitMethodLegacy = storage.SubmitMethodLegacy
)

func coreBlockHashV1(header []byte) ([32]byte, error) {
	var out [32]byte
	if len(header) != frozenHeaderBytesV1 {
		return out, fmt.Errorf("Core header length %d, want %d", len(header), frozenHeaderBytesV1)
	}
	first := blake2b.Sum256(header)
	return blake2b.Sum256(first[:]), nil
}

func candidateIDV1(expected [32]byte) [32]byte {
	var preimage [59]byte
	copy(preimage[:27], candidateIDPrefixV1)
	copy(preimage[27:], expected[:])
	return sha256.Sum256(preimage[:])
}

func minimalUnsignedBytesV1(value *big.Int) ([]byte, error) {
	if value == nil || value.Sign() < 0 {
		return nil, errors.New("unsigned value is nil or negative")
	}
	if value.Sign() == 0 {
		return []byte{0}, nil
	}
	return value.Bytes(), nil
}

func appendLengthPrefixedV1(dst, value []byte) ([]byte, error) {
	if uint64(len(value)) > uint64(^uint32(0)) {
		return nil, errors.New("length-prefixed value exceeds uint32")
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	dst = append(dst, length[:]...)
	dst = append(dst, value...)
	return dst, nil
}

func encodeFrozenCandidateV1(candidate FrozenCandidateV1) ([]byte, error) {
	if candidate.ChainID == nil || candidate.Height == nil || candidate.TemplateSeq == nil {
		return nil, errors.New("candidate numeric identity is incomplete")
	}
	chain, err := minimalUnsignedBytesV1(candidate.ChainID)
	if err != nil {
		return nil, err
	}
	height, err := minimalUnsignedBytesV1(candidate.Height)
	if err != nil {
		return nil, err
	}
	seq, err := minimalUnsignedBytesV1(candidate.TemplateSeq)
	if err != nil {
		return nil, err
	}
	if len(candidate.BackendID) == 0 || len(candidate.BackendID) > 64 || !submissionBackendIDPatternV1.MatchString(candidate.BackendID) {
		return nil, fmt.Errorf("invalid backend id %q", candidate.BackendID)
	}
	payload := make([]byte, 0, 320)
	payload = append(payload, frozenCandidatePrefixV1...)
	payload, err = appendLengthPrefixedV1(payload, chain)
	if err != nil {
		return nil, err
	}
	payload = append(payload, candidate.ExpectedBlockHash[:]...)
	payload, err = appendLengthPrefixedV1(payload, candidate.HeaderBytes[:])
	if err != nil {
		return nil, err
	}
	payload = append(payload, candidate.ExtraNonce2[:]...)
	payload = append(payload, candidate.SharePowHash[:]...)
	payload = append(payload, candidate.ParentHash[:]...)
	for _, part := range [][]byte{height, seq, []byte(candidate.BackendID)} {
		payload, err = appendLengthPrefixedV1(payload, part)
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func freezeBlockCandidateV1(job *JobData, header, extraNonce2, sharePowHash []byte, chainID *big.Int) (FrozenCandidateV1, error) {
	var candidate FrozenCandidateV1
	if job == nil {
		return candidate, errors.New("missing block candidate job")
	}
	if len(header) != frozenHeaderBytesV1 {
		return candidate, fmt.Errorf("candidate header length %d, want %d", len(header), frozenHeaderBytesV1)
	}
	if len(sharePowHash) != 32 {
		return candidate, fmt.Errorf("share PoW hash length %d, want 32", len(sharePowHash))
	}
	if chainID == nil || chainID.Sign() < 0 {
		return candidate, errors.New("invalid chain id")
	}
	if len(job.ParentBytes) != 32 || !bytes.Equal(job.ParentBytes, header[4:36]) {
		return candidate, errors.New("candidate parent does not match header")
	}
	if len(job.Height) != 8 {
		return candidate, errors.New("candidate height is not eight little-endian bytes")
	}
	if job.BackendName == "" || !submissionBackendIDPatternV1.MatchString(job.BackendName) {
		return candidate, fmt.Errorf("invalid candidate backend %q", job.BackendName)
	}
	if header[176] != 0x0a || !bytes.Equal(header[177:181], []byte{0, 0, 0, 0}) {
		return candidate, errors.New("invalid BlockDAG PoW header marker")
	}
	candidate.ChainID = new(big.Int).Set(chainID)
	copy(candidate.HeaderBytes[:], header)
	if len(extraNonce2) != 4 {
		return candidate, errors.New("extraNonce2 must contain exactly four bytes")
	}
	copy(candidate.ExtraNonce2[:], extraNonce2)
	copy(candidate.NonceBytes[:], header[181:185])
	copy(candidate.SharePowHash[:], sharePowHash)
	copy(candidate.ParentHash[:], header[4:36])
	if len(job.Height) >= 8 {
		candidate.Height = new(big.Int).SetUint64(binary.LittleEndian.Uint64(job.Height))
	} else {
		candidate.Height = new(big.Int)
	}
	candidate.TemplateSeq = new(big.Int).SetUint64(job.TemplateSeq)
	candidate.BackendID = job.BackendName
	hash, err := coreBlockHashV1(header)
	if err != nil {
		return candidate, err
	}
	candidate.ExpectedBlockHash = hash
	candidate.CandidateID = candidateIDV1(hash)
	payload, err := encodeFrozenCandidateV1(candidate)
	if err != nil {
		return candidate, err
	}
	candidate.FrozenPayload = payload
	candidate.FrozenDigest = sha256.Sum256(payload)
	return candidate, nil
}

// freezeCandidateWithExtraNonce2V1 completes the candidate after the caller
// supplies the exact Stratum bytes. It is separate so the header constructor
// cannot accidentally derive a different byte order for extraNonce2.
func freezeCandidateWithExtraNonce2V1(candidate FrozenCandidateV1, extraNonce2 []byte) (FrozenCandidateV1, error) {
	if len(extraNonce2) != 4 {
		return candidate, errors.New("extraNonce2 must contain exactly four bytes")
	}
	copy(candidate.ExtraNonce2[:], extraNonce2)
	payload, err := encodeFrozenCandidateV1(candidate)
	if err != nil {
		return candidate, err
	}
	candidate.FrozenPayload = payload
	candidate.FrozenDigest = sha256.Sum256(payload)
	return candidate, nil
}

func buildSubmitRequestV1(candidate FrozenCandidateV1, method SubmitMethod) ([]byte, error) {
	if method != storage.SubmitMethodV2 && method != storage.SubmitMethodLegacy {
		return nil, fmt.Errorf("unsupported submit method %q", method)
	}
	if len(candidate.HeaderBytes) != frozenHeaderBytesV1 {
		return nil, errors.New("candidate header is not frozen")
	}
	body := make([]byte, 0, 470)
	body = append(body, `{"jsonrpc":"2.0","id":1,"method":`...)
	body = strconv.AppendQuote(body, string(method))
	body = append(body, `,"params":["`...)
	body = append(body, hex.EncodeToString(candidate.HeaderBytes[:])...)
	body = append(body, `",`...)
	value := binary.BigEndian.Uint32(candidate.ExtraNonce2[:])
	body = strconv.AppendUint(body, uint64(value), 10)
	body = append(body, `]}`...)
	return body, nil
}

func decodeUniqueObjectV1(raw []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
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
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing JSON value")
	}
	if err := ensureJSONEOFV1(dec); err != nil {
		return nil, err
	}
	return fields, nil
}

func ensureJSONEOFV1(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func rawStringV1(raw json.RawMessage, field string) (string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", fmt.Errorf("%s is missing or null", field)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s is not a string", field)
	}
	return value, nil
}

func rawBoolV1(raw json.RawMessage, field string) (bool, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false, fmt.Errorf("%s is missing or null", field)
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("%s is not a boolean", field)
	}
	return value, nil
}

func rawUintV1(raw json.RawMessage, field string) (*big.Int, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, fmt.Errorf("%s is missing or null", field)
	}
	text := string(raw)
	if text == "" || text[0] == '-' || (len(text) > 1 && text[0] == '0') {
		return nil, fmt.Errorf("%s has invalid integer encoding", field)
	}
	for _, ch := range text {
		if ch < '0' || ch > '9' {
			return nil, fmt.Errorf("%s is not an unsigned integer", field)
		}
	}
	value, ok := new(big.Int).SetString(text, 10)
	if !ok || value.BitLen() > 64 {
		return nil, fmt.Errorf("%s is out of range", field)
	}
	return value, nil
}

func rawHashV1(raw json.RawMessage, field string) ([]byte, error) {
	value, err := rawStringV1(raw, field)
	if err != nil {
		return nil, err
	}
	value = strings.TrimPrefix(strings.TrimPrefix(value, "0x"), "0X")
	if len(value) != 64 {
		return nil, fmt.Errorf("%s must be 32-byte hex", field)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%s is malformed hex", field)
	}
	return decoded, nil
}

func decodeSubmitResponseV1(body []byte) (RPCObservationV1, error) {
	var observation RPCObservationV1
	if len(body) > maxSubmitResponseBytesV1 {
		observation.ResponseTruncated = true
		observation.ResponsePrefix = append([]byte(nil), body[:maxSubmitResponseBytesV1]...)
		return observation, errors.New("oversize submit response")
	}
	observation.ResponsePrefix = append([]byte(nil), body...)
	top, err := decodeUniqueObjectV1(body)
	if err != nil {
		return observation, fmt.Errorf("malformed JSON-RPC response: %w", err)
	}
	if raw, ok := top["jsonrpc"]; !ok {
		return observation, errors.New("missing jsonrpc")
	} else if value, err := rawStringV1(raw, "jsonrpc"); err != nil || value != "2.0" {
		return observation, errors.New("invalid jsonrpc version")
	}
	if raw, ok := top["id"]; !ok || string(bytes.TrimSpace(raw)) != "1" {
		return observation, errors.New("wrong JSON-RPC response id")
	}
	resultRaw, hasResult := top["result"]
	errorRaw, hasError := top["error"]
	if hasResult && hasError && !bytes.Equal(bytes.TrimSpace(errorRaw), []byte("null")) {
		return observation, errors.New("JSON-RPC result and error conflict")
	}
	if hasError && !bytes.Equal(bytes.TrimSpace(errorRaw), []byte("null")) {
		if len(top) != 3 {
			return observation, errors.New("non-strict JSON-RPC error fields")
		}
		errorFields, err := decodeUniqueObjectV1(errorRaw)
		if err != nil || len(errorFields) != 2 {
			return observation, errors.New("malformed JSON-RPC error")
		}
		if raw, ok := errorFields["code"]; !ok || string(bytes.TrimSpace(raw)) != "-32601" {
			return observation, errors.New("non-method-not-found JSON-RPC error")
		}
		message, err := rawStringV1(errorFields["message"], "error.message")
		if err != nil {
			return observation, err
		}
		code := "-32601"
		observation.Code = &code
		observation.Message = &message
		observation.MethodNotFound = true
		return observation, errors.New("submit method not found")
	}
	if !hasResult || bytes.Equal(bytes.TrimSpace(resultRaw), []byte("null")) {
		return observation, errors.New("non-structured submit result")
	}
	result, err := decodeUniqueObjectV1(resultRaw)
	if err != nil {
		return observation, fmt.Errorf("malformed structured result: %w", err)
	}
	accepted, err := rawBoolV1(result["accepted"], "accepted")
	if err != nil {
		return observation, err
	}
	final, err := rawBoolV1(result["final"], "final")
	if err != nil {
		return observation, err
	}
	retryable, err := rawBoolV1(result["retryable"], "retryable")
	if err != nil {
		return observation, err
	}
	code, err := rawStringV1(result["code"], "code")
	if err != nil {
		return observation, err
	}
	message, err := rawStringV1(result["message"], "message")
	if err != nil {
		return observation, err
	}
	height, err := rawUintV1(result["height"], "height")
	if err != nil {
		return observation, err
	}
	seq, err := rawUintV1(result["template_seq"], "template_seq")
	if err != nil {
		return observation, err
	}
	blockHash, err := rawHashV1(result["block_hash"], "block_hash")
	if err != nil {
		return observation, err
	}
	observation.Accepted = &accepted
	observation.Final = &final
	observation.Retryable = &retryable
	observation.Code = &code
	observation.Message = &message
	observation.BlockHash = blockHash
	observation.Height = height
	observation.TemplateSeq = seq
	return observation, nil
}

func classifySubmitDecisionV1(candidate FrozenCandidateV1, observation RPCObservationV1) SubmitDecisionV1 {
	if len(observation.BlockHash) > 0 && !bytes.Equal(observation.BlockHash, candidate.ExpectedBlockHash[:]) {
		return storage.SubmitDecisionConflict
	}
	if observation.Accepted == nil || observation.Final == nil || observation.Retryable == nil || observation.Code == nil || observation.Message == nil || observation.Height == nil || observation.TemplateSeq == nil {
		return storage.SubmitDecisionPending
	}
	code := strings.ToLower(strings.TrimSpace(*observation.Code))
	if code == "accepted" || code == "already_accepted" {
		if !*observation.Accepted || !*observation.Final || *observation.Retryable || len(observation.BlockHash) != 32 || !bytes.Equal(observation.BlockHash, candidate.ExpectedBlockHash[:]) {
			return storage.SubmitDecisionConflict
		}
		if code == "accepted" && observation.Height.Cmp(candidate.Height) != 0 {
			return storage.SubmitDecisionConflict
		}
		if code == "already_accepted" && observation.Height.Sign() != 0 && observation.Height.Cmp(candidate.Height) != 0 {
			return storage.SubmitDecisionConflict
		}
		// Core's response counter is independent of Pool publication sequence.
		// The exact candidate hash and height bind acceptance.
		if observation.TemplateSeq.Sign() < 0 || observation.TemplateSeq.BitLen() > 64 {
			return storage.SubmitDecisionConflict
		}
		return storage.SubmitDecisionAcceptedFinal
	}
	if code == "duplicate" || code == "already_known" {
		return storage.SubmitDecisionPending
	}
	if *observation.Accepted && *observation.Final && !*observation.Retryable {
		return storage.SubmitDecisionPending
	}
	if !*observation.Accepted && *observation.Final && !*observation.Retryable && finalRejectionCodeV1(code) {
		return storage.SubmitDecisionRejectedFinal
	}
	if !*observation.Accepted && !*observation.Final && *observation.Retryable {
		return storage.SubmitDecisionPending
	}
	return storage.SubmitDecisionPending
}

func finalRejectionCodeV1(code string) bool {
	switch code {
	case "already_rejected", "stale_parent", "tip_overdue", "expired", "invalid_pow", "txroot_mismatch", "governance_rejected":
		return true
	default:
		return false
	}
}
