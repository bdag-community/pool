package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Kept only for compatibility with existing router tests. V2 capability
// probing is removed; fallback is driven by a journaled method-not-found
// attempt.
var legacySubmitV2Probed atomic.Bool
var legacySubmitV2Unsupported atomic.Bool
var submitHTTPClient = buildSubmitHTTPClient(0)

const defaultSubmitRPCTimeout = 3 * time.Second

func buildSubmitHTTPClient(timeoutMS int) *http.Client {
	return &http.Client{Timeout: configuredSubmitRPCTimeout(timeoutMS)}
}
func configuredSubmitRPCTimeout(timeoutMS int) time.Duration {
	if timeoutMS <= 0 {
		return defaultSubmitRPCTimeout
	}
	return time.Duration(timeoutMS) * time.Millisecond
}

func appendSubmitBlockHeaderPayload(dst []byte, method, fullHeaderHex string, extraNonce2 uint64) []byte {
	dst = append(dst, `{"jsonrpc":"2.0","id":1,"method":`...)
	dst = strconv.AppendQuote(dst, method)
	dst = append(dst, `,"params":[`...)
	dst = strconv.AppendQuote(dst, fullHeaderHex)
	dst = append(dst, ',')
	dst = strconv.AppendUint(dst, extraNonce2, 10)
	return append(dst, `]}`...)
}

type submitBlockHeaderPayloadCache struct {
	fullHeaderHex string
	extraNonce2   uint64
	mu            sync.Mutex
	legacyPayload []byte
	v2Payload     []byte
}

func newSubmitBlockHeaderPayloadCache(fullHeaderHex string, extraNonce2 uint64) *submitBlockHeaderPayloadCache {
	return &submitBlockHeaderPayloadCache{fullHeaderHex: fullHeaderHex, extraNonce2: extraNonce2}
}
func (c *submitBlockHeaderPayloadCache) payload(method string) []byte {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch method {
	case "submitBlockHeaderV2":
		if c.v2Payload == nil {
			c.v2Payload = c.build(method)
		}
		return c.v2Payload
	case "submitBlockHeader":
		if c.legacyPayload == nil {
			c.legacyPayload = c.build(method)
		}
		return c.legacyPayload
	default:
		return c.build(method)
	}
}
func (c *submitBlockHeaderPayloadCache) build(method string) []byte {
	return appendSubmitBlockHeaderPayload(make([]byte, 0, len(c.fullHeaderHex)+80), method, c.fullHeaderHex, c.extraNonce2)
}
func truncateForLog(b []byte, max int) string {
	s := strings.TrimSpace(string(b))
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}

type structuredSubmitOutcome struct {
	Accepted bool   `json:"accepted"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

func structuredSubmitResult(raw json.RawMessage) (interface{}, error, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, false
	}
	if _, ok := fields["accepted"]; !ok {
		return nil, nil, false
	}
	var outcome structuredSubmitOutcome
	if err := json.Unmarshal(raw, &outcome); err != nil {
		return nil, &submitRPCError{kind: submitErrorTransport, reason: "decode-result", err: err}, true
	}
	if outcome.Accepted {
		var result interface{}
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, &submitRPCError{kind: submitErrorTransport, reason: "decode-result", err: err}, true
		}
		return result, nil, true
	}
	reason := normalizeStructuredSubmitCode(outcome.Code)
	message := strings.TrimSpace(outcome.Message)
	if message == "" {
		message = "structured submit rejected"
	}
	return nil, &submitRPCError{kind: submitErrorSemantic, reason: reason, err: fmt.Errorf("structured submit rejected: code=%s message=%s", outcome.Code, message)}, true
}
func normalizeStructuredSubmitCode(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	switch code {
	case "", "internal_error":
		return "rpc-error"
	case "accepted":
		return "ok"
	case "already_known", "already-known", "duplicate", "duplicate_block", "duplicate-block":
		return "duplicate"
	case "stale_parent", "stale-parent", "parent_not_current", "parent-not-current", "parent_no_longer_current", "parent-no-longer-current":
		return "stale-parent"
	case "tip_overdue", "tip-overdue", "block_overdue", "block-overdue", "overdue", "too_late", "too-late", "late_submit", "late-submit":
		return "tip-overdue"
	case "template_expired", "template-expired", "job_expired", "job-expired", "block_expired", "block-expired":
		return "expired"
	case "template_seq_unknown", "template-seq-unknown":
		return "template-seq-unknown"
	case "invalid_pow", "invalid-pow":
		return "invalid-pow"
	case "txroot_mismatch", "txroot-mismatch":
		return "txroot-mismatch"
	case "state_root_mismatch", "state-root-mismatch":
		return "state-root-mismatch"
	case "node_syncing", "node-syncing", "pool_syncing", "pool-syncing", "bdag_pool_syncing", "bdag-pool-syncing":
		return "node-syncing"
	case "template_not_ready", "template-not-ready", "template_source_unready", "template-source-unready":
		return "template-not-ready"
	case "p2p_not_current", "p2p-not-current":
		return "p2p-not-current"
	case "miner_disabled", "miner-disabled":
		return "miner-disabled"
	case "missing_trie", "missing-trie":
		return "missing-trie"
	case "nonce_too_low", "nonce-too-low":
		return "nonce-too-low"
	default:
		return strings.ReplaceAll(code, "_", "-")
	}
}

type submitErrorKind string

const (
	submitErrorTransport submitErrorKind = "transport"
	submitErrorSemantic  submitErrorKind = "semantic"
)

type submitRPCError struct {
	kind   submitErrorKind
	reason string
	err    error
}

func (e *submitRPCError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}
func (e *submitRPCError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}
func isRetryableSubmitError(err error) bool {
	var e *submitRPCError
	if errors.As(err, &e) {
		if e.kind == submitErrorSemantic {
			switch e.reason {
			case "node-syncing", "transient-tx", "too-busy", "miner-disabled", "nonce-too-low", "p2p-not-current", "missing-trie", "template-seq-unknown", "template-not-ready":
				return true
			case "tip-overdue", "expired", "stale-parent", "stale":
				return false
			default:
				return false
			}
		}
		return e.kind == submitErrorTransport
	}
	if isStaleSubmitReason(classifySubmitErrorReason(err)) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") || strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection reset") || strings.Contains(msg, "too busy") || strings.Contains(msg, "enable miner") || strings.Contains(msg, "http 5") || strings.Contains(msg, "http 429")
}
func classifySubmitErrorReason(err error) string {
	if err == nil {
		return "ok"
	}
	var e *submitRPCError
	if errors.As(err, &e) && e.reason != "" {
		return e.reason
	}
	return classifyRouterError(err)
}
func classifySubmitRPCError(raw json.RawMessage) string {
	msg := strings.ToLower(string(raw))
	switch {
	case strings.Contains(msg, "overdue") || strings.Contains(msg, "too late") || strings.Contains(msg, "late submit"):
		return "tip-overdue"
	case strings.Contains(msg, "expired"):
		return "expired"
	case strings.Contains(msg, "stale parent") || strings.Contains(msg, "stale block") || strings.Contains(msg, "orphan"):
		return "stale-parent"
	case strings.Contains(msg, "parent") && (strings.Contains(msg, "no longer current") || strings.Contains(msg, "not current")):
		return "stale-parent"
	case strings.Contains(msg, "duplicate") || strings.Contains(msg, "already known") || strings.Contains(msg, "already-known"):
		return "duplicate"
	case messageIndicatesNodeSyncing(msg):
		return "node-syncing"
	case strings.Contains(msg, "initial") && strings.Contains(msg, "download"):
		return "node-syncing"
	case strings.Contains(msg, "downloading blocks"):
		return "node-syncing"
	case strings.Contains(msg, "bdag pool syncing") || strings.Contains(msg, "pool syncing"):
		return "node-syncing"
	case strings.Contains(msg, "graph-state") || strings.Contains(msg, "graph state"):
		return "node-syncing"
	case strings.Contains(msg, "template") && strings.Contains(msg, "not ready"):
		return "template-not-ready"
	case strings.Contains(msg, "downloading tx") || strings.Contains(msg, "nonce too low"):
		return "transient-tx"
	case strings.Contains(msg, "missing trie") || strings.Contains(msg, "missing state"):
		return "missing-trie"
	case strings.Contains(msg, "p2p") && strings.Contains(msg, "not current"):
		return "p2p-not-current"
	case strings.Contains(msg, "enable miner") || strings.Contains(msg, "miner disabled"):
		return "miner-disabled"
	case strings.Contains(msg, "invalid proof") || strings.Contains(msg, "invalid pow") || strings.Contains(msg, "proof of work"):
		return "invalid-pow"
	case strings.Contains(msg, "txroot") || strings.Contains(msg, "tx root"):
		return "txroot-mismatch"
	case strings.Contains(msg, "state root") || strings.Contains(msg, "stateroot"):
		return "state-root-mismatch"
	case strings.Contains(msg, "method not found") || strings.Contains(msg, "method-not-found") || strings.Contains(msg, "-32601"):
		return "method-not-found"
	case strings.Contains(msg, "too busy"):
		return "too-busy"
	default:
		return "rpc-error"
	}
}
func isStaleSubmitReason(reason string) bool {
	switch reason {
	case "tip-overdue", "expired", "stale-parent", "stale":
		return true
	default:
		return false
	}
}
func isBackendReadinessSubmitReason(reason string) bool {
	switch reason {
	case "node-syncing", "p2p-not-current", "miner-disabled", "missing-trie", "template-seq-unknown", "template-not-ready":
		return true
	default:
		return false
	}
}
func isDuplicateSubmitReason(reason string) bool {
	switch reason {
	case "duplicate", "duplicate-block", "duplicate-block-global", "duplicate-block-job":
		return true
	default:
		return false
	}
}
func staleSubmitDepthAtLeast(err error, minimum int) bool {
	if !isStaleSubmitReason(classifySubmitErrorReason(err)) {
		return false
	}
	depth, ok := obsoleteDepth(err)
	return ok && depth >= minimum
}
func obsoleteDepth(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	const marker = "obsolete depth:"
	msg := strings.ToLower(err.Error())
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return 0, false
	}
	rest := strings.TrimSpace(msg[idx+len(marker):])
	if rest == "" {
		return 0, false
	}
	depth, found := 0, false
	for _, ch := range rest {
		if ch < '0' || ch > '9' {
			break
		}
		found = true
		depth = depth*10 + int(ch-'0')
	}
	return depth, found
}
