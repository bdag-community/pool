package server

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	// Rate limiting for getBlockTemplate
	lastGBTRequest               time.Time
	lastGBTRequestMu             sync.Mutex
	lastCachedTemplate           *BlockTemplate
	lastCachedBackendName        string
	templateFetchInFlight        bool
	templateFetchBackoffUntil    time.Time
	templateFetchBackoffReason   string
	templateFetchConsecutive     int
	mainChainHeightMu            sync.Mutex
	mainChainHeightBackoffUntil  time.Time
	mainChainHeightBackoffReason string
	mainChainHeightConsecutive   int

	// Pool Address for block template (fees/identification)
	poolAddress string
	// HTTP client with timeout to prevent blocking on slow/unresponsive nodes
	gbtHTTPClient = &http.Client{
		Timeout: 5 * time.Second,
	}

	// Freeze detection: track how long we've been on the same parent
	freezeDetectMu              sync.Mutex
	freezeDetectParent          string
	freezeDetectStart           time.Time
	freezeDetectLastWarn        time.Time
	freezeDetectLastRecover     time.Time
	freezeDetectWarnAfter       = 30 * time.Second // warn after 30s on same parent
	freezeDetectWarnRepeat      = 60 * time.Second // repeat warning every 60s
	freezeDetectRecoverRepeat   = 15 * time.Second
	templateErrorLogMu          sync.Mutex
	templateErrorLogState       = map[string]templateErrorThrottleState{}
	refreshLogMu                sync.Mutex
	refreshLogState             = map[string]templateErrorThrottleState{}
	wsUnsupportedMu             sync.Mutex
	wsUnsupportedUntil          = map[string]time.Time{}
	cleanRefreshMu              sync.Mutex
	lastCleanRefresh            time.Time
	templateRefreshMu           sync.Mutex
	templateRefreshLatest       templateRefreshRequest
	templateRefreshActive       bool
	templateRefreshSeq          uint64
	templateRefreshWorkers      sync.WaitGroup
	cleanTemplateRefreshMu      sync.Mutex
	cleanTemplateRefreshLatest  cleanTemplateRefreshRequest
	cleanTemplateRefreshActive  bool
	cleanTemplateRefreshSeq     uint64
	cleanTemplateRefreshWorkers sync.WaitGroup
)

const defaultTemplateTTLDur = 500 * time.Millisecond
const maxFastDAGTemplateTTLDur = 1 * time.Second
const defaultGBTMinInterval = 1100 * time.Millisecond // Back pressure: defaults above node TargetTimePerBlock.
const defaultGBTPressureInterval = 500 * time.Millisecond
const defaultGBTPressureWindow = 10 * time.Second
const defaultCleanRefreshThrottle = 250 * time.Millisecond
const templateStreamIdleTimeout = 45 * time.Second
const templateStreamPingInterval = 15 * time.Second
const benignTemplateErrorLogInterval = 60 * time.Second
const defaultRefreshLogInterval = 5 * time.Second
const defaultWSUnsupportedBackoff = 5 * time.Minute
const minGBTMinInterval = 100 * time.Millisecond
const defaultTemplateFetchBackoff = 500 * time.Millisecond
const maxTemplateFetchBackoff = 15 * time.Second

type templateErrorThrottleState struct {
	lastLog    time.Time
	suppressed int
}

type cleanTemplateRefreshRequest struct {
	seq         uint64
	reason      string
	backendName string
}

type templateRefreshRequest struct {
	seq    uint64
	reason string
}

// Start both broadcasters from main():
//   go parentWatcher()
//   go jobBroadcaster()

// 1s hygiene broadcaster (keep-alive + VarDiff ticks)
func jobBroadcaster() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		pushVarDiffTicks()
		updatePPLNSMetrics()
		publishPoolJobHealthMetrics(snapshotPoolJobHealth(time.Now(), false))
	}
}

func updatePPLNSMetrics() {
	if PPLNSWindow == nil {
		return
	}
	shares, work, miners := PPLNSWindow.Stats()
	pplnsWindowShares.Set(float64(shares))
	pplnsWindowWork.Set(float64(work))
	pplnsWindowMiners.Set(float64(miners))
}

// parentWatcher subscribes to the node's block template stream via WebSocket.
// On each pushed template it updates the pool's miner jobs immediately.
// Falls back to HTTP polling if the WebSocket connection is unavailable.
func parentWatcher() {
	for {
		backend := templateSubscriptionBackend()
		log.Printf("[WS] Connecting to node for block template subscription (backend=%s)...", backend.name)
		err := runIdentityVerifiedTemplateSubscription(backend)
		if err != nil {
			if router := activeRPCRouter; router != nil {
				router.recordWSDisconnected(backend, err.Error())
			}
			if isWSUnsupportedSubscriptionError(err) {
				markWSUnsupported(backend, err)
			}
			logTemplateFetchError("[WS] Subscription ended -- falling back to polling", err)
		}
		// Poll until we can reconnect
		pollUntilReconnect()
	}
}

func runTemplateSubscription(backend *rpcBackend) error {
	if backend == nil {
		return fmt.Errorf("no RPC backend configured")
	}
	if err := guardTemplateSubscription(activeRPCRouter, backend); err != nil {
		return err
	}
	wsURL := backend.wsURL
	if wsURL == "" {
		var err error
		wsURL, err = rpcToWSURL(backend.url)
		if err != nil {
			return fmt.Errorf("bad RPC URL: %w", err)
		}
	}

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	header := http.Header{}
	header.Set("Authorization", backend.authorizationHeader())
	conn, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Send subscribe request: Blockdag_subscribe("blockTemplate", powType)
	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "Blockdag_subscribe",
		"params":  []interface{}{"blockTemplate", 10},
	}
	if err := conn.WriteJSON(subReq); err != nil {
		return fmt.Errorf("write subscribe: %w", err)
	}

	// Read subscription ID response
	var subResp struct {
		ID     interface{} `json:"id"`
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := conn.ReadJSON(&subResp); err != nil {
		return fmt.Errorf("read subscribe response: %w", err)
	}
	if subResp.Error != nil {
		return fmt.Errorf("subscribe error: %v", subResp.Error)
	}
	log.Printf("[WS] Subscribed to blockTemplate stream (backend=%s sub_id=%s)", backend.name, subResp.Result)
	var initialTpl *BlockTemplate
	subscriptionValidated := false
	initialTpl, err = validateSubscriptionBackend(backend)
	if err != nil {
		return fmt.Errorf("initial template validation: %w", err)
	}
	if router := activeRPCRouter; router != nil {
		router.recordWSConnected(backend)
	}
	if initialTpl != nil {
		subscriptionValidated = true
		if processTemplateUpdate(initialTpl, "ws initial template", "ws tip change to", "ws TTL refresh") {
			return fmt.Errorf("template update requested polling fallback")
		}
	} else {
		// A subscribed WebSocket is not enough for mining. If the selected node
		// accepts the subscription but cannot provide a usable template, return
		// to the polling fallback instead of leaving ASICs connected without
		// current work.
		return fmt.Errorf("initial template validation produced no usable template for backend=%s", backend.name)
	}

	// Keep connection alive with pong handler that resets the deadline
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(templateStreamIdleTimeout))
		return nil
	})

	// Background ping keeps quiet WS streams alive before the read deadline.
	stopPing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(templateStreamPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
			case <-stopPing:
				return
			}
		}
	}()
	defer close(stopPing)

	// The node pushes a template every time it rebuilds one — far faster than
	// processTemplateUpdate can broadcast a job to every miner. Reading and
	// processing in the same loop turns the socket into an unbounded FIFO of
	// stale templates, and the miners fall further behind the tip every second.
	// Only the newest template has any value, so drain the socket at line rate
	// in its own goroutine and keep just the latest push.
	pushes := make(chan *remoteGBTResult, 1)
	readErr := make(chan error, 1)
	go func() {
		for {
			conn.SetReadDeadline(time.Now().Add(templateStreamIdleTimeout))

			var notification struct {
				Method string `json:"method"`
				Params struct {
					Subscription string          `json:"subscription"`
					Result       json.RawMessage `json:"result"`
				} `json:"params"`
			}
			if err := conn.ReadJSON(&notification); err != nil {
				readErr <- fmt.Errorf("read notification: %w", err)
				return
			}

			pushed := new(remoteGBTResult)
			if err := json.Unmarshal(notification.Params.Result, pushed); err != nil {
				log.Printf("[WS] Failed to decode template notification: %v", err)
				continue
			}
			queueLatestTemplatePush(pushes, pushed)
		}
	}()

	// Seq is monotonically increasing per the node's blockTemplate contract;
	// a lower Seq can only be a push that lost a race, never newer work.
	var highestSeq uint64
	for {
		var pushed *remoteGBTResult
		select {
		case err := <-readErr:
			return err
		case pushed = <-pushes:
		}

		if pushed.Seq != 0 && pushed.Seq <= highestSeq {
			continue
		}
		highestSeq = pushed.Seq

		if !subscriptionValidated {
			validationTpl, err := validateSubscriptionBackend(backend)
			if err != nil {
				return fmt.Errorf("template validation: %w", err)
			}
			if validationTpl == nil {
				return fmt.Errorf("template validation produced no usable template for backend=%s", backend.name)
			}
			subscriptionValidated = true
			if processTemplateUpdate(validationTpl, "ws validation template", "ws tip change to", "ws TTL refresh") {
				return fmt.Errorf("template update requested polling fallback")
			}
			continue
		}

		tpl, err := pushed.toBlockTemplate()
		if err != nil {
			return fmt.Errorf("ws template monetary fields: %w", err)
		}
		tpl.BackendName = backend.name
		if !routerAcceptTemplateForBroadcast(activeRPCRouter, backend, tpl, 0, "ws-push") {
			continue
		}
		cacheBlockTemplate(backend.name, tpl)
		if processTemplateUpdate(tpl, "ws push", "ws tip change to", "ws TTL refresh") {
			return fmt.Errorf("template update requested polling fallback")
		}
	}
}

// queueLatestTemplatePush stores push in slot, discarding any template already
// waiting there. The reader must never block on the consumer: a superseded
// template is worthless, so dropping it is always the right call.
func queueLatestTemplatePush(slot chan *remoteGBTResult, push *remoteGBTResult) {
	for {
		select {
		case slot <- push:
			return
		default:
		}
		select {
		case <-slot:
		default:
		}
	}
}

func validateSubscriptionBackend(backend *rpcBackend) (*BlockTemplate, error) {
	if err := guardTemplateSubscription(activeRPCRouter, backend); err != nil {
		return nil, err
	}
	tpl, latency, err := fetchBlockTemplateWithGate(backend)
	router := activeRPCRouter
	if err != nil {
		if router != nil {
			if !recordTemplateFetchReadinessHold(router, backend, err) {
				router.recordTemplateError(backend, err)
			}
		}
		if isSoftSubscriptionValidationError(err) {
			log.Printf("[WS] Initial template validation soft-failed on backend=%s: %v; keeping subscription open", backend.name, err)
			return nil, nil
		}
		return nil, err
	}
	if router != nil {
		router.recordTemplateSuccess(backend, tpl, latency)
	}
	cacheBlockTemplate(backend.name, tpl)
	return tpl, nil
}

func routerAcceptTemplateForBroadcast(router *rpcRouter, backend *rpcBackend, tpl *BlockTemplate, latency time.Duration, source string) bool {
	if router == nil {
		return true
	}
	if !router.recordTemplateSuccess(backend, tpl, latency) {
		return false
	}
	if ready, guardReason := router.backendReadyForActiveTemplateFallback(backend); !ready {
		router.recordTemplateFallbackGuardRejected(backend, source, guardReason)
		return false
	}
	return true
}

func isSoftSubscriptionValidationError(err error) bool {
	if err == nil {
		return false
	}
	reason := classifyRouterError(err)
	switch reason {
	case "initial-download", "too-busy", "http-retryable":
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "blockdag is downloading tx") ||
		strings.Contains(msg, "blockdag is downloading blocks") ||
		strings.Contains(msg, "client in initial download") ||
		strings.Contains(msg, "node is syncing") ||
		strings.Contains(msg, "nonce too low")
}

// pollUntilReconnect falls back to HTTP polling and keeps trying to re-establish
// the WebSocket subscription. Returns when a reconnect attempt succeeds.
func pollUntilReconnect() {
	pollTimer := time.NewTimer(currentGBTPollInterval())
	defer pollTimer.Stop()
	reconnectTicker := time.NewTicker(5 * time.Second)
	defer reconnectTicker.Stop()

	for {
		select {
		case <-pollTimer.C:
			tpl, err := getBlockTemplate()
			if err != nil {
				logTemplateFetchError("[POLL] getBlockTemplate failed", err)
				pollTimer.Reset(currentGBTPollInterval())
				continue
			}
			if processTemplateUpdate(tpl, "poll update", "poll tip change to", "poll TTL refresh") {
				return
			}
			pollTimer.Reset(currentGBTPollInterval())

		case <-reconnectTicker.C:
			backend := templateSubscriptionBackend()
			if backend == nil || backend.wsURL == "" {
				continue
			}
			if wsUnsupportedBackoffActive(backend) {
				continue
			}
			err := probeTemplateSubscription(backend)
			if err == nil {
				log.Printf("[POLL] Node WS subscription reachable, switching back to subscription mode (backend=%s)", backend.name)
				return
			}
			if isWSUnsupportedSubscriptionError(err) {
				markWSUnsupported(backend, err)
			}
		}
	}
}

func probeTemplateSubscription(backend *rpcBackend) error {
	if backend == nil {
		return fmt.Errorf("no RPC backend configured")
	}
	if err := guardTemplateSubscription(activeRPCRouter, backend); err != nil {
		return err
	}
	wsURL := backend.wsURL
	if wsURL == "" {
		var err error
		wsURL, err = rpcToWSURL(backend.url)
		if err != nil {
			return fmt.Errorf("bad RPC URL: %w", err)
		}
	}

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	header := http.Header{}
	header.Set("Authorization", backend.authorizationHeader())
	conn, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "Blockdag_subscribe",
		"params":  []interface{}{"blockTemplate", 10},
	}
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteJSON(subReq); err != nil {
		return fmt.Errorf("write subscribe: %w", err)
	}

	var subResp struct {
		ID     interface{} `json:"id"`
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := conn.ReadJSON(&subResp); err != nil {
		return fmt.Errorf("read subscribe response: %w", err)
	}
	if subResp.Error != nil {
		return fmt.Errorf("subscribe error: %v", subResp.Error)
	}
	return nil
}

func guardTemplateSubscription(router *rpcRouter, backend *rpcBackend) error {
	reason := router.templateFetchPauseReason(backend)
	if reason == "" {
		return nil
	}
	return fmt.Errorf("template subscription paused: %s", reason)
}

func isWSUnsupportedSubscriptionError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "subscribe error") {
		return false
	}
	return strings.Contains(msg, "-32601") ||
		strings.Contains(msg, "method blocktemplate does not exist") ||
		strings.Contains(msg, "method not found") ||
		(strings.Contains(msg, "blocktemplate") && strings.Contains(msg, "not available"))
}

func markWSUnsupported(backend *rpcBackend, err error) {
	if backend == nil {
		return
	}
	backoff := configuredWSUnsupportedBackoff()
	if backoff <= 0 {
		return
	}
	key := wsBackoffKey(backend)
	until := time.Now().Add(backoff)
	wsUnsupportedMu.Lock()
	wsUnsupportedUntil[key] = until
	wsUnsupportedMu.Unlock()
	log.Printf("[WS] backend=%s does not support blockTemplate subscription; staying in HTTP polling for %s: %v", backend.name, backoff, err)
}

func wsUnsupportedBackoffActive(backend *rpcBackend) bool {
	if backend == nil {
		return false
	}
	key := wsBackoffKey(backend)
	now := time.Now()
	wsUnsupportedMu.Lock()
	until, ok := wsUnsupportedUntil[key]
	if ok && now.After(until) {
		delete(wsUnsupportedUntil, key)
		ok = false
	}
	wsUnsupportedMu.Unlock()
	return ok
}

func wsBackoffKey(backend *rpcBackend) string {
	if backend.name != "" {
		return backend.name
	}
	return backend.wsURL
}

func processTemplateUpdate(tpl *BlockTemplate, defaultReason, changedReasonPrefix, ttlReason string) bool {
	if err := enforceTemplateCoinbase(tpl, defaultReason); err != nil {
		log.Printf("[COINBASE] rejecting template update source=%s: %v", defaultReason, err)
		return false
	}
	parent := normalizeHex(tpl.PreviousHash)
	changed := rememberTemplateParent(tpl)

	if checkForFreeze(parent, changed, tpl) {
		return true
	}

	reason := defaultReason
	force := false
	if changed {
		reason = fmt.Sprintf("%s %s", changedReasonPrefix, parent)
	} else if shouldForceTemplateTTLRefresh(time.Now()) {
		force = true
		reason = ttlReason
	}
	maybeBroadcastTemplate(tpl, force, reason)
	return false
}

func shouldForceTemplateTTLRefresh(now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	last, ok := miningWork.lastBroadcastAt()
	return !ok || now.Sub(last) > configuredTemplateTTLRefresh()
}

// remoteGBTResult mirrors the enriched RemoteGBTResult from the blockdag node.
type remoteGBTResult struct {
	Seq             uint64   `json:"seq"`
	HeaderHex       string   `json:"headerhex"`
	TxMerklePath    []string `json:"txmerklepath,omitempty"`
	Version         uint32   `json:"version"`
	PreviousHash    string   `json:"previousblockhash"`
	TxRoot          string   `json:"txroot"`
	StateRoot       string   `json:"stateroot"`
	NBits           string   `json:"nbits"`
	CurTime         int64    `json:"curtime"`
	Height          int64    `json:"height"`
	CoinbaseAddress string   `json:"coinbase_address"`
	Reward          uint64   `json:"reward"`
	BlockFees       int64    `json:"block_fees"`
}

func (r *remoteGBTResult) toBlockTemplate() (*BlockTemplate, error) {
	if r == nil || r.Reward > math.MaxInt64 {
		return nil, errors.New("template reward exceeds signed wire boundary")
	}
	if r.BlockFees < 0 {
		return nil, errors.New("template block fees are negative")
	}
	return &BlockTemplate{
		Version:   int(r.Version),
		TxRoot:    r.TxRoot,
		StateRoot: r.StateRoot,
		PoWDiffReference: struct {
			Nbits string `json:"nbits"`
		}{Nbits: r.NBits},
		CurTime:         int(r.CurTime),
		PreviousHash:    r.PreviousHash,
		Height:          r.Height,
		CoinbaseAddress: r.CoinbaseAddress,
		Reward:          int64(r.Reward),
		BlockFees:       r.BlockFees,
		SourceSeq:       r.Seq,
	}, nil
}

func rpcToWSURL(httpURL string) (string, error) {
	u, err := url.Parse(httpURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = "/ws"
	return u.String(), nil
}

func basicAuthHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func (backend *rpcBackend) authorizationHeader() string {
	if backend == nil {
		return basicAuthHeader("", "")
	}
	if backend.rpcAuthHeader != "" {
		return backend.rpcAuthHeader
	}
	return basicAuthHeader(backend.rpcUser, backend.rpcPassword)
}

// checkForFreeze detects when the blockchain appears frozen (same parent for too long).
// It returns true when router recovery selected a different backend and callers
// should stop using the current template stream.
func checkForFreeze(parent string, changed bool, tpl *BlockTemplate) bool {
	freezeDetectMu.Lock()

	now := time.Now()

	if changed || freezeDetectParent != parent {
		// Parent changed - reset tracking
		freezeDetectParent = parent
		freezeDetectStart = now
		freezeDetectLastWarn = time.Time{} // reset warning timer
		freezeDetectLastRecover = time.Time{}
		freezeDetectMu.Unlock()
		return false
	}

	// Same parent - check how long
	freezeDuration := now.Sub(freezeDetectStart)
	if freezeDuration < freezeDetectWarnAfter {
		freezeDetectMu.Unlock()
		return false
	}

	// Check if we should emit a warning (first time or repeat interval)
	if freezeDetectLastWarn.IsZero() || now.Sub(freezeDetectLastWarn) >= freezeDetectWarnRepeat {
		freezeDetectLastWarn = now

		// Log detailed diagnostic info
		log.Printf("⚠️⚠️⚠️ [FREEZE DETECTED] Same parent hash for %.1f seconds!", freezeDuration.Seconds())
		log.Printf("    Parent: %s", parent)
		log.Printf("    Height: %d", tpl.Height)
		log.Printf("    CurTime from template: %d (now: %d, age: %ds)",
			tpl.CurTime, now.Unix(), now.Unix()-int64(tpl.CurTime))
		log.Printf("    ⚠️ The blockchain node may be frozen or not accepting new blocks!")
		log.Printf("    ⚠️ Check node logs for errors, deadlocks, or sync issues.")
	}
	shouldRecover := freezeDetectLastRecover.IsZero() || now.Sub(freezeDetectLastRecover) >= freezeDetectRecoverRepeat
	if shouldRecover {
		freezeDetectLastRecover = now
	}
	freezeDetectMu.Unlock()

	if !shouldRecover {
		return false
	}

	return false
}

func templateAgeFromCurTime(now time.Time, tpl *BlockTemplate) time.Duration {
	if tpl == nil || tpl.CurTime <= 0 {
		return 0
	}
	age := now.Sub(time.Unix(int64(tpl.CurTime), 0))
	if age < 0 {
		return 0
	}
	return age
}

func fetchAndBroadcastNewJob(reason string) {
	tpl, err := getRefreshBlockTemplate()
	if err != nil {
		logTemplateFetchError("template fetch error", err)
		return
	}
	maybeBroadcastTemplate(tpl, true, reason)
}

func getRefreshBlockTemplate() (*BlockTemplate, error) {
	if refreshBlockTemplateFetcher == nil {
		return getUncachedRefreshBlockTemplate()
	}
	return refreshBlockTemplateFetcher()
}

var refreshBlockTemplateFetcher func() (*BlockTemplate, error)

func init() {
	refreshBlockTemplateFetcher = getUncachedRefreshBlockTemplate
}

func getUncachedRefreshBlockTemplate() (*BlockTemplate, error) {
	backend := singleNodeBackend()
	if backend == nil {
		return nil, fmt.Errorf("no RPC backend configured")
	}
	router := activeRPCRouter
	if err := guardTemplateFetch(router, backend); err != nil {
		return nil, err
	}
	tpl, latency, err := fetchBlockTemplateWithGate(backend)
	if err != nil {
		if router != nil {
			if router.shouldHoldTemplateBackendOnError(backend, err) {
				router.recordTransientTemplateError(backend, err)
			} else if recordTemplateFetchReadinessHold(router, backend, err) {
				// The hold keeps template polling and WS reconnect probes off a syncing node.
			} else {
				router.recordTemplateError(backend, err)
			}
		}
		return nil, err
	}
	if router != nil && !router.recordTemplateSuccess(backend, tpl, latency) {
		return nil, fmt.Errorf("single node returned stale template sequence")
	}
	return tpl, nil
}

func logTemplateFetchError(prefix string, err error) {
	if err == nil {
		return
	}
	reason, benign := classifyTemplateFetchError(err)
	if !benign {
		log.Printf("%s: %v", prefix, err)
		return
	}

	now := time.Now()
	key := prefix + "|" + reason
	templateErrorLogMu.Lock()
	state := templateErrorLogState[key]
	state.suppressed++
	shouldLog := state.lastLog.IsZero() || now.Sub(state.lastLog) >= benignTemplateErrorLogInterval
	suppressed := state.suppressed - 1
	if shouldLog {
		state.lastLog = now
		state.suppressed = 0
	}
	templateErrorLogState[key] = state
	templateErrorLogMu.Unlock()

	if shouldLog {
		log.Printf("%s: %v (transient=%s suppressed=%d)", prefix, err, reason, suppressed)
	}
}

func classifyTemplateFetchError(err error) (string, bool) {
	if err == nil {
		return "none", false
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "nonce too low"):
		return "nonce-too-low", true
	case messageIndicatesNodeSyncing(msg):
		return "node-syncing", true
	case strings.Contains(msg, "template-not-ready") || strings.Contains(msg, "template not ready") || strings.Contains(msg, "get-block-template-not-ready"):
		return "template-not-ready", true
	case strings.Contains(msg, "template-parent-stale") || strings.Contains(msg, "template parent stale"):
		return "template-parent-stale", true
	case strings.Contains(msg, "template-expired") || strings.Contains(msg, "template expired"):
		return "template-expired", true
	case strings.Contains(msg, "template-stale") || strings.Contains(msg, "template stale"):
		return "template-stale", true
	case strings.Contains(msg, "no-consensus-peers") || strings.Contains(msg, "no consensus peers") ||
		strings.Contains(msg, "no-fresh-consensus-peers") || strings.Contains(msg, "no fresh consensus peers"):
		return "no-consensus-peers", true
	case strings.Contains(msg, "p2p-not-current"):
		return "p2p-not-current", true
	case strings.Contains(msg, "p2p-not-fresh") || strings.Contains(msg, "peer-lag"):
		return "p2p-not-fresh", true
	case strings.Contains(msg, "node-health-pending"):
		return "node-health-pending", true
	case strings.Contains(msg, "node-health-stale"):
		return "node-health-stale", true
	case strings.Contains(msg, "bdag pool syncing") || strings.Contains(msg, "pool syncing"):
		return "pool-syncing", true
	case strings.Contains(msg, "graph-state") || strings.Contains(msg, "graph state"):
		return "graph-state-sync", true
	case strings.Contains(msg, "reorg"):
		return "reorg-pressure", true
	case messageIndicatesNodeOverloaded(msg):
		return "too-busy", true
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded"):
		return "timeout", true
	case strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection reset") || strings.Contains(msg, "no such host"):
		return "transport", true
	case strings.Contains(msg, "http 5") || strings.Contains(msg, "http 429"):
		return "http-retryable", true
	case strings.Contains(msg, "client in initial download"):
		return "initial-download", true
	case strings.Contains(msg, "blockdag is downloading blocks"):
		return "block-download", true
	case strings.Contains(msg, "blockdag is downloading tx"):
		return "tx-download", true
	default:
		return "hard", false
	}
}

func recordTemplateFetchReadinessHold(router *rpcRouter, backend *rpcBackend, err error) bool {
	if router == nil || backend == nil || err == nil {
		return false
	}
	reason, benign := classifyTemplateFetchError(err)
	if !benign || !templateFetchErrorBlocksMining(reason) {
		return false
	}
	router.recordTemplateFetchReadinessHold(backend, reason, err)
	return true
}

func pushToAllMinersWithVarDiff(baseNotify []interface{}, baseJob *JobData, seq uint64) {
	// Store for single-client resends
	stampJobInvalidationEpochs(baseJob)
	miningWork.rememberLatestBroadcast(baseNotify, baseJob, seq)

	baseJobID, _ := baseNotify[0].(string)

	now := time.Now()
	for _, c := range snapshotClients() {
		if changed, newP, _ := c.adjustOnTimer(now); changed {
			log.Printf("VarDiff timer update %s: pdiff=%.6f", c.addr, newP)
		}
		pushJobToClient(c, baseNotify, baseJob, seq, baseJobID, preserveNotifyClean)
	}
}

// Resend current job to a specific client (e.g. after difficulty change)
func ResendJobToClient(c *client) {
	ResendJobToClientClean(c, true)
}

func ResendJobToClientClean(c *client, clean bool) {
	if err := issueCurrentWorkToClient(c, cleanOverrideMode(clean)); err != nil {
		log.Printf("[ERROR]Error sending notify to %s: %v", c.addr, err)
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		if c.conn != nil {
			c.conn.Close()
		}
	}
}

func currentJobReadyForResend(c *client) bool {
	if !clientReadyForCurrentJob(c) {
		return false
	}
	return latestBroadcastReadyForResend()
}

func issueCurrentWorkToClient(c *client, clean notifyCleanMode) error {
	leavePublication, err := activeSubmissionDrain.enterPublication()
	if err != nil {
		return err
	}
	defer leavePublication()
	if !clientReadyForCurrentJob(c) {
		return nil
	}
	baseNotify, baseJob, seq, ok := latestBroadcastForResend()
	if !ok {
		return nil
	}
	baseJobID, _ := baseNotify[0].(string)
	return pushJobToClient(c, baseNotify, baseJob, seq, baseJobID, clean)
}

func latestBroadcastForResend() ([]interface{}, *JobData, uint64, bool) {
	baseNotify, baseJob, seq := miningWork.latestBroadcast()
	if baseNotify == nil || !jobReadyForResend(baseJob) {
		return nil, nil, 0, false
	}
	return baseNotify, baseJob, seq, true
}

func latestBroadcastReadyForResend() bool {
	_, _, _, ok := latestBroadcastForResend()
	return ok
}

func jobReadyForResend(job *JobData) bool {
	return job != nil && !jobInvalidatedByEpoch(job) && !job.invalidated.Load() && !job.stale.Load()
}

func stampJobInvalidationEpochs(job *JobData) {
	if job == nil {
		return
	}
	job.InvalidationEpoch = miningWork.currentInvalidationEpoch()
	job.BackendInvalidationEpoch = miningWork.backendInvalidationEpoch(job.BackendName)
}

func pushJobToClient(c *client, baseNotify []interface{}, baseJob *JobData, seq uint64, baseJobID string, cleanOverride notifyCleanMode) error {
	// 1) ensure extranonce1 exists
	c.mu.Lock()
	if c.extranonce1 == ([4]byte{}) {
		v := atomic.AddUint32(&en1Counter, 1)
		binary.LittleEndian.PutUint32(c.extranonce1[:], v)
		log.Printf("[DEBUG_NOTIFY] Extranonce1 was zero for %s, set to: %08x", c.addr, v)
	}
	en1 := c.extranonce1
	en1Hex := hex.EncodeToString(en1[:])
	c.mu.Unlock()

	if en1Hex == "00000000" {
		log.Printf("[ERROR] Extranonce1 is still zero for %s after initialization!", c.addr)
		return fmt.Errorf("extranonce1 is zero")
	}

	// 2) per-client jobID
	jobID := baseJobID + "_" + en1Hex

	// 3) Get current difficulty
	c.diffMu.Lock()
	pdiff := c.pdiff
	prevPdiff := c.prevPdiff
	c.diffMu.Unlock()

	// Bootstrap if needed
	if pdiff <= 0 {
		pdiff = startingPdiff
	}

	// 4) Send difficulty FIRST
	// Note: We send this every time we send a job to ensure sync
	if err := pushDifficultyUpdate(c, pdiff); err != nil {
		return err
	}

	// 5) clone JobData with CORRECT difficulty values
	jobCopy := &JobData{
		Version:                  baseJob.Version,
		TemplateSeq:              seq,
		BackendName:              baseJob.BackendName,
		NBits:                    baseJob.NBits,
		NetworkTarget:            baseJob.NetworkTarget,
		ParentHash:               baseJob.ParentHash,
		ParentBytes:              baseJob.ParentBytes,
		NTime:                    baseJob.NTime,
		CreatedAt:                baseJob.CreatedAt,
		TxRoot:                   baseJob.TxRoot,
		StateRoot:                baseJob.StateRoot,
		Height:                   baseJob.Height,
		CoinbaseAddress:          baseJob.CoinbaseAddress,
		Reward:                   baseJob.Reward,
		BlockFees:                baseJob.BlockFees,
		InvalidationEpoch:        miningWork.currentInvalidationEpoch(),
		BackendInvalidationEpoch: miningWork.backendInvalidationEpoch(baseJob.BackendName),
		TemplateKey:              baseJob.TemplateKey,
		SentAt:                   time.Now(),
		Extranonce1:              en1,
		Difficulty:               pdiff,     // Store the EXACT pdiff we sent
		PrevDiff:                 prevPdiff, // Store the EXACT prevPdiff
	}
	jobCopy.blockSubmitted.Store(false)

	// 6) build per-client notify
	notify := make([]interface{}, len(baseNotify))
	copy(notify, baseNotify)
	notify[0] = jobID

	// Normal broadcasts preserve the template's clean_jobs flag. Resends pass
	// an explicit override: clean=true for recovery, clean=false for routine
	// VarDiff/current-job refreshes.
	// BlockDAG Params: [0:jobID, 1:prev, 2:ver, 3:bits, 4:time, 5:clean]
	setNotifyCleanFlag(notify, cleanOverride)
	jobCopy.Clean = notifyCleanFlag(notify)

	// 7) store in acceptedJobs under per-client jobID
	miningWork.storeAcceptedJob(jobID, jobCopy)

	// 8) Send the job notification
	// log.Printf("[DEBUG_NOTIFY] Sending to %s: jobID=%s baseJobID=%s en1Hex=%s notify=%+v", c.addr, jobID, baseJobID, en1Hex, notify)
	if err := sendPush(c, "mining.notify", notify); err != nil {
		miningWork.deleteAcceptedJob(jobID)
		log.Printf("[ERROR]Error sending notify to %s: %v", c.addr, err)
		return err
	}
	rememberClientCurrentJob(c, jobID, jobCopy)

	// log.Printf("[DEBUG_NOTIFY] Successfully sent jobID=%s to %s (stored in acceptedJobs)", jobID, c.addr)
	return nil
}

func maybeBroadcastTemplate(tpl *BlockTemplate, force bool, reason string) {
	broadcastTemplateWithCleanMode(tpl, force, reason, preserveNotifyClean, nil)
}

func maybeBroadcastCleanTemplate(tpl *BlockTemplate, reason, backendName string) int {
	invalidated := 0
	broadcasted := broadcastTemplateWithCleanMode(tpl, true, reason, forceNotifyCleanTrue, func() {
		invalidated = invalidateAcceptedJobsForBackend(backendName, reason)
	})
	if !broadcasted {
		return 0
	}
	return invalidated
}

func broadcastTemplateWithCleanMode(tpl *BlockTemplate, force bool, reason string, cleanOverride notifyCleanMode, beforePush func()) bool {
	leavePublication, err := activeSubmissionDrain.enterPublication()
	if err != nil {
		return false
	}
	defer leavePublication()
	if err := enforceTemplateCoinbase(tpl, "broadcast"); err != nil {
		log.Printf("[COINBASE] refusing to broadcast template reason=%s: %v", reason, err)
		return false
	}

	// A template publication changes several independently synchronized pieces
	// of state: the strict parent, invalidation epoch, latest broadcast, and each
	// client's current job. Serialize publishers and keep health readers outside
	// that short mutation window so monitoring and auto-healing never react to a
	// partially installed template.
	jobHealthPublicationMu.Lock()
	defer jobHealthPublicationMu.Unlock()

	rememberTemplateParent(tpl)

	key := templateKey(tpl)
	prevKey := miningWork.currentTemplateKey()

	templateChanged := (key != prevKey)
	if !templateChanged && !force {
		return false
	}

	seq := miningWork.nextTemplateSeq()

	jobID := strconv.FormatUint(seq, 10) + "-" + strconv.FormatInt(time.Now().UnixNano(), 16)
	// Use templateChanged as the 'clean' flag. Periodic TTL refreshes (force=true but templateChanged=false)
	// will send clean=false, allowing miners to continue hashing without interruption.
	cleanJobs := templateChanged
	switch cleanOverride {
	case forceNotifyCleanFalse:
		cleanJobs = false
	case forceNotifyCleanTrue:
		cleanJobs = true
	}
	notify, job, err := buildNotify(jobID, tpl, cleanJobs)
	if err != nil {
		log.Printf("failed to build notify for job %s: %v", jobID, err)
		return false
	}
	job.TemplateSeq = seq

	miningWork.rememberTemplateKey(key)
	miningWork.recordBroadcastAt(time.Now())

	if templateBroadcasts != nil {
		templateBroadcasts.Inc()
	}
	if blockHeight != nil {
		blockHeight.Set(float64(tpl.Height))
	}
	if nbitsVal, err := strconv.ParseUint(
		strings.ToLower(strings.TrimPrefix(tpl.PoWDiffReference.Nbits, "0x")), 16, 32,
	); err == nil {
		target := bitsToTarget(uint32(nbitsVal))
		if target.Sign() > 0 {
			diff := new(big.Float).Quo(
				new(big.Float).SetInt(Diff1TargetPool),
				new(big.Float).SetInt(target),
			)
			fDiff, _ := diff.Float64()
			if networkDifficulty != nil {
				networkDifficulty.Set(fDiff)
			}
		}
	}

	if reason == "" {
		reason = "template refresh"
	}
	logTemplateRefresh(reason, seq, normalizeHex(tpl.PreviousHash))

	if beforePush != nil {
		beforePush()
	}
	pushToAllMinersWithVarDiff(notify, job, seq)
	return true
}

func rememberTemplateParent(tpl *BlockTemplate) bool {
	if tpl == nil {
		return false
	}
	parent := normalizeHex(tpl.PreviousHash)
	if parent == "" {
		return false
	}

	changed := miningWork.rememberParent(parent)

	if changed && jobsMarkedStale != nil {
		jobsMarkedStale.Inc()
	}
	return changed
}

func logTemplateRefresh(reason string, seq uint64, parent string) {
	shouldLog, suppressed := shouldLogRefresh(reason, time.Now(), refreshLogInterval())
	if !shouldLog {
		return
	}
	if suppressed > 0 {
		log.Printf("[REFRESH] %s (seq=%d parent=%s suppressed=%d)", reason, seq, parent, suppressed)
		return
	}
	log.Printf("[REFRESH] %s (seq=%d parent=%s)", reason, seq, parent)
}

func refreshLogInterval() time.Duration {
	raw := os.Getenv("POOL_REFRESH_LOG_INTERVAL_SECONDS")
	if raw == "" {
		return defaultRefreshLogInterval
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return defaultRefreshLogInterval
	}
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func shouldLogRefresh(reason string, now time.Time, interval time.Duration) (bool, int) {
	if strings.Contains(strings.ToLower(reason), "error") {
		return true, 0
	}
	if interval <= 0 {
		return false, 0
	}
	if now.IsZero() {
		now = time.Now()
	}

	key := refreshLogKey(reason)
	refreshLogMu.Lock()
	defer refreshLogMu.Unlock()

	state := refreshLogState[key]
	state.suppressed++
	if state.lastLog.IsZero() || now.Sub(state.lastLog) >= interval {
		suppressed := state.suppressed - 1
		state.suppressed = 0
		state.lastLog = now
		refreshLogState[key] = state
		return true, suppressed
	}
	refreshLogState[key] = state
	return false, 0
}

func refreshLogKey(reason string) string {
	reason = strings.TrimSpace(reason)
	lower := strings.ToLower(reason)
	switch {
	case strings.HasPrefix(lower, "ws tip change"):
		return "ws tip change"
	case strings.HasPrefix(lower, "poll tip change"):
		return "poll tip change"
	case strings.HasPrefix(lower, "head change"):
		return "head change"
	case strings.HasPrefix(lower, "block submission processed"):
		return "block submission processed"
	case reason == "":
		return "template refresh"
	default:
		return lower
	}
}

func templateKey(tpl *BlockTemplate) string {
	parent := normalizeHex(tpl.PreviousHash)
	state := normalizeHex(tpl.StateRoot)
	txRoot := normalizeHex(tpl.TxRoot)
	// CurTime is intentionally excluded: a non-clean refresh may advance ntime
	// while preserving the work assembled from the same consensus content. All
	// other header/template inputs that can change candidate validity are bound
	// so retained-job admission cannot cross a difficulty, height, payout, or
	// reward transition merely because the parent and roots happen to match.
	return strings.Join([]string{
		parent,
		state,
		txRoot,
		strconv.Itoa(tpl.Version),
		normalizeHex(tpl.PoWDiffReference.Nbits),
		strconv.FormatInt(tpl.Height, 10),
		normalizeHex(tpl.CoinbaseAddress),
		strconv.FormatInt(tpl.Reward, 10),
		strconv.FormatInt(tpl.BlockFees, 10),
	}, "|")
}

func pushVarDiffTicks() {
	now := time.Now()
	for _, c := range snapshotClients() {
		if changed, newP, _ := c.adjustOnTimer(now); changed {
			log.Printf("VarDiff timer update %s: pdiff=%.6f", c.addr, newP)
			ResendJobToClientClean(c, false) // Routine difficulty resends should not invalidate current work.
		}
	}
}

type notifyCleanMode uint8

const (
	preserveNotifyClean notifyCleanMode = iota
	forceNotifyCleanFalse
	forceNotifyCleanTrue
)

func cleanOverrideMode(clean bool) notifyCleanMode {
	if clean {
		return forceNotifyCleanTrue
	}
	return forceNotifyCleanFalse
}

func setNotifyCleanFlag(notify []interface{}, clean notifyCleanMode) {
	if len(notify) <= 5 {
		return
	}
	switch clean {
	case forceNotifyCleanFalse:
		notify[5] = false
	case forceNotifyCleanTrue:
		notify[5] = true
	}
}

func notifyCleanFlag(notify []interface{}) bool {
	if len(notify) <= 5 {
		return false
	}
	clean, _ := notify[5].(bool)
	return clean
}

func snapshotClients() []*client {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	if len(clients) == 0 {
		return nil
	}
	out := make([]*client, 0, len(clients))
	for c := range clients {
		out = append(out, c)
	}
	return out
}

func activeClientCount() int {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	return len(clients)
}

func forceTemplateRefresh(reason string) {
	clearCachedBlockTemplate()
	requestTemplateRefresh(reason)
}

func requestTemplateRefresh(reason string) {
	if strings.TrimSpace(reason) == "" {
		reason = "template refresh"
	}

	templateRefreshMu.Lock()
	templateRefreshSeq++
	templateRefreshLatest = templateRefreshRequest{
		seq:    templateRefreshSeq,
		reason: reason,
	}
	if templateRefreshActive {
		templateRefreshMu.Unlock()
		return
	}
	templateRefreshActive = true
	templateRefreshMu.Unlock()

	templateRefreshWorkers.Add(1)
	go func() {
		defer templateRefreshWorkers.Done()
		runTemplateRefreshWorker()
	}()
}

func runTemplateRefreshWorker() {
	for {
		req := latestTemplateRefreshRequest()
		fetchAndBroadcastNewJob(req.reason)
		if finishTemplateRefreshAttempt(req.seq) {
			return
		}
	}
}

func latestTemplateRefreshRequest() templateRefreshRequest {
	templateRefreshMu.Lock()
	defer templateRefreshMu.Unlock()
	return templateRefreshLatest
}

func finishTemplateRefreshAttempt(seq uint64) bool {
	templateRefreshMu.Lock()
	defer templateRefreshMu.Unlock()
	if templateRefreshLatest.seq != seq {
		return false
	}
	templateRefreshActive = false
	return true
}

func forceCleanTemplateRefresh(reason string) {
	forceCleanTemplateRefreshForBackend(reason, "")
}

func forceCleanTemplateRefreshForBackend(reason, backendName string) {
	clearCachedBlockTemplate()
	requestCleanTemplateRefresh(reason, backendName)
}

func requestCleanTemplateRefresh(reason, backendName string) {
	if strings.TrimSpace(reason) == "" {
		reason = "clean template refresh"
	}
	recordCleanTemplateRefreshEvent("requested")

	cleanTemplateRefreshMu.Lock()
	cleanTemplateRefreshSeq++
	cleanTemplateRefreshLatest = cleanTemplateRefreshRequest{
		seq:         cleanTemplateRefreshSeq,
		reason:      reason,
		backendName: backendName,
	}
	if cleanTemplateRefreshActive {
		cleanTemplateRefreshMu.Unlock()
		recordCleanTemplateRefreshEvent("coalesced")
		return
	}
	cleanTemplateRefreshActive = true
	cleanTemplateRefreshMu.Unlock()

	cleanTemplateRefreshWorkers.Add(1)
	go func() {
		defer cleanTemplateRefreshWorkers.Done()
		runCleanTemplateRefreshWorker()
	}()
}

func runCleanTemplateRefreshWorker() {
	for {
		req := latestCleanTemplateRefreshRequest()
		recordCleanTemplateRefreshEvent("fetch_started")

		tpl, err := getRefreshBlockTemplate()
		if err != nil {
			logTemplateFetchError("template fetch error", err)
			recordCleanTemplateRefreshEvent("fetch_error")
			handleCleanTemplateRefreshFetchError(req, err)
			if finishCleanTemplateRefreshAttempt(req.seq) {
				return
			}
			continue
		}

		if isCleanTemplateRefreshObsolete(req.seq) {
			recordCleanTemplateRefreshEvent("obsolete_skipped")
			continue
		}

		invalidated := maybeBroadcastCleanTemplate(tpl, req.reason, req.backendName)
		recordCleanTemplateRefreshEvent("broadcast")

		if finishCleanTemplateRefreshAttempt(req.seq) {
			log.Printf("[REFRESH] clean template refresh invalidated_jobs=%d reason=%s", invalidated, req.reason)
			return
		}
	}
}

func latestCleanTemplateRefreshRequest() cleanTemplateRefreshRequest {
	cleanTemplateRefreshMu.Lock()
	defer cleanTemplateRefreshMu.Unlock()
	return cleanTemplateRefreshLatest
}

func activeCleanTemplateRefreshRequest() (cleanTemplateRefreshRequest, bool) {
	cleanTemplateRefreshMu.Lock()
	defer cleanTemplateRefreshMu.Unlock()
	return cleanTemplateRefreshLatest, cleanTemplateRefreshActive
}

func isCleanTemplateRefreshObsolete(seq uint64) bool {
	cleanTemplateRefreshMu.Lock()
	defer cleanTemplateRefreshMu.Unlock()
	return cleanTemplateRefreshLatest.seq != seq
}

func finishCleanTemplateRefreshAttempt(seq uint64) bool {
	cleanTemplateRefreshMu.Lock()
	defer cleanTemplateRefreshMu.Unlock()
	if cleanTemplateRefreshLatest.seq != seq {
		return false
	}
	cleanTemplateRefreshActive = false
	return true
}

func recordCleanTemplateRefreshEvent(event string) {
	if cleanTemplateRefreshEvents != nil {
		cleanTemplateRefreshEvents.WithLabelValues(event).Inc()
	}
}

func handleCleanTemplateRefreshFetchError(req cleanTemplateRefreshRequest, err error) {
	reason, benign := classifyTemplateFetchError(err)
	if !benign || !templateFetchErrorBlocksMining(reason) {
		return
	}
	recordCleanTemplateRefreshEvent("readiness_hold")

	backend := singleNodeBackend()
	backendName := strings.TrimSpace(req.backendName)
	if backendName == "" && backend != nil {
		backendName = backend.name
	}
	if router := activeRPCRouter; router != nil && backend != nil {
		router.recordTemplateRefreshReadinessHold(backend, reason, err)
	}

	if templateFetchReadinessHoldCanPreserveCurrentJobs(reason) {
		requireCurrentParent := templateFetchReadinessHoldRequiresCurrentParent(reason)
		if preserved := preservableCurrentJobsForBackendWithPolicy(backendName, time.Now(), requireCurrentParent); preserved > 0 {
			recordCleanTemplateRefreshEvent("readiness_hold_preserved")
			logTemplateRefresh(
				fmt.Sprintf("template refresh backend not ready: %s preserving_current_jobs=%d", reason, preserved),
				miningWork.currentTemplateSeq(),
				CurrentParent(),
			)
			return
		}
	}

	invalidated := invalidateAcceptedJobsForBackend(backendName, "template refresh backend not ready: "+reason)
	logTemplateRefresh(
		fmt.Sprintf("template refresh backend not ready: %s current_jobs_invalidated=%d", reason, invalidated),
		miningWork.currentTemplateSeq(),
		CurrentParent(),
	)
}

func templateFetchReadinessHoldCanPreserveCurrentJobs(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "transport",
		"timeout",
		"http-retryable",
		"too-busy",
		// Tip-chase micro-sync: a failed GBT refresh must not idle ASICs that
		// still hold tip-aligned work. Replacement happens only when a new
		// template is actually fetched and broadcast.
		"node-syncing",
		"initial-download",
		"get-block-template-not-ready",
		"template-not-ready",
		"pool-syncing",
		"graph-state-sync",
		"reorg-pressure":
		return true
	default:
		return false
	}
}

func templateFetchReadinessHoldRequiresCurrentParent(reason string) bool {
	switch normalizeNodeHealthReason(reason) {
	case "node-syncing", "p2p-not-current", "p2p-not-fresh":
		return true
	default:
		return false
	}
}

func preservableCurrentJobsForBackend(backendName string, now time.Time) int {
	return preservableCurrentJobsForBackendWithPolicy(backendName, now, true)
}

func preservableCurrentJobsForBackendDuringReadinessHold(backendName string, now time.Time) int {
	return preservableCurrentJobsForBackendWithPolicy(backendName, now, false)
}

func preservableCurrentJobsForBackendWithPolicy(backendName string, now time.Time, requireCurrentParent bool) int {
	if now.IsZero() {
		now = time.Now()
	}
	currentParent := CurrentParent()
	preserved := 0
	for _, c := range snapshotClients() {
		current, _ := c.currentJob.Load().(*clientCurrentJob)
		if current == nil || current.job == nil {
			continue
		}
		job := current.job
		if backendName != "" && job.BackendName != backendName {
			continue
		}
		if jobInvalidatedByEpoch(job) || job.invalidated.Load() || job.stale.Load() {
			continue
		}
		if requireCurrentParent && currentParent != "" && job.ParentHash != "" && job.ParentHash != currentParent {
			continue
		}
		if jobTTL > 0 && !job.CreatedAt.IsZero() && now.Sub(job.CreatedAt) > jobTTL {
			continue
		}
		preserved++
	}
	return preserved
}

func templateFetchErrorBlocksMining(reason string) bool {
	switch reason {
	case "initial-download",
		"block-download",
		"node-syncing",
		"pool-syncing",
		"graph-state-sync",
		"template-not-ready",
		"template-expired",
		"template-parent-stale",
		"template-stale",
		"p2p-not-fresh",
		"p2p-not-current",
		"no-consensus-peers",
		"no-fresh-consensus-peers",
		"p2p-unavailable",
		"reorg-pressure":
		return true
	default:
		return false
	}
}

func forceCleanTemplateRefreshThrottled(reason string) bool {
	now := time.Now()
	cleanRefreshMu.Lock()
	if !lastCleanRefresh.IsZero() && now.Sub(lastCleanRefresh) < defaultCleanRefreshThrottle {
		cleanRefreshMu.Unlock()
		return false
	}
	lastCleanRefresh = now
	cleanRefreshMu.Unlock()

	forceCleanTemplateRefresh(reason)
	return true
}

func clearCachedBlockTemplate() {
	// Clear the cached template to force a fresh fetch from the node. Without
	// this, the rate limiter in getBlockTemplate() may return the old cached
	// template after a block submission, causing miners to work on outdated jobs.
	lastGBTRequestMu.Lock()
	lastCachedTemplate = nil
	lastCachedBackendName = ""
	lastGBTRequestMu.Unlock()
}

func invalidateAcceptedJobs(reason string) int {
	return invalidateAcceptedJobsForBackend("", reason)
}

func invalidateAcceptedJobsForBackend(backendName, reason string) int {
	now := time.Now()
	miningWork.incrementInvalidationEpoch(backendName)
	miningWork.recordInvalidationAt(now)

	count := 0
	for _, c := range snapshotClients() {
		current, _ := c.currentJob.Load().(*clientCurrentJob)
		if current == nil || current.job == nil {
			continue
		}
		job := current.job
		if backendName != "" && job.BackendName != backendName {
			continue
		}
		if !jobInvalidatedByEpoch(job) {
			continue
		}
		markJobInvalidated(job, now)
		count++
	}

	if count > 0 {
		log.Printf("[REFRESH] invalidated %d current miner jobs by epoch reason=%s", count, reason)
		return count
	}

	// Keep the historical diagnostic behavior when tests or tooling seed jobs
	// directly without live clients. Production invalidation above is O(active
	// miners), not O(retained jobs), so accepted block handling no longer scans
	// thousands of old jobs before ASICs receive fresh work.
	miningWork.rangeAcceptedJobs(func(_ string, job *JobData) bool {
		if job == nil || (backendName != "" && job.BackendName != backendName) {
			return true
		}
		if !jobInvalidatedByEpoch(job) {
			return true
		}
		markJobInvalidated(job, now)
		count++
		return true
	})
	if count > 0 {
		log.Printf("[REFRESH] invalidated %d accepted jobs by epoch fallback reason=%s", count, reason)
	}
	return count
}

func jobReaper() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-5 * time.Minute)
		miningWork.rangeAcceptedJobs(func(jobID string, job *JobData) bool {
			if job == nil {
				miningWork.deleteAcceptedJob(jobID)
				return true
			}
			if job.CreatedAt.Before(cutoff) {
				miningWork.deleteAcceptedJob(jobID)
			}
			return true
		})
	}
}

func getBlockTemplate() (*BlockTemplate, error) {
	backend := singleNodeBackend()
	if backend == nil {
		return nil, fmt.Errorf("no RPC backend configured")
	}
	router := activeRPCRouter
	if err := guardTemplateFetch(router, backend); err != nil {
		return nil, err
	}
	if cached, ok := cachedBlockTemplate(backend.name); ok {
		return cached, nil
	}
	tpl, latency, err := fetchBlockTemplateWithGate(backend)
	if err != nil {
		if router != nil {
			if router.shouldHoldTemplateBackendOnError(backend, err) {
				router.recordTransientTemplateError(backend, err)
			} else if recordTemplateFetchReadinessHold(router, backend, err) {
				// The hold keeps template polling and WS reconnect probes off a syncing node.
			} else {
				router.recordTemplateError(backend, err)
			}
		}
		return nil, err
	}
	if router != nil && !router.recordTemplateSuccess(backend, tpl, latency) {
		return nil, fmt.Errorf("single node returned stale template sequence")
	}
	cacheBlockTemplate(backend.name, tpl)
	return tpl, nil
}

func getLifecycleChainHeight() (uint64, error) {
	backend := singleNodeBackend()
	if backend == nil {
		return 0, fmt.Errorf("no RPC backend configured")
	}
	if delay, reason, active := mainChainHeightBackoffActive(time.Now()); active {
		return 0, fmt.Errorf("getMainChainHeight throttled: node-overloaded backoff active reason=%s remaining=%s", reason, delay.Round(time.Millisecond))
	}
	height, _, err := fetchMainChainHeightFromBackend(backend)
	if err == nil {
		recordMainChainHeightSuccess()
		return height, nil
	}
	if reason, ok := templateFetchRetryBackoffReason(err); ok {
		recordMainChainHeightBackoff(reason)
		return 0, err
	}
	tpl, templateErr := getBlockTemplate()
	if templateErr == nil {
		return uint64(tpl.Height), nil
	}
	return 0, err
}

func getLifecycleBlockTemplate() (*BlockTemplate, error) {
	return getBlockTemplate()
}

func recordMainChainHeightSuccess() {
	mainChainHeightMu.Lock()
	defer mainChainHeightMu.Unlock()
	mainChainHeightConsecutive = 0
	mainChainHeightBackoffUntil = time.Time{}
	mainChainHeightBackoffReason = ""
}

func recordMainChainHeightBackoff(reason string) {
	reason = normalizeNodeHealthReason(reason)
	if reason == "" || reason == "unknown" {
		reason = "retryable"
	}
	mainChainHeightMu.Lock()
	defer mainChainHeightMu.Unlock()
	mainChainHeightConsecutive++
	backoff := templateFetchBackoffDuration(mainChainHeightConsecutive)
	mainChainHeightBackoffUntil = time.Now().Add(backoff)
	mainChainHeightBackoffReason = reason
}

func mainChainHeightBackoffActive(now time.Time) (time.Duration, string, bool) {
	mainChainHeightMu.Lock()
	defer mainChainHeightMu.Unlock()
	if mainChainHeightBackoffUntil.IsZero() || !now.Before(mainChainHeightBackoffUntil) {
		return 0, "", false
	}
	reason := mainChainHeightBackoffReason
	if reason == "" {
		reason = "retryable"
	}
	return mainChainHeightBackoffUntil.Sub(now), reason, true
}

func guardTemplateFetch(router *rpcRouter, backend *rpcBackend) error {
	reason := router.templateFetchPauseReason(backend)
	if reason == "" {
		return nil
	}
	return fmt.Errorf("getBlockTemplate paused: %s", reason)
}

func templateSubscriptionBackend() *rpcBackend {
	return singleNodeBackend()
}

func singleNodeBackend() *rpcBackend {
	if router := activeRPCRouter; router != nil {
		if backend := router.selectedBackend(); backend != nil {
			return backend
		}
	}
	return legacyRPCBackend()
}

func legacyRPCBackend() *rpcBackend {
	wsURL, _ := rpcToWSURL(rpcURL)
	return &rpcBackend{
		name:          "legacy",
		url:           rpcURL,
		wsURL:         wsURL,
		rpcUser:       rpcUser,
		rpcPassword:   rpcPassword,
		rpcAuthHeader: basicAuthHeader(rpcUser, rpcPassword),
		gbtClient:     gbtHTTPClient,
		submitClient:  submitHTTPClient,
	}
}

func cachedBlockTemplate(backendName string) (*BlockTemplate, bool) {
	lastGBTRequestMu.Lock()
	defer lastGBTRequestMu.Unlock()

	elapsed := time.Since(lastGBTRequest)
	if elapsed < currentGBTPollInterval() && lastCachedTemplate != nil && lastCachedBackendName == backendName {
		if err := enforceTemplateCoinbase(lastCachedTemplate, "cache_read"); err != nil {
			log.Printf("[COINBASE] dropping cached template backend=%s: %v", backendName, err)
			lastCachedTemplate = nil
			lastCachedBackendName = ""
			return nil, false
		}
		return lastCachedTemplate, true
	}
	lastGBTRequest = time.Now()
	return nil, false
}

func cacheBlockTemplate(backendName string, tpl *BlockTemplate) {
	if err := enforceTemplateCoinbase(tpl, "cache_write"); err != nil {
		log.Printf("[COINBASE] refusing to cache template backend=%s: %v", backendName, err)
		return
	}
	lastGBTRequestMu.Lock()
	defer lastGBTRequestMu.Unlock()
	lastCachedTemplate = tpl
	lastCachedBackendName = backendName
	lastGBTRequest = time.Now()
}

func fetchBlockTemplateWithGate(backend *rpcBackend) (*BlockTemplate, time.Duration, error) {
	release, err := acquireTemplateFetchToken(backendNameForGate(backend))
	if err != nil {
		return nil, 0, err
	}
	tpl, latency, err := fetchBlockTemplateFromBackend(backend)
	release(err)
	return tpl, latency, err
}

func backendNameForGate(backend *rpcBackend) string {
	if backend == nil || strings.TrimSpace(backend.name) == "" {
		return "unknown"
	}
	return backend.name
}

func acquireTemplateFetchToken(backendName string) (func(error), error) {
	now := time.Now()
	lastGBTRequestMu.Lock()
	defer lastGBTRequestMu.Unlock()

	if !templateFetchBackoffUntil.IsZero() && now.Before(templateFetchBackoffUntil) {
		reason := templateFetchBackoffReason
		if reason == "" {
			reason = "retryable"
		}
		return nil, fmt.Errorf("getBlockTemplate throttled: node-overloaded backoff active backend=%s reason=%s remaining=%s",
			backendName, reason, templateFetchBackoffUntil.Sub(now).Round(time.Millisecond))
	}
	if templateFetchInFlight {
		return nil, fmt.Errorf("getBlockTemplate throttled: fetch already in flight backend=%s", backendName)
	}
	templateFetchInFlight = true
	lastGBTRequest = now
	return func(err error) {
		recordTemplateFetchGateResult(err)
	}, nil
}

func recordTemplateFetchGateResult(err error) {
	lastGBTRequestMu.Lock()
	defer lastGBTRequestMu.Unlock()

	templateFetchInFlight = false
	if err == nil {
		templateFetchConsecutive = 0
		templateFetchBackoffUntil = time.Time{}
		templateFetchBackoffReason = ""
		lastGBTRequest = time.Now()
		return
	}
	reason, ok := templateFetchRetryBackoffReason(err)
	if !ok {
		return
	}
	templateFetchConsecutive++
	backoff := templateFetchBackoffDuration(templateFetchConsecutive)
	templateFetchBackoffUntil = time.Now().Add(backoff)
	templateFetchBackoffReason = reason
}

func templateFetchRetryBackoffReason(err error) (string, bool) {
	reason, benign := classifyTemplateFetchError(err)
	if benign {
		switch normalizeNodeHealthReason(reason) {
		case "too-busy", "timeout", "transport", "http-retryable":
			return normalizeNodeHealthReason(reason), true
		}
	}
	switch classifyRouterError(err) {
	case "too-busy", "timeout", "transport", "http-retryable":
		return classifyRouterError(err), true
	default:
		return "", false
	}
}

func templateFetchBackoffDuration(consecutive int) time.Duration {
	if consecutive < 1 {
		consecutive = 1
	}
	backoff := defaultTemplateFetchBackoff
	for i := 1; i < consecutive && backoff < maxTemplateFetchBackoff; i++ {
		backoff *= 2
	}
	if backoff > maxTemplateFetchBackoff {
		backoff = maxTemplateFetchBackoff
	}
	return backoff + jitterDuration(backoff/2)
}

func jitterDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	n := time.Now().UnixNano()
	if n < 0 {
		n = -n
	}
	return time.Duration(n % int64(max+1))
}

func fetchBlockTemplateFromBackend(backend *rpcBackend) (*BlockTemplate, time.Duration, error) {
	if backend == nil {
		return nil, 0, fmt.Errorf("no RPC backend configured")
	}
	fetchStart := time.Now()
	latency := time.Duration(0)
	defer func() {
		latency = time.Since(fetchStart)
		if templateFetchDur != nil {
			templateFetchDur.Observe(latency.Seconds())
		}
		if rpcBackendTemplateDur != nil && backend.name != "" && backend.name != "legacy" {
			rpcBackendTemplateDur.WithLabelValues(backend.name).Observe(latency.Seconds())
		}
	}()

	buf := appendGetBlockTemplatePayload(make([]byte, 0, 96+len(poolAddress)), poolAddress)
	req, err := http.NewRequest("POST", backend.url, bytes.NewReader(buf))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", backend.authorizationHeader())
	req.Header.Set("Content-Type", "application/json")

	client := backend.gbtClient
	if client == nil {
		client = gbtHTTPClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, time.Since(fetchStart), err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 257))
		if err != nil {
			return nil, time.Since(fetchStart), fmt.Errorf("read getBlockTemplate error response body: %w", err)
		}
		return nil, time.Since(fetchStart), fmt.Errorf("getBlockTemplate HTTP %d: %s", resp.StatusCode, truncateForLog(body, 256))
	}

	var rpcResp struct {
		Result BlockTemplate   `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := decodeSingleJSON(resp.Body, &rpcResp); err != nil {
		return nil, time.Since(fetchStart), err
	}

	if len(rpcResp.Error) > 0 && string(rpcResp.Error) != "null" {
		return nil, time.Since(fetchStart), fmt.Errorf("RPC error in getBlockTemplate: %s", truncateForLog(rpcResp.Error, 512))
	}

	rpcResp.Result.BackendName = backend.name
	if err := enforceTemplateCoinbase(&rpcResp.Result, "http_getBlockTemplate"); err != nil {
		return nil, time.Since(fetchStart), err
	}

	return &rpcResp.Result, time.Since(fetchStart), nil
}

func fetchMainChainHeightFromBackend(backend *rpcBackend) (uint64, time.Duration, error) {
	if backend == nil {
		return 0, 0, fmt.Errorf("no RPC backend configured")
	}
	fetchStart := time.Now()

	buf := []byte(`{"jsonrpc":"2.0","id":1,"method":"getMainChainHeight","params":[]}`)
	req, err := http.NewRequest("POST", backend.url, bytes.NewReader(buf))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", backend.authorizationHeader())
	req.Header.Set("Content-Type", "application/json")

	client := backend.gbtClient
	if client == nil {
		client = gbtHTTPClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, time.Since(fetchStart), err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 257))
		if err != nil {
			return 0, time.Since(fetchStart), fmt.Errorf("read getMainChainHeight error response body: %w", err)
		}
		return 0, time.Since(fetchStart), fmt.Errorf("getMainChainHeight HTTP %d: %s", resp.StatusCode, truncateForLog(body, 256))
	}

	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := decodeSingleJSON(resp.Body, &rpcResp); err != nil {
		return 0, time.Since(fetchStart), err
	}

	if len(rpcResp.Error) > 0 && string(rpcResp.Error) != "null" {
		return 0, time.Since(fetchStart), fmt.Errorf("RPC error in getMainChainHeight: %s", truncateForLog(rpcResp.Error, 512))
	}

	height, err := parseMainChainHeight(rpcResp.Result)
	if err != nil {
		return 0, time.Since(fetchStart), err
	}
	return height, time.Since(fetchStart), nil
}

func parseMainChainHeight(raw json.RawMessage) (uint64, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		height, err := strconv.ParseUint(strings.TrimSpace(text), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse getMainChainHeight string %q: %w", text, err)
		}
		return height, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var number json.Number
	if err := decoder.Decode(&number); err != nil {
		return 0, fmt.Errorf("decode getMainChainHeight result: %w", err)
	}
	height, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse getMainChainHeight number %q: %w", number.String(), err)
	}
	return height, nil
}

func decodeSingleJSON(r io.Reader, v interface{}) error {
	dec := json.NewDecoder(r)
	if err := dec.Decode(v); err != nil {
		return err
	}
	var trailing struct{}
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values in response")
		}
		return err
	}
	return nil
}

func appendGetBlockTemplatePayload(dst []byte, poolAddress string) []byte {
	dst = append(dst, `{"jsonrpc":"2.0","id":1,"method":"getBlockTemplate","params":[[],10`...)
	if poolAddress != "" {
		dst = append(dst, ',')
		dst = strconv.AppendQuote(dst, poolAddress)
	}
	dst = append(dst, `]}`...)
	return dst
}

func buildNotify(jobID string, tmpl *BlockTemplate, clean bool) ([]interface{}, *JobData, error) {
	if tmpl == nil || tmpl.Reward < 0 || tmpl.BlockFees < 0 {
		return nil, nil, errors.New("template reward or block fees are negative")
	}
	// Standard fields
	versionHex := fixedHexUint32(uint32(tmpl.Version))
	ntimeHex := fixedHexUint32(uint32(tmpl.CurTime))

	// Use the nbits from template as-is, but normalise formatting
	nbitsHex := strings.ToLower(strings.TrimPrefix(tmpl.PoWDiffReference.Nbits, "0x"))

	if len(nbitsHex) != 8 {
		// in case template is sloppy, normalise to 8 chars
		if v, err := strconv.ParseUint(nbitsHex, 16, 32); err == nil {
			nbitsHex = fixedHexUint32(uint32(v))
		} else {
			return nil, nil, fmt.Errorf("invalid nbits: %v", err)
		}
	}

	nbitsVal, _ := strconv.ParseUint(nbitsHex, 16, 32)
	networkTarget := bitsToTarget(uint32(nbitsVal))
	if networkTarget.Sign() <= 0 {
		return nil, nil, fmt.Errorf("invalid nbits target: %s", nbitsHex)
	}

	job := &JobData{
		Version:       uint32(tmpl.Version),
		BackendName:   tmpl.BackendName,
		NBits:         uint32(nbitsVal),
		NetworkTarget: networkTarget,
		ParentHash:    normalizeHex(tmpl.PreviousHash),
		NTime:         uint32(tmpl.CurTime),
		CreatedAt:     time.Now(),
		TemplateKey:   templateKey(tmpl),
		Clean:         clean,
	}
	// Fill TxRoot.StateRoot / Height / Coinbase / Reward
	if txrootBytes, err := hex.DecodeString(strings.TrimPrefix(tmpl.TxRoot, "0x")); err == nil && len(txrootBytes) == 32 {
		job.TxRoot = txrootBytes
	}
	if staterootBytes, err := hex.DecodeString(strings.TrimPrefix(tmpl.StateRoot, "0x")); err == nil && len(staterootBytes) == 32 {
		job.StateRoot = staterootBytes
	}
	hLE := make([]byte, 8)
	binary.LittleEndian.PutUint64(hLE, uint64(tmpl.Height))
	job.Height = hLE

	coinbaseBytes, err := templateCoinbaseBytes(tmpl, "build_notify")
	if err != nil {
		return nil, nil, err
	}
	job.CoinbaseAddress = coinbaseBytes

	rewLE := make([]byte, 8)
	binary.LittleEndian.PutUint64(rewLE, uint64(tmpl.Reward))
	job.Reward = rewLE

	feeLE := make([]byte, 8)
	binary.LittleEndian.PutUint64(feeLE, uint64(tmpl.BlockFees))
	job.BlockFees = feeLE

	// Normalize and PAD to 64 hex chars (32 bytes)
	prevLE := normalizeHex(tmpl.PreviousHash)
	if len(prevLE) < 64 {
		prevLE = strings.Repeat("0", 64-len(prevLE)) + prevLE
	}
	job.ParentHash = prevLE
	parentBytes, err := hex.DecodeString(prevLE)
	if err != nil || len(parentBytes) != 32 {
		return nil, nil, fmt.Errorf("invalid previous hash: %s", tmpl.PreviousHash)
	}
	job.ParentBytes = parentBytes

	// BlockDAG Stratum: ["job_id","prevhash","bbversion","nbits","ntime",clean]
	// prevhash is hex string (32 bytes)
	// bbversion is hex string (4 bytes)
	// nbits is hex string (4 bytes)
	// ntime is hex string (4 bytes)

	notify := []interface{}{
		jobID,
		prevLE,     // prevhash (using same LE format as before, assuming spec implies this)
		versionHex, // bbversion
		nbitsHex,   // nbits
		ntimeHex,   // ntime
		clean,
	}
	return notify, job, nil
}

// CurrentParent returns the latest normalized parent hash (thread-safe)
func CurrentParent() string {
	return miningWork.currentParent()
}
