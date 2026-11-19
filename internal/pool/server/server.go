package server

/*
#cgo CFLAGS: -I${SRCDIR}
#include "scrypt.h"
*/
import "C"

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"

	"asicPool/internal/pool/pplns"
	"asicPool/internal/pool/storage"
	"asicPool/internal/pool/wallet"
)

// "os"

// Config controls how the stratum server connects to upstream RPC endpoints and
// exposes the listener for miners.
type Config struct {
	ListenAddr                                   string
	RPCURL                                       string
	RPCEndpointAliases                           map[string]string
	RPCRouterTemplateMaxAgeSeconds               int
	RPCRouterFreezeMaxTemplateAgeSeconds         int
	RPCRouterNodeHealthEnabled                   bool
	RPCRouterNodeHealthProbeSeconds              int
	RPCRouterNodeHealthMaxAgeSeconds             int
	RPCRouterNodeHealthUnreadyThreshold          int
	RPCRouterNodeHealthErrorThreshold            int
	RPCRouterNodeHealthFreshTemplateGraceSeconds int
	RPCRouterNodeHealthMinConsensusPeers         int
	SubmitRPCTimeoutMS                           int
	SubmissionLeaseMS                            int
	SubmissionReconcileIntervalMS                int
	SubmissionChainID                            string
	RejectStaleBlockCandidates                   bool
	RecentStaleBlockCandidateSubmitGraceMS       int
	MaxBlockCandidateJobAgeMS                    int
	AutoTuneBlockCandidateJobAge                 bool
	PreemptiveBlockCandidateRefreshEnabled       bool
	PreemptiveBlockCandidateCleanReissueEnabled  bool
	PreemptiveBlockCandidateRefreshDelayMS       int
	PreemptiveBlockCandidateRefreshIntervalMS    int
	PreemptiveBlockCandidateRefreshTimeoutMS     int
	NonCleanJobGraceMS                           int
	OrdinaryStaleShareGraceMS                    int
	GlobalBlockDedupeEnabled                     bool
	AllowMultipleBlockCandidatesPerJob           bool
	StrictCurrentJobSubmit                       bool
	DisableSubmitStallRecovery                   bool
	SubmitStallRecoveryWindowSeconds             int
	SubmitStallRecoveryMinAgeSeconds             int
	SubmitStallRecoveryFailureThreshold          int
	SubmitStallRecoveryStaleThreshold            int
	SubmitStallRecoveryDuplicateThreshold        int
	SubmitStallRecoveryCooldownSeconds           int
	DisableTemplateConversionStallRecovery       bool
	TemplateConversionStallWindowSeconds         int
	TemplateConversionStallMinAgeSeconds         int
	TemplateConversionStallMinCandidates         int
	TemplateConversionStallFailureRatioPercent   int
	TemplateConversionStallMinActiveMiners       int
	TemplateConversionStallCooldownSeconds       int
	BlockTimingControllerEnabled                 bool
	BlockTimingControllerWindowSeconds           int
	BlockTimingControllerMinCandidates           int
	BlockTimingControllerTargetWastePercent      int
	BlockTimingControllerMinJobAgeMS             int
	BlockTimingControllerMaxJobAgeMS             int
	BlockTimingControllerMinTemplateTTLMS        int
	BlockTimingControllerMaxTemplateTTLMS        int
	BlockTimingControllerStepMS                  int
	BlockTimingControllerCooldownSeconds         int
	DisableLowDiffFloodDisconnect                bool
	LowDiffRejectWindowSeconds                   int
	LowDiffRejectDisconnectThreshold             int
	DisableExpiredJobRecovery                    bool
	ExpiredJobRejectWindowSeconds                int
	ExpiredJobClientResendThreshold              int
	ExpiredJobClientReconnectThreshold           int
	ExpiredJobClientReconnectWindowSeconds       int
	ExpiredJobClientReconnectCooldownSeconds     int
	ExpiredJobGlobalCleanRefreshThreshold        int
	ExpiredJobGlobalCleanRefreshMinClients       int
	ExpiredJobRecoveryCooldownSeconds            int
	StaleRaceRejectWindowSeconds                 int
	StaleRaceClientResendThreshold               int
	StaleRaceClientReconnectThreshold            int
	StaleRaceClientReconnectWindowSeconds        int
	StaleRaceClientReconnectCooldownSeconds      int
	StaleRaceRecoveryCooldownSeconds             int
	EnableDuplicateMACLaneCleanup                bool
	DuplicateMACLaneStaleGraceMS                 int
	EnableCleanJobPropagationRepair              bool
	EnableCleanJobPropagationReconnect           bool
	CleanJobPropagationRepairCooldownMS          int
	CleanJobPropagationRepairMinAgeMS            int
	CleanJobPropagationReconnectThreshold        int
	CleanJobPropagationReconnectCooldownSeconds  int
	SubmitBlockHeaderV2Enabled                   bool
	SubmitDebugResultEnabled                     bool
	WalletRPCURL                                 string // EVM RPC for Payouts
	RewardIPCPath                                string // owner-only node reward socket
	RewardEVMRPCURL                              string // EVM RPC for canonical reward checks
	RPCUser                                      string
	RPCPassword                                  string
	PPLNSTargetWork                              float64
	// Maturity Configuration
	BlockMaturityDepth                int // Confirmations for Balance Maturity (Default 10)
	PayoutMaturityDepth               int // Confirmations for Payout Unlock (Default 4096)
	PoolFeePercentage                 float64
	PGURL                             string
	PoolPrivateKey                    string // Hex private key for payouts
	PoolCoinbaseAddress               string // Explicit non-zero block reward address for mined templates
	MetricsAddr                       string // Address for Prometheus /metrics endpoint (e.g. ":9090")
	PoolInstanceID                    string // Unique instance ID (0-255) for extranonce1 space partitioning
	MinPdiff                          float64
	StartingPdiff                     float64
	VarDiffTargetSharesPerMinute      float64
	MaxInflightRequestsPerClient      int
	SubmitOrderingEnabled             bool // Deprecated: submits now run concurrently; atomic dedupe preserves semantic ordering.
	StratumServerFirstDifficultyProbe bool
	PoolStatusLogIntervalSeconds      int
}

// RPC config
var (
	listenAddr                  = ":3334"
	rpcURL                      = "http://127.0.0.1:38131"
	rpcUser                     = "test"
	rpcPassword                 = "test"
	activeRPCRouter             *rpcRouter
	submitBlockHeaderV2Enabled  bool
	activeSubmissionJournal     storage.SubmissionJournal
	activeSubmissionChainID     *big.Int
	activeSubmissionBackendID   = "node"
	activeSubmissionCoordinator *submissionCoordinator

	// PPLNS
	PPLNSWindow *pplns.Window
)

// Static subscribe reply
var (
	subscribeExtranonce2Size = 4
	en1Counter               uint32 // for generating unique extranonce1
)

const defaultMaxInflightRequestsPerClient = 64

type JobData struct {
	Version         uint32
	TxRoot          []byte
	StateRoot       []byte
	NBits           uint32
	NetworkTarget   *big.Int
	Height          []byte // Block height (LE)
	CoinbaseAddress []byte
	Reward          []byte    // LittleEndian uint64
	BlockFees       []byte    // LittleEndian uint64
	ParentHash      string    // normalized (no 0x, lowercase) previous hash
	ParentBytes     []byte    // decoded previous hash bytes
	NTime           uint32    // template curtime
	CreatedAt       time.Time // when we created/stored this job
	TemplateSeq     uint64
	BackendName     string
	// Invalidation epochs captured when the job is issued. Do not remove these
	// in favor of scanning acceptedJobs: that scan was observed to retain
	// thousands of jobs and adds latency exactly when ASICs need new work after
	// an accepted block or stale parent.
	InvalidationEpoch        uint64
	BackendInvalidationEpoch uint64
	TemplateKey              string
	Clean                    bool
	SentAt                   time.Time
	Extranonce1              [4]byte
	// Difficulty snapshot for this job (VarDiff pdiff)
	Difficulty float64
	// Previous difficulty snapshot (for JS-style fallback)
	PrevDiff       float64
	stale          atomic.Bool
	invalidated    atomic.Bool
	invalidatedAt  atomic.Int64
	blockSubmitted atomic.Bool
	usedNonces     sync.Map // Prevent duplicate shares: key=shareNonceKey -> bool
}

// ------------------- Stratum Structures -------------------

type stratumRequest struct {
	ID                    interface{}            `json:"id"`
	Method                string                 `json:"method"`
	Params                []json.RawMessage      `json:"params"`
	ReceivedAt            time.Time              `json:"-"`
	submissionCoordinator *submissionCoordinator `json:"-"`
}

type stratumResponse struct {
	ID     interface{} `json:"id,omitempty"`
	Result interface{} `json:"result,omitempty"`
	Error  interface{} `json:"error,omitempty"`
}

type client struct {
	conn       net.Conn
	rw         *bufio.ReadWriter
	mu         sync.Mutex
	closed     bool
	addr       string
	remoteHost string
	asicMAC    string
	laneID     string

	diffMu                     sync.Mutex
	pdiff                      float64 // current VarDiff difficulty
	prevPdiff                  float64 // previous difficulty
	windowStart                time.Time
	windowShares               int
	lastAdjust                 time.Time
	lastDiffPushLogAt          time.Time
	lastDiffPushLogValue       float64
	lastRoutineSubmitLogAt     time.Time
	routineSubmitLogSuppressed int
	lastValidShareLogAt        time.Time
	validShareLogSuppressed    int
	validShareLogDiffSum       float64
	validShareLogWorkSum       float64
	expiredJobRejects          int
	expiredJobWindowStart      time.Time
	expiredJobWindowRejects    int
	lastExpiredJobRecovery     time.Time
	expiredJobRecoveryStart    time.Time
	expiredJobRecoveryCount    int
	lastExpiredJobReconnect    time.Time
	staleRaceWindowStart       time.Time
	staleRaceWindowRejects     int
	lastStaleRaceRecovery      time.Time
	staleRaceRecoveryStart     time.Time
	staleRaceRecoveryCount     int
	lastStaleRaceReconnect     time.Time
	lowDiffWindowStart         time.Time
	lowDiffRejects             int
	lowDiffRecoveryWindowStart time.Time
	lowDiffRecoveryRejects     int
	lastLowDiffRecovery        time.Time
	lastLowDiffLogAt           time.Time
	lowDiffLogSuppressed       int
	duplicateReplayMu          sync.Mutex
	duplicateReplayWindowStart time.Time
	lastDuplicateBlockHash     string
	lastDuplicateBlockJobID    string
	duplicateReplayCount       int
	duplicateMACStaleSince     time.Time
	lastCleanJobID             string
	lastCleanTemplateSeq       uint64
	lastCleanParent            string
	lastCleanSentAt            time.Time
	lastCleanSubmitObserved    bool
	cleanJobStaleSubmitStreak  int
	lastCleanJobRepair         time.Time
	lastCleanJobReconnect      time.Time
	username                   string
	authorized                 bool
	extranonce1                [4]byte //unique per connection
	sessionID                  string  // Unique session ID for Stratum
	extranonceSubscribed       bool
	currentJob                 atomic.Value // *clientCurrentJob

	// Config Reference
	cfg         *Config
	storage     storage.Storage
	submission  storage.SubmissionJournal
	coordinator *submissionCoordinator
}

type BlockTemplate struct {
	Version int `json:"version"`
	Parents []struct {
		Data string `json:"data"`
	} `json:"parents"`
	TxRoot           string `json:"txroot"`
	StateRoot        string `json:"stateroot"`
	PoWDiffReference struct {
		Nbits string `json:"nbits"`
	} `json:"pow_diff_reference"`
	CurTime         int    `json:"curtime"`
	PreviousHash    string `json:"previousblockhash"`
	Height          int64  `json:"height"`
	CoinbaseAddress string `json:"coinbase_address"`
	Reward          int64  `json:"reward"`
	BlockFees       int64  `json:"block_fees"`
	BackendName     string `json:"-"`
	SourceSeq       uint64 `json:"seq,omitempty"`
}

var clientsMu sync.Mutex
var clients = map[*client]struct{}{}

// Run boots the stratum server and blocks while serving miner connections.
func Run(cfg Config) error {
	if err := applyConfig(cfg); err != nil {
		return err
	}
	chainID, err := parseSubmissionChainIDV1(cfg.SubmissionChainID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.PGURL) == "" {
		return fmt.Errorf("PG_URL is required for durable submission journal")
	}
	// Core is the sole chain, template, readiness, validation, and submit
	// authority. Pin its strict identity before storage initialization, template
	// work, or any submit-capable listener can start.
	if err := verifySelectedCoreChainIdentity(activeRPCRouter); err != nil {
		return err
	}
	if err := validateSubmissionChainIDAgainstCore(activeRPCRouter, chainID); err != nil {
		return err
	}

	// Partition extranonce1 space: each instance gets a unique top byte,
	// giving 16M unique en1 values per instance across up to 256 instances.
	poolID := cfg.PoolInstanceID
	if poolID == "" {
		poolID = "0"
	}
	instanceNum := uint32(0)
	if n, err := strconv.ParseUint(poolID, 10, 8); err == nil {
		instanceNum = uint32(n)
	}
	atomic.StoreUint32(&en1Counter, instanceNum<<24)
	log.Printf("ExtraNonce1 partition: instance=%s offset=0x%08x", poolID, instanceNum<<24)

	InitMetrics(poolID)
	publishBlockTimingControllerRuntime()

	if err := rejectUnjournaledPayout(cfg); err != nil {
		return err
	}
	if err := configurePoolCoinbaseAddress(cfg); err != nil {
		return err
	}

	// Initialize PPLNS Window
	targetWork := cfg.PPLNSTargetWork
	if targetWork <= 0 {
		targetWork = 1000.0 // Default fallback if not set
		log.Printf("⚠️ PPLNS target work not set, defaulting to %.1f", targetWork)
	}
	PPLNSWindow = pplns.NewWindow(targetWork)
	log.Printf("PPLNS initialized with target work: %.1f", targetWork)

	// Maturity Defaults
	if cfg.BlockMaturityDepth <= 0 {
		cfg.BlockMaturityDepth = 10
	}
	if cfg.PayoutMaturityDepth <= 0 {
		cfg.PayoutMaturityDepth = 4096
	}
	log.Printf("Maturity Policy: Balance=%d blocks, Payout=%d blocks", cfg.BlockMaturityDepth, cfg.PayoutMaturityDepth)

	// Initialize and verify the required durable submission journal before the
	// listener or any submit-capable client can exist.
	var poolStorage storage.Storage
	pg := storage.NewPostgresStorage(cfg.PGURL)
	pg.SetBestEffortAccounting(os.Getenv("POOL_ACCOUNTING_BEST_EFFORT") != "0")
	if err := pg.Connect(context.Background()); err != nil {
		log.Printf("❌ Database Connection Failed: %v", err)
		return err
	}
	defer pg.Close()
	poolStorage = pg
	journal, ok := any(pg).(storage.SubmissionJournal)
	if !ok {
		return errors.New("PostgreSQL storage does not implement durable submission journal")
	}
	if err := journal.VerifySubmissionJournalV1(context.Background(), chainID, activeSubmissionBackendID); err != nil {
		return fmt.Errorf("durable submission journal verification failed: %w", err)
	}
	reward, err := newRewardRuntime(cfg, pg, chainID, activeSubmissionBackendID)
	if err != nil {
		return fmt.Errorf("durable reward startup failed: %w", err)
	}
	if reward != nil {
		defer reward.close()
	}
	startupAccounting := func() error {
		if reward == nil {
			return reconcileAcceptedAccountingAtStartup(context.Background(), journal)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		return reward.verifyAndReconcile(ctx)
	}
	if err := startupAccounting(); err != nil {
		return fmt.Errorf("accepted accounting readiness failed: %w", err)
	}
	if pg.UnpricedSubmissionHoldActive() {
		log.Print("WARNING: exact duplicate discovery is qualified; all rewards still require independent work and payout verification")
	}
	if _, err := journal.ReclaimExpiredLeases(context.Background()); err != nil {
		return fmt.Errorf("durable submission lease reclamation failed: %w", err)
	}
	activeSubmissionJournal = journal
	activeSubmissionChainID = new(big.Int).Set(chainID)
	activeSubmissionDrain = newSubmissionDrainGate()
	leaseDuration, reconcileInterval, err := submissionDurations(&cfg)
	if err != nil {
		return err
	}

	// Initialize Wallet Client
	var walletClient wallet.Client
	if cfg.WalletRPCURL != "" && cfg.PoolPrivateKey != "" {
		// Inject Basic Auth credentials into URL if present
		authURL := cfg.WalletRPCURL
		// Only inject auth if user provided it AND it's not already in the URL
		if cfg.RPCUser != "" && cfg.RPCPassword != "" && !strings.Contains(authURL, "@") {
			parts := strings.Split(authURL, "://")
			if len(parts) == 2 {
				// We assume RPCUser/Pass applies to both interfaces if set,
				// or user can embed credentials in WALLET_RPC_URL directly.
				authURL = fmt.Sprintf("%s://%s:%s@%s", parts[0], cfg.RPCUser, cfg.RPCPassword, parts[1])
			}
		}

		w, err := wallet.NewEthWalletClient(context.Background(), authURL, cfg.PoolPrivateKey)
		if err != nil {
			log.Printf("⚠️ Failed to connect to Wallet RPC: %v (Payouts will stay PENDING)", err)
		} else {
			walletClient = w
			defer walletClient.Close()
			log.Println("✅ Wallet Client Connected (Local Signing)")
		}
	} else {
		if reward != nil {
			log.Println("Pool rewards use the separate wallet-scoped payment worker.")
		} else {
			log.Println("⚠️ Wallet RPC or Private Key missing. Payouts will NOT be processed.")
		}
	}

	shutdownChan := make(chan struct{})
	activeSubmissionCoordinator = newSubmissionCoordinator(journal, chainID, activeSubmissionBackendID, &cfg)
	if reward != nil {
		activeSubmissionCoordinator.shareAccounting = reward.accounting
	}
	reconciler := newSubmissionReconciler(journal, activeSubmissionCoordinator, leaseDuration, reconcileInterval, shutdownChan)
	if err := reconciler.reconcileOne(context.Background()); err != nil {
		close(shutdownChan)
		return fmt.Errorf("durable submission readiness scan failed: %w", err)
	}
	// The readiness scan can accept a prepared orphan. Drain and verify once
	// more so Stratum never opens with an accepted receipt lacking its effect.
	if err := startupAccounting(); err != nil {
		close(shutdownChan)
		return fmt.Errorf("post-reconciliation accepted accounting readiness failed: %w", err)
	}
	// Priming health may fetch a template. All slow financial audits, including
	// the final post-reconciliation audit, must finish before that side effect;
	// otherwise the first miner receives an already expired job/cached health.
	if activeRPCRouter != nil {
		primeRouterNodeHealth(activeRPCRouter)
		activeRPCRouter.publishAllMetrics()
	}
	reconciler.start()
	accountingConsumer := newAcceptedAccountingConsumer(journal, reconcileInterval, shutdownChan, activeSubmissionCoordinator.metricsPublisher)
	if reward != nil {
		accountingConsumer.reconcileOverride = func(ctx context.Context) (storage.AcceptedAccountingReconcileResultV2, error) {
			return pg.ReconcileAccountingWithUnpricedV1(ctx, chainID, activeSubmissionBackendID, reward.wallet)
		}
	}
	accountingConsumer.start()

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		close(shutdownChan)
		reconciler.wait()
		accountingConsumer.wait()
		return err
	}
	if reward != nil {
		reward.start()
	}

	// Graceful Shutdown Channel
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	// WaitGroup for client connections
	var wg sync.WaitGroup

	go func() {
		<-sigChan
		close(shutdownChan)
		log.Println("⚠️  Shutdown signal received. Closing listener...")
		ln.Close() // Stop accepting NEW connections

		// Optional: close existing Stratum connections immediately or wait.
		// Cutting them lets ASIC clients reconnect according to their own pool config.
		clientsMu.Lock()
		count := len(clients)
		log.Printf("Check: %d active clients to disconnect", count)
		for c := range clients {
			c.conn.Close() // Force close to unblock Read/Write
		}
		clientsMu.Unlock()
	}()

	log.Printf("Stratum server is listening on %s", listenAddr)

	if cfg.MetricsAddr != "" {
		go startMetricsServer(cfg.MetricsAddr)
	}

	// Start background services
	go parentWatcher()
	go jobBroadcaster()
	go jobReaper()
	go blockLifecycleManager(poolStorage, walletClient, cfg.BlockMaturityDepth, cfg.PayoutMaturityDepth)
	startRouterMetricsPublisher(activeRPCRouter, shutdownChan)
	startRouterNodeHealthProbe(activeRPCRouter, shutdownChan)
	startPoolStatusLogger(cfg.PoolStatusLogIntervalSeconds, shutdownChan)
	startDuplicateMACLaneCleanup(&cfg, shutdownChan)

	// broadcaster needs no changes, it just runs.

	for {
		conn, err := ln.Accept()
		if err != nil {
			// If shutdown closed the listener, we exit loop
			select {
			case <-shutdownChan:
				reconciler.wait()
				accountingConsumer.wait()
				return nil
			default:
			}
			log.Printf("accept err: %v", err)
			continue
		}
		remoteAddr := conn.RemoteAddr().String()
		remoteHost := remoteHostFromAddr(remoteAddr)
		asicMAC := resolveASICMACForRemote(remoteAddr)
		if reason := rejectReasonForASICMACAllowlist(remoteHost, asicMAC); reason != "" {
			log.Printf("[STRATUM] rejected client addr=%s host=%s mac=%s reason=%s", remoteAddr, remoteHost, emptyAsUnknown(asicMAC), reason)
			_ = conn.Close()
			continue
		}
		c := &client{
			conn:        conn,
			rw:          bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
			addr:        remoteAddr,
			remoteHost:  remoteHost,
			asicMAC:     asicMAC,
			laneID:      clientLaneIdentity(remoteAddr, asicMAC),
			cfg:         &cfg, // Inject Config
			storage:     poolStorage,
			submission:  journal,
			coordinator: activeSubmissionCoordinator,
		}
		log.Printf("[STRATUM] accepted client addr=%s host=%s lane=%s mac=%s", c.addr, c.remoteHost, c.laneIdentity(), emptyAsUnknown(c.asicMAC))

		clientsMu.Lock()
		clients[c] = struct{}{}
		clientsMu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			serveClient(c)
		}()
	}
}

func applyConfig(cfg Config) error {
	if cfg.SubmitRPCTimeoutMS != 0 && (cfg.SubmitRPCTimeoutMS < 250 || cfg.SubmitRPCTimeoutMS > 10000) {
		return fmt.Errorf("POOL_SUBMIT_RPC_TIMEOUT_MS must be between 250 and 10000")
	}
	if cfg.ListenAddr != "" {
		listenAddr = cfg.ListenAddr
	}
	if cfg.RPCURL != "" {
		rpcURL = cfg.RPCURL
	}
	// wallet RPC not used in global vars
	if cfg.RPCUser != "" {
		rpcUser = cfg.RPCUser
	}
	if cfg.RPCPassword != "" {
		rpcPassword = cfg.RPCPassword
	}
	submitBlockHeaderV2Enabled = cfg.SubmitBlockHeaderV2Enabled
	submitHTTPClient = buildSubmitHTTPClient(cfg.SubmitRPCTimeoutMS)
	legacySubmitV2Unsupported.Store(false)
	router, err := newRPCRouterFromConfig(cfg)
	if err != nil {
		return err
	}
	if cfg.MinPdiff > 0 {
		setMinPdiff(cfg.MinPdiff)
	}
	if cfg.StartingPdiff > 0 {
		setStartingPdiff(cfg.StartingPdiff)
	}
	if cfg.VarDiffTargetSharesPerMinute > 0 {
		setVarDiffTargetSharesPerMinute(cfg.VarDiffTargetSharesPerMinute)
	}
	configureSubmitStallRecovery(cfg)
	configureBlockTimingController(cfg)
	configureExpiredJobRecovery(cfg)
	activeRPCRouter = router
	return nil
}

func serveClient(c *client) {
	connectionsTotal.Inc()
	activeConnections.Inc()
	defer func() {
		c.conn.Close()
		clientsMu.Lock()
		delete(clients, c)
		clientsMu.Unlock()
		activeConnections.Dec()
		disconnectionsTotal.Inc()
	}()

	// Standard Stratum: Roll extranonce every 20 minutes to prevent nonce exhaustion
	stopRolling := make(chan struct{})
	defer close(stopRolling)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-stopRolling:
				return
			case <-ticker.C:
				// Try to roll (helper.go will check if subscribed)
				sendSetExtranonce(c)
			}
		}
	}()

	// DoS Protection:
	// 1. Max Message Size: 10KB (prevent memory exhaustion)
	// 2. Read Deadline: 10 minutes (disconnect idle/slowloris)
	scanner := bufio.NewScanner(c.conn)
	buf := make([]byte, 1024)
	scanner.Buffer(buf, 10*1024) // 10KB max
	inflightRequests := make(chan struct{}, maxInflightRequestsPerClient(c.cfg))
	sawStratumRequest := false

	sendServerFirstDifficultyProbe(c)

	for {
		// Reset deadling before every read
		_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Minute))

		if !scanner.Scan() {
			authorized, subscribed, extranonceSubscribed, username := clientStateForLog(c)
			reason := classifyStratumDisconnect(scanner.Err(), sawStratumRequest)
			if err := scanner.Err(); err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					if !sawStratumRequest {
						recordNoRequestDisconnect(c, reason)
					}
					log.Printf("[STRATUM] client read error addr=%s reason=%s err=%v authorized=%t subscribed=%t extranonce_subscribed=%t user=%s", c.addr, reason, err, authorized, subscribed, extranonceSubscribed, username)
				}
			} else {
				if !sawStratumRequest {
					recordNoRequestDisconnect(c, reason)
				}
				log.Printf("[STRATUM] client closed addr=%s reason=%s authorized=%t subscribed=%t extranonce_subscribed=%t user=%s", c.addr, reason, authorized, subscribed, extranonceSubscribed, username)
			}
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		sawStratumRequest = true
		var req stratumRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			log.Printf("[%s] invalid json: %v -- %s", c.addr, err, line)
			continue
		}
		authorized, subscribed, _, _ := clientStateForLog(c)
		if req.Method != "mining.submit" || !authorized || !subscribed {
			log.Printf("[STRATUM] request addr=%s method=%q id=%v authorized=%t subscribed=%t", c.addr, req.Method, req.ID, authorized, subscribed)
		}
		req.ReceivedAt = time.Now()
		dispatchClientRequest(c, &req, inflightRequests)
	}
}

func clientStateForLog(c *client) (authorized bool, subscribed bool, extranonceSubscribed bool, username string) {
	if c == nil {
		return false, false, false, ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authorized, c.sessionID != "", c.extranonceSubscribed, c.username
}

func maxInflightRequestsPerClient(cfg *Config) int {
	if cfg == nil || cfg.MaxInflightRequestsPerClient <= 0 {
		return defaultMaxInflightRequestsPerClient
	}
	return cfg.MaxInflightRequestsPerClient
}

func serverFirstDifficultyProbeEnabled(cfg *Config) bool {
	return cfg != nil && cfg.StratumServerFirstDifficultyProbe
}

func sendServerFirstDifficultyProbe(c *client) {
	if c == nil || !serverFirstDifficultyProbeEnabled(c.cfg) {
		return
	}
	pdiff := startingPdiff
	if pdiff <= 0 {
		pdiff = minPdiff
	}
	if pdiff <= 0 {
		pdiff = defaultMinPdiff
	}
	if err := sendPushWithTimeout(c, "mining.set_difficulty", []interface{}{pdiff}, 250*time.Millisecond); err != nil {
		recordServerFirstDifficultyProbe("failed")
		log.Printf("[STRATUM] server-first difficulty probe failed addr=%s lane=%s err=%v", c.addr, c.laneIdentity(), err)
		return
	}
	recordServerFirstDifficultyProbe("sent")
	log.Printf("[STRATUM] server-first difficulty probe sent addr=%s lane=%s pdiff=%.8f", c.addr, c.laneIdentity(), pdiff)
}

func classifyStratumDisconnect(err error, sawStratumRequest bool) string {
	if sawStratumRequest {
		if err != nil {
			return "read-error"
		}
		return "eof"
	}
	if err != nil {
		return "no-request-read-error"
	}
	return "no-request-eof"
}

func dispatchClientRequest(c *client, req *stratumRequest, inflight chan struct{}) {
	dispatchClientRequestWithHandler(c, req, inflight, handleRequest)
}

func dispatchClientRequestWithHandler(c *client, req *stratumRequest, inflight chan struct{}, handler func(*client, *stratumRequest)) {
	inflight <- struct{}{}
	go func() {
		defer func() { <-inflight }()
		handler(c, req)
	}()
}

func handleRequest(c *client, req *stratumRequest) {
	switch req.Method {
	case "mining.subscribe":
		handleSubscribe(c, req)
	case "mining.authorize":
		handleAuthorize(c, req)
	case "mining.extranonce.subscribe":
		handleExtranonceSubscribe(c, req)
	case "mining.submit":
		if err := handleSubmit(c, req); err != nil {
			log.Printf("[ERROR]Error handling submit for %s: %v", c.addr, err)
			c.mu.Lock()
			c.closed = true
			c.mu.Unlock()
			c.conn.Close()
		}
	default:
		log.Printf("[STRATUM] unsupported method addr=%s method=%q id=%v", c.addr, req.Method, req.ID)
		sendError(c, req.ID, -32601, "Method not found")
	}
}

// ------------------- Crypto / Hashing -------------------

func scryptHash(header []byte) common.Hash {
	out := make([]byte, 32)

	C.scrypt_hash_data(
		unsafe.Pointer(&out[0]),
		unsafe.Pointer(&header[0]),
	)
	return common.BytesToHash(out)
}
