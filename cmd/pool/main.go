package main

import (
	"asicPool/internal/buildinfo"
	"asicPool/internal/pool/server"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

var productVersion = "2.1.0-rc.1"

func main() {

	if err := godotenv.Load(); err != nil {
		log.Printf("load env: %v", err)
	} else {
		log.Println("env file found and loaded")
	}
	port := os.Getenv("POOL_PORT")
	if port == "" {
		port = "3334"
	}
	listenAddr := "0.0.0.0:" + port
	if ba := os.Getenv("POOL_BIND_ADDR"); ba != "" {
		listenAddr = ba
	}
	log.Printf("BlockDAG Pool %s source=%s", productVersion, buildinfo.SourceCommit)
	rpcEndpointAliases := map[string]string{
		"NODE_RPC_URL":         os.Getenv("NODE_RPC_URL"),
		"NODE_RPC_URLS":        os.Getenv("NODE_RPC_URLS"),
		"POOL_SUBMIT_RPC_URLS": os.Getenv("POOL_SUBMIT_RPC_URLS"),
		"BDAG_NODE_RPC_URL":    os.Getenv("BDAG_NODE_RPC_URL"),
		"BDAG_NODE_RPC_URLS":   os.Getenv("BDAG_NODE_RPC_URLS"),
	}
	rpcURL, err := server.ResolveCoreRPCEndpoint(rpcEndpointAliases, true)
	if err != nil {
		log.Fatalf("invalid Core RPC endpoint configuration: %v", err)
	}
	rpcRouterNodeHealthEnabled := parseBoolEnv("POOL_RPC_ROUTER_NODE_HEALTH_ENABLED", true)
	rpcRouterNodeHealthProbeSeconds, err := parseMinIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS", 15, 10)
	if err != nil {
		log.Fatalf("invalid node health probe configuration: %v", err)
	}
	rpcRouterNodeHealthMaxAgeSeconds := parseIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_MAX_AGE_SECONDS", 30)
	rpcRouterNodeHealthUnreadyThreshold := parseIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_UNREADY_THRESHOLD", 3)
	rpcRouterNodeHealthErrorThreshold := parseIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_ERROR_THRESHOLD", 3)
	rpcRouterNodeHealthFreshTemplateGraceSeconds := parseIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_FRESH_TEMPLATE_GRACE_SECONDS", 15)
	rpcRouterNodeHealthMinConsensusPeers := parseIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_MIN_CONSENSUS_PEERS", 2)
	submitRPCTimeoutMS := parseIntEnv("POOL_SUBMIT_RPC_TIMEOUT_MS", 3000)
	submissionLeaseMS := parseIntEnv("POOL_SUBMISSION_LEASE_MS", 15000)
	submissionReconcileIntervalMS := parseIntEnv("POOL_SUBMISSION_RECONCILE_INTERVAL_MS", 1000)
	submissionChainID := os.Getenv("POOL_SUBMISSION_CHAIN_ID")
	submitStaleBlockCandidates := parseBoolEnv("POOL_SUBMIT_STALE_BLOCK_CANDIDATES", false)
	recentStaleBlockCandidateSubmitGraceMS := parseIntEnv("POOL_RECENT_STALE_BLOCK_CANDIDATE_SUBMIT_GRACE_MS", 0)
	maxBlockCandidateJobAgeMS := parseIntEnv("POOL_MAX_BLOCK_CANDIDATE_JOB_AGE_MS", 2500)
	autoTuneBlockCandidateJobAge := parseBoolEnv("POOL_AUTO_TUNE_BLOCK_CANDIDATE_JOB_AGE", true)
	preemptiveBlockCandidateRefreshEnabled := parseBoolEnv("POOL_PREEMPTIVE_BLOCK_CANDIDATE_REFRESH_ENABLED", true)
	preemptiveBlockCandidateCleanReissueEnabled := parseBoolEnv("POOL_PREEMPTIVE_BLOCK_CANDIDATE_CLEAN_REISSUE_ENABLED", false)
	preemptiveBlockCandidateRefreshDelayMS := parseIntEnv("POOL_PREEMPTIVE_BLOCK_CANDIDATE_REFRESH_DELAY_MS", 250)
	preemptiveBlockCandidateRefreshIntervalMS := parseIntEnv("POOL_PREEMPTIVE_BLOCK_CANDIDATE_REFRESH_INTERVAL_MS", 1100)
	preemptiveBlockCandidateRefreshTimeoutMS := parseIntEnv("POOL_PREEMPTIVE_BLOCK_CANDIDATE_REFRESH_TIMEOUT_MS", 1500)
	nonCleanJobGraceMS := parseIntEnv("POOL_NON_CLEAN_JOB_GRACE_MS", 5000)
	ordinaryStaleShareGraceMS := parseIntEnv("POOL_ORDINARY_STALE_SHARE_GRACE_MS", 1200)
	globalBlockDedupeEnabled := parseBoolEnv("POOL_GLOBAL_BLOCK_DEDUPE_ENABLED", true)
	allowMultipleBlockCandidatesPerJob := parseBoolEnv("POOL_ALLOW_MULTIPLE_BLOCK_CANDIDATES_PER_JOB", true)
	strictCurrentJobSubmit := parseBoolEnv("POOL_STRICT_CURRENT_JOB_SUBMIT", true)
	submitStallRecoveryEnabled := parseBoolEnv("POOL_SUBMIT_STALL_RECOVERY_ENABLED", true)
	submitStallRecoveryWindowSeconds := parseIntEnv("POOL_SUBMIT_STALL_RECOVERY_WINDOW_SECONDS", 60)
	submitStallRecoveryMinAgeSeconds := parseIntEnv("POOL_SUBMIT_STALL_RECOVERY_MIN_AGE_SECONDS", 30)
	submitStallRecoveryFailureThreshold := parseIntEnv("POOL_SUBMIT_STALL_RECOVERY_FAILURE_THRESHOLD", 8)
	submitStallRecoveryStaleThreshold := parseIntEnv("POOL_SUBMIT_STALL_RECOVERY_STALE_THRESHOLD", 3)
	submitStallRecoveryDuplicateThreshold := parseIntEnv("POOL_SUBMIT_STALL_RECOVERY_DUPLICATE_THRESHOLD", 6)
	submitStallRecoveryCooldownSeconds := parseIntEnv("POOL_SUBMIT_STALL_RECOVERY_COOLDOWN_SECONDS", 20)
	templateConversionStallRecoveryEnabled := parseBoolEnv("POOL_TEMPLATE_CONVERSION_STALL_RECOVERY_ENABLED", true)
	templateConversionStallWindowSeconds := parseIntEnv("POOL_TEMPLATE_CONVERSION_STALL_WINDOW_SECONDS", 120)
	templateConversionStallMinAgeSeconds := parseIntEnv("POOL_TEMPLATE_CONVERSION_STALL_MIN_AGE_SECONDS", 30)
	templateConversionStallMinCandidates := parseIntEnv("POOL_TEMPLATE_CONVERSION_STALL_MIN_CANDIDATES", 40)
	templateConversionStallFailureRatioPercent := parseIntEnv("POOL_TEMPLATE_CONVERSION_STALL_FAILURE_RATIO_PERCENT", 85)
	templateConversionStallMinActiveMiners := parseIntEnv("POOL_TEMPLATE_CONVERSION_STALL_MIN_ACTIVE_MINERS", 1)
	templateConversionStallCooldownSeconds := parseIntEnv("POOL_TEMPLATE_CONVERSION_STALL_COOLDOWN_SECONDS", 30)
	blockTimingControllerEnabled := parseBoolEnv("POOL_BLOCK_TIMING_CONTROLLER_ENABLED", true)
	blockTimingControllerWindowSeconds := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_WINDOW_SECONDS", 60)
	blockTimingControllerMinCandidates := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_MIN_CANDIDATES", 20)
	blockTimingControllerTargetWastePercent := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_TARGET_WASTE_PERCENT", 5)
	blockTimingControllerMinJobAgeMS := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_MIN_JOB_AGE_MS", 250)
	blockTimingControllerMaxJobAgeMS := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_MAX_JOB_AGE_MS", 2500)
	blockTimingControllerMinTemplateTTLMS := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_MIN_TEMPLATE_TTL_MS", 100)
	blockTimingControllerMaxTemplateTTLMS := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_MAX_TEMPLATE_TTL_MS", 1000)
	blockTimingControllerStepMS := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_STEP_MS", 250)
	blockTimingControllerCooldownSeconds := parseIntEnv("POOL_BLOCK_TIMING_CONTROLLER_COOLDOWN_SECONDS", 20)
	lowDiffFloodDisconnectEnabled := parseBoolEnv("POOL_LOW_DIFF_FLOOD_DISCONNECT_ENABLED", true)
	lowDiffRejectWindowSeconds := parseIntEnv("POOL_LOW_DIFF_REJECT_WINDOW_SECONDS", 10)
	lowDiffRejectDisconnectThreshold := parseIntEnv("POOL_LOW_DIFF_REJECT_DISCONNECT_THRESHOLD", 100)
	expiredJobRecoveryEnabled := parseBoolEnv("POOL_EXPIRED_JOB_RECOVERY_ENABLED", true)
	expiredJobRejectWindowSeconds := parseIntEnv("POOL_EXPIRED_JOB_REJECT_WINDOW_SECONDS", 10)
	expiredJobClientResendThreshold := parseIntEnv("POOL_EXPIRED_JOB_CLIENT_RESEND_THRESHOLD", 3)
	expiredJobClientReconnectThreshold := parseIntEnv("POOL_EXPIRED_JOB_CLIENT_RECONNECT_THRESHOLD", 3)
	expiredJobClientReconnectWindowSeconds := parseIntEnv("POOL_EXPIRED_JOB_CLIENT_RECONNECT_WINDOW_SECONDS", 120)
	expiredJobClientReconnectCooldownSeconds := parseIntEnv("POOL_EXPIRED_JOB_CLIENT_RECONNECT_COOLDOWN_SECONDS", 60)
	expiredJobGlobalCleanRefreshThreshold := parseIntEnv("POOL_EXPIRED_JOB_GLOBAL_CLEAN_REFRESH_THRESHOLD", 100)
	expiredJobGlobalCleanRefreshMinClients := parseIntEnv("POOL_EXPIRED_JOB_GLOBAL_CLEAN_REFRESH_MIN_CLIENTS", 2)
	expiredJobRecoveryCooldownSeconds := parseIntEnv("POOL_EXPIRED_JOB_RECOVERY_COOLDOWN_SECONDS", 15)
	staleRaceRejectWindowSeconds := parseIntEnv("POOL_STALE_RACE_REJECT_WINDOW_SECONDS", 10)
	staleRaceClientResendThreshold := parseIntEnv("POOL_STALE_RACE_CLIENT_RESEND_THRESHOLD", 3)
	staleRaceClientReconnectThreshold := parseIntEnv("POOL_STALE_RACE_CLIENT_RECONNECT_THRESHOLD", 0)
	staleRaceClientReconnectWindowSeconds := parseIntEnv("POOL_STALE_RACE_CLIENT_RECONNECT_WINDOW_SECONDS", 120)
	staleRaceClientReconnectCooldownSeconds := parseIntEnv("POOL_STALE_RACE_CLIENT_RECONNECT_COOLDOWN_SECONDS", 60)
	staleRaceRecoveryCooldownSeconds := parseIntEnv("POOL_STALE_RACE_RECOVERY_COOLDOWN_SECONDS", 15)
	duplicateMACLaneCleanupEnabled := parseBoolEnv("POOL_DUPLICATE_MAC_LANE_CLEANUP_ENABLED", false)
	duplicateMACLaneStaleGraceMS := parseIntEnv("POOL_DUPLICATE_MAC_LANE_STALE_GRACE_MS", 5000)
	cleanJobPropagationRepairEnabled := parseBoolEnv("POOL_CLEAN_JOB_PROPAGATION_REPAIR_ENABLED", false)
	cleanJobPropagationReconnectEnabled := parseBoolEnv("POOL_CLEAN_JOB_PROPAGATION_RECONNECT_ENABLED", false)
	cleanJobPropagationRepairCooldownMS := parseIntEnv("POOL_CLEAN_JOB_PROPAGATION_REPAIR_COOLDOWN_MS", 1000)
	cleanJobPropagationRepairMinAgeMS := parseIntEnv("POOL_CLEAN_JOB_PROPAGATION_REPAIR_MIN_AGE_MS", 250)
	cleanJobPropagationReconnectThreshold := parseIntEnv("POOL_CLEAN_JOB_PROPAGATION_RECONNECT_THRESHOLD", 3)
	cleanJobPropagationReconnectCooldownSeconds := parseIntEnv("POOL_CLEAN_JOB_PROPAGATION_RECONNECT_COOLDOWN_SECONDS", 60)
	submitBlockHeaderV2Enabled := parseBoolEnv("POOL_SUBMIT_BLOCK_HEADER_V2_ENABLED", true)
	submitDebugResultEnabled := parseBoolEnv("POOL_SUBMIT_DEBUG_RESULT", false)
	rpcUser := os.Getenv("NODE_RPC_USER")
	rpcPassword := os.Getenv("NODE_RPC_PASS")
	pplnsWorkStr := os.Getenv("PPLNS_N_WORK")
	var pplnsWork float64
	if pplnsWorkStr != "" {
		if val, err := strconv.ParseFloat(pplnsWorkStr, 64); err == nil {
			pplnsWork = val
		} else {
			log.Printf("Invalid PPLNS_N_WORK: %v", err)
		}
	}

	// Parse Maturity Config
	blockMaturity := 0 // default handled in server
	if val := os.Getenv("POOL_BLOCK_MATURITY"); val != "" {
		if x, err := strconv.Atoi(val); err == nil {
			blockMaturity = x
		}
	}
	payoutMaturity := 0 // default handled in server
	if val := os.Getenv("POOL_PAYOUT_MATURITY"); val != "" {
		if x, err := strconv.Atoi(val); err == nil {
			payoutMaturity = x
		}
	}

	poolFee := 0.0
	if val := os.Getenv("POOL_FEE_PERCENTAGE"); val != "" {
		if x, err := strconv.ParseFloat(val, 64); err == nil {
			poolFee = x
		}
	}

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		log.Println("⚠️ PG_URL not set, database persistence disabled")
	}

	walletRPCURL := os.Getenv("WALLET_RPC_URL")
	if walletRPCURL == "" {
		// Fallback or just empty
		log.Println("Note: WALLET_RPC_URL not set, checking RPC_URL or defaulting")
	}

	if rpcUser == "" {
		rpcUser = "test"
	}
	if rpcPassword == "" {
		rpcPassword = "test"
	}
	poolPrivateKey := os.Getenv("POOL_PRIVATE_KEY")
	poolCoinbaseAddress := firstNonEmptyEnv("POOL_COINBASE_ADDRESS", "MINING_POOL_ADDRESS", "MINING_ADDRESS")
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	poolInstanceID := os.Getenv("POOL_INSTANCE_ID")
	if poolInstanceID == "" {
		poolInstanceID = "0"
	}
	minPdiff := 0.0
	if val := os.Getenv("POOL_MIN_PDIFF"); val != "" {
		if x, err := strconv.ParseFloat(val, 64); err == nil && x > 0 {
			minPdiff = x
		} else {
			log.Printf("Invalid POOL_MIN_PDIFF: %q", val)
		}
	}
	startingPdiff := 0.0
	if val := os.Getenv("POOL_STARTING_PDIFF"); val != "" {
		if x, err := strconv.ParseFloat(val, 64); err == nil && x > 0 {
			startingPdiff = x
		} else {
			log.Printf("Invalid POOL_STARTING_PDIFF: %q", val)
		}
	}
	varDiffTargetSharesPerMinute := 20.0
	if val := os.Getenv("POOL_VARDIFF_TARGET_SHARES_PER_MINUTE"); val != "" {
		if x, err := strconv.ParseFloat(val, 64); err == nil && x > 0 {
			varDiffTargetSharesPerMinute = x
		} else {
			log.Printf("Invalid POOL_VARDIFF_TARGET_SHARES_PER_MINUTE: %q", val)
		}
	}
	maxInflightRequestsPerClient := parseIntEnv("POOL_STRATUM_MAX_INFLIGHT_REQUESTS_PER_CLIENT", 64)
	submitOrderingEnabled := parseBoolEnv("POOL_SUBMIT_ORDERING_ENABLED", true)
	stratumServerFirstDifficultyProbe := parseBoolEnv("POOL_STRATUM_SERVER_FIRST_DIFFICULTY_PROBE", false)
	poolStatusLogIntervalSeconds := parseIntEnv("POOL_STATUS_LOG_INTERVAL_SECONDS", 30)

	cfg := server.Config{
		ListenAddr:                                   listenAddr,
		RPCURL:                                       rpcURL,
		RPCEndpointAliases:                           rpcEndpointAliases,
		RPCRouterNodeHealthEnabled:                   rpcRouterNodeHealthEnabled,
		RPCRouterNodeHealthProbeSeconds:              rpcRouterNodeHealthProbeSeconds,
		RPCRouterNodeHealthMaxAgeSeconds:             rpcRouterNodeHealthMaxAgeSeconds,
		RPCRouterNodeHealthUnreadyThreshold:          rpcRouterNodeHealthUnreadyThreshold,
		RPCRouterNodeHealthErrorThreshold:            rpcRouterNodeHealthErrorThreshold,
		RPCRouterNodeHealthFreshTemplateGraceSeconds: rpcRouterNodeHealthFreshTemplateGraceSeconds,
		RPCRouterNodeHealthMinConsensusPeers:         rpcRouterNodeHealthMinConsensusPeers,
		SubmitRPCTimeoutMS:                           submitRPCTimeoutMS,
		SubmissionLeaseMS:                            submissionLeaseMS,
		SubmissionReconcileIntervalMS:                submissionReconcileIntervalMS,
		SubmissionChainID:                            submissionChainID,
		RejectStaleBlockCandidates:                   !submitStaleBlockCandidates,
		RecentStaleBlockCandidateSubmitGraceMS:       recentStaleBlockCandidateSubmitGraceMS,
		MaxBlockCandidateJobAgeMS:                    maxBlockCandidateJobAgeMS,
		AutoTuneBlockCandidateJobAge:                 autoTuneBlockCandidateJobAge,
		PreemptiveBlockCandidateRefreshEnabled:       preemptiveBlockCandidateRefreshEnabled,
		PreemptiveBlockCandidateCleanReissueEnabled:  preemptiveBlockCandidateCleanReissueEnabled,
		PreemptiveBlockCandidateRefreshDelayMS:       preemptiveBlockCandidateRefreshDelayMS,
		PreemptiveBlockCandidateRefreshIntervalMS:    preemptiveBlockCandidateRefreshIntervalMS,
		PreemptiveBlockCandidateRefreshTimeoutMS:     preemptiveBlockCandidateRefreshTimeoutMS,
		NonCleanJobGraceMS:                           nonCleanJobGraceMS,
		OrdinaryStaleShareGraceMS:                    ordinaryStaleShareGraceMS,
		GlobalBlockDedupeEnabled:                     globalBlockDedupeEnabled,
		AllowMultipleBlockCandidatesPerJob:           allowMultipleBlockCandidatesPerJob,
		StrictCurrentJobSubmit:                       strictCurrentJobSubmit,
		DisableSubmitStallRecovery:                   !submitStallRecoveryEnabled,
		SubmitStallRecoveryWindowSeconds:             submitStallRecoveryWindowSeconds,
		SubmitStallRecoveryMinAgeSeconds:             submitStallRecoveryMinAgeSeconds,
		SubmitStallRecoveryFailureThreshold:          submitStallRecoveryFailureThreshold,
		SubmitStallRecoveryStaleThreshold:            submitStallRecoveryStaleThreshold,
		SubmitStallRecoveryDuplicateThreshold:        submitStallRecoveryDuplicateThreshold,
		SubmitStallRecoveryCooldownSeconds:           submitStallRecoveryCooldownSeconds,
		DisableTemplateConversionStallRecovery:       !templateConversionStallRecoveryEnabled,
		TemplateConversionStallWindowSeconds:         templateConversionStallWindowSeconds,
		TemplateConversionStallMinAgeSeconds:         templateConversionStallMinAgeSeconds,
		TemplateConversionStallMinCandidates:         templateConversionStallMinCandidates,
		TemplateConversionStallFailureRatioPercent:   templateConversionStallFailureRatioPercent,
		TemplateConversionStallMinActiveMiners:       templateConversionStallMinActiveMiners,
		TemplateConversionStallCooldownSeconds:       templateConversionStallCooldownSeconds,
		BlockTimingControllerEnabled:                 blockTimingControllerEnabled,
		BlockTimingControllerWindowSeconds:           blockTimingControllerWindowSeconds,
		BlockTimingControllerMinCandidates:           blockTimingControllerMinCandidates,
		BlockTimingControllerTargetWastePercent:      blockTimingControllerTargetWastePercent,
		BlockTimingControllerMinJobAgeMS:             blockTimingControllerMinJobAgeMS,
		BlockTimingControllerMaxJobAgeMS:             blockTimingControllerMaxJobAgeMS,
		BlockTimingControllerMinTemplateTTLMS:        blockTimingControllerMinTemplateTTLMS,
		BlockTimingControllerMaxTemplateTTLMS:        blockTimingControllerMaxTemplateTTLMS,
		BlockTimingControllerStepMS:                  blockTimingControllerStepMS,
		BlockTimingControllerCooldownSeconds:         blockTimingControllerCooldownSeconds,
		DisableLowDiffFloodDisconnect:                !lowDiffFloodDisconnectEnabled,
		LowDiffRejectWindowSeconds:                   lowDiffRejectWindowSeconds,
		LowDiffRejectDisconnectThreshold:             lowDiffRejectDisconnectThreshold,
		DisableExpiredJobRecovery:                    !expiredJobRecoveryEnabled,
		ExpiredJobRejectWindowSeconds:                expiredJobRejectWindowSeconds,
		ExpiredJobClientResendThreshold:              expiredJobClientResendThreshold,
		ExpiredJobClientReconnectThreshold:           expiredJobClientReconnectThreshold,
		ExpiredJobClientReconnectWindowSeconds:       expiredJobClientReconnectWindowSeconds,
		ExpiredJobClientReconnectCooldownSeconds:     expiredJobClientReconnectCooldownSeconds,
		ExpiredJobGlobalCleanRefreshThreshold:        expiredJobGlobalCleanRefreshThreshold,
		ExpiredJobGlobalCleanRefreshMinClients:       expiredJobGlobalCleanRefreshMinClients,
		ExpiredJobRecoveryCooldownSeconds:            expiredJobRecoveryCooldownSeconds,
		StaleRaceRejectWindowSeconds:                 staleRaceRejectWindowSeconds,
		StaleRaceClientResendThreshold:               staleRaceClientResendThreshold,
		StaleRaceClientReconnectThreshold:            staleRaceClientReconnectThreshold,
		StaleRaceClientReconnectWindowSeconds:        staleRaceClientReconnectWindowSeconds,
		StaleRaceClientReconnectCooldownSeconds:      staleRaceClientReconnectCooldownSeconds,
		StaleRaceRecoveryCooldownSeconds:             staleRaceRecoveryCooldownSeconds,
		EnableDuplicateMACLaneCleanup:                duplicateMACLaneCleanupEnabled,
		DuplicateMACLaneStaleGraceMS:                 duplicateMACLaneStaleGraceMS,
		EnableCleanJobPropagationRepair:              cleanJobPropagationRepairEnabled,
		EnableCleanJobPropagationReconnect:           cleanJobPropagationReconnectEnabled,
		CleanJobPropagationRepairCooldownMS:          cleanJobPropagationRepairCooldownMS,
		CleanJobPropagationRepairMinAgeMS:            cleanJobPropagationRepairMinAgeMS,
		CleanJobPropagationReconnectThreshold:        cleanJobPropagationReconnectThreshold,
		CleanJobPropagationReconnectCooldownSeconds:  cleanJobPropagationReconnectCooldownSeconds,
		SubmitBlockHeaderV2Enabled:                   submitBlockHeaderV2Enabled,
		SubmitDebugResultEnabled:                     submitDebugResultEnabled,
		WalletRPCURL:                                 walletRPCURL,
		RewardIPCPath:                                os.Getenv("POOL_REWARD_IPC_PATH"),
		RewardEVMRPCURL:                              os.Getenv("EVM_RPC_URL"),
		RPCUser:                                      rpcUser,
		RPCPassword:                                  rpcPassword,
		PPLNSTargetWork:                              pplnsWork,
		BlockMaturityDepth:                           blockMaturity,
		PayoutMaturityDepth:                          payoutMaturity,
		PoolFeePercentage:                            poolFee,
		PGURL:                                        pgURL,
		PoolCoinbaseAddress:                          poolCoinbaseAddress,
		PoolPrivateKey:                               poolPrivateKey,
		MetricsAddr:                                  metricsAddr,
		PoolInstanceID:                               poolInstanceID,
		MinPdiff:                                     minPdiff,
		StartingPdiff:                                startingPdiff,
		VarDiffTargetSharesPerMinute:                 varDiffTargetSharesPerMinute,
		MaxInflightRequestsPerClient:                 maxInflightRequestsPerClient,
		SubmitOrderingEnabled:                        submitOrderingEnabled,
		StratumServerFirstDifficultyProbe:            stratumServerFirstDifficultyProbe,
		PoolStatusLogIntervalSeconds:                 poolStatusLogIntervalSeconds,
	}
	if err := server.Run(cfg); err != nil {
		log.Fatalf("pool server failed: %v", err)
	}
}

func parseBoolEnv(name string, defaultValue bool) bool {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		log.Printf("Invalid %s: %v", name, err)
		return defaultValue
	}
	return value
}

func parseIntEnv(name string, defaultValue int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("Invalid %s: %v", name, err)
		return defaultValue
	}
	return value
}

func parseMinIntEnv(name string, defaultValue, minimum int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	if value < minimum {
		return 0, fmt.Errorf("%s must be at least %d", name, minimum)
	}
	return value, nil
}

func firstNonEmptyEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
