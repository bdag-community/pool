package server

import (
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// ── Shares ──────────────────────────────────────────────────
	sharesRejected  *prometheus.CounterVec
	sharesAccepted  prometheus.Counter
	shareDiffSum    prometheus.Counter
	shareProcessing prometheus.Histogram

	// ── Submit Lifecycle ────────────────────────────────────────
	submitReadToHandleLatency     prometheus.Histogram
	submitJobNotifyToSubmitAge    *prometheus.HistogramVec
	submitJobLifecycleEvents      *prometheus.CounterVec
	submitJobLifecycleSeqDistance *prometheus.HistogramVec
	submitJobIDAliases            *prometheus.CounterVec

	// ── Blocks ──────────────────────────────────────────────────
	blocksFound                        prometheus.Counter
	staleBlockCandidates               prometheus.Counter
	staleBlocksLocalDrop               prometheus.Counter
	duplicateBlocksLocalDrop           prometheus.Counter
	blocksRejectedByNode               *prometheus.CounterVec
	blockSubmitOutcomes                *prometheus.CounterVec
	blockSubmitBackendOutcomes         *prometheus.CounterVec
	blockCandidateRejectJobAge         *prometheus.HistogramVec
	submitStallRecoveries              *prometheus.CounterVec
	templateConversionStallRecoveries  *prometheus.CounterVec
	blockTimingControllerWasteRatio    prometheus.Gauge
	blockTimingControllerJobAgeMS      prometheus.Gauge
	blockTimingControllerTemplateTTLMS prometheus.Gauge
	blockTimingControllerStaleGraceMS  prometheus.Gauge
	blockTimingControllerAdjustments   *prometheus.CounterVec
	blocksSubmittedTotal               prometheus.Counter
	blocksMatureTotal                  prometheus.Counter
	blocksPaidTotal                    prometheus.Counter
	submissionPendingGauge             prometheus.Gauge
	submissionInFlightGauge            prometheus.Gauge
	submissionAcceptedGauge            prometheus.Gauge
	submissionRejectedGauge            prometheus.Gauge
	submissionConflictGauge            prometheus.Gauge
	submissionRecoveredConflictGauge   prometheus.Gauge
	submissionUnresolvedConflictGauge  prometheus.Gauge
	submissionReceiptGauge             prometheus.Gauge
	accountingIntentGauge              prometheus.Gauge
	accountingPendingGauge             prometheus.Gauge
	accountingEffectGauge              prometheus.Gauge
	accountingConflictGauge            prometheus.Gauge
	accountingRecoveredConflictGauge   prometheus.Gauge
	submissionOldestPendingAgeGauge    prometheus.Gauge
	submissionLastAcceptedTimestamp    prometheus.Gauge
	submissionAttemptOutcomes          *prometheus.CounterVec

	// ── Jobs / Templates ────────────────────────────────────────
	jobsMarkedStale            prometheus.Counter
	templateBroadcasts         prometheus.Counter
	templateFetchDur           prometheus.Histogram
	templateFetchGateInFlight  prometheus.GaugeFunc
	templateFetchGateAge       prometheus.GaugeFunc
	blockHeight                prometheus.Gauge
	networkDifficulty          prometheus.Gauge
	templateCoinbaseEvents     *prometheus.CounterVec
	expiredJobRecoveries       *prometheus.CounterVec
	staleJobRecoveries         *prometheus.CounterVec
	cleanTemplateRefreshEvents *prometheus.CounterVec

	// ── Miner / Job Health ──────────────────────────────────────
	poolJobHealthOKGauge                        prometheus.Gauge
	poolJobHealthIdentityOKGauge                prometheus.Gauge
	poolJobHealthAuthorizedMiners               prometheus.Gauge
	poolJobHealthReadyMiners                    prometheus.Gauge
	poolJobHealthMinersWithoutCurrentJobGauge   prometheus.Gauge
	poolJobHealthStaleCurrentJobMiners          prometheus.Gauge
	poolJobHealthInvalidatedCurrentJobMiners    prometheus.Gauge
	poolJobHealthExpiredCurrentJobMiners        prometheus.Gauge
	poolJobHealthPhysicalMiners                 prometheus.Gauge
	poolJobHealthReadyPhysicalMiners            prometheus.Gauge
	poolJobHealthUnknownIdentityConnections     prometheus.Gauge
	poolJobHealthDuplicateMACs                  prometheus.Gauge
	poolJobHealthDuplicateMACLanes              prometheus.Gauge
	poolJobHealthMaxCurrentJobAge               prometheus.Gauge
	poolJobHealthAcceptedJobs                   *prometheus.GaugeVec
	poolExpiredJobGlobalWindowRejects           prometheus.Gauge
	poolExpiredJobGlobalWindowClients           prometheus.Gauge
	poolSubmitStallWindowFailures               *prometheus.GaugeVec
	poolTemplateConversionStallWindowCandidates *prometheus.GaugeVec
	poolTemplateConversionStallFailureRatio     prometheus.Gauge
	poolTemplateConversionStallActiveMiners     prometheus.Gauge
	duplicateMACLaneDuplicateGroups             prometheus.Gauge
	duplicateMACLaneCloses                      *prometheus.CounterVec
	cleanJobSubmitObservations                  *prometheus.CounterVec
	cleanJobSubmitSeqLag                        *prometheus.HistogramVec
	cleanJobPropagationRepairs                  *prometheus.CounterVec

	// ── RPC Router ──────────────────────────────────────────────
	rpcBackendSelected                              *prometheus.GaugeVec
	rpcBackendHealth                                *prometheus.GaugeVec
	rpcBackendScore                                 *prometheus.GaugeVec
	rpcBackendTemplateAge                           *prometheus.GaugeVec
	rpcBackendTemplateSeq                           *prometheus.GaugeVec
	rpcBackendTemplateSeqGap                        *prometheus.CounterVec
	rpcBackendTemplateSeqResets                     *prometheus.CounterVec
	rpcBackendTemplateSeqReject                     *prometheus.CounterVec
	rpcBackendTemplateDur                           *prometheus.HistogramVec
	rpcBackendTemplateErrors                        *prometheus.CounterVec
	rpcBackendWSConnected                           *prometheus.GaugeVec
	rpcBackendWSReconnects                          *prometheus.CounterVec
	rpcBackendSubmitTotal                           *prometheus.CounterVec
	rpcBackendSubmitDur                             *prometheus.HistogramVec
	rpcBackendNodeHealthMineable                    *prometheus.GaugeVec
	rpcBackendNodeHealthSubmitReady                 *prometheus.GaugeVec
	rpcBackendNodeHealthTemplateAge                 *prometheus.GaugeVec
	rpcBackendNodeHealthTemplateSeq                 *prometheus.GaugeVec
	rpcBackendNodeHealthMainOrder                   *prometheus.GaugeVec
	rpcBackendNodeHealthLastTemplateBuildAge        *prometheus.GaugeVec
	rpcBackendNodeHealthLastTemplateBuildDuration   *prometheus.GaugeVec
	rpcBackendNodeHealthLastTemplateBuildBlocking   *prometheus.GaugeVec
	rpcBackendNodeHealthLastTemplateInvalidationSeq *prometheus.GaugeVec
	rpcBackendNodeHealthTemplateInvalidations       *prometheus.CounterVec
	rpcBackendNodeHealthPendingTemplateBuild        *prometheus.GaugeVec
	rpcBackendNodeHealthPendingTemplatePriority     *prometheus.GaugeVec
	rpcBackendNodeHealthPendingTemplateInvalidation *prometheus.GaugeVec
	rpcBackendNodeHealthP2PMiningFresh              *prometheus.GaugeVec
	rpcBackendNodeHealthP2PConsensusPeerCount       *prometheus.GaugeVec
	rpcBackendNodeHealthP2PFreshPeerCount           *prometheus.GaugeVec
	rpcBackendNodeHealthP2PStalePeerCount           *prometheus.GaugeVec
	rpcBackendNodeHealthP2PBestPeerLeadBlocks       *prometheus.GaugeVec
	rpcBackendNodeHealthP2PBestPeerGraphAge         *prometheus.GaugeVec
	rpcBackendNodeHealthP2PAvgGraphAge              *prometheus.GaugeVec
	rpcBackendNodeHealthP2PMaxGraphAge              *prometheus.GaugeVec
	rpcBackendNodeHealthP2PSyncPeerPresent          *prometheus.GaugeVec
	rpcBackendNodeHealthP2PSyncPeerFresh            *prometheus.GaugeVec
	rpcBackendNodeHealthP2PSyncPeerGraphAge         *prometheus.GaugeVec
	rpcBackendNodeHealthErrors                      *prometheus.CounterVec

	// ── Connections ─────────────────────────────────────────────
	activeConnections           prometheus.Gauge
	connectionsTotal            prometheus.Counter
	disconnectionsTotal         prometheus.Counter
	authorizationsTotal         *prometheus.CounterVec
	noRequestDisconnects        *prometheus.CounterVec
	serverFirstDifficultyProbes *prometheus.CounterVec

	// ── VarDiff ─────────────────────────────────────────────────
	vardiffAdjustments *prometheus.CounterVec
	vardiffFloorHits   prometheus.Counter

	// ── Payouts ─────────────────────────────────────────────────
	payoutsTotal    prometheus.Counter
	payoutAmountWei prometheus.Counter
	payoutFailures  prometheus.Counter

	// ── PPLNS Window ────────────────────────────────────────────
	pplnsWindowWork   prometheus.Gauge
	pplnsWindowShares prometheus.Gauge
	pplnsWindowMiners prometheus.Gauge
)

func InitMetrics(poolID string) {
	constLabels := prometheus.Labels{"pool_id": poolID}

	// ── Shares ──────────────────────────────────────────────────

	sharesRejected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_shares_rejected_total",
			Help:        "Total number of shares rejected.",
			ConstLabels: constLabels,
		},
		[]string{"reason"},
	)

	sharesAccepted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_shares_accepted_total",
			Help:        "Total number of valid shares accepted.",
			ConstLabels: constLabels,
		},
	)

	shareDiffSum = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_share_difficulty_sum",
			Help:        "Cumulative sum of accepted share difficulties (use with rate() for hashrate estimation).",
			ConstLabels: constLabels,
		},
	)

	shareProcessing = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:        "pool_share_processing_duration_seconds",
			Help:        "Time to validate and process a submitted share.",
			ConstLabels: constLabels,
			Buckets:     []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
		},
	)

	// ── Submit Lifecycle ────────────────────────────────────────

	submitReadToHandleLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:        "pool_submit_read_to_handle_latency_seconds",
			Help:        "Time between reading a mining.submit request from the Stratum socket and starting submit handling.",
			ConstLabels: constLabels,
			Buckets:     []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
		},
	)

	submitJobNotifyToSubmitAge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "pool_submit_job_notify_to_submit_age_seconds",
			Help:        "Age of the Stratum job at submit handling start, measured from mining.notify send time when available.",
			ConstLabels: constLabels,
			Buckets:     []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"classification", "outcome", "reason"},
	)

	submitJobLifecycleEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_submit_job_lifecycle_events_total",
			Help:        "Submit outcomes grouped by job lifecycle classification and low-cardinality reason.",
			ConstLabels: constLabels,
		},
		[]string{"classification", "outcome", "reason"},
	)

	submitJobLifecycleSeqDistance = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "pool_submit_job_sequence_distance",
			Help:        "Current template sequence minus submitted job template sequence at submit handling start.",
			ConstLabels: constLabels,
			Buckets:     []float64{1, 2, 3, 5, 10, 25, 50, 100, 250, 1000},
		},
		[]string{"classification", "outcome", "reason"},
	)

	submitJobIDAliases = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_submit_job_id_aliases_total",
			Help:        "Submitted Stratum job IDs normalized to the canonical job ID already assigned to that client.",
			ConstLabels: constLabels,
		},
		[]string{"kind"},
	)

	// ── Blocks ──────────────────────────────────────────────────

	blocksFound = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_blocks_found_total",
			Help:        "Total number of blocks found and submitted by the pool.",
			ConstLabels: constLabels,
		},
	)

	staleBlockCandidates = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_stale_block_candidates_submitted_total",
			Help:        "Blocks genuinely rejected by the node (overdue/expired RPC error).",
			ConstLabels: constLabels,
		},
	)

	staleBlocksLocalDrop = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_stale_block_candidates_rejected_local_total",
			Help:        "Block candidates rejected locally because their job was already stale.",
			ConstLabels: constLabels,
		},
	)

	duplicateBlocksLocalDrop = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_duplicate_block_candidates_rejected_local_total",
			Help:        "Block candidates rejected locally because the same block hash was already submitted recently.",
			ConstLabels: constLabels,
		},
	)

	blocksRejectedByNode = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_blocks_rejected_by_node_total",
			Help:        "Blocks that met PoW target but were rejected by the node on submission.",
			ConstLabels: constLabels,
		},
		[]string{"reason"},
	)

	blockSubmitOutcomes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_block_submit_outcomes_total",
			Help:        "Candidate block submit outcomes after local PoW validation.",
			ConstLabels: constLabels,
		},
		[]string{"outcome", "reason"},
	)

	blockSubmitBackendOutcomes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_block_submit_backend_outcomes_total",
			Help:        "Candidate block submit outcomes attributed to the selected mining RPC node.",
			ConstLabels: constLabels,
		},
		[]string{"node", "outcome", "reason"},
	)

	blockCandidateRejectJobAge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "pool_block_candidate_reject_job_age_seconds",
			Help:        "Age of candidate block jobs rejected locally before RPC submit, labelled by low-cardinality reason and node.",
			ConstLabels: constLabels,
			Buckets:     []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"reason", "node"},
	)

	submitStallRecoveries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_submit_stall_recoveries_total",
			Help:        "Pool-native recoveries after repeated block candidates produce no successful node submissions.",
			ConstLabels: constLabels,
		},
		[]string{"action", "reason"},
	)

	templateConversionStallRecoveries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_template_conversion_stall_recoveries_total",
			Help:        "Pool-native recoveries when candidate blocks are mostly failing despite intermittent successes.",
			ConstLabels: constLabels,
		},
		[]string{"action", "reason"},
	)

	blockTimingControllerWasteRatio = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_block_timing_controller_waste_ratio",
			Help:        "Latest block-candidate conversion waste ratio observed by the timing controller.",
			ConstLabels: constLabels,
		},
	)

	blockTimingControllerJobAgeMS = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_block_timing_controller_job_age_ms",
			Help:        "Current max block-candidate job age selected by the timing controller.",
			ConstLabels: constLabels,
		},
	)

	blockTimingControllerTemplateTTLMS = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_block_timing_controller_template_ttl_ms",
			Help:        "Current same-parent template refresh TTL selected by the timing controller.",
			ConstLabels: constLabels,
		},
	)

	blockTimingControllerStaleGraceMS = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_block_timing_controller_recent_stale_grace_ms",
			Help:        "Current recent stale block-candidate submit grace selected by the timing controller.",
			ConstLabels: constLabels,
		},
	)

	blockTimingControllerAdjustments = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_block_timing_controller_adjustments_total",
			Help:        "Timing-controller decisions by action and reason.",
			ConstLabels: constLabels,
		},
		[]string{"action", "reason"},
	)

	blocksSubmittedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_blocks_submitted_total",
			Help:        "Total block submissions sent to the node (successful or not).",
			ConstLabels: constLabels,
		},
	)

	blocksMatureTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_blocks_mature_total",
			Help:        "Total blocks that reached maturity (PENDING -> MATURE).",
			ConstLabels: constLabels,
		},
	)

	blocksPaidTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_blocks_paid_total",
			Help:        "Total blocks fully paid out (MATURE -> PAID).",
			ConstLabels: constLabels,
		},
	)

	submissionPendingGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_pending", Help: "Pending durable block-submission candidates.", ConstLabels: constLabels})
	submissionInFlightGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_in_flight", Help: "In-flight durable block-submission candidates.", ConstLabels: constLabels})
	submissionAcceptedGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_accepted_final", Help: "Durably accepted block-submission candidates.", ConstLabels: constLabels})
	submissionRejectedGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_rejected_final", Help: "Durably rejected block-submission candidates.", ConstLabels: constLabels})
	submissionConflictGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_conflict", Help: "Historical durable conflict states, including recovered conflicts.", ConstLabels: constLabels})
	submissionRecoveredConflictGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_recovered_conflict", Help: "Exact recovery proofs validated at startup and still matching durable receipts/effects.", ConstLabels: constLabels})
	submissionUnresolvedConflictGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_unresolved_conflict", Help: "Conflicts without a currently verified recovery; mining must stop.", ConstLabels: constLabels})
	submissionReceiptGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_accepted_receipts", Help: "Immutable accepted-submission receipts.", ConstLabels: constLabels})
	accountingIntentGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_accepted_accounting_intents", Help: "Immutable block-accounting intents frozen before submission.", ConstLabels: constLabels})
	accountingPendingGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_accepted_accounting_pending", Help: "Accepted receipts awaiting local block and credit materialization.", ConstLabels: constLabels})
	accountingEffectGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_accepted_accounting_effects", Help: "Exactly-once accepted accounting effect markers.", ConstLabels: constLabels})
	accountingConflictGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_accepted_accounting_conflicts", Help: "Durable accepted-accounting conflict latch rows.", ConstLabels: constLabels})
	accountingRecoveredConflictGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_accepted_accounting_recovered_conflicts", Help: "Exact historical accounting conflicts qualified by preserved evidence, original receipt and effect.", ConstLabels: constLabels})
	submissionOldestPendingAgeGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_oldest_pending_age_seconds", Help: "Age of the oldest pending durable submission candidate.", ConstLabels: constLabels})
	submissionLastAcceptedTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "pool_submission_journal_last_accepted_timestamp_seconds", Help: "Unix timestamp of the latest durable accepted-submission receipt.", ConstLabels: constLabels})
	submissionAttemptOutcomes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_submission_journal_attempts_total",
			Help:        "Prepared durable submission attempts by caller, bounded decision, and transport class.",
			ConstLabels: constLabels,
		},
		[]string{"source", "decision", "transport_class"},
	)

	// ── Jobs / Templates ────────────────────────────────────────

	jobsMarkedStale = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_jobs_marked_stale_total",
			Help:        "Jobs invalidated by a DAG tip change before a block was found.",
			ConstLabels: constLabels,
		},
	)

	templateBroadcasts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_template_broadcasts_total",
			Help:        "Total block templates broadcast to connected miners.",
			ConstLabels: constLabels,
		},
	)

	templateFetchDur = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:        "pool_template_fetch_duration_seconds",
			Help:        "Time to fetch a block template from the node via RPC.",
			ConstLabels: constLabels,
			Buckets:     []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
		},
	)
	templateFetchGateInFlight = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        "pool_template_fetch_gate_in_flight",
			Help:        "Whether a getBlockTemplate request currently owns the global fetch gate.",
			ConstLabels: constLabels,
		},
		func() float64 {
			lastGBTRequestMu.Lock()
			defer lastGBTRequestMu.Unlock()
			if templateFetchInFlight {
				return 1
			}
			return 0
		},
	)
	templateFetchGateAge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        "pool_template_fetch_gate_age_seconds",
			Help:        "Age in seconds of the active getBlockTemplate fetch gate holder, or zero when idle.",
			ConstLabels: constLabels,
		},
		func() float64 {
			lastGBTRequestMu.Lock()
			defer lastGBTRequestMu.Unlock()
			if !templateFetchInFlight || lastGBTRequest.IsZero() {
				return 0
			}
			return time.Since(lastGBTRequest).Seconds()
		},
	)

	blockHeight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_block_height",
			Help:        "Latest block height from the most recent block template.",
			ConstLabels: constLabels,
		},
	)

	networkDifficulty = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_network_difficulty",
			Help:        "Current network difficulty derived from template nBits.",
			ConstLabels: constLabels,
		},
	)

	templateCoinbaseEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_template_coinbase_events_total",
			Help:        "Template and submit coinbase validation, fill, and rejection events.",
			ConstLabels: constLabels,
		},
		[]string{"event", "source"},
	)

	expiredJobRecoveries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_expired_job_recoveries_total",
			Help:        "Pool-native recoveries after expired Stratum job bursts.",
			ConstLabels: constLabels,
		},
		[]string{"action"},
	)

	staleJobRecoveries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_stale_job_race_recoveries_total",
			Help:        "Pool-native recoveries for normal stale-template races, separated from true expired Stratum jobs.",
			ConstLabels: constLabels,
		},
		[]string{"action", "reason"},
	)

	cleanTemplateRefreshEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_clean_template_refresh_events_total",
			Help:        "Forced clean template refresh coalescer events.",
			ConstLabels: constLabels,
		},
		[]string{"event"},
	)

	// ── Miner / Job Health ──────────────────────────────────────

	poolJobHealthOKGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_ok",
			Help:        "Whether all authorized Stratum miners have a current usable job.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthIdentityOKGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_identity_ok",
			Help:        "Whether connected Stratum clients can be cleanly counted by physical ASIC MAC identity.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthAuthorizedMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_authorized_miners",
			Help:        "Connected Stratum clients that are authorized to mine.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthReadyMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_ready_miners",
			Help:        "Authorized Stratum clients with a current non-stale, non-invalidated job.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthMinersWithoutCurrentJobGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_miners_without_current_job",
			Help:        "Authorized Stratum clients that do not currently have a usable job assigned.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthStaleCurrentJobMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_current_job_stale_miners",
			Help:        "Authorized Stratum clients whose current job is stale or points at an old parent.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthInvalidatedCurrentJobMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_current_job_invalidated_miners",
			Help:        "Authorized Stratum clients whose current job was explicitly invalidated by pool recovery.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthExpiredCurrentJobMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_current_job_expired_miners",
			Help:        "Authorized Stratum clients whose current job is older than the local job TTL.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthPhysicalMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_physical_miners",
			Help:        "Distinct connected physical ASIC miners identified by normalized MAC address.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthReadyPhysicalMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_ready_physical_miners",
			Help:        "Distinct MAC-identified physical ASIC miners with at least one current usable Stratum lane.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthUnknownIdentityConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_unknown_identity_connections",
			Help:        "Connected Stratum clients without a known ASIC MAC identity.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthDuplicateMACs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_duplicate_macs",
			Help:        "MAC identities currently represented by more than one Stratum connection.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthDuplicateMACLanes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_duplicate_mac_lanes",
			Help:        "Extra Stratum lanes beyond one lane per MAC-identified physical ASIC.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthMaxCurrentJobAge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_max_current_job_age_seconds",
			Help:        "Maximum current job age among connected Stratum clients.",
			ConstLabels: constLabels,
		},
	)

	poolJobHealthAcceptedJobs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_job_health_accepted_jobs",
			Help:        "Accepted job map entries by aggregate state.",
			ConstLabels: constLabels,
		},
		[]string{"state"},
	)

	poolExpiredJobGlobalWindowRejects = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_expired_job_global_window_rejects",
			Help:        "Expired-job rejects observed in the current global recovery window.",
			ConstLabels: constLabels,
		},
	)

	poolExpiredJobGlobalWindowClients = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_expired_job_global_window_clients",
			Help:        "Distinct Stratum clients contributing expired-job rejects in the current global recovery window.",
			ConstLabels: constLabels,
		},
	)

	poolSubmitStallWindowFailures = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_submit_stall_window_failures",
			Help:        "Submit-stall recovery failures observed in the current window by kind.",
			ConstLabels: constLabels,
		},
		[]string{"kind"},
	)

	poolTemplateConversionStallWindowCandidates = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_template_conversion_stall_window_candidates",
			Help:        "Candidate block outcomes observed in the current template-conversion stall window by kind.",
			ConstLabels: constLabels,
		},
		[]string{"kind"},
	)

	poolTemplateConversionStallFailureRatio = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_template_conversion_stall_failure_ratio",
			Help:        "Failed candidate block percentage in the current template-conversion stall window.",
			ConstLabels: constLabels,
		},
	)

	poolTemplateConversionStallActiveMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_template_conversion_stall_active_miners",
			Help:        "Active authorized miners observed by the template-conversion stall detector.",
			ConstLabels: constLabels,
		},
	)

	duplicateMACLaneDuplicateGroups = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_duplicate_mac_lane_groups",
			Help:        "MAC identities currently represented by more than one active Stratum lane.",
			ConstLabels: constLabels,
		},
	)
	duplicateMACLaneCloses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_duplicate_mac_lane_closes_total",
			Help:        "Duplicate Stratum lanes closed after another lane for the same MAC was already current, grouped by low-cardinality reason and normalized ASIC MAC.",
			ConstLabels: constLabels,
		},
		[]string{"reason", "mac"},
	)
	cleanJobSubmitObservations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_clean_job_submit_observations_total",
			Help:        "First submit observed after a clean Stratum job, grouped by whether the lane used current or older work.",
			ConstLabels: constLabels,
		},
		[]string{"result", "reason"},
	)
	cleanJobSubmitSeqLag = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "pool_clean_job_submit_sequence_lag",
			Help:        "Template sequence lag on the first submit observed after a clean Stratum job.",
			ConstLabels: constLabels,
			Buckets:     []float64{0, 1, 2, 3, 5, 10, 25, 50},
		},
		[]string{"result"},
	)
	cleanJobPropagationRepairs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_clean_job_propagation_repairs_total",
			Help:        "Pool-side clean-job propagation repairs after a Stratum lane submits old or missing work after a clean notify.",
			ConstLabels: constLabels,
		},
		[]string{"action", "reason"},
	)

	// ── RPC Router ──────────────────────────────────────────────

	rpcBackendSelected = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_selected",
			Help:        "Whether the configured mining RPC node is currently selected.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_healthy",
			Help:        "Mining RPC backend health, where 1 is healthy and 0 is unhealthy.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendScore = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_score",
			Help:        "Mining RPC backend health score used by the pool router.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendTemplateAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_template_age_seconds",
			Help:        "Age of the most recent healthy block template observed from each backend.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendTemplateSeq = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_template_sequence",
			Help:        "Latest node-provided WebSocket template sequence observed from each backend.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendTemplateSeqGap = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_template_sequence_gap_total",
			Help:        "Cumulative missed node WebSocket template sequence numbers by backend.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendTemplateSeqResets = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_template_sequence_resets_total",
			Help:        "Times a backend's node WebSocket template sequence moved backwards, usually after node restart.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendTemplateSeqReject = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_template_sequence_reject_total",
			Help:        "WebSocket template notifications rejected because their source sequence was duplicate or old for the same backend template.",
			ConstLabels: constLabels,
		},
		[]string{"node", "reason"},
	)

	rpcBackendTemplateDur = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "pool_rpc_backend_template_fetch_duration_seconds",
			Help:        "Time to fetch a block template from the mining RPC node.",
			ConstLabels: constLabels,
			Buckets:     []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
		},
		[]string{"node"},
	)

	rpcBackendTemplateErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_template_errors_total",
			Help:        "Template fetch errors by mining RPC backend and classified reason.",
			ConstLabels: constLabels,
		},
		[]string{"node", "reason"},
	)

	rpcBackendWSConnected = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_ws_connected",
			Help:        "Whether the block template WebSocket stream is connected for each backend.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendWSReconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_ws_reconnects_total",
			Help:        "WebSocket disconnect or reconnect attempts by the mining RPC node.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendSubmitTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_submit_total",
			Help:        "Block submission attempts by mining RPC backend and result.",
			ConstLabels: constLabels,
		},
		[]string{"node", "result"},
	)

	rpcBackendSubmitDur = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "pool_rpc_backend_submit_duration_seconds",
			Help:        "Time to submit a candidate block to the mining RPC node.",
			ConstLabels: constLabels,
			Buckets:     []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
		},
		[]string{"node", "result"},
	)

	rpcBackendNodeHealthMineable = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_mineable",
			Help:        "Node-reported mining-template health where 1 means the backend is mineable now.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthSubmitReady = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_submit_ready",
			Help:        "Node-reported submit readiness where 1 means solved blocks should be accepted by this backend.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthTemplateAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_template_age_seconds",
			Help:        "Node-reported age of the current mining template by backend.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthTemplateSeq = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_template_sequence",
			Help:        "Node-reported latest mining template sequence by backend.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthMainOrder = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_main_order",
			Help:        "Node-reported main-order height from the mining-template health RPC.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthLastTemplateBuildAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_last_template_build_age_seconds",
			Help:        "Age of the latest node template build attempt reported by the mining-template health RPC.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthLastTemplateBuildDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_last_template_build_duration_seconds",
			Help:        "Duration of the latest node template build attempt reported by the mining-template health RPC.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthLastTemplateBuildBlocking = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_last_template_build_error_blocking",
			Help:        "Whether the latest node template build error currently blocks usable mining templates.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthLastTemplateInvalidationSeq = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_last_template_invalidation_sequence",
			Help:        "Latest source-level template invalidation sequence reported by backend and cause.",
			ConstLabels: constLabels,
		},
		[]string{"node", "cause"},
	)

	rpcBackendNodeHealthTemplateInvalidations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_node_health_template_invalidations_total",
			Help:        "New source-level template invalidation events observed from node health by backend and cause.",
			ConstLabels: constLabels,
		},
		[]string{"node", "cause"},
	)

	rpcBackendNodeHealthPendingTemplateBuild = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_pending_template_build",
			Help:        "Whether the node reports a queued mining-template build request.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthPendingTemplatePriority = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_pending_template_build_priority",
			Help:        "Priority of the node's queued mining-template build request.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthPendingTemplateInvalidation = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_pending_template_invalidation",
			Help:        "Whether the node reports a pending source-level template invalidation by backend and cause.",
			ConstLabels: constLabels,
		},
		[]string{"node", "cause"},
	)

	rpcBackendNodeHealthP2PMiningFresh = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_mining_fresh",
			Help:        "Node-reported P2P mining freshness where 1 means peer state is fresh enough for mining.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PConsensusPeerCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_consensus_peer_count",
			Help:        "Consensus peer count from node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PFreshPeerCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_fresh_consensus_peer_count",
			Help:        "Fresh consensus peer count from node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PStalePeerCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_stale_consensus_peer_count",
			Help:        "Stale consensus peer count from node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PBestPeerLeadBlocks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_best_peer_lead_blocks",
			Help:        "How many blocks the best peer is ahead of this backend according to node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PBestPeerGraphAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_best_peer_graph_state_age_seconds",
			Help:        "Graph-state age for the backend's best peer from node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PAvgGraphAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_avg_graph_state_age_seconds",
			Help:        "Average peer graph-state age from node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PMaxGraphAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_max_graph_state_age_seconds",
			Help:        "Maximum peer graph-state age from node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PSyncPeerPresent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_sync_peer_present",
			Help:        "Whether the node reports a selected sync peer.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PSyncPeerFresh = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_sync_peer_fresh",
			Help:        "Whether the node reports its selected sync peer as fresh.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthP2PSyncPeerGraphAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "pool_rpc_backend_node_health_p2p_sync_peer_graph_state_age_seconds",
			Help:        "Selected sync peer graph-state age from node mining-template health.",
			ConstLabels: constLabels,
		},
		[]string{"node"},
	)

	rpcBackendNodeHealthErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_rpc_backend_node_health_errors_total",
			Help:        "Node mining-template health RPC failures by backend and classified reason.",
			ConstLabels: constLabels,
		},
		[]string{"node", "reason"},
	)

	// ── Connections ─────────────────────────────────────────────

	activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_active_connections",
			Help:        "Number of currently connected Stratum miners.",
			ConstLabels: constLabels,
		},
	)

	connectionsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_connections_total",
			Help:        "Total Stratum connections accepted.",
			ConstLabels: constLabels,
		},
	)

	disconnectionsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_disconnections_total",
			Help:        "Total Stratum connections closed.",
			ConstLabels: constLabels,
		},
	)

	authorizationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_authorizations_total",
			Help:        "Miner authorization attempts.",
			ConstLabels: constLabels,
		},
		[]string{"result"},
	)

	noRequestDisconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_stratum_no_request_disconnects_total",
			Help:        "Stratum connections that closed before sending any non-empty request line.",
			ConstLabels: constLabels,
		},
		[]string{"lane_source", "reason"},
	)

	serverFirstDifficultyProbes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_stratum_server_first_difficulty_probes_total",
			Help:        "Compatibility difficulty notifications sent immediately after accepting a Stratum connection.",
			ConstLabels: constLabels,
		},
		[]string{"result"},
	)

	// ── VarDiff ─────────────────────────────────────────────────

	vardiffAdjustments = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "pool_vardiff_adjustments_total",
			Help:        "VarDiff difficulty adjustments by direction.",
			ConstLabels: constLabels,
		},
		[]string{"direction"},
	)

	vardiffFloorHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_vardiff_floor_hits_total",
			Help:        "VarDiff retarget attempts clamped by the configured minimum pool difficulty.",
			ConstLabels: constLabels,
		},
	)

	// ── Payouts ─────────────────────────────────────────────────

	payoutsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_payouts_total",
			Help:        "Total successful payout transactions sent.",
			ConstLabels: constLabels,
		},
	)

	payoutAmountWei = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_payout_amount_wei_total",
			Help:        "Cumulative wei paid out to miners.",
			ConstLabels: constLabels,
		},
	)

	payoutFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "pool_payout_failures_total",
			Help:        "Total failed payout transactions.",
			ConstLabels: constLabels,
		},
	)

	// ── PPLNS Window ────────────────────────────────────────────

	pplnsWindowWork = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_pplns_window_work",
			Help:        "Current total work units in the PPLNS window.",
			ConstLabels: constLabels,
		},
	)

	pplnsWindowShares = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_pplns_window_shares",
			Help:        "Current number of shares in the PPLNS window.",
			ConstLabels: constLabels,
		},
	)

	pplnsWindowMiners = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "pool_pplns_window_miners",
			Help:        "Current number of unique miners in the PPLNS window.",
			ConstLabels: constLabels,
		},
	)

	prometheus.MustRegister(
		// Shares
		sharesRejected,
		sharesAccepted,
		shareDiffSum,
		shareProcessing,
		// Submit Lifecycle
		submitReadToHandleLatency,
		submitJobNotifyToSubmitAge,
		submitJobLifecycleEvents,
		submitJobLifecycleSeqDistance,
		submitJobIDAliases,
		// Blocks
		blocksFound,
		staleBlockCandidates,
		staleBlocksLocalDrop,
		duplicateBlocksLocalDrop,
		blocksRejectedByNode,
		blockSubmitOutcomes,
		blockSubmitBackendOutcomes,
		blockCandidateRejectJobAge,
		submitStallRecoveries,
		templateConversionStallRecoveries,
		blockTimingControllerWasteRatio,
		blockTimingControllerJobAgeMS,
		blockTimingControllerTemplateTTLMS,
		blockTimingControllerStaleGraceMS,
		blockTimingControllerAdjustments,
		blocksSubmittedTotal,
		blocksMatureTotal,
		blocksPaidTotal,
		submissionPendingGauge,
		submissionInFlightGauge,
		submissionAcceptedGauge,
		submissionRejectedGauge,
		submissionConflictGauge,
		submissionRecoveredConflictGauge,
		submissionUnresolvedConflictGauge,
		submissionReceiptGauge,
		accountingIntentGauge,
		accountingPendingGauge,
		accountingEffectGauge,
		accountingConflictGauge,
		accountingRecoveredConflictGauge,
		submissionOldestPendingAgeGauge,
		submissionLastAcceptedTimestamp,
		submissionAttemptOutcomes,
		// Jobs / Templates
		jobsMarkedStale,
		templateBroadcasts,
		templateFetchDur,
		templateFetchGateInFlight,
		templateFetchGateAge,
		blockHeight,
		networkDifficulty,
		templateCoinbaseEvents,
		expiredJobRecoveries,
		staleJobRecoveries,
		cleanTemplateRefreshEvents,
		// Miner / Job Health
		poolJobHealthOKGauge,
		poolJobHealthIdentityOKGauge,
		poolJobHealthAuthorizedMiners,
		poolJobHealthReadyMiners,
		poolJobHealthMinersWithoutCurrentJobGauge,
		poolJobHealthStaleCurrentJobMiners,
		poolJobHealthInvalidatedCurrentJobMiners,
		poolJobHealthExpiredCurrentJobMiners,
		poolJobHealthPhysicalMiners,
		poolJobHealthReadyPhysicalMiners,
		poolJobHealthUnknownIdentityConnections,
		poolJobHealthDuplicateMACs,
		poolJobHealthDuplicateMACLanes,
		poolJobHealthMaxCurrentJobAge,
		poolJobHealthAcceptedJobs,
		poolExpiredJobGlobalWindowRejects,
		poolExpiredJobGlobalWindowClients,
		poolSubmitStallWindowFailures,
		poolTemplateConversionStallWindowCandidates,
		poolTemplateConversionStallFailureRatio,
		poolTemplateConversionStallActiveMiners,
		duplicateMACLaneDuplicateGroups,
		duplicateMACLaneCloses,
		cleanJobSubmitObservations,
		cleanJobSubmitSeqLag,
		cleanJobPropagationRepairs,
		// RPC Router
		rpcBackendSelected,
		rpcBackendHealth,
		rpcBackendScore,
		rpcBackendTemplateAge,
		rpcBackendTemplateSeq,
		rpcBackendTemplateSeqGap,
		rpcBackendTemplateSeqResets,
		rpcBackendTemplateSeqReject,
		rpcBackendTemplateDur,
		rpcBackendTemplateErrors,
		rpcBackendWSConnected,
		rpcBackendWSReconnects,
		rpcBackendSubmitTotal,
		rpcBackendSubmitDur,
		rpcBackendNodeHealthMineable,
		rpcBackendNodeHealthSubmitReady,
		rpcBackendNodeHealthTemplateAge,
		rpcBackendNodeHealthTemplateSeq,
		rpcBackendNodeHealthMainOrder,
		rpcBackendNodeHealthLastTemplateBuildAge,
		rpcBackendNodeHealthLastTemplateBuildDuration,
		rpcBackendNodeHealthLastTemplateBuildBlocking,
		rpcBackendNodeHealthLastTemplateInvalidationSeq,
		rpcBackendNodeHealthTemplateInvalidations,
		rpcBackendNodeHealthPendingTemplateBuild,
		rpcBackendNodeHealthPendingTemplatePriority,
		rpcBackendNodeHealthPendingTemplateInvalidation,
		rpcBackendNodeHealthP2PMiningFresh,
		rpcBackendNodeHealthP2PConsensusPeerCount,
		rpcBackendNodeHealthP2PFreshPeerCount,
		rpcBackendNodeHealthP2PStalePeerCount,
		rpcBackendNodeHealthP2PBestPeerLeadBlocks,
		rpcBackendNodeHealthP2PBestPeerGraphAge,
		rpcBackendNodeHealthP2PAvgGraphAge,
		rpcBackendNodeHealthP2PMaxGraphAge,
		rpcBackendNodeHealthP2PSyncPeerPresent,
		rpcBackendNodeHealthP2PSyncPeerFresh,
		rpcBackendNodeHealthP2PSyncPeerGraphAge,
		rpcBackendNodeHealthErrors,
		// Connections
		activeConnections,
		connectionsTotal,
		disconnectionsTotal,
		authorizationsTotal,
		noRequestDisconnects,
		serverFirstDifficultyProbes,
		// VarDiff
		vardiffAdjustments,
		vardiffFloorHits,
		// Payouts
		payoutsTotal,
		payoutAmountWei,
		payoutFailures,
		// PPLNS
		pplnsWindowWork,
		pplnsWindowShares,
		pplnsWindowMiners,
	)
}

func recordNoRequestDisconnect(c *client, reason string) {
	if noRequestDisconnects == nil {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	laneSource := "unknown"
	if c != nil {
		laneSource = clientLaneIdentitySource(c.laneID)
	}
	noRequestDisconnects.WithLabelValues(laneSource, reason).Inc()
}

func recordServerFirstDifficultyProbe(result string) {
	if serverFirstDifficultyProbes == nil {
		return
	}
	if result == "" {
		result = "unknown"
	}
	serverFirstDifficultyProbes.WithLabelValues(result).Inc()
}

func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health/job-state", handlePoolJobHealth)
	mux.HandleFunc("/admin/template-ttl-refresh", handleTemplateTTLRefreshAdmin)
	mux.HandleFunc("/admin/block-candidate-job-age", handleBlockCandidateJobAgeAdmin)
	mux.HandleFunc("/admin/allow-multiple-block-candidates", handleAllowMultipleBlockCandidatesAdmin)
	mux.HandleFunc("/admin/submission-drain", handleSubmissionDrainAdmin)
	log.Printf("Prometheus metrics server listening on %s/metrics", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics server error: %v", err)
	}
}
