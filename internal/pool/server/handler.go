// pool_handlers.go
package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"asicPool/internal/pool/storage"
)

// -------------------- Handlers --------------------
// TTL for a job locally (if older than jobTTL, reject locally)
const jobTTL = 60 * time.Second

// TTL for local block-candidate submit marks. This is long enough to suppress
// duplicate stratum submits across job rotations without retaining old hashes.
const submittedBlockMarkTTL = 2 * time.Minute
const submittedBlockMarkCleanupInterval = 30 * time.Second

// Routine set_difficulty pushes happen on every job notify. Logging every push
// creates avoidable disk I/O, so unchanged values are summarized periodically.
const defaultDifficultyPushLogInterval = 5 * time.Minute
const routineSubmitLogInterval = 30 * time.Second
const validShareLogInterval = 30 * time.Second
const lowDiffShareLogInterval = 30 * time.Second
const defaultNonCleanJobGrace = 5 * time.Second
const defaultOrdinaryStaleShareGrace = 1200 * time.Millisecond

var evmAddressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// A miner that floods low-difficulty rejects is usually working at an old or
// ignored target. Close only that Stratum connection so it reconnects cleanly.
const defaultLowDiffRejectWindow = 10 * time.Second
const defaultLowDiffRejectDisconnectThreshold = 100
const defaultDuplicateBlockReplayWindow = 30 * time.Second
const defaultDuplicateBlockReplayDisconnectThreshold = 4

type shareNonceKey struct {
	nonce       uint32
	extranonce2 uint32
	ntime       uint32
}

type clientCurrentJob struct {
	id  string
	job *JobData
}

func newShareNonceKey(nonceBytes, extranonce2Bytes, ntimeBytes []byte) shareNonceKey {
	return shareNonceKey{
		nonce:       binary.BigEndian.Uint32(nonceBytes),
		extranonce2: binary.BigEndian.Uint32(extranonce2Bytes),
		ntime:       binary.BigEndian.Uint32(ntimeBytes),
	}
}

func (k shareNonceKey) String() string {
	return fmt.Sprintf("%08x-%08x-%08x", k.nonce, k.extranonce2, k.ntime)
}

func rememberClientCurrentJob(c *client, jobID string, job *JobData) {
	if c == nil || jobID == "" || job == nil {
		return
	}
	c.currentJob.Store(&clientCurrentJob{id: jobID, job: job})
	rememberClientCleanJob(c, jobID, job)
}

func currentClientJob(c *client) *clientCurrentJob {
	if c == nil {
		return nil
	}
	current, _ := c.currentJob.Load().(*clientCurrentJob)
	return current
}

func currentJobForClient(c *client, jobID string) (*JobData, bool) {
	if c == nil || jobID == "" {
		return nil, false
	}
	current := currentClientJob(c)
	if current == nil || current.id != jobID || current.job == nil {
		return nil, false
	}
	return current.job, true
}

func canonicalSubmittedJobID(c *client, submittedJobID, extranonce2 string) (string, string, bool) {
	submittedJobID = strings.TrimSpace(submittedJobID)
	extranonce2 = strings.TrimSpace(extranonce2)
	if c == nil || submittedJobID == "" || len(extranonce2) != subscribeExtranonce2Size*2 || !isHexString(extranonce2) {
		return submittedJobID, "", false
	}

	en1Hex, ok := clientExtranonce1Hex(c)
	if !ok {
		return submittedJobID, "", false
	}

	if len(submittedJobID) > len(extranonce2) && strings.EqualFold(submittedJobID[len(submittedJobID)-len(extranonce2):], extranonce2) {
		candidate := submittedJobID[:len(submittedJobID)-len(extranonce2)]
		if submittedJobAliasTargetAssignedToClient(c, candidate, en1Hex) {
			return candidate, "appended_extranonce2", true
		}
	}

	return submittedJobID, "", false
}

func clientExtranonce1Hex(c *client) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.Lock()
	en1 := c.extranonce1
	c.mu.Unlock()
	if en1 == ([4]byte{}) {
		return "", false
	}
	return hex.EncodeToString(en1[:]), true
}

func submittedJobAliasTargetAssignedToClient(c *client, candidateJobID, en1Hex string) bool {
	if candidateJobID == "" || !jobIDHasClientExtranonce(candidateJobID, en1Hex) {
		return false
	}
	if current := currentClientJob(c); current != nil && current.id == candidateJobID && current.job != nil {
		return true
	}
	return miningWork.acceptedJobExists(candidateJobID)
}

func jobIDHasClientExtranonce(jobID, en1Hex string) bool {
	underscore := strings.LastIndexByte(jobID, '_')
	if underscore < 0 || underscore == len(jobID)-1 {
		return false
	}
	return strings.EqualFold(jobID[underscore+1:], en1Hex)
}

func isHexString(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') {
			continue
		}
		return false
	}
	return true
}

func canAcceptRetainedJobForClient(c *client, job *JobData, now time.Time) bool {
	if c == nil || job == nil {
		return false
	}
	if jobInvalidatedByEpoch(job) || job.stale.Load() || job.invalidated.Load() {
		return false
	}
	grace := nonCleanJobGrace(c.cfg)
	if grace <= 0 {
		return false
	}
	issuedAt := job.SentAt
	if issuedAt.IsZero() {
		issuedAt = job.CreatedAt
	}
	if issuedAt.IsZero() || now.Sub(issuedAt) > grace {
		return false
	}
	current := currentClientJob(c)
	if current == nil || current.job == nil || current.job.TemplateKey == "" || job.TemplateKey == "" {
		return false
	}
	// clean_jobs describes the effect of the *new* notification on all work
	// issued before it. It is not an intrinsic lifetime property of the older
	// job. A clean job followed by a non-clean TTL/VarDiff refresh with exactly
	// the same mining content remains valid; conversely, a latest clean
	// notification revokes every retained job even when the content key matches.
	if current.job.Clean || !jobReadyForResend(current.job) {
		return false
	}
	return current.job.TemplateKey == job.TemplateKey
}

func nonCleanJobGrace(cfg *Config) time.Duration {
	if cfg == nil || cfg.NonCleanJobGraceMS <= 0 {
		return 0
	}
	return time.Duration(cfg.NonCleanJobGraceMS) * time.Millisecond
}

func ordinaryStaleShareGrace(cfg *Config) time.Duration {
	if cfg == nil || cfg.OrdinaryStaleShareGraceMS <= 0 {
		return 0
	}
	return time.Duration(cfg.OrdinaryStaleShareGraceMS) * time.Millisecond
}

func canAcceptStaleOrdinaryShare(cfg *Config, job *JobData, reason string, now time.Time) bool {
	if reason != "stale-job" {
		return false
	}
	return canEvaluateStaleOrdinaryShareGrace(cfg, job, now)
}

func canEvaluateStaleOrdinaryShareGrace(cfg *Config, job *JobData, now time.Time) bool {
	if job == nil {
		return false
	}
	grace := ordinaryStaleShareGrace(cfg)
	if grace <= 0 {
		return false
	}
	if !jobInvalidatedByEpoch(job) && !job.stale.Load() && !job.invalidated.Load() {
		return false
	}
	invalidatedAt := jobInvalidatedAt(job)
	if invalidatedAt.IsZero() {
		invalidatedAt = miningWork.lastInvalidationAt()
	}
	if invalidatedAt.IsZero() {
		return false
	}
	if now.Before(invalidatedAt) || now.Sub(invalidatedAt) > grace {
		return false
	}
	issuedAt := job.SentAt
	if issuedAt.IsZero() {
		issuedAt = job.CreatedAt
	}
	if issuedAt.IsZero() || issuedAt.After(invalidatedAt.Add(100*time.Millisecond)) {
		return false
	}
	return true
}

func canEvaluateRetainedJobForClient(c *client, job *JobData, now time.Time) bool {
	if c == nil {
		return false
	}
	return canAcceptRetainedJobForClient(c, job, now) || canEvaluateStaleOrdinaryShareGrace(c.cfg, job, now)
}

func shouldRejectInvalidatedJobBeforeHash(c *client, job *JobData, now time.Time) bool {
	if job == nil {
		return false
	}
	if !jobInvalidatedByEpoch(job) && !job.invalidated.Load() {
		return false
	}
	return !canEvaluateStaleOrdinaryShareGrace(c.cfg, job, now)
}

func submitExtranonce1(c *client) [4]byte {
	if c == nil {
		return [4]byte{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.extranonce1
}

func handleSubscribe(c *client, req *stratumRequest) {
	c.mu.Lock()
	v := atomic.AddUint32(&en1Counter, 1)
	binary.LittleEndian.PutUint32(c.extranonce1[:], v)

	extranonce1 := hex.EncodeToString(c.extranonce1[:])
	c.mu.Unlock()
	extranonce2Size := subscribeExtranonce2Size // standard extranonce2 size

	// Build dynamic subscriptions (Stratum)
	// Spec: [[["mining.set_difficulty","sessionid"],["mining.notify","sessionid"]],"extra_nonce1",len]

	// Generate unique random Session ID (16 bytes = 32 hex chars)
	sidBytes := make([]byte, 16)
	_, _ = rand.Read(sidBytes)
	sessionID := hex.EncodeToString(sidBytes)
	c.mu.Lock()
	c.sessionID = sessionID
	c.mu.Unlock()

	subscriptions := [][]interface{}{
		{"mining.set_difficulty", sessionID},
		{"mining.notify", sessionID},
	}

	// Prepare response
	res := []interface{}{
		subscriptions,
		extranonce1,
		extranonce2Size,
	}
	// Send response to miner
	sendResult(c, req.ID, res)
	log.Printf("[STRATUM] subscribe accepted addr=%s extranonce1=%s extranonce2_size=%d", c.addr, extranonce1, extranonce2Size)
	resendCurrentJobIfReady(c)
}

func validateUser(username string) (bool, string) {
	if username == "" {
		return false, "empty username"
	}

	// Strict: Username MUST be just the address
	address := username

	// EVM Address Validation: ^0x[0-9a-fA-F]{40}$
	// Length must be 42 (2 prefix + 40 hex)
	if len(address) != 42 || !strings.HasPrefix(address, "0x") {
		return false, "username should be a valid evm address (start with 0x...)"
	}

	if !evmAddressPattern.MatchString(address) {
		return false, "username should be a valid evm address"
	}

	return true, ""
}

func handleAuthorize(c *client, req *stratumRequest) {
	params := rawParamsToStrings(req.Params)
	if len(params) < 1 {
		sendError(c, req.ID, 24, "missing parameters")
		return
	}
	username := params[0] // logic: user.worker

	ok, errMsg := validateUser(username)
	if !ok {
		authorizationsTotal.WithLabelValues("failure").Inc()
		sendError(c, req.ID, 24, errMsg)
		log.Printf("❌ [%s] authorize failed (bad user: %s) -- %s", c.addr, username, errMsg)
		return
	}

	authorizationsTotal.WithLabelValues("success").Inc()
	c.mu.Lock()
	c.username = username
	c.authorized = true
	c.mu.Unlock()

	// Track Miner in DB
	if c.storage != nil {
		shouldTrack, trackedAt := globalMinerTrackCache.shouldTrack(username, time.Now())
		if shouldTrack {
			go func() {
				if err := addMinerWithTimeout(func(ctx context.Context) error {
					return c.storage.AddMiner(ctx, username)
				}); err != nil {
					globalMinerTrackCache.forgetIfMatch(username, trackedAt)
					log.Printf("⚠️ Failed to track miner %s: %v", username, err)
				}
			}()
		}
	}

	log.Printf("Miner authorized: %s", username)
	sendResult(c, req.ID, true)
	log.Printf("✅ [%s] authorize accepted user=%s", c.addr, username)
	resendCurrentJobIfReady(c)
}

func resendCurrentJobIfReady(c *client) {
	if currentJobReadyForResend(c) {
		ResendJobToClientClean(c, false)
	}
}

func clientReadyForCurrentJob(c *client) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authorized && c.sessionID != ""
}

func handleExtranonceSubscribe(c *client, req *stratumRequest) {
	c.mu.Lock()
	c.extranonceSubscribed = true
	c.mu.Unlock()

	sendResult(c, req.ID, true)
	log.Printf("[%s] extranonce.subscribe accepted (state=true)", c.addr)
}

// ------------------- Submit Handler (uses stored template) -------------------

func handleSubmit(c *client, req *stratumRequest) error {
	coordinator := c.coordinator
	if coordinator == nil {
		// Unit-level request validation has historically constructed a bare
		// client. It must still reject malformed/unauthorized requests before
		// reaching a candidate side effect; production clients always receive
		// the one shared coordinator during startup.
		coordinator = newSubmissionCoordinator(c.submission, activeSubmissionChainID, activeSubmissionBackendID, c.cfg)
	}
	req.submissionCoordinator = coordinator
	defer func() { req.submissionCoordinator = nil }()
	return coordinator.SubmitCandidate(c, req)
}

func (p *submitPipeline) run() error {
	c := p.client
	req := p.request
	submitStart := p.start
	observeSubmitReadToHandleLatency(req, submitStart)
	lifecycleClassification := submitLifecycleNoJob
	defer func() {
		p.observeLifecycle()
		if shareProcessing != nil {
			shareProcessing.Observe(time.Since(submitStart).Seconds())
		}
	}()

	if !clientReadyForCurrentJob(c) {
		p.setLifecycle(nil, submitLifecycleNoJob, "rejected", "unauthorized")
		if sharesRejected != nil {
			sharesRejected.WithLabelValues("unauthorized").Inc()
		}
		sendError(c, req.ID, 24, "unauthorized")
		return nil
	}

	submitParams, ok := p.params()
	if !ok {
		p.setLifecycle(nil, submitLifecycleNoJob, "rejected", "invalid_params")
		sendError(c, req.ID, -32602, "invalid params")
		return nil
	}

	worker, jobid, extranonce2, ntime, nonce := submitParams.worker, submitParams.jobID, submitParams.extranonce2, submitParams.ntime, submitParams.nonce
	logRoutineSubmit(c, worker, jobid, extranonce2, ntime, nonce)
	submittedJobID := jobid

	resolution := resolveSubmitJob(c, submittedJobID, extranonce2, submitStart)
	jobid = resolution.jobID
	jobData := resolution.job
	lifecycleClassification = resolution.lifecycleClassification
	p.noteResolvedJob(jobData, lifecycleClassification)
	if resolution.reject != nil {
		applySubmitJobReject(c, req, jobid, resolution.reject, p.setLifecycle)
		return nil
	}
	if shouldRejectInvalidatedJobBeforeHash(c, jobData, submitStart) {
		p.setLifecycle(jobData, lifecycleClassification, "rejected", "invalidated_job")
		handleExpiredJobReject(c, jobid)
		sharesRejected.WithLabelValues("invalidated_job").Inc()
		sendError(c, req.ID, 21, "stale job")
		return nil
	}
	if jobTTL > 0 && time.Since(jobData.CreatedAt) > jobTTL {
		forceTemplateRefresh("local TTL expired")
	}

	// Decode extranonce2
	en2Bytes, err := hex.DecodeString(extranonce2)
	if err != nil || len(en2Bytes) != 4 {
		log.Printf("⚠️ [Submit Error] Invalid extranonce2: %s (len=%d, err=%v)", extranonce2, len(en2Bytes), err)
		p.setLifecycle(jobData, lifecycleClassification, "rejected", "invalid_extranonce2")
		sendError(c, req.ID, -32602, "invalid extranonce2")
		return nil
	}

	header := make([]byte, 80)

	// BlockDAG Spec Header Construction (80 bytes)
	// input = bbversion + prevhash + sha256(sha256(extra_nonce1 + extra_nonce2)) + ntime + nbits + nonce
	// ALL FIELDS must match the hex string order (Big Endian equivalent)

	// 1. Version (4 bytes)
	binary.BigEndian.PutUint32(header[0:4], jobData.Version)

	// 2. PrevHash (32 bytes)
	if len(jobData.ParentBytes) == 32 {
		copy(header[4:36], jobData.ParentBytes)
	} else {
		prevBytes, _ := hex.DecodeString(jobData.ParentHash)
		copy(header[4:36], prevBytes)
	}

	// 3. Merkle Root (32 bytes) = dsha256(en1 + en2)
	en1 := jobData.Extranonce1
	if en1 == ([4]byte{}) {
		en1 = submitExtranonce1(c)
	}
	merkleRoot := dummyMerkleRoot(en1[:], en2Bytes)
	copy(header[36:68], merkleRoot[:]) // Direct copy, NO reversal
	// 4. nTime (4 bytes)
	ntimeBytes, err := hex.DecodeString(ntime)
	if err != nil || len(ntimeBytes) != 4 {
		log.Printf("⚠️ [Submit Error] Invalid ntime: %s", ntime)
		p.setLifecycle(jobData, lifecycleClassification, "rejected", "invalid_ntime")
		sendError(c, req.ID, 20, "invalid ntime")
		return nil
	}
	// Direct copy (miner sends BE string matching input)
	copy(header[68:72], ntimeBytes)
	// 5. nBits (4 bytes)
	binary.BigEndian.PutUint32(header[72:76], jobData.NBits)

	// 6. Nonce (4 bytes)
	nonceBytes, err := hex.DecodeString(nonce)
	if err != nil || len(nonceBytes) != 4 {
		log.Printf("⚠️ [Submit Error] Invalid nonce: %s", nonce)
		p.setLifecycle(jobData, lifecycleClassification, "rejected", "invalid_nonce")
		sendError(c, req.ID, 20, "invalid nonce")
		return nil
	}
	// Direct copy
	copy(header[76:80], nonceBytes)

	assignedPdiff := jobData.Difficulty
	prevDiff := jobData.PrevDiff
	if assignedPdiff <= 0 {
		log.Printf("⚠️ [Submit Error] Invalid assigned difficulty: %.6f", assignedPdiff)
		p.setLifecycle(jobData, lifecycleClassification, "rejected", "invalid_assigned_difficulty")
		sendError(c, req.ID, 23, "server error: invalid assigned difficulty")
		return nil
	}

	// Current work can reject duplicate nonce submissions before the scrypt
	// check. Stale work still goes through the existing post-hash path because
	// a stale block candidate may be intentionally submitted under policy.
	currentTip := CurrentParent()
	jobStaleReason := staleShareReason(jobData, currentTip)
	candidateLocalStaleReason := ""
	if jobStaleReason == "" {
		candidateLocalStaleReason = blockCandidateLocalStaleReason(c.cfg, jobData, submitStart, currentTip)
	}
	dupKey := newShareNonceKey(nonceBytes, en2Bytes, ntimeBytes)
	nonceMarked := false
	if jobStaleReason == "" && candidateLocalStaleReason == "" {
		if _, loaded := jobData.usedNonces.LoadOrStore(dupKey, true); loaded {
			log.Printf("⚠️ REJECT duplicate share worker=%s job=%s key=%s", worker, jobid, dupKey)
			p.setLifecycle(jobData, lifecycleClassification, "rejected", "duplicate_share")
			sharesRejected.WithLabelValues("duplicate_share").Inc()
			sendError(c, req.ID, 22, "duplicate share")
			return nil
		}
		nonceMarked = true
	}

	result := scryptHash(header)
	hashInt := HashToBig(&result)
	networkTarget := jobData.NetworkTarget
	if networkTarget == nil {
		networkTarget = bitsToTarget(jobData.NBits)
	}

	shareDiffBF := new(big.Float).Quo(
		new(big.Float).Mul(new(big.Float).SetInt(Diff1TargetPool), big.NewFloat(ShareMultiplier)),
		new(big.Float).SetInt(hashInt),
	)
	shareDiff, _ := shareDiffBF.Float64()

	// Only reject if the job's parent hash doesn't match the current network tip.
	// We ignore sequence lag (template updates on same tip) to minimize stale rejects.
	isBlockCandidate := hashInt.Cmp(networkTarget) <= 0
	var accountingSnapshot map[string]uint64
	var accountingTotalWork uint64
	accountingCutCaptured := false
	if isBlockCandidate && jobStaleReason == "" {
		jobStaleReason = candidateLocalStaleReason
	}

	// ----------------------------------------------------------------
	// Stale job check: never turn stale work into blocks.
	// ----------------------------------------------------------------
	if jobStaleReason != "" {
		if isBlockCandidate {
			lateDecision := evaluateStaleBlockCandidateSubmit(lateBlockCandidateInput{
				cfg:            c.cfg,
				job:            jobData,
				now:            submitStart,
				currentTip:     currentTip,
				jobStaleReason: jobStaleReason,
			})
			applyLateBlockCandidateDecisionEffects(c, jobData, jobid, lateDecision, submitStart)
			switch lateDecision.action {
			case lateBlockCandidateRejectLocal:
				if lateDecision.shareRejectLabel != "" {
					sharesRejected.WithLabelValues(lateDecision.shareRejectLabel).Inc()
				}
				p.setLifecycle(jobData, lifecycleClassification, "rejected-local", lateDecision.reason)
				log.Printf("[STALE JOB] Block candidate rejected locally worker=%s job=%s reason=%s backend=%s job_age=%s template_seq=%d current_seq=%d job_parent=%s current_parent=%s hash=%x",
					worker, jobid, lateDecision.reason, lateDecision.rejectContext.Backend, lateDecision.rejectContext.JobAge.Round(time.Millisecond),
					lateDecision.rejectContext.TemplateSeq, lateDecision.rejectContext.CurrentSeq, lateDecision.rejectContext.JobParent, lateDecision.rejectContext.CurrentParent, result[:])
				sendError(c, req.ID, 21, "stale block candidate")
				return nil
			case lateBlockCandidateSubmitStale:
				log.Printf("[STALE JOB] Block candidate FOUND in stale job - submitting because stale candidate submit is enabled worker=%s job=%s reason=%s hash=%x", worker, jobid, lateDecision.reason, result[:])
			}
		} else if !canAcceptStaleOrdinaryShare(c.cfg, jobData, jobStaleReason, submitStart) {
			handleStaleTemplateRace(c, jobid, jobStaleReason)
			if jobStaleReason == "stale-job" {
				sharesRejected.WithLabelValues("invalidated_job").Inc()
			}
			p.setLifecycle(jobData, lifecycleClassification, "rejected", jobStaleReason)
			sendError(c, req.ID, 21, "stale job")
			return nil
		}
	}

	// ----------------------------------------------------------------
	// Duplicate Share Check (Replay Protection)
	// Key = nonce + extranonce2 + ntime (sufficient uniqueness per job)
	// ----------------------------------------------------------------
	if !nonceMarked {
		if _, loaded := jobData.usedNonces.LoadOrStore(dupKey, true); loaded {
			log.Printf("⚠️ REJECT duplicate share worker=%s job=%s key=%s", worker, jobid, dupKey)
			p.setLifecycle(jobData, lifecycleClassification, "rejected", "duplicate_share")
			sharesRejected.WithLabelValues("duplicate_share").Inc()
			sendError(c, req.ID, 22, "duplicate share")
			return nil
		}
	}

	// --------------------------------------------------------------------
	// 1) VALIDATE SHARE DIFFICULTY FIRST (Before Block Check)
	//    or check if it's a block. Ideally a block is also a valid share.
	// --------------------------------------------------------------------

	// Add 1% tolerance for rounding/precision issues
	threshold := assignedPdiff * 0.99
	isLowDiff := false

	if shareDiff < threshold {
		// Also apply tolerance to previous difficulty check
		prevThreshold := prevDiff * 0.99
		if prevDiff > 0 && shareDiff >= prevThreshold {
			// Treat as valid at previous difficulty (VarDiff transition safety)
			log.Printf("ℹ️ share meets previous diff but not new diff; "+
				"worker=%s job=%s shareDiff≈%.6f prevDiff=%.6f pdiff=%.6f",
				worker, jobid, shareDiff, prevDiff, assignedPdiff)
			assignedPdiff = prevDiff
		} else {
			isLowDiff = true
		}
	}

	// Variables to capture recordShareAndAdjust results
	var diffChanged bool
	var newPdiff float64

	// ----------------------------------------------------------------
	// 2) PUSH TO PPLNS (If valid share)
	// ----------------------------------------------------------------
	// We do this BEFORE the block check return, so the block share itself counts!
	if !isLowDiff {
		now := time.Now()
		resetLowDifficultyRejects(c)
		resetExpiredJobRecoveryState(c)
		// Update VarDiff stats
		diffChanged, newPdiff, _ = c.recordShareAndAdjust(now)

		// Push to PPLNS Window
		if PPLNSWindow != nil {
			if isBlockCandidate {
				accountingSnapshot, accountingTotalWork = PPLNSWindow.PushShareAndSnapshot(c.username, assignedPdiff)
				accountingCutCaptured = true
			} else {
				PPLNSWindow.PushShare(c.username, assignedPdiff)
			}
		}
	}

	// ----------------------------------------------------------------
	// 3) LOW DIFFICULTY CHECK
	// ----------------------------------------------------------------
	if isLowDiff {
		// STRICT: If it didn't meet pool target, we usually reject it.
		// BUT: What if it's a BLOCK?
		// Stratum spec: If hash <= networkTarget, it is a valid block, even if shareDiff < pdiff.
		// (Though practically pdiff is usually << networkTarget, so this is rare/impossible unless diff=1).
		// We will allow block submission even if "low share diff",
		// but we WON'T count it as a PPLNS share if it's below pdiff (to prevent spam).

		if !isBlockCandidate {
			now := time.Now()
			if lowDiffFloodDisconnectEnabled(c) && recordLowDifficultyReject(c, now) {
				sharesRejected.WithLabelValues("low_difficulty_flood").Inc()
				p.setLifecycle(jobData, lifecycleClassification, "rejected", "low_difficulty_flood")
				log.Printf("[LOWDIFF FLOOD] closing miner connection addr=%s worker=%s job=%s pdiff=%.6f", c.addr, worker, jobid, assignedPdiff)
				sendError(c, req.ID, 23, "low difficulty flood")
				closeClientConnection(c)
				return nil
			}
			sharesRejected.WithLabelValues("low_difficulty").Inc()
			p.setLifecycle(jobData, lifecycleClassification, "rejected", "low_difficulty")
			errPayload := []interface{}{23, "low difficulty share", hex.EncodeToString(header), hex.EncodeToString(result[:])}
			resp := stratumResponse{ID: req.ID, Result: nil, Error: errPayload}
			sendJSON(c, resp)
			logLowDifficultyShare(c, worker, jobid, shareDiff, assignedPdiff)
			if changed, recoveredPdiff := c.recordLowDifficultyAndAdjust(now, assignedPdiff); changed {
				log.Printf("[vardiff] %s → recovery pdiff %.8f", c.addr, recoveredPdiff)
				ResendJobToClientClean(c, false)
			}
			return nil
		}
	}

	// ----------------------------------------------------------------
	// 4) BLOCK CHECK
	// ----------------------------------------------------------------
	if isBlockCandidate {
		if PPLNSWindow == nil {
			sharesRejected.WithLabelValues("accounting_unavailable").Inc()
			p.setLifecycle(jobData, lifecycleClassification, "rejected-local", "accounting_unavailable")
			sendError(c, req.ID, 20, "block accounting unavailable")
			return nil
		}
		if !accountingCutCaptured {
			// A network-valid block that is below the assigned pool difficulty is
			// intentionally excluded from PPLNS, preserving the existing policy.
			accountingSnapshot, accountingTotalWork = PPLNSWindow.Snapshot()
			accountingCutCaptured = true
		}
		blockKey := hex.EncodeToString(result[:])
		lateDecision := evaluateDuplicateBlockCandidateSubmit(lateBlockCandidateInput{
			cfg:        c.cfg,
			job:        jobData,
			blockKey:   blockKey,
			now:        submitStart,
			currentTip: currentTip,
			// Once the durable journal is configured, the SQL candidate identity
			// is the submission authority; volatile process dedupe is telemetry
			// only and cannot suppress reconciliation.
			durableSubmission: c.submission != nil,
		})
		applyLateBlockCandidateDecisionEffects(c, jobData, jobid, lateDecision, submitStart)
		switch lateDecision.action {
		case lateBlockCandidateRejectLocal:
			if lateDecision.duplicate {
				if lateDecision.reason == "duplicate-block-job" {
					log.Printf("[DUP BLOCK] duplicate block submission worker=%s job=%s reason=%s backend=%s job_age=%s template_seq=%d current_seq=%d job_parent=%s current_parent=%s hash=%x",
						worker, jobid, lateDecision.reason, lateDecision.rejectContext.Backend, lateDecision.rejectContext.JobAge.Round(time.Millisecond),
						lateDecision.rejectContext.TemplateSeq, lateDecision.rejectContext.CurrentSeq, lateDecision.rejectContext.JobParent, lateDecision.rejectContext.CurrentParent, result[:])
				} else {
					log.Printf("[DUP BLOCK] duplicate block candidate suppressed globally worker=%s job=%s reason=%s backend=%s job_age=%s template_seq=%d current_seq=%d job_parent=%s current_parent=%s hash=%s",
						worker, jobid, lateDecision.reason, lateDecision.rejectContext.Backend, lateDecision.rejectContext.JobAge.Round(time.Millisecond),
						lateDecision.rejectContext.TemplateSeq, lateDecision.rejectContext.CurrentSeq, lateDecision.rejectContext.JobParent, lateDecision.rejectContext.CurrentParent, blockKey)
				}
				disconnectReplay := false
				if lateDecision.duplicateBlockReplayCheck {
					disconnectReplay = recordDuplicateBlockReplay(c, blockKey, jobid, submitStart)
				}
				if lateDecision.shareRejectLabel != "" {
					sharesRejected.WithLabelValues(lateDecision.shareRejectLabel).Inc()
				}
				if disconnectReplay {
					sharesRejected.WithLabelValues("duplicate_block_replay").Inc()
					p.setLifecycle(jobData, lifecycleClassification, "rejected-local", "duplicate_block_replay")
					log.Printf("[DUP BLOCK] closing miner connection after repeated duplicate block replay worker=%s job=%s hash=%s addr=%s", worker, jobid, blockKey, c.addr)
					sendError(c, req.ID, 22, "duplicate block replay")
					closeClientConnection(c)
					return nil
				}
				p.setLifecycle(jobData, lifecycleClassification, "rejected-local", lateDecision.reason)
				sendError(c, req.ID, 22, "duplicate block candidate")
				return nil
			}
		}
		coinbaseBytes, err := jobCoinbaseBytes(jobData, "submit_candidate")
		if err != nil {
			sharesRejected.WithLabelValues("invalid_coinbase").Inc()
			p.setLifecycle(jobData, lifecycleClassification, "rejected-local", "invalid_coinbase")
			log.Printf("[COINBASE] refusing block submit worker=%s job=%s hash=%s: %v", worker, jobid, blockKey, err)
			sendError(c, req.ID, 22, "invalid coinbase")
			return nil
		}
		shareDiffSum.Add(assignedPdiff)
		log.Printf("🎯 BLOCK FOUND height(le)=%d job=%s hash=%x target=%x",
			binary.LittleEndian.Uint64(jobData.Height), jobid, result[:], networkTarget.Bytes())

		full := make([]byte, 144+9+32)
		binary.LittleEndian.PutUint32(full[0:4], jobData.Version)
		copy(full[4:36], header[4:36])
		copy(full[36:68], jobData.TxRoot)
		copy(full[68:100], jobData.StateRoot)
		copy(full[100:132], header[36:68])
		binary.LittleEndian.PutUint32(full[132:136], jobData.NBits)
		ntVal := binary.BigEndian.Uint32(header[68:72])
		binary.LittleEndian.PutUint32(full[136:140], ntVal)
		copy(full[140:148], jobData.Height)
		copy(full[148:168], coinbaseBytes)
		copy(full[168:176], jobData.Reward)
		full[176] = 0x0a
		copy(full[181:185], header[76:80])

		// result/hashInt were already computed from this exact header above.
		// Re-running scrypt here doubles block-candidate hashing latency.

		coordinator := req.submissionCoordinator
		if coordinator == nil {
			coordinator = c.coordinator
			if coordinator == nil {
				sendError(c, req.ID, 20, "block submission pending reconciliation")
				return nil
			}
		}
		durable := coordinator.submitFrozenCandidate(c, jobData, full, en2Bytes, result[:], accountingSnapshot, accountingTotalWork, req.ID)
		if durable.Disposition == storage.PutDispositionInserted && blocksFound != nil {
			blocksFound.Inc()
		}
		if durable.Err != nil {
			durable.Decision = storage.SubmitDecisionPending
			log.Printf("[SUBMIT] durable candidate finalization unavailable hash=%s: %v", blockKey, durable.Err)
		}
		if durable.AccountingErr != nil {
			log.Printf("[ACCOUNTING] accepted receipt remains pending materialization candidate=%x: %v", durable.Candidate.CandidateID, durable.AccountingErr)
		}
		reason := safeDecisionCodeV1(durable.Observation)
		switch durable.Transition.State {
		case storage.SubmissionStateAcceptedFinal:
			if blockSubmitOutcomes != nil {
				blockSubmitOutcomes.WithLabelValues("accepted", reason).Inc()
			}
			if durable.Transition.FirstAccepted {
				recordSubmitRecoveryAccepted()
				clearTemplatePressure()
			}
			log.Printf("✅ Block submission finalized candidate=%x first_accepted=%t", durable.Candidate.CandidateID, durable.Transition.FirstAccepted)
			p.setLifecycle(jobData, lifecycleClassification, "accepted-block", reason)
		case storage.SubmissionStateRejectedFinal:
			if blockSubmitOutcomes != nil {
				blockSubmitOutcomes.WithLabelValues("rejected", reason).Inc()
			}
			if blocksRejectedByNode != nil {
				blocksRejectedByNode.WithLabelValues(reason).Inc()
			}
			recordSubmitRecoveryFailure(reason, jobData)
			p.setLifecycle(jobData, lifecycleClassification, "rejected-node", reason)
		case storage.SubmissionStateConflict:
			if blockSubmitOutcomes != nil {
				blockSubmitOutcomes.WithLabelValues("conflict", reason).Inc()
			}
			p.setLifecycle(jobData, lifecycleClassification, "rejected-node", "submission_conflict")
		default:
			if blockSubmitOutcomes != nil {
				blockSubmitOutcomes.WithLabelValues("pending", reason).Inc()
			}
			recordSubmitRecoveryFailure("pending_reconciliation", jobData)
			p.setLifecycle(jobData, lifecycleClassification, "pending", "pending_reconciliation")
		}
		coordinator.respondToSubmissionDecision(c, req, durable)
		return nil
	}

	// --------------------------------------------------------------------
	// 5) VALID SHARE RESPONSE (If not a block)
	// --------------------------------------------------------------------
	// We already recorded the share above.

	sharesAccepted.Inc()
	shareDiffSum.Add(assignedPdiff)
	logValidShare(c, worker, jobid, shareDiff)

	sendSubmitAccepted(c, req.ID, header, result[:])
	p.setLifecycle(jobData, lifecycleClassification, "accepted-share", "ok")

	// Push new difficulty immediately if it changed
	// (Note: we updated diff inside the 'if !isLowDiff' block via recordShareAndAdjust, but didn't capture return values.
	//  We need those return values! Refactoring to capture them.)
	if diffChanged {
		log.Printf("[vardiff] %s → pdiff %.8f", c.addr, newPdiff)
		ResendJobToClientClean(c, false)
	}
	return nil
}

func sendSubmitAccepted(c *client, id interface{}, header, result []byte) {
	var cfg *Config
	if c != nil {
		cfg = c.cfg
	}
	if cfg == nil || !cfg.SubmitDebugResultEnabled {
		_ = sendCompactSubmitAccepted(c, id)
		return
	}
	sendResult(c, id, submitAcceptedResult(cfg, header, result))
}

func submitAcceptedResult(cfg *Config, header, result []byte) interface{} {
	if cfg != nil && cfg.SubmitDebugResultEnabled {
		return []interface{}{true, hex.EncodeToString(header), hex.EncodeToString(result)}
	}
	return true
}

func applyLateBlockCandidateDecisionEffects(c *client, job *JobData, jobID string, decision lateBlockCandidateDecision, now time.Time) {
	var cfg *Config
	if c != nil {
		cfg = c.cfg
	}
	if decision.recordAgeFeedback {
		recordBlockCandidateAgeFeedback(cfg, decision.reason, decision.rejectContext.JobAge, now)
	}
	if decision.recordLocalRejectAge {
		observeLocalBlockCandidateReject(decision.reason, decision.rejectContext)
	}
	if decision.incrementStaleLocalDrop && staleBlocksLocalDrop != nil {
		staleBlocksLocalDrop.Inc()
	}
	if decision.incrementDuplicateDrop && duplicateBlocksLocalDrop != nil {
		duplicateBlocksLocalDrop.Inc()
	}
	if decision.blockSubmitOutcomeReason != "" && blockSubmitOutcomes != nil {
		blockSubmitOutcomes.WithLabelValues("rejected-local", decision.blockSubmitOutcomeReason).Inc()
	}
	if decision.recordSubmitRecovery {
		recordSubmitRecoveryFailure(decision.reason, job)
	}
	if decision.recordTemplatePressure {
		recordTemplatePressure(decision.reason)
	}
	if decision.forceCleanRefreshReason != "" {
		if strings.HasPrefix(decision.forceCleanRefreshReason, "stale block candidate") {
			requestCleanRefreshAfterStaleCandidate(job, decision.forceCleanRefreshReason, decision.reason, now)
		} else {
			forceCleanTemplateRefreshThrottled(decision.forceCleanRefreshReason)
		}
	}
	if decision.handleStaleTemplateRace && c != nil && job != nil {
		handleStaleTemplateRace(c, jobID, decision.reason)
	}
}

type blockCandidateRejectContext struct {
	Backend       string
	JobAge        time.Duration
	TemplateSeq   uint64
	CurrentSeq    uint64
	JobParent     string
	CurrentParent string
}

func newBlockCandidateRejectContext(job *JobData, now time.Time, currentParent string) blockCandidateRejectContext {
	ctx := blockCandidateRejectContext{
		Backend:       "unknown",
		CurrentSeq:    miningWork.currentTemplateSeq(),
		CurrentParent: currentParent,
	}
	if job == nil {
		return ctx
	}
	if job.BackendName != "" {
		ctx.Backend = job.BackendName
	}
	if !job.CreatedAt.IsZero() {
		ctx.JobAge = now.Sub(job.CreatedAt)
		if ctx.JobAge < 0 {
			ctx.JobAge = 0
		}
	}
	ctx.TemplateSeq = job.TemplateSeq
	ctx.JobParent = job.ParentHash
	return ctx
}

func observeLocalBlockCandidateReject(reason string, ctx blockCandidateRejectContext) {
	if blockCandidateRejectJobAge == nil {
		return
	}
	if reason == "" {
		reason = "unknown"
	}
	backend := ctx.Backend
	if backend == "" {
		backend = "unknown"
	}
	blockCandidateRejectJobAge.WithLabelValues(reason, backend).Observe(ctx.JobAge.Seconds())
}

func shouldSubmitStaleBlockCandidate(cfg *Config) bool {
	return cfg == nil || !cfg.RejectStaleBlockCandidates
}

func shouldSubmitRecentStaleBlockCandidate(cfg *Config, job *JobData, reason string, ctx blockCandidateRejectContext, now time.Time) bool {
	if cfg == nil || ctx.JobAge <= 0 {
		return false
	}
	grace := currentRecentStaleBlockCandidateSubmitGrace(cfg)
	if grace <= 0 {
		return false
	}
	switch reason {
	case "old-template-sequence":
		if normalizeHex(ctx.JobParent) == "" || normalizeHex(ctx.JobParent) != normalizeHex(ctx.CurrentParent) {
			return false
		}
		return ctx.JobAge <= grace
	case "stale-job":
		if job == nil {
			return false
		}
		invalidatedAt := jobInvalidatedAt(job)
		if invalidatedAt.IsZero() {
			return false
		}
		if now.IsZero() {
			now = time.Now()
		}
		invalidatedAge := now.Sub(invalidatedAt)
		if invalidatedAge < 0 {
			invalidatedAge = 0
		}
		return invalidatedAge <= grace
	default:
		return false
	}
}

func shouldUseGlobalBlockDedupe(cfg *Config) bool {
	return cfg != nil && cfg.GlobalBlockDedupeEnabled
}

func shouldRequireCurrentJobSubmit(cfg *Config) bool {
	return cfg != nil && cfg.StrictCurrentJobSubmit
}

func markBlockCandidateForSubmit(cfg *Config, job *JobData, blockKey string, now time.Time) string {
	if !shouldAllowMultipleBlockCandidatesPerJob(cfg) && job != nil && !job.blockSubmitted.CompareAndSwap(false, true) {
		return "duplicate-block-job"
	}
	if shouldUseGlobalBlockDedupe(cfg) && !markGlobalBlockCandidate(blockKey, now) {
		return "duplicate-block-global"
	}
	if job != nil {
		job.blockSubmitted.Store(true)
	}
	return ""
}

func staleShareReason(job *JobData, currentTip string) string {
	if job == nil {
		return ""
	}
	if jobInvalidatedByEpoch(job) || job.stale.Load() || job.invalidated.Load() {
		return "stale-job"
	}
	if currentTip != "" && job.ParentHash != "" && job.ParentHash != currentTip {
		return "stale-parent"
	}
	return ""
}

func advanceCurrentParentAfterAcceptedBlock(job *JobData, result interface{}) bool {
	parent := acceptedSubmitBlockHash(result)
	if parent == "" {
		return false
	}
	jobParent := ""
	if job != nil {
		jobParent = normalizeHex(job.ParentHash)
	}
	currentParent := normalizeHex(CurrentParent())
	if jobParent != "" && currentParent != "" && currentParent != jobParent && currentParent != parent {
		logTemplateRefresh("accepted block parent advance skipped: current parent already superseded", miningWork.currentTemplateSeq(), currentParent)
		return false
	}
	if miningWork.rememberParent(parent) {
		logTemplateRefresh("accepted block parent advance", miningWork.currentTemplateSeq(), parent)
		return true
	}
	return false
}

func acceptedSubmitBlockHash(result interface{}) string {
	fields, ok := result.(map[string]interface{})
	if !ok {
		return ""
	}
	raw, ok := fields["block_hash"]
	if !ok {
		raw, ok = fields["blockHash"]
	}
	if !ok {
		return ""
	}
	hash, ok := raw.(string)
	if !ok {
		return ""
	}
	hash = normalizeHex(hash)
	if len(hash) != 64 || !isHexString(hash) {
		return ""
	}
	return hash
}

func blockCandidateLocalStaleReason(cfg *Config, job *JobData, now time.Time, currentTip string) string {
	if reason := staleShareReason(job, currentTip); reason != "" {
		return reason
	}
	maxAge := maxBlockCandidateJobAge(cfg)
	if maxAge <= 0 || job == nil || job.CreatedAt.IsZero() || now.Sub(job.CreatedAt) <= maxAge {
		return ""
	}
	currentSeq := miningWork.currentTemplateSeq()
	if currentSeq > 0 && job.TemplateSeq > 0 && currentSeq > job.TemplateSeq {
		return "old-template-sequence"
	}
	return ""
}

func requestCleanRefreshAfterStaleSubmit(job *JobData, reason string, now time.Time) bool {
	if active, activeReason := supersededCandidateRefreshInFlight(job); active {
		logTemplateRefresh(
			fmt.Sprintf("stale submit already superseded: %s refresh_in_flight=%s", reason, activeReason),
			miningWork.currentTemplateSeq(),
			CurrentParent(),
		)
		return false
	}
	return forceCleanTemplateRefreshThrottled("block submit stale error: " + reason)
}

func requestCleanRefreshAfterAcceptedSubmit(job *JobData, now time.Time) bool {
	forceCleanTemplateRefresh("block submission accepted")
	return true
}

func requestCleanRefreshAfterStaleCandidate(job *JobData, refreshReason, reason string, now time.Time) bool {
	if active, activeReason := supersededCandidateRefreshInFlight(job); active {
		logTemplateRefresh(
			fmt.Sprintf("stale candidate already superseded: %s refresh_in_flight=%s", reason, activeReason),
			miningWork.currentTemplateSeq(),
			CurrentParent(),
		)
		return false
	}
	return forceCleanTemplateRefreshThrottled(refreshReason)
}

func supersededCandidateRefreshInFlight(job *JobData) (bool, string) {
	if !candidateParentSuperseded(job) {
		return false, ""
	}
	req, active := activeCleanTemplateRefreshRequest()
	if !active {
		return false, ""
	}
	reason := req.reason
	if reason == "" {
		reason = "clean template refresh"
	}
	return true, reason
}

func candidateParentSuperseded(job *JobData) bool {
	if job == nil {
		return false
	}
	jobParent := normalizeHex(job.ParentHash)
	currentParent := normalizeHex(CurrentParent())
	return jobParent != "" && currentParent != "" && jobParent != currentParent
}

func markGlobalBlockCandidate(key string, now time.Time) bool {
	if key == "" {
		return true
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !miningWork.markBlockCandidate(key, now, submittedBlockMarkTTL) {
		return false
	}
	maybeCleanupSubmittedBlockMarks(now)
	return true
}

func maybeCleanupSubmittedBlockMarks(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	miningWork.maybeCleanupBlockCandidateMarks(now, submittedBlockMarkCleanupInterval, submittedBlockMarkTTL)
}

func cleanupSubmittedBlockMarks(now time.Time) {
	miningWork.cleanupBlockCandidateMarks(now, submittedBlockMarkTTL)
}

// ShareTargetFromDiff computes the share target for a given pool difficulty.
//
// For diff >= 1: target = powLimit / diff
// For diff < 1:  target > powLimit (easier shares), which is OK for Stratum.
func ShareTargetFromDiff(Diff1TargetPool *big.Int, diff float64) *big.Int {
	fLimit := new(big.Float).SetInt(Diff1TargetPool)
	fDiff := big.NewFloat(diff)
	fT := new(big.Float).Quo(fLimit, fDiff)

	t := new(big.Int)
	fT.Int(t) // floor
	return t
}

func TargetToLittleEndianHex(target *big.Int) string {
	if target == nil {
		return strings.Repeat("00", 32)
	}
	if target.Sign() < 0 {
		target = new(big.Int)
	}
	bytes := target.Bytes()
	if len(bytes) > 32 {
		bytes = bytes[len(bytes)-32:]
	}
	buf := make([]byte, 32)
	copy(buf[32-len(bytes):], bytes)
	for i := 0; i < len(buf)/2; i++ {
		buf[i], buf[len(buf)-1-i] = buf[len(buf)-1-i], buf[i]
	}
	return hex.EncodeToString(buf)
}

func pushDifficultyUpdate(c *client, pdiff float64) error {
	if shouldLogDifficultyPush(c, pdiff, time.Now()) {
		log.Printf("PUSHDIF -> %s mining.set_difficulty %.8f", c.addr, pdiff)
	}
	if err := sendPush(c, "mining.set_difficulty", []interface{}{pdiff}); err != nil {
		log.Printf("[ERROR]Error setting difficulty %v", err)
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return err
	}
	return nil
}

func shouldLogDifficultyPush(c *client, pdiff float64, now time.Time) bool {
	if c == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	if c.lastDiffPushLogAt.IsZero() ||
		!almostEqual(c.lastDiffPushLogValue, pdiff) ||
		now.Sub(c.lastDiffPushLogAt) >= difficultyPushLogInterval() {
		c.lastDiffPushLogAt = now
		c.lastDiffPushLogValue = pdiff
		return true
	}
	return false
}

func difficultyPushLogInterval() time.Duration {
	raw := os.Getenv("POOL_DIFFICULTY_PUSH_LOG_INTERVAL_SECONDS")
	if raw == "" {
		return defaultDifficultyPushLogInterval
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return defaultDifficultyPushLogInterval
	}
	return time.Duration(seconds) * time.Second
}

func logRoutineSubmit(c *client, worker, jobid, extranonce2, ntime, nonce string) {
	shouldLog, suppressed := shouldLogRoutineSubmit(c, time.Now())
	if !shouldLog {
		return
	}
	log.Printf("submit from worker=%s job=%s extranonce2=%s ntime=%s nonce=%s suppressed=%d",
		worker, jobid, extranonce2, ntime, nonce, suppressed)
}

func shouldLogRoutineSubmit(c *client, now time.Time) (bool, int) {
	if c == nil {
		return true, 0
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	c.routineSubmitLogSuppressed++
	if c.lastRoutineSubmitLogAt.IsZero() || now.Sub(c.lastRoutineSubmitLogAt) >= routineSubmitLogInterval {
		suppressed := c.routineSubmitLogSuppressed - 1
		c.routineSubmitLogSuppressed = 0
		c.lastRoutineSubmitLogAt = now
		return true, suppressed
	}
	return false, 0
}

type validShareLogSummary struct {
	ShouldLog      bool
	Suppressed     int
	SuppressedDiff float64
	SuppressedWork float64
}

func logValidShare(c *client, worker, jobid string, shareDiff float64) {
	currentWork := shareDiff * 65536
	summary := shouldLogValidShare(c, time.Now(), shareDiff, currentWork)
	if !summary.ShouldLog {
		return
	}
	log.Printf("✅ valid share accepted %.6f → %.0f  worker=%s  job=%s suppressed=%d suppressedDiff=%.6f suppressedWork=%.0f",
		shareDiff, currentWork, worker, jobid, summary.Suppressed, summary.SuppressedDiff, summary.SuppressedWork)
}

func shouldLogValidShare(c *client, now time.Time, shareDiff, shareWork float64) validShareLogSummary {
	if c == nil {
		return validShareLogSummary{ShouldLog: true}
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	c.validShareLogSuppressed++
	c.validShareLogDiffSum += shareDiff
	c.validShareLogWorkSum += shareWork
	if c.lastValidShareLogAt.IsZero() || now.Sub(c.lastValidShareLogAt) >= validShareLogInterval {
		suppressed := c.validShareLogSuppressed - 1
		suppressedDiff := c.validShareLogDiffSum - shareDiff
		suppressedWork := c.validShareLogWorkSum - shareWork
		c.validShareLogSuppressed = 0
		c.validShareLogDiffSum = 0
		c.validShareLogWorkSum = 0
		c.lastValidShareLogAt = now
		return validShareLogSummary{
			ShouldLog:      true,
			Suppressed:     suppressed,
			SuppressedDiff: suppressedDiff,
			SuppressedWork: suppressedWork,
		}
	}
	return validShareLogSummary{}
}

func logLowDifficultyShare(c *client, worker, jobid string, shareDiff, assignedPdiff float64) {
	shouldLog, suppressed := shouldLogLowDifficultyShare(c, time.Now())
	if !shouldLog {
		return
	}
	log.Printf("❌ low difficulty share worker=%s job=%s shareDiff≈%.6f pdiff=%.6f suppressed=%d",
		worker, jobid, shareDiff, assignedPdiff, suppressed)
}

func shouldLogLowDifficultyShare(c *client, now time.Time) (bool, int) {
	if c == nil {
		return true, 0
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	c.lowDiffLogSuppressed++
	if c.lastLowDiffLogAt.IsZero() || now.Sub(c.lastLowDiffLogAt) >= lowDiffShareLogInterval {
		suppressed := c.lowDiffLogSuppressed - 1
		c.lowDiffLogSuppressed = 0
		c.lastLowDiffLogAt = now
		return true, suppressed
	}
	return false, 0
}

func resetLowDifficultyRejects(c *client) {
	if c == nil {
		return
	}
	c.diffMu.Lock()
	c.lowDiffWindowStart = time.Time{}
	c.lowDiffRejects = 0
	c.lowDiffRecoveryWindowStart = time.Time{}
	c.lowDiffRecoveryRejects = 0
	c.diffMu.Unlock()
}

func recordLowDifficultyReject(c *client, now time.Time) bool {
	if c == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.diffMu.Lock()
	defer c.diffMu.Unlock()

	if c.lowDiffWindowStart.IsZero() || now.Sub(c.lowDiffWindowStart) > lowDiffRejectWindow(c) {
		c.lowDiffWindowStart = now
		c.lowDiffRejects = 1
		return false
	}
	c.lowDiffRejects++
	return c.lowDiffRejects >= lowDiffRejectDisconnectThreshold(c)
}

func lowDiffFloodDisconnectEnabled(c *client) bool {
	if c == nil || c.cfg == nil {
		return true
	}
	return !c.cfg.DisableLowDiffFloodDisconnect
}

func lowDiffRejectWindow(c *client) time.Duration {
	if c != nil && c.cfg != nil && c.cfg.LowDiffRejectWindowSeconds > 0 {
		return time.Duration(c.cfg.LowDiffRejectWindowSeconds) * time.Second
	}
	return defaultLowDiffRejectWindow
}

func lowDiffRejectDisconnectThreshold(c *client) int {
	if c != nil && c.cfg != nil && c.cfg.LowDiffRejectDisconnectThreshold > 0 {
		return c.cfg.LowDiffRejectDisconnectThreshold
	}
	return defaultLowDiffRejectDisconnectThreshold
}

func closeClientConnection(c *client) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func recordDuplicateBlockReplay(c *client, blockHash, jobID string, now time.Time) bool {
	if c == nil || blockHash == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}

	c.duplicateReplayMu.Lock()
	defer c.duplicateReplayMu.Unlock()

	if c.duplicateReplayWindowStart.IsZero() ||
		now.Sub(c.duplicateReplayWindowStart) > defaultDuplicateBlockReplayWindow ||
		c.lastDuplicateBlockHash != blockHash {
		c.duplicateReplayWindowStart = now
		c.lastDuplicateBlockHash = blockHash
		c.lastDuplicateBlockJobID = jobID
		c.duplicateReplayCount = 1
		return false
	}

	if jobID != "" && jobID != c.lastDuplicateBlockJobID {
		c.lastDuplicateBlockJobID = jobID
	}
	c.duplicateReplayCount++
	return c.duplicateReplayCount >= defaultDuplicateBlockReplayDisconnectThreshold
}

// targetToCompact packs a full target (*big.Int) into Bitcoin-style compact uint32
func targetToCompact(target *big.Int) uint32 {
	if target.Sign() == 0 {
		return 0
	}
	tb := target.Bytes() // big-endian bytes
	size := len(tb)
	var compact uint32
	if size <= 3 {
		var mantissa uint32
		for i := 0; i < size; i++ {
			mantissa <<= 8
			mantissa |= uint32(tb[i])
		}
		mantissa <<= 8 * (3 - size)
		compact = uint32(mantissa) | uint32(size<<24)
	} else {
		mantissa := uint32(tb[0])<<16 | uint32(tb[1])<<8 | uint32(tb[2])
		compact = uint32(mantissa) | uint32(size<<24)
		if (mantissa & 0x00800000) != 0 {
			mantissa >>= 8
			size++
			compact = uint32(mantissa) | uint32(size<<24)
		}
	}
	return compact
}

// TargetToCompactHex returns 8-char hex string of compact bits (nBits)
func TargetToCompactHex(target *big.Int) string {
	bits := targetToCompact(target)
	return fixedHexUint32(bits)
}

func GetPowLimit() *big.Int {
	// For SHARE difficulty math we want the diff baseline,
	// not your chain's consensus powLimit.
	return Diff1TargetPool
}
