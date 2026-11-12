package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolJobHealthSnapshotHealthy(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	job := testHealthJob(now.Add(-time.Second), "parent")
	c := testHealthClient("198.51.100.114:4000", "session-1", job)
	c.username = "0x4BE5B1a9100cf96Dea7CCA4109051ba015d56580"
	setPoolHealthClients(c)
	acceptedJobs.Store("job-1", job)

	snapshot := snapshotPoolJobHealth(now, true)
	if snapshot.Status != "ok" || snapshot.ReasonCode != poolJobHealthOK {
		t.Fatalf("snapshot verdict = %s/%s, want ok/ok: %#v", snapshot.Status, snapshot.ReasonCode, snapshot)
	}
	if snapshot.ActiveConnections != 1 || snapshot.AuthorizedConnections != 1 || snapshot.ReadyConnections != 1 {
		t.Fatalf("unexpected connection counts: %#v", snapshot)
	}
	if snapshot.AcceptedJobs.Total != 1 {
		t.Fatalf("accepted job total = %d, want 1", snapshot.AcceptedJobs.Total)
	}
	if snapshot.IdentityStatus != "warning" || snapshot.IdentityReasonCode != poolJobIdentityNoPhysicalMiners {
		t.Fatalf("identity verdict = %s/%s, want warning/%s", snapshot.IdentityStatus, snapshot.IdentityReasonCode, poolJobIdentityNoPhysicalMiners)
	}
	if len(snapshot.Clients) != 1 || !snapshot.Clients[0].Ready || snapshot.Clients[0].CurrentJobID != "job-1" {
		t.Fatalf("unexpected client snapshot: %#v", snapshot.Clients)
	}
	if snapshot.Clients[0].AuthorizedWorker != c.username {
		t.Fatalf("authorized worker = %q, want %q", snapshot.Clients[0].AuthorizedWorker, c.username)
	}
	if snapshot.Clients[0].LaneID != "ip:198.51.100.114" || snapshot.Clients[0].LaneIdentitySource != "ip" {
		t.Fatalf("unexpected default lane identity: %#v", snapshot.Clients[0])
	}
}

func TestPoolJobHealthSnapshotDoesNotExposeUnverifiedWorker(t *testing.T) {
	now := time.Unix(1000, 0)
	c := testHealthClient("198.51.100.115:4000", "session-1", testHealthJob(now.Add(-time.Second), "parent"))
	c.authorized = false
	c.username = "unverified-worker"

	snapshot := snapshotClientJobHealth(c, now, "parent")
	if snapshot.AuthorizedWorker != "" {
		t.Fatalf("authorized worker = %q, want empty for unauthorized client", snapshot.AuthorizedWorker)
	}
}

func TestPoolJobHealthSnapshotDoesNotUseIPLaneForConfiguredASICLAN(t *testing.T) {
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24")
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	job := testHealthJob(now.Add(-time.Second), "parent")
	c := testHealthClient("192.168.1.114:4000", "session-1", job)
	setPoolHealthClients(c)
	acceptedJobs.Store("job-1", job)

	snapshot := snapshotPoolJobHealth(now, true)
	if len(snapshot.Clients) != 1 {
		t.Fatalf("unexpected client snapshot: %#v", snapshot.Clients)
	}
	if snapshot.Clients[0].LaneID != "unknown" || snapshot.Clients[0].LaneIdentitySource != "unknown" {
		t.Fatalf("configured ASIC LAN without MAC should not use IP lane: %#v", snapshot.Clients[0])
	}
}

func TestPoolJobHealthSnapshotDetectsClientJobProblems(t *testing.T) {
	now := time.Unix(1000, 0)
	tests := []struct {
		name       string
		mutateJob  func(*JobData)
		wantReason string
	}{
		{
			name:       "missing current job",
			mutateJob:  nil,
			wantReason: poolJobHealthMinersWithoutCurrentJob,
		},
		{
			name: "stale current job",
			mutateJob: func(job *JobData) {
				job.stale.Store(true)
			},
			wantReason: poolJobHealthStaleCurrentJob,
		},
		{
			name: "invalidated current job",
			mutateJob: func(job *JobData) {
				job.invalidated.Store(true)
			},
			wantReason: poolJobHealthInvalidatedCurrentJob,
		},
		{
			name: "expired current job",
			mutateJob: func(job *JobData) {
				job.CreatedAt = now.Add(-jobTTL - time.Second)
			},
			wantReason: poolJobHealthExpiredCurrentJob,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := isolatePoolJobHealthGlobals(t, now, "parent")
			defer restore()

			c := &client{addr: "192.168.1.115:4000", authorized: true, sessionID: "session-1"}
			if test.mutateJob != nil {
				job := testHealthJob(now.Add(-time.Second), "parent")
				test.mutateJob(job)
				c.currentJob.Store(&clientCurrentJob{id: "job-1", job: job})
				acceptedJobs.Store("job-1", job)
			}
			setPoolHealthClients(c)

			snapshot := snapshotPoolJobHealth(now, true)
			if snapshot.Status != "degraded" || snapshot.ReasonCode != test.wantReason {
				t.Fatalf("snapshot verdict = %s/%s, want degraded/%s: %#v", snapshot.Status, snapshot.ReasonCode, test.wantReason, snapshot)
			}
			if len(snapshot.Clients) != 1 || snapshot.Clients[0].ReasonCode != test.wantReason || snapshot.Clients[0].Ready {
				t.Fatalf("unexpected client snapshot: %#v", snapshot.Clients)
			}
		})
	}
}

func TestPoolJobHealthSnapshotNormalizesCurrentParentBeforeStaleCheck(t *testing.T) {
	now := time.Unix(1000, 0)
	currentParent := "abcdef1234567890"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	if staleCurrentJobParent("0xABCDEF1234567890", currentParent) {
		t.Fatal("same parent with 0x prefix and uppercase hex must not be stale")
	}
	if !staleCurrentJobParent("0x1111111234567890", currentParent) {
		t.Fatal("different normalized parent must be stale")
	}

	job := testHealthJob(now.Add(-time.Second), "0xABCDEF1234567890")
	c := testHealthClient("198.51.100.114:4000", "session-1", job)
	setPoolHealthClients(c)

	snapshot := snapshotPoolJobHealth(now, true)
	if snapshot.Status != "ok" || snapshot.ReasonCode != poolJobHealthOK {
		t.Fatalf("snapshot verdict = %s/%s, want ok/ok for same normalized parent: %#v", snapshot.Status, snapshot.ReasonCode, snapshot)
	}
	if len(snapshot.Clients) != 1 || !snapshot.Clients[0].Ready || snapshot.Clients[0].ReasonCode != poolJobHealthOK {
		t.Fatalf("client should be ready with same normalized parent: %#v", snapshot.Clients)
	}
}

func TestPoolJobHealthSnapshotKeepsFreshPublishedJobsReadyDuringParentTransition(t *testing.T) {
	now := time.Now()
	oldParent := strings.Repeat("a", 64)
	newParent := strings.Repeat("b", 64)
	restore := isolatePoolJobHealthGlobals(t, now, oldParent)
	defer restore()

	job := testHealthJob(now.Add(-time.Second), oldParent)
	miningWork.rememberLatestBroadcast([]interface{}{"job-1"}, job, job.TemplateSeq)
	c := testHealthClient("192.168.1.112:4000", "session-1", job)
	c.asicMAC = "2a:71:c7:f5:1f:1e"
	setPoolHealthClients(c)

	if !miningWork.rememberParent(newParent) {
		t.Fatal("expected strict submission parent to advance")
	}
	snapshot := snapshotPoolJobHealth(time.Now(), true)
	if !snapshot.ParentTransition || snapshot.PublishedParent != oldParent || snapshot.CurrentParent != newParent {
		t.Fatalf("unexpected transition metadata: %#v", snapshot)
	}
	if snapshot.ReadinessParent != oldParent {
		t.Fatalf("readiness parent = %q, want published parent %q", snapshot.ReadinessParent, oldParent)
	}
	if snapshot.Status != "ok" || snapshot.ReadyConnections != 1 || snapshot.ReadyPhysicalMiners != 1 {
		t.Fatalf("fresh publication transition must retain productive readiness: %#v", snapshot)
	}
}

func TestPoolJobHealthSnapshotDegradesWhenParentTransitionStalls(t *testing.T) {
	now := time.Now()
	oldParent := strings.Repeat("c", 64)
	newParent := strings.Repeat("d", 64)
	restore := isolatePoolJobHealthGlobals(t, now, oldParent)
	defer restore()

	job := testHealthJob(now.Add(-time.Second), oldParent)
	miningWork.rememberLatestBroadcast([]interface{}{"job-1"}, job, job.TemplateSeq)
	c := testHealthClient("192.168.1.112:4000", "session-1", job)
	c.asicMAC = "2a:71:c7:f5:1f:1e"
	setPoolHealthClients(c)

	if !miningWork.rememberParent(newParent) {
		t.Fatal("expected strict submission parent to advance")
	}
	currentParentChangedUnixNano.Store(now.Add(-poolJobParentTransitionGrace - time.Millisecond).UnixNano())
	snapshot := snapshotPoolJobHealth(now, true)
	if !snapshot.ParentTransition || snapshot.ReadinessParent != newParent {
		t.Fatalf("stalled transition must use strict parent for readiness: %#v", snapshot)
	}
	if snapshot.Status != "degraded" || snapshot.ReasonCode != poolJobHealthStaleCurrentJob || snapshot.StaleCurrentJobConnections != 1 {
		t.Fatalf("stalled transition verdict = %s/%s stale=%d, want degraded/%s/1: %#v",
			snapshot.Status, snapshot.ReasonCode, snapshot.StaleCurrentJobConnections, poolJobHealthStaleCurrentJob, snapshot)
	}
}

func TestPoolJobHealthSnapshotWaitsForAtomicTemplatePublication(t *testing.T) {
	now := time.Now()
	oldParent := strings.Repeat("e", 64)
	newParent := strings.Repeat("f", 64)
	restore := isolatePoolJobHealthGlobals(t, now, oldParent)
	defer restore()

	oldJob := testHealthJob(now.Add(-time.Second), oldParent)
	miningWork.rememberLatestBroadcast([]interface{}{"old-job"}, oldJob, oldJob.TemplateSeq)
	c := testHealthClient("192.168.1.112:4000", "session-1", oldJob)
	c.asicMAC = "2a:71:c7:f5:1f:1e"
	setPoolHealthClients(c)

	jobHealthPublicationMu.Lock()
	locked := true
	defer func() {
		if locked {
			jobHealthPublicationMu.Unlock()
		}
	}()

	started := make(chan struct{})
	result := make(chan poolJobHealthSnapshot, 1)
	go func() {
		close(started)
		result <- snapshotPoolJobHealth(time.Now(), true)
	}()
	<-started
	select {
	case snapshot := <-result:
		t.Fatalf("health snapshot escaped an in-progress publication: %#v", snapshot)
	case <-time.After(20 * time.Millisecond):
	}

	newJob := testHealthJob(time.Now(), newParent)
	newJob.TemplateSeq = oldJob.TemplateSeq + 1
	miningWork.rememberParent(newParent)
	miningWork.rememberLatestBroadcast([]interface{}{"new-job"}, newJob, newJob.TemplateSeq)
	c.currentJob.Store(&clientCurrentJob{id: "new-job", job: newJob})
	jobHealthPublicationMu.Unlock()
	locked = false

	select {
	case snapshot := <-result:
		if snapshot.Status != "ok" || snapshot.CurrentTemplateSeq != newJob.TemplateSeq || snapshot.PublishedParent != newParent || snapshot.ReadyConnections != 1 {
			t.Fatalf("snapshot did not observe the complete publication: %#v", snapshot)
		}
	case <-time.After(time.Second):
		t.Fatal("health snapshot did not resume after publication completed")
	}
}

func TestPoolJobHealthSnapshotSortsClients(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	firstJob := testHealthJob(now.Add(-time.Second), "parent")
	secondJob := testHealthJob(now.Add(-time.Second), "parent")
	setPoolHealthClients(
		testHealthClient("192.168.1.120:4000", "session-2", secondJob),
		testHealthClient("192.168.1.011:4000", "session-1", firstJob),
	)

	snapshot := snapshotPoolJobHealth(now, true)
	if len(snapshot.Clients) != 2 {
		t.Fatalf("client count = %d, want 2", len(snapshot.Clients))
	}
	if snapshot.Clients[0].Address != "192.168.1.011:4000" || snapshot.Clients[1].Address != "192.168.1.120:4000" {
		t.Fatalf("clients not sorted: %#v", snapshot.Clients)
	}
}

func TestPoolJobHealthSnapshotIncludesMACLaneIdentity(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	job := testHealthJob(now.Add(-time.Second), "parent")
	c := testHealthClient("192.168.1.105:4000", "session-1", job)
	c.remoteHost = "192.168.1.105"
	c.asicMAC = "28:E2:97:4D:44:3A"
	c.laneID = clientLaneIdentity(c.addr, c.asicMAC)
	setPoolHealthClients(c)

	snapshot := snapshotPoolJobHealth(now, true)
	if len(snapshot.Clients) != 1 {
		t.Fatalf("client count = %d, want 1", len(snapshot.Clients))
	}
	client := snapshot.Clients[0]
	if client.RemoteHost != "192.168.1.105" || client.ASICMAC != "28:e2:97:4d:44:3a" {
		t.Fatalf("unexpected hardware identity: %#v", client)
	}
	if client.LaneID != "mac:28:e2:97:4d:44:3a" || client.LaneIdentitySource != "mac" {
		t.Fatalf("unexpected lane identity: %#v", client)
	}
}

func TestPoolJobHealthSnapshotCountsPhysicalMinersAndDuplicateMACLanes(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	firstJob := testHealthJob(now.Add(-time.Second), "parent")
	duplicateJob := testHealthJob(now.Add(-time.Second), "parent")
	secondJob := testHealthJob(now.Add(-time.Second), "parent")
	unknownJob := testHealthJob(now.Add(-time.Second), "parent")

	first := testHealthClient("192.0.2.77:40210", "session-first", firstJob)
	first.asicMAC = "02:00:00:00:00:01"
	duplicate := testHealthClient("192.0.2.97:40210", "session-duplicate", duplicateJob)
	duplicate.asicMAC = "02-00-00-00-00-01"
	second := testHealthClient("192.0.2.83:40210", "session-second", secondJob)
	second.asicMAC = "02:00:00:00:00:02"
	unknown := testHealthClient("198.51.100.90:40210", "session-unknown", unknownJob)

	setPoolHealthClients(first, duplicate, second, unknown)

	snapshot := snapshotPoolJobHealth(now, true)
	if snapshot.ActiveConnections != 4 || snapshot.ReadyConnections != 4 {
		t.Fatalf("connection counts = active:%d ready:%d, want 4/4", snapshot.ActiveConnections, snapshot.ReadyConnections)
	}
	if snapshot.PhysicalMiners != 2 || snapshot.ReadyPhysicalMiners != 2 {
		t.Fatalf("physical miner counts = physical:%d ready:%d, want 2/2: %#v", snapshot.PhysicalMiners, snapshot.ReadyPhysicalMiners, snapshot)
	}
	if snapshot.UnknownIdentityConnections != 1 {
		t.Fatalf("unknown identity connections = %d, want 1", snapshot.UnknownIdentityConnections)
	}
	if snapshot.DuplicateMACs != 1 || snapshot.DuplicateMACLanes != 1 {
		t.Fatalf("duplicate MAC counts = macs:%d lanes:%d, want 1/1: %#v", snapshot.DuplicateMACs, snapshot.DuplicateMACLanes, snapshot)
	}
	if snapshot.IdentityStatus != "warning" || snapshot.IdentityReasonCode != poolJobIdentityDuplicateMACLanes {
		t.Fatalf("identity verdict = %s/%s, want warning/%s", snapshot.IdentityStatus, snapshot.IdentityReasonCode, poolJobIdentityDuplicateMACLanes)
	}
}

func TestPoolJobHealthSnapshotIdentityOKWithUniqueMACs(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	firstJob := testHealthJob(now.Add(-time.Second), "parent")
	secondJob := testHealthJob(now.Add(-time.Second), "parent")
	first := testHealthClient("192.0.2.77:40210", "session-first", firstJob)
	first.asicMAC = "02:00:00:00:00:01"
	second := testHealthClient("192.0.2.83:40210", "session-second", secondJob)
	second.asicMAC = "02:00:00:00:00:02"
	setPoolHealthClients(first, second)

	snapshot := snapshotPoolJobHealth(now, true)
	if snapshot.Status != "ok" || snapshot.ReasonCode != poolJobHealthOK {
		t.Fatalf("job health verdict = %s/%s, want ok/ok", snapshot.Status, snapshot.ReasonCode)
	}
	if snapshot.PhysicalMiners != 2 || snapshot.ReadyPhysicalMiners != 2 {
		t.Fatalf("physical miner counts = physical:%d ready:%d, want 2/2", snapshot.PhysicalMiners, snapshot.ReadyPhysicalMiners)
	}
	if snapshot.IdentityStatus != "ok" || snapshot.IdentityReasonCode != poolJobHealthOK {
		t.Fatalf("identity verdict = %s/%s, want ok/ok", snapshot.IdentityStatus, snapshot.IdentityReasonCode)
	}
}

func TestPoolJobHealthHandlerReturnsJSON(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	job := testHealthJob(time.Now().Add(-time.Second), "parent")
	setPoolHealthClients(testHealthClient("192.168.1.116:4000", "session-1", job))

	req := httptest.NewRequest("GET", "/health/job-state", nil)
	rec := httptest.NewRecorder()
	handlePoolJobHealth(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var snapshot poolJobHealthSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode health JSON: %v", err)
	}
	if snapshot.Status != "ok" || snapshot.ActiveConnections != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
}

func TestPoolJobHealthSnapshotIncludesRecoveryWindows(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()

	globalExpiredJobRecovery.mu.Lock()
	globalExpiredJobRecovery.windowStart = now.Add(-2 * time.Second)
	globalExpiredJobRecovery.rejects = 12
	globalExpiredJobRecovery.clientKeys = map[string]struct{}{"client-a": {}, "client-b": {}}
	globalExpiredJobRecovery.lastRecovery = now.Add(-time.Second)
	globalExpiredJobRecovery.mu.Unlock()

	globalSubmitStallRecovery.mu.Lock()
	globalSubmitStallRecovery.windowStart = now.Add(-3 * time.Second)
	globalSubmitStallRecovery.failures = 4
	globalSubmitStallRecovery.staleFailures = 2
	globalSubmitStallRecovery.duplicateFailures = 1
	globalSubmitStallRecovery.mu.Unlock()

	globalTemplateConversionStallRecovery.mu.Lock()
	globalTemplateConversionStallRecovery.windowStart = now.Add(-4 * time.Second)
	globalTemplateConversionStallRecovery.accepted = 1
	globalTemplateConversionStallRecovery.failures = 9
	globalTemplateConversionStallRecovery.staleFailures = 7
	globalTemplateConversionStallRecovery.duplicateFailures = 2
	globalTemplateConversionStallRecovery.totalCandidates = 10
	globalTemplateConversionStallRecovery.activeMiners = 6
	globalTemplateConversionStallRecovery.lastReason = "tip-overdue"
	globalTemplateConversionStallRecovery.mu.Unlock()

	snapshot := snapshotPoolJobHealth(now, false)
	if snapshot.ExpiredJob.WindowRejects != 12 || snapshot.ExpiredJob.WindowClients != 2 || !snapshot.ExpiredJob.CooldownActive {
		t.Fatalf("unexpected expired-job health: %#v", snapshot.ExpiredJob)
	}
	if snapshot.SubmitStall.Failures != 4 ||
		snapshot.SubmitStall.StaleFailures != 2 ||
		snapshot.SubmitStall.DuplicateFailures != 1 {
		t.Fatalf("unexpected submit-stall health: %#v", snapshot.SubmitStall)
	}
	if snapshot.TemplateConversionStall.TotalCandidates != 10 ||
		snapshot.TemplateConversionStall.Failures != 9 ||
		snapshot.TemplateConversionStall.FailureRatio != 90 ||
		snapshot.TemplateConversionStall.LastReason != "tip-overdue" {
		t.Fatalf("unexpected template-conversion-stall health: %#v", snapshot.TemplateConversionStall)
	}
}

func testHealthJob(createdAt time.Time, parent string) *JobData {
	job := &JobData{
		CreatedAt:   createdAt,
		TemplateSeq: 7,
		BackendName: "node",
		ParentHash:  parent,
		Difficulty:  1.25,
	}
	return job
}

func testHealthClient(addr, sessionID string, job *JobData) *client {
	c := &client{
		addr:                    addr,
		authorized:              true,
		sessionID:               sessionID,
		extranonceSubscribed:    true,
		pdiff:                   1.25,
		expiredJobRejects:       2,
		expiredJobWindowRejects: 1,
		lowDiffRejects:          3,
	}
	c.currentJob.Store(&clientCurrentJob{id: "job-1", job: job})
	return c
}

func setPoolHealthClients(values ...*client) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	clients = make(map[*client]struct{}, len(values))
	for _, c := range values {
		clients[c] = struct{}{}
	}
}

func isolatePoolJobHealthGlobals(t *testing.T, now time.Time, parent string) func() {
	t.Helper()

	clientsMu.Lock()
	previousClients := clients
	clients = map[*client]struct{}{}
	clientsMu.Unlock()

	previousAcceptedJobs := drainSyncMap(&acceptedJobs)
	previousBackendInvalidationEpochs := drainSyncMap(&backendJobInvalidationEpochs)
	previousLastBroadcastTime := lastBroadcastTime
	previousCurrentParent := currentParent
	previousCurrentParentChangedUnixNano := currentParentChangedUnixNano.Load()
	parentMu.Lock()
	previousLastParent := lastParent
	lastParent = parent
	parentMu.Unlock()
	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	previousNotify, previousNotifyOK := lastBaseNotifyVal.Load().([]interface{})
	previousRouter := activeRPCRouter
	previousExpired := snapshotExpiredTrackerState()
	previousSubmit := snapshotSubmitStallTrackerState()
	previousConversion := snapshotTemplateConversionStallTrackerState()

	lastBroadcastTime = atomic.Value{}
	lastBroadcastTime.Store(now.Add(-500 * time.Millisecond))
	currentParent = atomic.Value{}
	currentParent.Store(parent)
	currentParentChangedUnixNano.Store(now.Add(-time.Second).UnixNano())
	lastBaseJobVal = atomic.Value{}
	lastBaseNotifyVal = atomic.Value{}
	miningWork.rememberLatestBroadcast([]interface{}{"health-test"}, testHealthJob(now.Add(-500*time.Millisecond), parent), 7)
	activeRPCRouter = nil
	globalExpiredJobRecovery.configure(defaultExpiredJobRecoveryConfig())
	globalSubmitStallRecovery.configure(defaultSubmitStallRecoveryConfig())
	globalTemplateConversionStallRecovery.configure(defaultTemplateConversionStallRecoveryConfig())

	return func() {
		clientsMu.Lock()
		clients = previousClients
		clientsMu.Unlock()

		drainSyncMap(&acceptedJobs)
		restoreSyncMap(&acceptedJobs, previousAcceptedJobs)
		drainSyncMap(&backendJobInvalidationEpochs)
		restoreSyncMap(&backendJobInvalidationEpochs, previousBackendInvalidationEpochs)
		lastBroadcastTime = previousLastBroadcastTime
		currentParent = previousCurrentParent
		currentParentChangedUnixNano.Store(previousCurrentParentChangedUnixNano)
		parentMu.Lock()
		lastParent = previousLastParent
		parentMu.Unlock()
		lastSeq.Store(previousSeq)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
		lastBaseNotifyVal = atomic.Value{}
		if previousNotifyOK {
			lastBaseNotifyVal.Store(previousNotify)
		}
		activeRPCRouter = previousRouter
		restoreExpiredTrackerState(previousExpired)
		restoreSubmitStallTrackerState(previousSubmit)
		restoreTemplateConversionStallTrackerState(previousConversion)
	}
}

type expiredTrackerState struct {
	config       expiredJobRecoveryConfig
	windowStart  time.Time
	rejects      int
	clientKeys   map[string]struct{}
	lastRecovery time.Time
}

type submitStallTrackerState struct {
	config            submitStallRecoveryConfig
	windowStart       time.Time
	failures          int
	staleFailures     int
	duplicateFailures int
	lastRecovery      time.Time
}

type templateConversionStallTrackerState struct {
	config            templateConversionStallRecoveryConfig
	windowStart       time.Time
	accepted          int
	failures          int
	staleFailures     int
	duplicateFailures int
	totalCandidates   int
	activeMiners      int
	lastReason        string
	lastRecovery      time.Time
}

func snapshotExpiredTrackerState() expiredTrackerState {
	globalExpiredJobRecovery.mu.Lock()
	defer globalExpiredJobRecovery.mu.Unlock()

	state := expiredTrackerState{
		config:       globalExpiredJobRecovery.config,
		windowStart:  globalExpiredJobRecovery.windowStart,
		rejects:      globalExpiredJobRecovery.rejects,
		lastRecovery: globalExpiredJobRecovery.lastRecovery,
	}
	if globalExpiredJobRecovery.clientKeys != nil {
		state.clientKeys = make(map[string]struct{}, len(globalExpiredJobRecovery.clientKeys))
		for key, value := range globalExpiredJobRecovery.clientKeys {
			state.clientKeys[key] = value
		}
	}
	return state
}

func restoreExpiredTrackerState(state expiredTrackerState) {
	globalExpiredJobRecovery.mu.Lock()
	defer globalExpiredJobRecovery.mu.Unlock()

	globalExpiredJobRecovery.config = state.config
	globalExpiredJobRecovery.windowStart = state.windowStart
	globalExpiredJobRecovery.rejects = state.rejects
	globalExpiredJobRecovery.clientKeys = state.clientKeys
	globalExpiredJobRecovery.lastRecovery = state.lastRecovery
}

func snapshotSubmitStallTrackerState() submitStallTrackerState {
	globalSubmitStallRecovery.mu.Lock()
	defer globalSubmitStallRecovery.mu.Unlock()

	return submitStallTrackerState{
		config:            globalSubmitStallRecovery.config,
		windowStart:       globalSubmitStallRecovery.windowStart,
		failures:          globalSubmitStallRecovery.failures,
		staleFailures:     globalSubmitStallRecovery.staleFailures,
		duplicateFailures: globalSubmitStallRecovery.duplicateFailures,
		lastRecovery:      globalSubmitStallRecovery.lastRecovery,
	}
}

func restoreSubmitStallTrackerState(state submitStallTrackerState) {
	globalSubmitStallRecovery.mu.Lock()
	defer globalSubmitStallRecovery.mu.Unlock()

	globalSubmitStallRecovery.config = state.config
	globalSubmitStallRecovery.windowStart = state.windowStart
	globalSubmitStallRecovery.failures = state.failures
	globalSubmitStallRecovery.staleFailures = state.staleFailures
	globalSubmitStallRecovery.duplicateFailures = state.duplicateFailures
	globalSubmitStallRecovery.lastRecovery = state.lastRecovery
}

func snapshotTemplateConversionStallTrackerState() templateConversionStallTrackerState {
	globalTemplateConversionStallRecovery.mu.Lock()
	defer globalTemplateConversionStallRecovery.mu.Unlock()

	return templateConversionStallTrackerState{
		config:            globalTemplateConversionStallRecovery.config,
		windowStart:       globalTemplateConversionStallRecovery.windowStart,
		accepted:          globalTemplateConversionStallRecovery.accepted,
		failures:          globalTemplateConversionStallRecovery.failures,
		staleFailures:     globalTemplateConversionStallRecovery.staleFailures,
		duplicateFailures: globalTemplateConversionStallRecovery.duplicateFailures,
		totalCandidates:   globalTemplateConversionStallRecovery.totalCandidates,
		activeMiners:      globalTemplateConversionStallRecovery.activeMiners,
		lastReason:        globalTemplateConversionStallRecovery.lastReason,
		lastRecovery:      globalTemplateConversionStallRecovery.lastRecovery,
	}
}

func restoreTemplateConversionStallTrackerState(state templateConversionStallTrackerState) {
	globalTemplateConversionStallRecovery.mu.Lock()
	defer globalTemplateConversionStallRecovery.mu.Unlock()

	globalTemplateConversionStallRecovery.config = state.config
	globalTemplateConversionStallRecovery.windowStart = state.windowStart
	globalTemplateConversionStallRecovery.accepted = state.accepted
	globalTemplateConversionStallRecovery.failures = state.failures
	globalTemplateConversionStallRecovery.staleFailures = state.staleFailures
	globalTemplateConversionStallRecovery.duplicateFailures = state.duplicateFailures
	globalTemplateConversionStallRecovery.totalCandidates = state.totalCandidates
	globalTemplateConversionStallRecovery.activeMiners = state.activeMiners
	globalTemplateConversionStallRecovery.lastReason = state.lastReason
	globalTemplateConversionStallRecovery.lastRecovery = state.lastRecovery
}

func drainSyncMap(target *sync.Map) map[interface{}]interface{} {
	values := map[interface{}]interface{}{}
	target.Range(func(key, value interface{}) bool {
		values[key] = value
		target.Delete(key)
		return true
	})
	return values
}

func restoreSyncMap(target *sync.Map, values map[interface{}]interface{}) {
	for key, value := range values {
		target.Store(key, value)
	}
}
