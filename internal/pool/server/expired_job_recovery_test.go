package server

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRecordExpiredJobRejectTriggersClientResend(t *testing.T) {
	globalExpiredJobRecovery.configure(expiredJobRecoveryConfig{
		enabled:                     true,
		window:                      10 * time.Second,
		clientResendThreshold:       3,
		globalCleanRefreshThreshold: 1000,
		cooldown:                    time.Minute,
	})
	t.Cleanup(func() {
		globalExpiredJobRecovery.configure(defaultExpiredJobRecoveryConfig())
	})

	c := &client{cfg: &Config{
		ExpiredJobRejectWindowSeconds:         10,
		ExpiredJobClientResendThreshold:       3,
		ExpiredJobGlobalCleanRefreshThreshold: 1000,
		ExpiredJobRecoveryCooldownSeconds:     60,
	}}
	now := time.Unix(100, 0)

	if decision := recordExpiredJobReject(c, now); decision.ClientResend {
		t.Fatal("first expired job reject should not resend")
	}
	if decision := recordExpiredJobReject(c, now.Add(time.Second)); decision.ClientResend {
		t.Fatal("second expired job reject should not resend")
	}
	decision := recordExpiredJobReject(c, now.Add(2*time.Second))
	if !decision.ClientResend {
		t.Fatal("third expired job reject should trigger client resend")
	}
	if decision.ClientRejects != 3 {
		t.Fatalf("client reject count = %d, want 3", decision.ClientRejects)
	}
	if c.expiredJobRejects != 3 {
		t.Fatalf("vardiff expired reject count = %d, want 3", c.expiredJobRejects)
	}
	if c.windowStart != now {
		t.Fatalf("vardiff window start = %v, want %v", c.windowStart, now)
	}
}

func TestRecordExpiredJobRejectRespectsClientCooldown(t *testing.T) {
	c := &client{cfg: &Config{
		ExpiredJobRejectWindowSeconds:     60,
		ExpiredJobClientResendThreshold:   2,
		ExpiredJobRecoveryCooldownSeconds: 30,
	}}
	now := time.Unix(100, 0)

	recordExpiredJobReject(c, now)
	if decision := recordExpiredJobReject(c, now.Add(time.Second)); !decision.ClientResend {
		t.Fatal("threshold should trigger initial client resend")
	}
	if decision := recordExpiredJobReject(c, now.Add(2*time.Second)); decision.ClientResend {
		t.Fatal("client resend should be suppressed inside cooldown")
	}
	if decision := recordExpiredJobReject(c, now.Add(31*time.Second)); !decision.ClientResend {
		t.Fatal("client resend should trigger again after cooldown")
	}
}

func TestHandleExpiredJobRejectRecoversConnectedAcceptedJobsExpiredStorm(t *testing.T) {
	now := time.Unix(1000, 0)
	restore := isolatePoolJobHealthGlobals(t, now, "parent")
	defer restore()
	defer seedResendableLatestBroadcastForRecoveryTest()()

	cfg := &Config{
		ExpiredJobRejectWindowSeconds:          10,
		ExpiredJobClientResendThreshold:        2,
		ExpiredJobGlobalCleanRefreshThreshold:  4,
		ExpiredJobGlobalCleanRefreshMinClients: 2,
		ExpiredJobRecoveryCooldownSeconds:      60,
	}
	globalExpiredJobRecovery.configure(expiredJobRecoveryConfigFromPoolConfig(*cfg))

	c1 := &client{addr: "192.168.1.14:4000", authorized: true, sessionID: "session-a", cfg: cfg}
	c2 := &client{addr: "192.168.1.15:4000", authorized: true, sessionID: "session-b", cfg: cfg}
	setPoolHealthClients(c1, c2)

	before := snapshotPoolJobHealth(now, true)
	if before.ActiveConnections != 2 || before.AuthorizedConnections != 2 || before.ReadyConnections != 0 {
		t.Fatalf("expected connected authorized miners with no usable job, got %#v", before)
	}
	if before.ReasonCode != poolJobHealthMinersWithoutCurrentJob {
		t.Fatalf("reason = %s, want %s", before.ReasonCode, poolJobHealthMinersWithoutCurrentJob)
	}
	if before.AcceptedJobs.Total != 0 {
		t.Fatalf("accepted jobs = %d, want 0", before.AcceptedJobs.Total)
	}

	var resent []string
	var cleanReasons []string
	nowCalls := 0
	actions := expiredJobRecoveryActions{
		nowFn: func() time.Time {
			value := now.Add(time.Duration(nowCalls) * time.Second)
			nowCalls++
			return value
		},
		resendClient: func(c *client) {
			resent = append(resent, c.sessionID)
		},
		cleanRefresh: func(reason string) {
			cleanReasons = append(cleanReasons, reason)
		},
	}

	handleExpiredJobRejectWithActions(c1, "expired-job-a", actions)
	handleExpiredJobRejectWithActions(c1, "expired-job-a", actions)
	handleExpiredJobRejectWithActions(c2, "expired-job-b", actions)
	decision := handleExpiredJobRejectWithActions(c2, "expired-job-b", actions)

	if !decision.CleanRefresh || decision.GlobalRejects != 4 {
		t.Fatalf("final decision = %+v, want clean refresh after four global rejects", decision)
	}
	if len(resent) != 2 || resent[0] != "session-a" || resent[1] != "session-b" {
		t.Fatalf("resent clients = %#v, want both connected miners", resent)
	}
	if len(cleanReasons) != 1 || cleanReasons[0] != "expired job flood" {
		t.Fatalf("clean refresh reasons = %#v, want expired job flood", cleanReasons)
	}
	if c1.closed || c2.closed {
		t.Fatal("expired-job recovery should not disconnect connected miners")
	}

	after := snapshotPoolJobHealth(now.Add(4*time.Second), false)
	if after.ExpiredJob.WindowRejects != 4 || after.ExpiredJob.WindowClients != 2 || !after.ExpiredJob.CooldownActive {
		t.Fatalf("unexpected expired-job recovery window: %#v", after.ExpiredJob)
	}
}

func TestHandleExpiredJobRejectReconnectsSingleClientAfterRepeatedResends(t *testing.T) {
	now := time.Unix(2000, 0)
	defer seedResendableLatestBroadcastForRecoveryTest()()

	cfg := &Config{
		ExpiredJobRejectWindowSeconds:            10,
		ExpiredJobClientResendThreshold:          2,
		ExpiredJobClientReconnectThreshold:       2,
		ExpiredJobClientReconnectWindowSeconds:   120,
		ExpiredJobClientReconnectCooldownSeconds: 60,
		ExpiredJobGlobalCleanRefreshThreshold:    1000,
		ExpiredJobGlobalCleanRefreshMinClients:   2,
		ExpiredJobRecoveryCooldownSeconds:        1,
	}
	globalExpiredJobRecovery.configure(expiredJobRecoveryConfigFromPoolConfig(*cfg))
	t.Cleanup(func() {
		globalExpiredJobRecovery.configure(defaultExpiredJobRecoveryConfig())
	})

	c := &client{addr: "192.168.1.14:4000", authorized: true, sessionID: "session-a", cfg: cfg}
	var resent []string
	var reconnected []string
	var cleanReasons []string
	nowCalls := 0
	actions := expiredJobRecoveryActions{
		nowFn: func() time.Time {
			value := now.Add(time.Duration(nowCalls) * time.Second)
			nowCalls++
			return value
		},
		resendClient: func(c *client) {
			resent = append(resent, c.sessionID)
		},
		reconnectClient: func(c *client) {
			reconnected = append(reconnected, c.sessionID)
		},
		cleanRefresh: func(reason string) {
			cleanReasons = append(cleanReasons, reason)
		},
	}

	handleExpiredJobRejectWithActions(c, "expired-job-a", actions)
	firstRecovery := handleExpiredJobRejectWithActions(c, "expired-job-a", actions)
	if !firstRecovery.ClientResend || firstRecovery.ClientReconnect {
		t.Fatalf("first recovery = %+v, want resend without reconnect", firstRecovery)
	}
	secondRecovery := handleExpiredJobRejectWithActions(c, "expired-job-b", actions)
	if !secondRecovery.ClientReconnect {
		t.Fatalf("second recovery = %+v, want reconnect escalation", secondRecovery)
	}
	if len(resent) != 1 || resent[0] != "session-a" {
		t.Fatalf("resent clients = %#v, want one ordinary resend before reconnect", resent)
	}
	if len(reconnected) != 1 || reconnected[0] != "session-a" {
		t.Fatalf("reconnected clients = %#v, want stale session reconnect", reconnected)
	}
	if len(cleanReasons) != 1 || cleanReasons[0] != "expired job client reconnect" {
		t.Fatalf("clean reasons = %#v, want client reconnect clean refresh", cleanReasons)
	}
}

func TestExpiredJobGlobalTrackerTriggersCleanRefresh(t *testing.T) {
	tracker := newExpiredJobGlobalTracker(expiredJobRecoveryConfig{
		enabled:                      true,
		window:                       10 * time.Second,
		clientResendThreshold:        3,
		globalCleanRefreshThreshold:  3,
		globalCleanRefreshMinClients: 1,
		cooldown:                     time.Minute,
	})
	now := time.Unix(100, 0)

	if _, recover := tracker.record(now, "client-a"); recover {
		t.Fatal("first global reject should not trigger clean refresh")
	}
	if _, recover := tracker.record(now.Add(time.Second), "client-a"); recover {
		t.Fatal("second global reject should not trigger clean refresh")
	}
	rejects, recover := tracker.record(now.Add(2*time.Second), "client-a")
	if !recover {
		t.Fatal("third global reject should trigger clean refresh")
	}
	if rejects != 3 {
		t.Fatalf("global rejects = %d, want 3", rejects)
	}
	if _, recover := tracker.record(now.Add(3*time.Second), "client-a"); recover {
		t.Fatal("global clean refresh should be suppressed inside cooldown")
	}
}

func TestExpiredJobGlobalTrackerRequiresDistinctClients(t *testing.T) {
	tracker := newExpiredJobGlobalTracker(expiredJobRecoveryConfig{
		enabled:                      true,
		window:                       10 * time.Second,
		clientResendThreshold:        3,
		globalCleanRefreshThreshold:  3,
		globalCleanRefreshMinClients: 2,
		cooldown:                     time.Minute,
	})
	now := time.Unix(100, 0)

	tracker.record(now, "client-a")
	tracker.record(now.Add(time.Second), "client-a")
	rejects, recover := tracker.record(now.Add(2*time.Second), "client-a")
	if recover {
		t.Fatal("one noisy client should not trigger global clean refresh")
	}
	if rejects != 3 {
		t.Fatalf("global rejects = %d, want 3", rejects)
	}

	rejects, recover = tracker.record(now.Add(3*time.Second), "client-b")
	if !recover {
		t.Fatal("second distinct client should allow global clean refresh")
	}
	if rejects != 4 {
		t.Fatalf("global rejects = %d, want 4", rejects)
	}
}

func TestExpiredJobGlobalTrackerDistinctClientsResetWithWindow(t *testing.T) {
	tracker := newExpiredJobGlobalTracker(expiredJobRecoveryConfig{
		enabled:                      true,
		window:                       10 * time.Second,
		clientResendThreshold:        3,
		globalCleanRefreshThreshold:  2,
		globalCleanRefreshMinClients: 2,
		cooldown:                     time.Minute,
	})
	now := time.Unix(100, 0)

	tracker.record(now, "client-a")
	if _, recover := tracker.record(now.Add(11*time.Second), "client-b"); recover {
		t.Fatal("client seen before window reset should not count in the new window")
	}
	if _, recover := tracker.record(now.Add(12*time.Second), "client-b"); recover {
		t.Fatal("same client repeated in new window should not satisfy distinct-client gate")
	}
	if _, recover := tracker.record(now.Add(13*time.Second), "client-a"); !recover {
		t.Fatal("two distinct clients in the new window should trigger clean refresh")
	}
}

func TestExpiredJobRecoveryConfigDefaultAndOverrideMinClients(t *testing.T) {
	defaultCfg := expiredJobRecoveryConfigFromPoolConfig(Config{})
	if defaultCfg.globalCleanRefreshMinClients != defaultExpiredJobGlobalCleanRefreshMinClients {
		t.Fatalf("default min clients = %d, want %d", defaultCfg.globalCleanRefreshMinClients, defaultExpiredJobGlobalCleanRefreshMinClients)
	}
	if defaultCfg.clientReconnectThreshold != defaultExpiredJobClientReconnectThreshold {
		t.Fatalf("default reconnect threshold = %d, want %d", defaultCfg.clientReconnectThreshold, defaultExpiredJobClientReconnectThreshold)
	}
	if defaultCfg.clientReconnectWindow != time.Duration(defaultExpiredJobClientReconnectWindowSeconds)*time.Second {
		t.Fatalf("default reconnect window = %s", defaultCfg.clientReconnectWindow)
	}

	override := expiredJobRecoveryConfigFromPoolConfig(Config{ExpiredJobGlobalCleanRefreshMinClients: 1})
	if override.globalCleanRefreshMinClients != 1 {
		t.Fatalf("override min clients = %d, want 1", override.globalCleanRefreshMinClients)
	}
}

func TestExpiredJobClientKeyPrefersMACLaneIdentity(t *testing.T) {
	c := &client{
		addr:      "192.168.1.10:1234",
		asicMAC:   "28:E2:97:4D:44:3A",
		username:  "wallet.worker",
		sessionID: "session-1",
	}
	c.extranonce1 = [4]byte{0xaa, 0xbb, 0xcc, 0xdd}

	if got := expiredJobClientKey(c); got != "mac:28:e2:97:4d:44:3a" {
		t.Fatalf("client key = %q, want MAC lane identity", got)
	}

	c.asicMAC = ""
	c.laneID = ""
	if got := expiredJobClientKey(c); got != "session:session-1" {
		t.Fatalf("client key = %q, want session identity", got)
	}

	c.sessionID = ""
	if got := expiredJobClientKey(c); got != "en1:aabbccdd" {
		t.Fatalf("client key = %q, want extranonce identity", got)
	}

	c.extranonce1 = [4]byte{}
	c.addr = "198.51.100.10:1234"
	if got := expiredJobClientKey(c); got != "ip:198.51.100.10" {
		t.Fatalf("client key = %q, want IP lane fallback", got)
	}

	c.addr = "192.168.1.10:1234"
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24")
	if got := expiredJobClientKey(c); got != "user:wallet.worker" {
		t.Fatalf("client key = %q, want username fallback for local ASIC LAN without MAC", got)
	}

	c.addr = ""
	if got := expiredJobClientKey(c); got != "user:wallet.worker" {
		t.Fatalf("client key = %q, want username identity", got)
	}
}

func TestRecordExpiredJobRejectFeedsVarDiffWhenRecoveryDisabled(t *testing.T) {
	now := time.Unix(100, 0)
	c := &client{cfg: &Config{DisableExpiredJobRecovery: true}}
	decision := recordExpiredJobReject(c, now)

	if decision.ClientResend || decision.CleanRefresh {
		t.Fatalf("disabled recovery should not trigger actions: %+v", decision)
	}
	if c.expiredJobRejects != 1 {
		t.Fatalf("vardiff expired reject count = %d, want 1", c.expiredJobRejects)
	}
	if c.windowStart != now {
		t.Fatalf("vardiff window start = %v, want %v", c.windowStart, now)
	}
}

func TestResetExpiredJobRecoveryStateClearsReconnectEscalation(t *testing.T) {
	now := time.Unix(3000, 0)
	c := &client{
		expiredJobWindowStart:   now,
		expiredJobWindowRejects: 4,
		expiredJobRecoveryStart: now,
		expiredJobRecoveryCount: 2,
	}

	resetExpiredJobRecoveryState(c)

	if !c.expiredJobWindowStart.IsZero() || c.expiredJobWindowRejects != 0 {
		t.Fatalf("reject window was not reset: start=%v rejects=%d", c.expiredJobWindowStart, c.expiredJobWindowRejects)
	}
	if !c.expiredJobRecoveryStart.IsZero() || c.expiredJobRecoveryCount != 0 {
		t.Fatalf("recovery window was not reset: start=%v count=%d", c.expiredJobRecoveryStart, c.expiredJobRecoveryCount)
	}
}

func TestRecordStaleTemplateRaceDoesNotIncrementExpiredRejects(t *testing.T) {
	c := &client{}
	now := time.Unix(200, 0)

	recordStaleTemplateRace(c, now)
	recordStaleTemplateRace(c, now.Add(time.Second))
	decision := recordStaleTemplateRace(c, now.Add(2*time.Second))

	if c.expiredJobRejects != 0 {
		t.Fatalf("stale-template races must not affect expired reject vardiff signal, got %d", c.expiredJobRejects)
	}
	if c.staleRaceWindowRejects != 3 {
		t.Fatalf("stale race rejects = %d, want 3", c.staleRaceWindowRejects)
	}
	if !decision.ClientResend {
		t.Fatal("third stale-template race should trigger a throttled client resend")
	}
}

func TestRecordStaleTemplateRaceUsesConfiguredThresholdAndCooldown(t *testing.T) {
	now := time.Unix(200, 0)
	c := &client{cfg: &Config{
		StaleRaceRejectWindowSeconds:     2,
		StaleRaceClientResendThreshold:   1,
		StaleRaceRecoveryCooldownSeconds: 1,
	}}

	first := recordStaleTemplateRace(c, now)
	if !first.ClientResend {
		t.Fatal("configured first stale-template race should trigger client resend")
	}

	second := recordStaleTemplateRace(c, now.Add(500*time.Millisecond))
	if second.ClientResend {
		t.Fatal("configured cooldown should suppress immediate stale-template resend")
	}

	third := recordStaleTemplateRace(c, now.Add(1100*time.Millisecond))
	if !third.ClientResend {
		t.Fatal("configured cooldown should allow the next stale-template resend")
	}
}

func TestRecordStaleTemplateRaceReconnectsAfterConfiguredRecoveries(t *testing.T) {
	now := time.Unix(200, 0)
	c := &client{cfg: &Config{
		StaleRaceRejectWindowSeconds:            10,
		StaleRaceClientResendThreshold:          1,
		StaleRaceRecoveryCooldownSeconds:        1,
		StaleRaceClientReconnectThreshold:       2,
		StaleRaceClientReconnectWindowSeconds:   30,
		StaleRaceClientReconnectCooldownSeconds: 60,
	}}

	first := recordStaleTemplateRace(c, now)
	if !first.ClientResend || first.ClientReconnect {
		t.Fatalf("first recovery = %+v, want resend without reconnect", first)
	}

	second := recordStaleTemplateRace(c, now.Add(time.Second))
	if !second.ClientResend || !second.ClientReconnect {
		t.Fatalf("second recovery = %+v, want reconnect escalation", second)
	}

	third := recordStaleTemplateRace(c, now.Add(2*time.Second))
	if third.ClientReconnect {
		t.Fatalf("reconnect cooldown should suppress immediate reconnect, got %+v", third)
	}
}

func TestRecordStaleTemplateRaceDisabledWithExpiredJobRecovery(t *testing.T) {
	now := time.Unix(200, 0)
	c := &client{cfg: &Config{
		DisableExpiredJobRecovery:      true,
		StaleRaceClientResendThreshold: 1,
	}}

	decision := recordStaleTemplateRace(c, now)
	if decision.ClientResend || decision.ClientRejects != 0 {
		t.Fatalf("disabled recovery should ignore stale-template race, got %+v", decision)
	}
}

func seedResendableLatestBroadcastForRecoveryTest() func() {
	previousSeq := lastSeq.Load()
	previousJob, previousJobOK := lastBaseJobVal.Load().(*JobData)
	previousNotify, previousNotifyOK := lastBaseNotifyVal.Load().([]interface{})

	baseNotify := []interface{}{"recovery-base-job", "prev", "ver", "bits", "time", false}
	baseJob := &JobData{
		NBits:             0x1d00ffff,
		CreatedAt:         time.Now(),
		InvalidationEpoch: miningWork.currentInvalidationEpoch(),
	}
	miningWork.rememberLatestBroadcast(baseNotify, baseJob, previousSeq)

	return func() {
		lastSeq.Store(previousSeq)
		lastBaseJobVal = atomic.Value{}
		if previousJobOK {
			lastBaseJobVal.Store(previousJob)
		}
		lastBaseNotifyVal = atomic.Value{}
		if previousNotifyOK {
			lastBaseNotifyVal.Store(previousNotify)
		}
	}
}
