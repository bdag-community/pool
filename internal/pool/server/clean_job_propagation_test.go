package server

import (
	"testing"
	"time"
)

func TestCleanJobPropagationObservationCurrentClean(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	job := testCleanJob(now.Add(-time.Second), 12, "parent-current")
	rememberClientCurrentJob(c, "job-clean", job)

	event, ok := cleanJobPropagationObservation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-clean",
		jobID:          "job-clean",
		job:            job,
		current:        true,
	}, now)
	if !ok {
		t.Fatal("expected first submit after clean job to be observed")
	}
	if event.Result != "current-clean" || event.SeqLag != 0 || event.ExpectedSeq != 12 || event.SubmittedSeq != 12 {
		t.Fatalf("unexpected clean-job event: %#v", event)
	}
	if event.Lane != "mac:02:00:00:00:00:02" || event.MAC != "02:00:00:00:00:02" || event.Worker != "worker.synthetic" {
		t.Fatalf("event missing lane identity: %#v", event)
	}

	if _, ok := cleanJobPropagationObservation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-clean",
		jobID:          "job-clean",
		job:            job,
		current:        true,
	}, now.Add(time.Second)); ok {
		t.Fatal("same clean job should only observe the next submit once")
	}
}

func TestCleanJobPropagationObservationOldAfterClean(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	clean := testCleanJob(now.Add(-2*time.Second), 12, "parent-current")
	old := testCleanJob(now.Add(-5*time.Second), 9, "parent-old")
	old.Clean = false
	rememberClientCurrentJob(c, "job-clean", clean)

	event, ok := cleanJobPropagationObservation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-old",
		jobID:          "job-old",
		job:            old,
		current:        false,
	}, now)
	if !ok {
		t.Fatal("expected old submit after clean job to be observed")
	}
	if event.Result != "old-after-clean" || event.SeqLag != 3 || event.ExpectedSeq != 12 || event.SubmittedSeq != 9 {
		t.Fatalf("unexpected old-after-clean event: %#v", event)
	}
	if event.ExpectedParent != "parent-current" || event.SubmittedParent != "parent-old" {
		t.Fatalf("unexpected parent evidence: %#v", event)
	}
}

func TestCleanJobPropagationObservationParentMismatchOverridesSequence(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	clean := testCleanJob(now.Add(-2*time.Second), 12, "parent-current")
	wrongParent := testCleanJob(now.Add(-time.Second), 13, "parent-other")
	rememberClientCurrentJob(c, "job-clean", clean)

	event, ok := cleanJobPropagationObservation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-other-parent",
		jobID:          "job-other-parent",
		job:            wrongParent,
		current:        false,
	}, now)
	if !ok {
		t.Fatal("expected parent mismatch after clean job to be observed")
	}
	if event.Result != "old-after-clean" {
		t.Fatalf("parent mismatch result = %q, want old-after-clean; event=%#v", event.Result, event)
	}
	if event.SeqLag != 0 {
		t.Fatalf("parent mismatch seq lag = %d, want 0 because submitted seq is newer", event.SeqLag)
	}
	if event.ExpectedParent != "parent-current" || event.SubmittedParent != "parent-other" {
		t.Fatalf("unexpected parent evidence: %#v", event)
	}
}

func TestClassifyCleanJobPropagationNormalizesParentBeforeMismatch(t *testing.T) {
	job := &JobData{TemplateSeq: 12}
	event := cleanJobPropagationEvent{
		ExpectedJobID:   "job-clean",
		SubmittedJobID:  "job-submitted",
		ResolvedJobID:   "job-submitted",
		ExpectedSeq:     12,
		SubmittedSeq:    12,
		ExpectedParent:  "0xABCDEF1234567890",
		SubmittedParent: "abcdef1234567890",
	}

	result := classifyCleanJobPropagation(event, submitJobResolution{
		submittedJobID: "job-submitted",
		jobID:          "job-submitted",
		job:            job,
		current:        false,
	})
	if result != "current-or-newer" {
		t.Fatalf("expected equivalent parent formatting to remain current-or-newer, got %q", result)
	}
	if cleanJobPropagationParentsDiffer("0xABCDEF1234567890", "abcdef1234567890") {
		t.Fatal("expected formatting-only parent difference to normalize away")
	}
	if !cleanJobPropagationParentsDiffer("0xABCDEF1234567890", "abcdef1234567891") {
		t.Fatal("expected real parent mismatch to be detected")
	}
}

func TestCleanJobPropagationObservationMissingAfterClean(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	clean := testCleanJob(now.Add(-2*time.Second), 12, "parent-current")
	rememberClientCurrentJob(c, "job-clean", clean)

	event, ok := cleanJobPropagationObservation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-missing",
		jobID:          "job-missing",
		reject: &submitJobReject{
			lifecycleReason: "job_not_found",
		},
	}, now)
	if !ok {
		t.Fatal("expected missing submit after clean job to be observed")
	}
	if event.Result != "missing-after-clean" || event.Reason != "job_not_found" || event.SeqLag != 12 {
		t.Fatalf("unexpected missing-after-clean event: %#v", event)
	}
}

func TestRememberClientCurrentJobResetsCleanObservationForNewCleanJob(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	first := testCleanJob(now.Add(-2*time.Second), 12, "parent-a")
	second := testCleanJob(now.Add(-time.Second), 13, "parent-b")
	rememberClientCurrentJob(c, "job-clean-a", first)

	if _, ok := cleanJobPropagationObservation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-clean-a",
		jobID:          "job-clean-a",
		job:            first,
		current:        true,
	}, now); !ok {
		t.Fatal("expected first clean submit observation")
	}

	rememberClientCurrentJob(c, "job-clean-b", second)
	event, ok := cleanJobPropagationObservation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-clean-b",
		jobID:          "job-clean-b",
		job:            second,
		current:        true,
	}, now.Add(time.Second))
	if !ok {
		t.Fatal("expected new clean job to reset observation")
	}
	if event.ExpectedJobID != "job-clean-b" || event.ExpectedSeq != 13 || event.Result != "current-clean" {
		t.Fatalf("unexpected second clean observation: %#v", event)
	}
}

func TestCleanJobPropagationRepairResendsAfterOldSubmit(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		CleanJobPropagationRepairCooldownMS:         1000,
		CleanJobPropagationReconnectThreshold:       3,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}

	decision := cleanJobPropagationRepairDecisionForEvent(c, cleanJobPropagationEvent{
		Result: "old-after-clean",
		Reason: "first-submit-after-clean",
		Lane:   "mac:02:00:00:00:00:02",
		MAC:    "02:00:00:00:00:02",
	}, now)
	if decision.Action != cleanJobPropagationRepairActionResend || decision.Streak != 1 {
		t.Fatalf("expected clean job resend decision, got %#v", decision)
	}
	if decision.Reason != "old-after-clean" {
		t.Fatalf("expected repair reason to use stale result, got %#v", decision)
	}
}

func TestCleanJobPropagationRepairSuppressesDuringRepairCooldown(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		CleanJobPropagationRepairCooldownMS:         1000,
		CleanJobPropagationReconnectThreshold:       5,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}
	event := cleanJobPropagationEvent{Result: "old-after-clean", Reason: "first-submit-after-clean"}

	first := cleanJobPropagationRepairDecisionForEvent(c, event, now)
	if first.Action != cleanJobPropagationRepairActionResend {
		t.Fatalf("expected first stale clean submit to resend, got %#v", first)
	}
	second := cleanJobPropagationRepairDecisionForEvent(c, event, now.Add(500*time.Millisecond))
	if second.Action != cleanJobPropagationRepairActionSuppressed || second.Reason != cleanJobPropagationRepairReasonRepairCooldown {
		t.Fatalf("expected repair cooldown suppression, got %#v", second)
	}
}

func TestCleanJobPropagationRepairSuppressesDuringPropagationGrace(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		CleanJobPropagationRepairCooldownMS:         1000,
		CleanJobPropagationRepairMinAgeMS:           250,
		CleanJobPropagationReconnectThreshold:       3,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}
	c.lastCleanJobID = "job-clean"
	c.lastCleanTemplateSeq = 12
	c.lastCleanSubmitObserved = true

	decision := cleanJobPropagationRepairDecisionForEvent(c, cleanJobPropagationEvent{
		Result:        "old-after-clean",
		Reason:        "first-submit-after-clean",
		Lane:          "mac:02:00:00:00:00:02",
		MAC:           "02:00:00:00:00:02",
		ExpectedJobID: "job-clean",
		ExpectedSeq:   12,
		Age:           100 * time.Millisecond,
	}, now)
	if decision.Action != cleanJobPropagationRepairActionSuppressed || decision.Reason != cleanJobPropagationRepairReasonPropagationGrace {
		t.Fatalf("expected propagation-grace suppression, got %#v", decision)
	}
	if c.cleanJobStaleSubmitStreak != 0 {
		t.Fatalf("propagation grace should not increment stale streak, got %d", c.cleanJobStaleSubmitStreak)
	}
	if c.lastCleanSubmitObserved {
		t.Fatal("propagation grace should allow the next submit after this clean job to be observed")
	}
}

func TestCleanJobPropagationRepairAfterGraceCanResend(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		CleanJobPropagationRepairCooldownMS:         1000,
		CleanJobPropagationRepairMinAgeMS:           250,
		CleanJobPropagationReconnectThreshold:       3,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}

	decision := cleanJobPropagationRepairDecisionForEvent(c, cleanJobPropagationEvent{
		Result:        "old-after-clean",
		Reason:        "first-submit-after-clean",
		Lane:          "mac:02:00:00:00:00:02",
		MAC:           "02:00:00:00:00:02",
		ExpectedJobID: "job-clean",
		ExpectedSeq:   12,
		Age:           300 * time.Millisecond,
	}, now)
	if decision.Action != cleanJobPropagationRepairActionResend || decision.Streak != 1 {
		t.Fatalf("expected resend after propagation grace, got %#v", decision)
	}
}

func TestCleanJobPropagationGraceAllowsNextSubmitObservation(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		CleanJobPropagationRepairCooldownMS:         1000,
		CleanJobPropagationRepairMinAgeMS:           250,
		CleanJobPropagationReconnectThreshold:       3,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}
	clean := testCleanJob(now.Add(-100*time.Millisecond), 12, "parent-current")
	old := testCleanJob(now.Add(-time.Second), 11, "parent-old")
	rememberClientCurrentJob(c, "job-clean", clean)

	early := observeCleanJobPropagation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-old-a",
		jobID:          "job-old-a",
		job:            old,
		current:        false,
	}, now)
	if early.Action != cleanJobPropagationRepairActionSuppressed || early.Reason != cleanJobPropagationRepairReasonPropagationGrace {
		t.Fatalf("expected early submit to be suppressed by propagation grace, got %#v", early)
	}

	later := observeCleanJobPropagation(c, "worker.synthetic", submitJobResolution{
		submittedJobID: "job-old-b",
		jobID:          "job-old-b",
		job:            old,
		current:        false,
	}, now.Add(250*time.Millisecond))
	if later.Action != cleanJobPropagationRepairActionResend || later.Streak != 1 {
		t.Fatalf("expected later stale submit to trigger clean-job resend, got %#v", later)
	}
}

func TestCleanJobPropagationRepairReconnectsAfterRepeatedOldSubmits(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		EnableCleanJobPropagationReconnect:          true,
		CleanJobPropagationRepairCooldownMS:         1,
		CleanJobPropagationReconnectThreshold:       3,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}
	event := cleanJobPropagationEvent{Result: "missing-after-clean", Reason: "job_not_found"}

	if got := cleanJobPropagationRepairDecisionForEvent(c, event, now); got.Action != cleanJobPropagationRepairActionResend || got.Streak != 1 {
		t.Fatalf("unexpected first decision: %#v", got)
	}
	if got := cleanJobPropagationRepairDecisionForEvent(c, event, now.Add(time.Second)); got.Action != cleanJobPropagationRepairActionResend || got.Streak != 2 {
		t.Fatalf("unexpected second decision: %#v", got)
	}
	third := cleanJobPropagationRepairDecisionForEvent(c, event, now.Add(2*time.Second))
	if third.Action != cleanJobPropagationRepairActionReconnect || third.Streak != 3 {
		t.Fatalf("expected reconnect decision, got %#v", third)
	}
	if c.cleanJobStaleSubmitStreak != 0 {
		t.Fatalf("expected reconnect to reset stale-submit streak, got %d", c.cleanJobStaleSubmitStreak)
	}
}

func TestCleanJobPropagationRepairSuppressesReconnectCooldown(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		EnableCleanJobPropagationReconnect:          true,
		CleanJobPropagationRepairCooldownMS:         1,
		CleanJobPropagationReconnectThreshold:       1,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}
	event := cleanJobPropagationEvent{Result: "old-after-clean", Reason: "first-submit-after-clean"}

	first := cleanJobPropagationRepairDecisionForEvent(c, event, now)
	if first.Action != cleanJobPropagationRepairActionReconnect {
		t.Fatalf("expected immediate reconnect with threshold 1, got %#v", first)
	}
	second := cleanJobPropagationRepairDecisionForEvent(c, event, now.Add(time.Second))
	if second.Action != cleanJobPropagationRepairActionSuppressed || second.Reason != cleanJobPropagationRepairReasonReconnectCooldown {
		t.Fatalf("expected reconnect cooldown suppression, got %#v", second)
	}
}

func TestCleanJobPropagationRepairDefaultsOff(t *testing.T) {
	c := testCleanJobClient()
	c.cfg = &Config{}

	decision := cleanJobPropagationRepairDecisionForEvent(c, cleanJobPropagationEvent{
		Result: "old-after-clean",
	}, time.Unix(3000, 0))
	if decision.Action != "" {
		t.Fatalf("expected no repair decision when disabled, got %#v", decision)
	}
}

func TestCleanJobPropagationReconnectRequiresSeparateOptIn(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:       true,
		CleanJobPropagationRepairCooldownMS:   1,
		CleanJobPropagationReconnectThreshold: 1,
	}
	event := cleanJobPropagationEvent{Result: "old-after-clean", Reason: "first-submit-after-clean"}

	first := cleanJobPropagationRepairDecisionForEvent(c, event, now)
	if first.Action != cleanJobPropagationRepairActionResend {
		t.Fatalf("first repair = %#v, want resend without reconnect opt-in", first)
	}
	second := cleanJobPropagationRepairDecisionForEvent(c, event, now.Add(time.Second))
	if second.Action != cleanJobPropagationRepairActionResend {
		t.Fatalf("second repair = %#v, want bounded resend without reconnect opt-in", second)
	}
}

func TestCleanJobPropagationRepairResetsAfterCurrentSubmit(t *testing.T) {
	now := time.Unix(3000, 0)
	c := testCleanJobClient()
	c.cfg = &Config{
		EnableCleanJobPropagationRepair:             true,
		CleanJobPropagationRepairCooldownMS:         1,
		CleanJobPropagationReconnectThreshold:       3,
		CleanJobPropagationReconnectCooldownSeconds: 60,
	}
	c.cleanJobStaleSubmitStreak = 2

	decision := cleanJobPropagationRepairDecisionForEvent(c, cleanJobPropagationEvent{
		Result: "current-clean",
	}, now)
	if decision.Action != "" {
		t.Fatalf("expected no repair for current clean submit, got %#v", decision)
	}
	if c.cleanJobStaleSubmitStreak != 0 {
		t.Fatalf("expected current clean submit to reset stale streak, got %d", c.cleanJobStaleSubmitStreak)
	}
}

func testCleanJobClient() *client {
	return &client{
		addr:       "192.0.2.83:40210",
		remoteHost: "192.0.2.83",
		asicMAC:    "02:00:00:00:00:02",
		laneID:     "mac:02:00:00:00:00:02",
		sessionID:  "session-clean",
	}
}

func testCleanJob(sentAt time.Time, seq uint64, parent string) *JobData {
	return &JobData{
		TemplateSeq: seq,
		ParentHash:  parent,
		SentAt:      sentAt,
		CreatedAt:   sentAt,
		Clean:       true,
	}
}
