package server

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestCleanupDuplicateMACLanesClosesOnlyStaleSiblingAfterGrace(t *testing.T) {
	now := time.Unix(2000, 0)
	currentParent := "parent-current"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	readyJob := testHealthJob(now.Add(-500*time.Millisecond), currentParent)
	readyJob.TemplateSeq = 12
	staleJob := testHealthJob(now.Add(-3*time.Second), "parent-old")
	staleJob.TemplateSeq = 9
	ready := testDuplicateMACLaneClient("192.0.2.77:40210", "session-ready", "job-ready", "02:00:00:00:00:01", readyJob)
	stale := testDuplicateMACLaneClient("192.0.2.97:40210", "session-stale", "job-stale", "02:00:00:00:00:01", staleJob)
	setPoolHealthClients(ready, stale)

	cfg := &Config{EnableDuplicateMACLaneCleanup: true, DuplicateMACLaneStaleGraceMS: 5000}
	if closed := cleanupDuplicateMACLanes(cfg, now, currentParent, "test"); closed != 0 {
		t.Fatalf("first cleanup closed %d lanes, want observation only", closed)
	}
	if clientIsClosed(stale) {
		t.Fatal("stale lane closed before grace elapsed")
	}

	if closed := cleanupDuplicateMACLanes(cfg, now.Add(6*time.Second), currentParent, "test"); closed != 1 {
		t.Fatalf("second cleanup closed %d lanes, want one stale duplicate lane", closed)
	}
	if !clientIsClosed(stale) {
		t.Fatal("stale duplicate lane was not closed after grace")
	}
	if clientIsClosed(ready) {
		t.Fatal("ready same-MAC lane must remain connected")
	}
}

func TestCleanupDuplicateMACLanesLabelsCloseMetricByMAC(t *testing.T) {
	now := time.Unix(2000, 0)
	currentParent := "parent-current"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	previousMetric := duplicateMACLaneCloses
	duplicateMACLaneCloses = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_duplicate_mac_lane_closes_total"},
		[]string{"reason", "mac"},
	)
	t.Cleanup(func() { duplicateMACLaneCloses = previousMetric })

	readyJob := testHealthJob(now.Add(-500*time.Millisecond), currentParent)
	readyJob.TemplateSeq = 12
	staleJob := testHealthJob(now.Add(-3*time.Second), "parent-old")
	staleJob.TemplateSeq = 9
	ready := testDuplicateMACLaneClient("192.0.2.77:40210", "session-ready", "job-ready", "02-00-00-00-00-01", readyJob)
	stale := testDuplicateMACLaneClient("192.0.2.97:40210", "session-stale", "job-stale", "02-00-00-00-00-01", staleJob)
	setPoolHealthClients(ready, stale)

	cfg := &Config{EnableDuplicateMACLaneCleanup: true, DuplicateMACLaneStaleGraceMS: 1}
	_ = cleanupDuplicateMACLanes(cfg, now, currentParent, "test")
	if closed := cleanupDuplicateMACLanes(cfg, now.Add(time.Second), currentParent, "test"); closed != 1 {
		t.Fatalf("cleanup closed %d lanes, want one stale duplicate lane", closed)
	}
	if got := counterValue(t, duplicateMACLaneCloses.WithLabelValues("stale_current_job", "02:00:00:00:00:01")); got != 1 {
		t.Fatalf("duplicate MAC lane close metric = %v, want 1", got)
	}
}

func TestCleanupDuplicateMACLanesClosesDuplicateReadyCurrentJobAfterGrace(t *testing.T) {
	now := time.Unix(2000, 0)
	currentParent := "parent-current"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	previousMetric := duplicateMACLaneCloses
	duplicateMACLaneCloses = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_duplicate_current_mac_lane_closes_total"},
		[]string{"reason", "mac"},
	)
	t.Cleanup(func() { duplicateMACLaneCloses = previousMetric })

	keepJob := testHealthJob(now.Add(-500*time.Millisecond), currentParent)
	keepJob.TemplateSeq = 12
	duplicateJob := testHealthJob(now.Add(-700*time.Millisecond), currentParent)
	duplicateJob.TemplateSeq = 12
	keep := testDuplicateMACLaneClient("192.0.2.77:40210", "session-keep", "job-current", "02:00:00:00:00:01", keepJob)
	duplicate := testDuplicateMACLaneClient("192.0.2.97:40210", "session-duplicate", "job-current", "02:00:00:00:00:01", duplicateJob)
	setPoolHealthClients(keep, duplicate)

	cfg := &Config{EnableDuplicateMACLaneCleanup: true, DuplicateMACLaneStaleGraceMS: 5000}
	if closed := cleanupDuplicateMACLanes(cfg, now, currentParent, "test"); closed != 0 {
		t.Fatalf("first cleanup closed %d lanes, want observation only", closed)
	}
	if clientIsClosed(duplicate) {
		t.Fatal("duplicate ready lane closed before grace elapsed")
	}

	if closed := cleanupDuplicateMACLanes(cfg, now.Add(6*time.Second), currentParent, "test"); closed != 1 {
		t.Fatalf("second cleanup closed %d lanes, want one duplicate ready lane", closed)
	}
	if !clientIsClosed(duplicate) {
		t.Fatal("duplicate ready lane was not closed after grace")
	}
	if clientIsClosed(keep) {
		t.Fatal("preferred same-MAC lane must remain connected")
	}
	if got := counterValue(t, duplicateMACLaneCloses.WithLabelValues("duplicate_current_job", "02:00:00:00:00:01")); got != 1 {
		t.Fatalf("duplicate-current MAC lane close metric = %v, want 1", got)
	}
}

func TestCleanupDuplicateMACLanesDoesNotCloseReadySiblingOnDifferentJob(t *testing.T) {
	now := time.Unix(2000, 0)
	currentParent := "parent-current"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	keepJob := testHealthJob(now.Add(-500*time.Millisecond), currentParent)
	keepJob.TemplateSeq = 12
	otherJob := testHealthJob(now.Add(-700*time.Millisecond), currentParent)
	otherJob.TemplateSeq = 12
	keep := testDuplicateMACLaneClient("192.0.2.77:40210", "session-keep", "job-current-a", "02:00:00:00:00:01", keepJob)
	other := testDuplicateMACLaneClient("192.0.2.97:40210", "session-other", "job-current-b", "02:00:00:00:00:01", otherJob)
	setPoolHealthClients(keep, other)

	cfg := &Config{EnableDuplicateMACLaneCleanup: true, DuplicateMACLaneStaleGraceMS: 1}
	_ = cleanupDuplicateMACLanes(cfg, now, currentParent, "test")
	if closed := cleanupDuplicateMACLanes(cfg, now.Add(time.Second), currentParent, "test"); closed != 0 {
		t.Fatalf("cleanup closed %d lanes for different current jobs", closed)
	}
	if clientIsClosed(keep) || clientIsClosed(other) {
		t.Fatal("ready lanes with different current job IDs must remain connected")
	}
}

func TestCleanupDuplicateMACLanesDoesNotCloseReadySiblingOnDifferentCurrentTemplate(t *testing.T) {
	now := time.Unix(2000, 0)
	currentParent := "parent-current"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	keepJob := testHealthJob(now.Add(-500*time.Millisecond), currentParent)
	keepJob.TemplateSeq = 12
	olderCurrentJob := testHealthJob(now.Add(-1200*time.Millisecond), currentParent)
	olderCurrentJob.TemplateSeq = 11
	keep := testDuplicateMACLaneClient("192.0.2.77:40210", "session-keep", "job-current-a", "02:00:00:00:00:01", keepJob)
	olderCurrent := testDuplicateMACLaneClient("192.0.2.97:40210", "session-older-current", "job-current-b", "02:00:00:00:00:01", olderCurrentJob)
	setPoolHealthClients(keep, olderCurrent)

	cfg := &Config{EnableDuplicateMACLaneCleanup: true, DuplicateMACLaneStaleGraceMS: 1}
	_ = cleanupDuplicateMACLanes(cfg, now, currentParent, "test")
	if closed := cleanupDuplicateMACLanes(cfg, now.Add(time.Second), currentParent, "test"); closed != 0 {
		t.Fatalf("cleanup closed %d lanes for different current templates", closed)
	}
	if clientIsClosed(keep) || clientIsClosed(olderCurrent) {
		t.Fatal("same-MAC lanes with different current template sequences must remain connected")
	}
}

func TestNormalizeMetricMAC(t *testing.T) {
	for _, test := range []struct {
		name string
		mac  string
		want string
	}{
		{name: "colon", mac: "02:00:00:00:00:01", want: "02:00:00:00:00:01"},
		{name: "dash", mac: "02-00-00-00-00-01", want: "02:00:00:00:00:01"},
		{name: "missing", mac: "", want: "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizeMetricMAC(test.mac); got != test.want {
				t.Fatalf("normalizeMetricMAC(%q) = %q, want %q", test.mac, got, test.want)
			}
		})
	}
}

func TestCleanupDuplicateMACLanesDoesNotCloseWithoutReadyOwner(t *testing.T) {
	now := time.Unix(2000, 0)
	currentParent := "parent-current"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	staleA := testHealthJob(now.Add(-3*time.Second), "parent-old-a")
	staleB := testHealthJob(now.Add(-4*time.Second), "parent-old-b")
	first := testDuplicateMACLaneClient("192.0.2.77:40210", "session-a", "job-a", "02:00:00:00:00:01", staleA)
	second := testDuplicateMACLaneClient("192.0.2.97:40210", "session-b", "job-b", "02:00:00:00:00:01", staleB)
	setPoolHealthClients(first, second)

	cfg := &Config{EnableDuplicateMACLaneCleanup: true, DuplicateMACLaneStaleGraceMS: 1000}
	if closed := cleanupDuplicateMACLanes(cfg, now, currentParent, "test"); closed != 0 {
		t.Fatalf("cleanup closed %d lanes without a ready owner", closed)
	}
	if closed := cleanupDuplicateMACLanes(cfg, now.Add(2*time.Second), currentParent, "test"); closed != 0 {
		t.Fatalf("cleanup closed %d lanes without a ready owner after grace", closed)
	}
	if clientIsClosed(first) || clientIsClosed(second) {
		t.Fatal("duplicate stale lanes must not be closed when no same-MAC lane is current")
	}
}

func TestCleanupDuplicateMACLanesDefaultsOff(t *testing.T) {
	now := time.Unix(2000, 0)
	currentParent := "parent-current"
	restore := isolatePoolJobHealthGlobals(t, now, currentParent)
	defer restore()

	readyJob := testHealthJob(now.Add(-500*time.Millisecond), currentParent)
	staleJob := testHealthJob(now.Add(-3*time.Second), "parent-old")
	ready := testDuplicateMACLaneClient("192.0.2.77:40210", "session-ready", "job-ready", "02:00:00:00:00:01", readyJob)
	stale := testDuplicateMACLaneClient("192.0.2.97:40210", "session-stale", "job-stale", "02:00:00:00:00:01", staleJob)
	setPoolHealthClients(ready, stale)

	cfg := &Config{DuplicateMACLaneStaleGraceMS: 1}
	if closed := cleanupDuplicateMACLanes(cfg, now.Add(time.Second), currentParent, "test"); closed != 0 {
		t.Fatalf("disabled cleanup closed %d lanes", closed)
	}
	if clientIsClosed(stale) {
		t.Fatal("disabled cleanup closed stale lane")
	}
}

func testDuplicateMACLaneClient(addr, sessionID, jobID, mac string, job *JobData) *client {
	c := testHealthClient(addr, sessionID, job)
	c.remoteHost = remoteHostFromAddr(addr)
	c.asicMAC = normalizeASICMAC(mac)
	c.laneID = clientLaneIdentity(addr, c.asicMAC)
	c.currentJob.Store(&clientCurrentJob{id: jobID, job: job})
	return c
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("write counter metric: %v", err)
	}
	if metric.Counter == nil || metric.Counter.Value == nil {
		t.Fatal("counter metric has no value")
	}
	return *metric.Counter.Value
}
