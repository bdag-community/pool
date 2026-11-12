package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func installSubmissionDrainTestState(t *testing.T, journal *coordinatorJournal) {
	t.Helper()
	previousGate := activeSubmissionDrain
	previousJournal := activeSubmissionJournal
	activeSubmissionDrain = newSubmissionDrainGate()
	activeSubmissionJournal = journal
	t.Cleanup(func() {
		activeSubmissionDrain = previousGate
		activeSubmissionJournal = previousJournal
	})
}

func TestSubmissionDrainWaitsForIngressThenFencesNewWork(t *testing.T) {
	gate := newSubmissionDrainGate()
	leave, err := gate.enterSubmission()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- gate.begin(context.Background(), "dataset-switch-0001")
	}()
	select {
	case err := <-done:
		t.Fatalf("pause returned before active ingress left: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	leave()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pause did not finish after active ingress left")
	}
	if _, err := gate.enterSubmission(); !errors.Is(err, errSubmissionDrainActive) {
		t.Fatalf("new submit request error = %v", err)
	}
	if _, err := gate.enterPublication(); !errors.Is(err, errSubmissionDrainActive) {
		t.Fatalf("new job publication error = %v", err)
	}
	if err := gate.resume("dataset-switch-0001"); err != nil {
		t.Fatal(err)
	}
	leave, err = gate.enterPublication()
	if err != nil {
		t.Fatalf("publication remained fenced after resume: %v", err)
	}
	leave()
}

func TestSubmissionDrainAdminReadyAndIdentityBoundResume(t *testing.T) {
	journal := &coordinatorJournal{}
	installSubmissionDrainTestState(t, journal)
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")

	pause := httptest.NewRequest(http.MethodPost, "/admin/submission-drain?action=pause&operation_id=dataset-switch-0001&timeout_ms=500", nil)
	pause.RemoteAddr = "127.0.0.1:12345"
	pauseResponse := httptest.NewRecorder()
	handleSubmissionDrainAdmin(pauseResponse, pause)
	if pauseResponse.Code != http.StatusOK || !strings.Contains(pauseResponse.Body.String(), `"classification":"drained"`) {
		t.Fatalf("pause response = %d %s", pauseResponse.Code, pauseResponse.Body.String())
	}

	wrongResume := httptest.NewRequest(http.MethodPost, "/admin/submission-drain?action=resume&operation_id=dataset-switch-other", nil)
	wrongResume.RemoteAddr = "127.0.0.1:12345"
	wrongResponse := httptest.NewRecorder()
	handleSubmissionDrainAdmin(wrongResponse, wrongResume)
	if wrongResponse.Code != http.StatusConflict || !strings.Contains(wrongResponse.Body.String(), `"classification":"drain_owner_mismatch"`) {
		t.Fatalf("wrong resume response = %d %s", wrongResponse.Code, wrongResponse.Body.String())
	}

	resume := httptest.NewRequest(http.MethodPost, "/admin/submission-drain?action=resume&operation_id=dataset-switch-0001", nil)
	resume.RemoteAddr = "127.0.0.1:12345"
	resumeResponse := httptest.NewRecorder()
	handleSubmissionDrainAdmin(resumeResponse, resume)
	if resumeResponse.Code != http.StatusOK || !strings.Contains(resumeResponse.Body.String(), `"classification":"resumed"`) {
		t.Fatalf("resume response = %d %s", resumeResponse.Code, resumeResponse.Body.String())
	}
}

func TestSubmissionDrainAdminTimesOutFailClosedAndRequiresLoopback(t *testing.T) {
	journal := &coordinatorJournal{haveCandidate: true}
	journal.view.State = "pending"
	installSubmissionDrainTestState(t, journal)
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")

	remote := httptest.NewRequest(http.MethodPost, "/admin/submission-drain?action=pause&operation_id=dataset-switch-0002&timeout_ms=100", nil)
	remote.RemoteAddr = "192.168.1.20:12345"
	remoteResponse := httptest.NewRecorder()
	handleSubmissionDrainAdmin(remoteResponse, remote)
	if remoteResponse.Code != http.StatusNotFound {
		t.Fatalf("non-loopback status = %d, want 404", remoteResponse.Code)
	}

	pause := httptest.NewRequest(http.MethodPost, "/admin/submission-drain?action=pause&operation_id=dataset-switch-0002&timeout_ms=100", nil)
	pause.RemoteAddr = "127.0.0.1:12345"
	pauseResponse := httptest.NewRecorder()
	handleSubmissionDrainAdmin(pauseResponse, pause)
	if pauseResponse.Code != http.StatusConflict || !strings.Contains(pauseResponse.Body.String(), `"classification":"candidate_drain_timeout"`) {
		t.Fatalf("timeout response = %d %s", pauseResponse.Code, pauseResponse.Body.String())
	}
	if !activeSubmissionDrain.snapshot().Paused {
		t.Fatal("timed-out drain reopened candidate ingress")
	}
}

func TestPausedCoordinatorRejectsBeforeCandidateCreation(t *testing.T) {
	journal := &coordinatorJournal{}
	installSubmissionDrainTestState(t, journal)
	if err := activeSubmissionDrain.begin(context.Background(), "dataset-switch-0003"); err != nil {
		t.Fatal(err)
	}
	coordinator := newSubmissionCoordinator(journal, nil, "node", nil)
	var output bytes.Buffer
	client := &client{rw: bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&output))}
	request := &stratumRequest{ID: 7}
	if err := coordinator.SubmitCandidate(client, request); err != nil {
		t.Fatal(err)
	}
	if err := client.rw.Flush(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "pool paused for dataset activation") {
		t.Fatalf("response = %q", output.String())
	}
	journal.mu.Lock()
	haveCandidate := journal.haveCandidate
	journal.mu.Unlock()
	if haveCandidate {
		t.Fatal("paused coordinator wrote a new durable candidate")
	}
}

func TestPausedDrainSuppressesTemplatePublication(t *testing.T) {
	installSubmissionDrainTestState(t, &coordinatorJournal{})
	if err := activeSubmissionDrain.begin(context.Background(), "dataset-switch-0004"); err != nil {
		t.Fatal(err)
	}
	if broadcastTemplateWithCleanMode(nil, true, "must-not-publish", preserveNotifyClean, nil) {
		t.Fatal("paused drain published a template")
	}
	if _, err := activeSubmissionDrain.enterPublication(); !errors.Is(err, errSubmissionDrainActive) {
		t.Fatalf("publication fence error = %v", err)
	}
}
