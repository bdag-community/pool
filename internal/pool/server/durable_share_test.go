package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"asicPool/internal/pool/sharejournal"
	"asicPool/internal/pool/storage"
	"github.com/prometheus/client_golang/prometheus"
)

type acceptedDiscoverySpy struct {
	share  *sharejournal.Share
	called bool
}

type lostShareAcknowledgement struct {
	acceptedDiscoverySpy
	share sharejournal.Share
	calls int
}

func (s *lostShareAcknowledgement) AppendAcceptedShareV1(_ context.Context, share sharejournal.Share) (int64, bool, error) {
	s.calls++
	if s.calls == 1 {
		s.share = share
		return 0, false, errors.New("commit acknowledgement lost")
	}
	if share != s.share {
		return 0, false, errors.New("retry changed durable share")
	}
	return 1, false, nil
}

func TestDurableShareLostCommitAcknowledgementCanRetry(t *testing.T) {
	oldRejected := sharesRejected
	sharesRejected = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "durable_test_rejected", Help: "test"}, []string{"reason"})
	t.Cleanup(func() { sharesRejected = oldRejected })
	var writes bytes.Buffer
	journal := &lostShareAcknowledgement{}
	coordinator := newSubmissionCoordinator(&coordinatorJournal{}, big.NewInt(1404), "node", nil)
	coordinator.shareAccounting = &durableShareAccounting{journal: journal, target: 10, policy: sharejournal.PartialShare}
	address := "0x1111111111111111111111111111111111111111"
	c := &client{addr: "test", username: address, authorized: true, sessionID: "test", coordinator: coordinator, rw: bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes))}
	job := &JobData{CreatedAt: time.Now(), SentAt: time.Now(), Difficulty: 1e-9, ParentBytes: make([]byte, 32), NetworkTarget: big.NewInt(1)}
	rememberClientCurrentJob(c, "job-1", job)
	req := &stratumRequest{ID: 1, Method: "mining.submit", submissionCoordinator: coordinator, Params: []json.RawMessage{json.RawMessage(`"` + address + `"`), json.RawMessage(`"job-1"`), json.RawMessage(`"00000001"`), json.RawMessage(`"00000002"`), json.RawMessage(`"00000003"`)}}
	if err := newSubmitPipeline(c, req, time.Now()).run(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(writes.String(), "share accounting pending") {
		t.Fatalf("first response=%s", writes.String())
	}
	writes.Reset()
	if err := newSubmitPipeline(c, req, time.Now()).run(); err != nil {
		t.Fatal(err)
	}
	if journal.calls != 2 || !strings.Contains(writes.String(), `"result":true`) {
		t.Fatalf("retry calls=%d response=%s", journal.calls, writes.String())
	}
}

func (s *acceptedDiscoverySpy) AppendAcceptedShareV1(context.Context, sharejournal.Share) (int64, bool, error) {
	return 1, true, nil
}

func (s *acceptedDiscoverySpy) PutUnpricedDiscoveryV1(_ context.Context, candidate storage.FrozenCandidateV1, share *sharejournal.Share, _ uint64, _ sharejournal.BoundaryPolicy, _ float64) (storage.UnpricedDiscoveryV1, storage.PutDisposition, error) {
	s.called = true
	s.share = share
	return storage.UnpricedDiscoveryV1{Candidate: storage.CandidateViewV1{Candidate: candidate, State: storage.SubmissionStateAcceptedFinal}}, storage.PutDispositionExisting, nil
}

func TestDurableDiscoveryDoesNotPriceRewardAtSubmission(t *testing.T) {
	job, header, extra, proof := testSubmissionJob()
	legacy := &coordinatorJournal{}
	journal := &acceptedDiscoverySpy{}
	c := newSubmissionCoordinator(legacy, big.NewInt(1404), "node", nil)
	c.shareAccounting = &durableShareAccounting{journal: journal, target: 10, policy: sharejournal.PartialShare}
	var id [32]byte
	copy(id[:], proof)
	share := &sharejournal.Share{ID: id, Miner: "0x1111111111111111111111111111111111111111", Work: 2}
	result := c.submitFrozenCandidateWithShare(&client{}, job, header, extra, proof, nil, 0, 1, share)
	if result.Err != nil || !journal.called || journal.share != share || result.Transition.State != storage.SubmissionStateAcceptedFinal {
		t.Fatalf("unpriced discovery failed: %+v", result)
	}
	if legacy.haveCandidate || legacy.accountingApplied {
		t.Fatal("discovery was priced through legacy accounting")
	}
}

func TestAcceptedShareWorkRejectsInvalidDifficulty(t *testing.T) {
	work, err := acceptedShareWork(0.125)
	if err != nil || work != 125000000 {
		t.Fatalf("work=%d err=%v", work, err)
	}
	for _, difficulty := range []float64{0, -1, math.NaN(), math.Inf(1), 1e-20, 1e30} {
		if _, err = acceptedShareWork(difficulty); err == nil {
			t.Fatalf("invalid difficulty accepted: %v", difficulty)
		}
	}
}
