package server

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
)

type rewardEvidenceStub struct {
	t        *testing.T
	evidence rewardEvidence
}

type rewardEvidenceSinkStub struct {
	id          [32]byte
	observation storage.RewardObservationV1
	writes      int
	candidate   *storage.FrozenCandidateV1
}

func (s *rewardEvidenceSinkStub) NextRewardObservationCandidateExcludingV1(_ context.Context, excluded [][32]byte) (*storage.FrozenCandidateV1, error) {
	for _, id := range excluded {
		if s.candidate != nil && s.candidate.CandidateID == id {
			return nil, nil
		}
	}
	return s.candidate, nil
}

func (s *rewardEvidenceSinkStub) RecordRewardObservationV1(_ context.Context, id [32]byte, _ common.Address, observation storage.RewardObservationV1) (bool, error) {
	s.id, s.observation, s.writes = id, observation, s.writes+1
	return true, nil
}

func (s rewardEvidenceStub) CallContext(_ context.Context, out interface{}, method string, args ...interface{}) error {
	if method != "bdag_getPoolRewardEvidence" || len(args) != 2 || args[0] != s.evidence.NativeBlockHash || args[1] != s.evidence.Wallet {
		s.t.Fatalf("unexpected reward request %s %v", method, args)
	}
	*(out.(*rewardEvidence)) = s.evidence
	return nil
}

func TestRewardEvidenceBindsNodeResultToCandidate(t *testing.T) {
	job, header, extra, proof := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, proof, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	wallet := common.BytesToAddress(candidate.HeaderBytes[148:168])
	evidence := rewardEvidence{NativeBlockHash: common.Hash(candidate.ExpectedBlockHash), NativeOrder: 4, NativeHeight: candidate.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "5", SelfPaidFeesWei: "2"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := readRewardEvidence(ctx, rewardEvidenceStub{t, evidence}, candidate, wallet)
	if err != nil || got != evidence {
		t.Fatalf("valid result rejected: %+v %v", got, err)
	}
	sink := &rewardEvidenceSinkStub{}
	if err = captureRewardEvidence(ctx, rewardEvidenceStub{t, evidence}, sink, candidate, wallet); err != nil || sink.writes != 1 || sink.id != candidate.CandidateID || sink.observation != evidence {
		t.Fatalf("reward observation not captured: %+v %v", sink, err)
	}
	for _, bad := range []rewardEvidence{
		func() rewardEvidence { e := evidence; e.NativeHeight++; return e }(),
		func() rewardEvidence { e := evidence; e.EVMStateRoot = common.Hash{}; return e }(),
		func() rewardEvidence { e := evidence; e.SubsidyWei = "0700"; return e }(),
		func() rewardEvidence { e := evidence; e.OtherFeesWei = "-1"; return e }(),
	} {
		if _, err = readRewardEvidence(ctx, rewardEvidenceStub{t, bad}, candidate, wallet); err == nil {
			t.Fatalf("invalid evidence accepted: %+v", bad)
		}
		if err = captureRewardEvidence(ctx, rewardEvidenceStub{t, bad}, sink, candidate, wallet); err == nil || sink.writes != 1 {
			t.Fatalf("invalid evidence was stored: %+v %v", bad, err)
		}
	}
	sink.candidate = &candidate
	collector := rewardEvidenceCollector{node: rewardEvidenceStub{t, evidence}, store: sink, wallet: wallet}
	if processed, err := collector.captureNext(ctx); err != nil || !processed || sink.writes != 2 {
		t.Fatalf("pending reward was not captured: processed=%t writes=%d err=%v", processed, sink.writes, err)
	}
	sink.candidate = nil
	if processed, err := collector.captureNext(ctx); err != nil || processed || sink.writes != 2 {
		t.Fatalf("empty collector changed state: processed=%t writes=%d err=%v", processed, sink.writes, err)
	}
}

type rotatingEvidenceStore struct {
	candidates []storage.FrozenCandidateV1
	writes     [][32]byte
}

func (s *rotatingEvidenceStore) NextRewardObservationCandidateExcludingV1(_ context.Context, excluded [][32]byte) (*storage.FrozenCandidateV1, error) {
	for i := range s.candidates {
		candidate := &s.candidates[i]
		skip := false
		for _, id := range append(excluded, s.writes...) {
			if id == candidate.CandidateID {
				skip = true
				break
			}
		}
		if !skip {
			return candidate, nil
		}
	}
	return nil, nil
}
func (s *rotatingEvidenceStore) RecordRewardObservationV1(_ context.Context, id [32]byte, _ common.Address, _ storage.RewardObservationV1) (bool, error) {
	s.writes = append(s.writes, id)
	return true, nil
}

type rotatingEvidenceRPC struct {
	bad      common.Hash
	evidence rewardEvidence
	attempts []common.Hash
}

func (r *rotatingEvidenceRPC) CallContext(_ context.Context, out interface{}, _ string, args ...interface{}) error {
	hash := args[0].(common.Hash)
	r.attempts = append(r.attempts, hash)
	if hash == r.bad {
		return errors.New("reward evidence unavailable")
	}
	*(out.(*rewardEvidence)) = r.evidence
	return nil
}
func TestRewardCollectorRotatesFailedObservationsAndRetries(t *testing.T) {
	job, header, extra, proof := testSubmissionJob()
	first, err := freezeBlockCandidateV1(job, header, extra, proof, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.CandidateID = [32]byte{99}
	second.ExpectedBlockHash = common.HexToHash("0x1234")
	wallet := common.BytesToAddress(first.HeaderBytes[148:168])
	evidence := rewardEvidence{NativeBlockHash: common.Hash(second.ExpectedBlockHash), NativeOrder: 4, NativeHeight: second.Height.Uint64(), EVMBlockHash: common.HexToHash("0x11"), EVMBlockNumber: 8, EVMStateRoot: common.HexToHash("0x12"), Wallet: wallet, SubsidyWei: "700", OtherFeesWei: "5", SelfPaidFeesWei: "2"}
	store := &rotatingEvidenceStore{candidates: []storage.FrozenCandidateV1{first, second}}
	rpc := &rotatingEvidenceRPC{bad: common.Hash(first.ExpectedBlockHash), evidence: evidence}
	collector := rewardEvidenceCollector{node: rpc, store: store, wallet: wallet}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = collector.captureNext(ctx); err == nil {
		t.Fatal("unavailable evidence accepted")
	}
	if ok, err := collector.captureNext(ctx); err != nil || !ok || len(store.writes) != 1 || store.writes[0] != second.CandidateID {
		t.Fatalf("later evidence not recorded: %t %v", ok, err)
	}
	if ok, err := collector.captureNext(ctx); err != nil || ok {
		t.Fatalf("candidate retried before backoff elapsed: %t %v", ok, err)
	}
	hold := collector.held[first.CandidateID]
	if hold.failures != 1 || time.Until(hold.until) < 4*time.Second {
		t.Fatal("first retry did not receive a short backoff")
	}
	hold.until = time.Now().Add(-time.Second)
	collector.held[first.CandidateID] = hold
	if _, err = collector.captureNext(ctx); err == nil || len(rpc.attempts) != 3 || rpc.attempts[2] != rpc.bad {
		t.Fatalf("failed observation was not retried: %v", err)
	}
	if hold = collector.held[first.CandidateID]; hold.failures != 2 || time.Until(hold.until) < 9*time.Second {
		t.Fatal("repeated failures did not increase backoff")
	}
}
