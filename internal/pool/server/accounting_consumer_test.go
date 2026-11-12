package server

import (
	"context"
	"math/big"
	"testing"
	"time"

	"asicPool/internal/pool/storage"
)

func TestAcceptedAccountingConsumerDrainsAcceptedReceiptIdempotently(t *testing.T) {
	ensureSubmissionTestMetrics()
	job, header, extra, share := testSubmissionJob()
	candidate, err := freezeBlockCandidateV1(job, header, extra, share, big.NewInt(1404))
	if err != nil {
		t.Fatal(err)
	}
	j := &coordinatorJournal{}
	if _, disposition, err := j.PutCandidate(context.Background(), candidate, testAcceptedAccounting(t, candidate)); err != nil || disposition != storage.PutDispositionInserted {
		t.Fatalf("put candidate: disposition=%q err=%v", disposition, err)
	}
	leased, err := j.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{1}, time.Second)
	if err != nil || leased == nil {
		t.Fatalf("claim candidate: lease=%#v err=%v", leased, err)
	}
	prepared, err := j.PrepareAttempt(context.Background(), leased.Lease, storage.SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	accepted, final, retryable := true, true, false
	code, message := "accepted", "ok"
	observation := RPCObservationV1{
		Accepted:    &accepted,
		Final:       &final,
		Retryable:   &retryable,
		Code:        &code,
		Message:     &message,
		BlockHash:   append([]byte(nil), candidate.ExpectedBlockHash[:]...),
		Height:      new(big.Int).Set(candidate.Height),
		TemplateSeq: new(big.Int).Set(candidate.TemplateSeq),
	}
	transition, err := j.CompleteAttempt(context.Background(), leased.Lease, prepared.AttemptID, observation, storage.SubmitDecisionAcceptedFinal)
	if err != nil || !transition.FirstAccepted || j.accountingApplied {
		t.Fatalf("accepted receipt setup: transition=%+v applied=%t err=%v", transition, j.accountingApplied, err)
	}
	consumer := newAcceptedAccountingConsumer(j, time.Millisecond, nil)
	if err := consumer.reconcileOne(context.Background()); err != nil || !j.accountingApplied {
		t.Fatalf("first reconciliation applied=%t err=%v", j.accountingApplied, err)
	}
	if err := consumer.reconcileOne(context.Background()); err != nil {
		t.Fatalf("idempotent reconciliation: %v", err)
	}
}
