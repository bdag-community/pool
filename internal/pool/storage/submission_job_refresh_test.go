package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"
)

func refreshedJobCandidate(t *testing.T, original FrozenCandidateV1) FrozenCandidateV1 {
	t.Helper()
	duplicate := original
	duplicate.TemplateSeq = new(big.Int).Add(original.TemplateSeq, big.NewInt(1))
	payload, err := encodeFrozenCandidateJournalV1(duplicate)
	if err != nil {
		t.Fatal(err)
	}
	duplicate.FrozenPayload = payload
	duplicate.FrozenDigest = sha256.Sum256(payload)
	return duplicate
}

func TestSubmissionRefreshedJobPreservesEconomicConflict(t *testing.T) {
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	c := storageTestCandidate()
	if _, _, err := s.PutCandidateV2(context.Background(), c, wideAccountingIntentV2(t, c)); err != nil {
		t.Fatal(err)
	}
	duplicate := refreshedJobCandidate(t, c)
	changed := wideAccountingIntentV2(t, duplicate)
	changed.CreditLines[0].MinerAddress = "different-recipient"
	changed, err := SealAcceptedAccountingIntentV2(changed)
	if err != nil {
		t.Fatal(err)
	}
	view, disposition, err := s.PutCandidateV2(context.Background(), duplicate, changed)
	if err != nil || disposition != PutDispositionConflict || view.State != SubmissionStateConflict {
		t.Fatalf("economic mismatch no longer latches: %s %+v %v", disposition, view, err)
	}
	if err := s.VerifyAcceptedAccountingV2(context.Background()); err == nil {
		t.Fatal("economic conflict startup check weakened")
	}
}

func TestSubmissionConcurrentRefreshedJobs(t *testing.T) {
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	c := storageTestCandidate()
	if _, _, err := s.PutCandidateV2(context.Background(), c, wideAccountingIntentV2(t, c)); err != nil {
		t.Fatal(err)
	}
	intent := wideAccountingIntentV2(t, c)
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		duplicate := refreshedJobCandidate(t, c)
		wg.Add(1)
		go func() {
			defer wg.Done()
			view, disposition, err := s.PutCandidateV2(context.Background(), duplicate, intent)
			if err != nil {
				failures <- err
				return
			}
			if disposition != PutDispositionExisting || !candidateEqual(view.Candidate, c) {
				failures <- fmt.Errorf("concurrent duplicate changed original: %s", disposition)
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if err := s.VerifySubmissionJournalV1(context.Background(), c.ChainID, c.BackendID); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionRefreshedJobReusesFirstFrozenCandidate(t *testing.T) {
	for _, wide := range []bool{false, true} {
		name := "v1"
		if wide {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			s := migratedJournal(t)
			applyWideAccountingMigration(t, s)
			ctx := context.Background()
			original := storageTestCandidate()
			put := func(candidate FrozenCandidateV1) (CandidateViewV1, PutDisposition, error) {
				if wide {
					return s.PutCandidateV2(ctx, candidate, wideAccountingIntentV2(t, candidate))
				}
				return s.PutCandidate(ctx, candidate, storageTestAccountingIntent(candidate))
			}
			if _, disposition, err := put(original); err != nil || disposition != PutDispositionInserted {
				t.Fatalf("initial insertion: disposition=%q err=%v", disposition, err)
			}
			duplicate := refreshedJobCandidate(t, original)
			view, disposition, err := put(duplicate)
			if err != nil || disposition != PutDispositionExisting || view.State != SubmissionStatePending {
				t.Fatalf("same block from refreshed job must reuse first candidate: disposition=%q state=%q err=%v", disposition, view.State, err)
			}
			if !candidateEqual(view.Candidate, original) {
				t.Fatal("refreshed job rewrote the first frozen candidate")
			}
			leased, err := s.ClaimCandidate(ctx, original.CandidateID, [16]byte{7}, time.Minute)
			if err != nil || leased == nil {
				t.Fatalf("claim: candidate=%v err=%v", leased, err)
			}
			prepared, err := s.PrepareAttempt(ctx, leased.Lease, SubmitMethodV2)
			if err != nil {
				t.Fatal(err)
			}
			observation := acceptedObservation(original)
			observation.TemplateSeq = big.NewInt(11708)
			result, err := s.CompleteAttempt(ctx, leased.Lease, prepared.AttemptID, observation, SubmitDecisionAcceptedFinal)
			if err != nil || !result.FirstAccepted {
				t.Fatalf("first acceptance: result=%+v err=%v", result, err)
			}
			view, disposition, err = put(duplicate)
			if err != nil || disposition != PutDispositionExisting || view.State != SubmissionStateAcceptedFinal {
				t.Fatalf("accepted duplicate: disposition=%q state=%q err=%v", disposition, view.State, err)
			}
			if wide {
				first, err := s.ApplyAcceptedAccountingV2(ctx, original.CandidateID)
				if err != nil || !first.Applied {
					t.Fatalf("first accounting: result=%+v err=%v", first, err)
				}
				second, err := s.ApplyAcceptedAccountingV2(ctx, original.CandidateID)
				if err != nil || !second.AlreadyApplied {
					t.Fatalf("duplicate accounting: result=%+v err=%v", second, err)
				}
			}
			var candidates, receipts, conflicts, attempts int
			if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_candidates_v1),(SELECT count(*) FROM accepted_block_submission_receipts_v1),(SELECT count(*) FROM block_submission_conflicts_v1),(SELECT count(*) FROM block_submission_attempts_v1)`).Scan(&candidates, &receipts, &conflicts, &attempts); err != nil {
				t.Fatal(err)
			}
			if candidates != 1 || receipts != 1 || conflicts != 0 || attempts != 1 {
				t.Fatalf("candidate/receipt/conflict/attempt counts = %d/%d/%d/%d, want 1/1/0/1", candidates, receipts, conflicts, attempts)
			}
			if err := s.VerifySubmissionJournalV1(ctx, original.ChainID, original.BackendID); err != nil {
				t.Fatalf("journal verification after refreshed-job duplicate: %v", err)
			}
		})
	}
}

func TestSubmissionRefreshedJobStillRejectsDifferentWork(t *testing.T) {
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	ctx := context.Background()
	original := storageTestCandidate()
	if _, _, err := s.PutCandidateV2(ctx, original, wideAccountingIntentV2(t, original)); err != nil {
		t.Fatal(err)
	}
	modified := refreshedJobCandidate(t, original)
	modified.SharePowHash[0] ^= 1
	payload, err := encodeFrozenCandidateJournalV1(modified)
	if err != nil {
		t.Fatal(err)
	}
	modified.FrozenPayload = payload
	modified.FrozenDigest = sha256.Sum256(payload)
	view, disposition, err := s.PutCandidateV2(ctx, modified, wideAccountingIntentV2(t, modified))
	if err != nil || disposition != PutDispositionConflict || view.State != SubmissionStateConflict {
		t.Fatalf("true work mismatch must fail closed: disposition=%q state=%q err=%v", disposition, view.State, err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, original.ChainID, original.BackendID); err == nil {
		t.Fatal("true mismatch no longer latches startup verification")
	}
}
