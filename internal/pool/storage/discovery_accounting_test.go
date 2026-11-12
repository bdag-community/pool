package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type discoveryWriter interface {
	DiscoverCandidateV2(context.Context, FrozenCandidateV1, func() (AcceptedAccountingIntentV2, error)) (CandidateViewV1, PutDisposition, error)
}

func TestDiscoveryActualIncomingDigestReconstruction(t *testing.T) {
	raw := os.Getenv("POOL_DISCOVERY_ACTUAL_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated actual dump required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/pool_submission_journal_test_actual_discovery") {
		t.Fatal("non-isolated fixture")
	}
	s := NewPostgresStorage(raw)
	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b, _ := hex.DecodeString("682c5550eab9725eb33e1fd158cad1e52299ae98f0ab50846e6f1f50f7c7d7c8")
	var cid [32]byte
	copy(cid[:], b)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	first, err := readAcceptedAccountingIntentTxV2(ctx, tx, cid, false)
	if err != nil {
		t.Fatal(err)
	}
	if first.TotalWork != 5000000000 || len(first.CreditLines) != 1 {
		t.Fatal("actual fixture changed")
	}
	second := first
	second.CreditLines = append([]AcceptedAccountingCreditLineV2(nil), first.CreditLines...)
	var incoming []byte
	if err := tx.QueryRow(ctx, "SELECT incoming_intent_digest FROM accepted_block_accounting_conflicts_v1 WHERE candidate_id=$1", cid[:]).Scan(&incoming); err != nil {
		t.Fatal(err)
	}
	for work := uint64(50000000); work <= 5500000000; work += 50000000 {
		for _, reward := range []string{first.GrossReward, "10000000000000000000"} {
			second.TotalWork = work
			second.CreditLines[0].Work = work
			second.GrossReward = reward
			second.Distributable = reward
			second.CreditLines[0].Amount = reward
			second, err = SealAcceptedAccountingIntentV2(second)
			if err != nil {
				t.Fatal(err)
			}
			if second.IntentDigest == *(*[32]byte)(incoming) {
				t.Logf("ACTUAL_DIGEST_RECONSTRUCTED: work=%d gross=%s recipient=%s", work, reward, second.CreditLines[0].MinerAddress)
				return
			}
		}
	}
	t.Fatalf("bounded work/reward reconstruction did not match incoming digest %x", incoming)
}

func TestDiscoveryFreezesAllocationOnlyOnce(t *testing.T) {
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	writer, ok := any(s).(discoveryWriter)
	if !ok {
		t.Fatal("missing first-discovery accounting entrypoint")
	}
	c := storageTestCandidate()
	ctx := context.Background()
	calls := 0
	freeze := func() (AcceptedAccountingIntentV2, error) { calls++; return wideAccountingIntentV2(t, c), nil }
	if _, d, e := writer.DiscoverCandidateV2(ctx, c, freeze); e != nil || d != PutDispositionInserted {
		t.Fatalf("first discovery: %s %v", d, e)
	}
	replay := refreshedJobCandidate(t, c)
	changed := func() (AcceptedAccountingIntentV2, error) {
		calls++
		a := wideAccountingIntentV2(t, c)
		a.TotalWork++
		a.CreditLines[0].Work++
		return SealAcceptedAccountingIntentV2(a)
	}
	if v, d, e := writer.DiscoverCandidateV2(ctx, replay, changed); e != nil || d != PutDispositionExisting || !candidateEqual(v.Candidate, c) {
		t.Fatalf("replay reallocated: %s %v", d, e)
	}
	if calls != 1 {
		t.Fatalf("allocation factory called %d times", calls)
	}
	if e := s.VerifyAcceptedAccountingV2(ctx); e != nil {
		t.Fatal(e)
	}
}

func TestDiscoveryConcurrentFirstWriterOwnsAllocation(t *testing.T) {
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	writer, ok := any(s).(discoveryWriter)
	if !ok {
		t.Fatal("missing first-discovery accounting entrypoint")
	}
	c := storageTestCandidate()
	var calls atomic.Int32
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, e := writer.DiscoverCandidateV2(context.Background(), c, func() (AcceptedAccountingIntentV2, error) { calls.Add(1); return wideAccountingIntentV2(t, c), nil })
			if e != nil {
				failures <- e
			}
		}()
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent allocation factory ran %d times", calls.Load())
	}
	if e := s.VerifyAcceptedAccountingV2(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func TestDiscoveryFactoryFailureRollsBackCandidate(t *testing.T) {
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	c := storageTestCandidate()
	ctx := context.Background()
	if _, _, err := s.DiscoverCandidateV2(ctx, c, func() (AcceptedAccountingIntentV2, error) {
		return AcceptedAccountingIntentV2{}, errors.New("injected allocation failure")
	}); err == nil {
		t.Fatal("factory failure ignored")
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_candidates_v1)+(SELECT count(*) FROM accepted_block_accounting_intents_v1)`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial first discovery: %d %v", count, err)
	}
	if _, d, err := s.DiscoverCandidateV2(ctx, c, func() (AcceptedAccountingIntentV2, error) { return wideAccountingIntentV2(t, c), nil }); err != nil || d != PutDispositionInserted {
		t.Fatalf("retry: %s %v", d, err)
	}
}
