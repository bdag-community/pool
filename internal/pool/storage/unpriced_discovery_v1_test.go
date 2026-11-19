package storage

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"asicPool/internal/pool/sharejournal"
)

func unpricedFixture(t *testing.T) (*PostgresStorage, context.Context) {
	t.Helper()
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	if _, err := s.pool.Exec(ctx, `DROP SCHEMA IF EXISTS pool_reward_observations_v1 CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `DROP SCHEMA IF EXISTS pool_share_journal_v1 CASCADE`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"20261117_durable_share_journal_v1.sql", "20261117_unpriced_discovery_v1.sql"} {
		path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, string(data)); err != nil {
			t.Fatalf("isolated migration %s: %v", name, err)
		}
	}
	return s, ctx
}

func unpricedShare(candidate FrozenCandidateV1) *sharejournal.Share {
	return &sharejournal.Share{ID: candidate.SharePowHash, Miner: "0x2222222222222222222222222222222222222222", Work: 6}
}

func unpricedCounts(t *testing.T, s *PostgresStorage, ctx context.Context) (int, int, int, int, int) {
	t.Helper()
	var candidates, shares, discoveries, intents, credits int
	err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_candidates_v1), (SELECT count(*) FROM pool_share_journal_v1.shares), (SELECT count(*) FROM pool_share_journal_v1.unpriced_discoveries_v1), (SELECT count(*) FROM accepted_block_accounting_intents_v1), (SELECT count(*) FROM credits)`).Scan(&candidates, &shares, &discoveries, &intents, &credits)
	if err != nil {
		t.Fatal(err)
	}
	return candidates, shares, discoveries, intents, credits
}

func TestPostgresUnpricedDiscoveryAtomicAndReplay(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	j, err := sharejournal.New(s.pool)
	if err != nil {
		t.Fatal(err)
	}
	prior := sharejournal.Share{ID: [32]byte{1}, Miner: "0x1111111111111111111111111111111111111111", Work: 4}
	if _, _, err = j.Append(ctx, prior); err != nil {
		t.Fatal(err)
	}
	share := unpricedShare(candidate)
	got, disposition, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.PartialShare, 0)
	if err != nil || disposition != PutDispositionInserted || got.Candidate.State != SubmissionStatePending || got.Work.Total != 7 || got.Work.Work[prior.Miner] != 1 || got.Work.Work[share.Miner] != 6 {
		t.Fatalf("insert: %+v %s %v", got, disposition, err)
	}
	candidates, shares, discoveries, intents, credits := unpricedCounts(t, s, ctx)
	if candidates != 1 || shares != 2 || discoveries != 1 || intents != 0 || credits != 0 {
		t.Fatalf("unexpected candidate/share/discovery/intent/credit counts: %d/%d/%d/%d/%d", candidates, shares, discoveries, intents, credits)
	}
	if err := s.VerifyAcceptedAccountingV2(ctx); err == nil {
		t.Fatal("legacy startup verification accepted an unpriced candidate")
	}
	later := sharejournal.Share{ID: [32]byte{2}, Miner: prior.Miner, Work: 50}
	if _, _, err = j.Append(ctx, later); err != nil {
		t.Fatal(err)
	}
	replay, disposition, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.PartialShare, 0)
	if err != nil || disposition != PutDispositionExisting || replay.Work.Created || replay.Work.Sequence != got.Work.Sequence || replay.Work.Total != got.Work.Total || replay.WorkDigest != got.WorkDigest {
		t.Fatalf("replay changed original cut: %+v %s %v", replay, disposition, err)
	}
	if _, disposition, err = s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.PartialShare, 1); err == nil || disposition != PutDispositionConflict {
		t.Fatal("changed fee policy accepted")
	}
	var conflictCount int
	var state string
	if err = s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_conflicts_v1), state FROM block_submission_state_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&conflictCount, &state); err != nil || conflictCount != 1 || state != SubmissionStateConflict {
		t.Fatalf("changed fee did not durably latch conflict: count=%d state=%s err=%v", conflictCount, state, err)
	}
	var reason, incomingFee, incomingTarget, incomingPolicy, incomingMiner, incomingWork string
	var originalDigest, incomingShare, incomingPayload []byte
	err = s.pool.QueryRow(ctx, `SELECT u.reason,u.incoming_fee_bits::text,u.incoming_target_work::text,u.incoming_policy,u.incoming_miner_address,u.incoming_share_work::text,u.original_work_digest,u.incoming_share_id,c.incoming_frozen_payload FROM pool_share_journal_v1.unpriced_conflicts_v1 u JOIN block_submission_conflicts_v1 c USING(conflict_id) WHERE u.candidate_id=$1`, candidate.CandidateID[:]).Scan(&reason, &incomingFee, &incomingTarget, &incomingPolicy, &incomingMiner, &incomingWork, &originalDigest, &incomingShare, &incomingPayload)
	if err != nil || reason != "discovery_policy" || incomingFee != strconv.FormatUint(math.Float64bits(1), 10) || incomingTarget != "7" || incomingPolicy != string(sharejournal.PartialShare) || incomingMiner != share.Miner || incomingWork != "6" || !bytes.Equal(originalDigest, got.WorkDigest[:]) || !bytes.Equal(incomingShare, share.ID[:]) || !bytes.Equal(incomingPayload, candidate.FrozenPayload) {
		t.Fatalf("fee conflict evidence is incomplete: %v", err)
	}
	conflict := *share
	conflict.Work++
	if _, _, err = s.PutUnpricedDiscoveryV1(ctx, candidate, &conflict, 7, sharejournal.PartialShare, 0); err == nil {
		t.Fatal("changed discovery work accepted")
	}
	if _, err = s.pool.Exec(ctx, `UPDATE pool_share_journal_v1.unpriced_discoveries_v1 SET total_work=0 WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("discovery is mutable")
	}
	if _, err = s.pool.Exec(ctx, `DELETE FROM pool_share_journal_v1.unpriced_discoveries_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("discovery can be deleted")
	}
	if _, err = s.pool.Exec(ctx, `TRUNCATE pool_share_journal_v1.unpriced_discoveries_v1`); err == nil {
		t.Fatal("discovery can be truncated")
	}
	if _, err = s.pool.Exec(ctx, `UPDATE pool_share_journal_v1.unpriced_conflicts_v1 SET incoming_fee_bits=0`); err == nil {
		t.Fatal("conflict evidence is mutable")
	}
	if _, err = s.pool.Exec(ctx, `DELETE FROM pool_share_journal_v1.unpriced_conflicts_v1`); err == nil {
		t.Fatal("conflict evidence can be deleted")
	}
	if _, err = s.pool.Exec(ctx, `TRUNCATE pool_share_journal_v1.unpriced_conflicts_v1`); err == nil {
		t.Fatal("conflict evidence can be truncated")
	}
}

func TestPostgresUnpricedDiscoveryChangedWorkRejectsWithoutLatching(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	share := unpricedShare(candidate)
	original, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
	if err != nil {
		t.Fatal(err)
	}
	changed := *share
	changed.Work++
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, &changed, 7, sharejournal.WholeShare, 0); err == nil {
		t.Fatal("changed work retry must be rejected")
	}
	var conflictCount int
	var state string
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_conflicts_v1), state FROM block_submission_state_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&conflictCount, &state); err != nil || conflictCount != 0 || state != SubmissionStatePending {
		t.Fatalf("changed work altered submission state: count=%d state=%s err=%v", conflictCount, state, err)
	}
	retry, disposition, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
	if err != nil || disposition != PutDispositionExisting || retry.WorkDigest != original.WorkDigest || retry.Work.Sequence != original.Work.Sequence {
		t.Fatalf("original discovery could not be retried unchanged: %s %v", disposition, err)
	}
	changed = *share
	changed.Miner = "0x3333333333333333333333333333333333333333"
	if _, disposition, err := s.PutUnpricedDiscoveryV1(ctx, candidate, &changed, 7, sharejournal.WholeShare, 0); err == nil || disposition != PutDispositionConflict {
		t.Fatal("changed miner must still latch an integrity conflict")
	}
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_conflicts_v1), state FROM block_submission_state_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&conflictCount, &state); err != nil || conflictCount != 1 || state != SubmissionStateConflict {
		t.Fatalf("changed miner did not latch: count=%d state=%s err=%v", conflictCount, state, err)
	}
}

func TestPostgresUnpricedDiscoveryDoesNotLatchLegacyCandidate(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	if _, _, err := s.PutCandidate(ctx, candidate, storageTestAccountingIntent(candidate)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, unpricedShare(candidate), 7, sharejournal.WholeShare, 0); err == nil {
		t.Fatal("legacy candidate accepted as unpriced")
	}
	var conflictCount int
	var state string
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM block_submission_conflicts_v1), state FROM block_submission_state_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&conflictCount, &state); err != nil || conflictCount != 0 || state != SubmissionStatePending {
		t.Fatalf("legacy candidate changed: count=%d state=%s err=%v", conflictCount, state, err)
	}
}

func TestPostgresUnpricedDiscoveryFailureAndCommitAckLoss(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	share := unpricedShare(candidate)
	t.Cleanup(func() { submissionFaultHookV1 = nil })
	for _, point := range []string{"unpriced_candidate_insert", "unpriced_marker_insert", "unpriced_discovery_commit"} {
		submissionFaultHookV1 = func(got string, after bool) error {
			if got == point && !after {
				return errors.New("injected failure before commit")
			}
			return nil
		}
		if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0); err == nil {
			t.Fatalf("fault %s accepted", point)
		}
		candidates, shares, discoveries, intents, credits := unpricedCounts(t, s, ctx)
		if candidates != 0 || shares != 0 || discoveries != 0 || intents != 0 || credits != 0 {
			t.Fatalf("fault %s left partial writes", point)
		}
	}
	submissionFaultHookV1 = func(point string, after bool) error {
		if point == "unpriced_discovery_commit" && after {
			return errors.New("injected lost commit acknowledgement")
		}
		return nil
	}
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0); err == nil {
		t.Fatal("lost acknowledgement reported success")
	}
	submissionFaultHookV1 = nil
	replay, disposition, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
	if err != nil || disposition != PutDispositionExisting || replay.Work.Created || replay.Work.Total != 6 {
		t.Fatalf("lost acknowledgement recovery: %+v %s %v", replay, disposition, err)
	}
	candidates, shares, discoveries, intents, credits := unpricedCounts(t, s, ctx)
	if candidates != 1 || shares != 1 || discoveries != 1 || intents != 0 || credits != 0 {
		t.Fatal("acknowledgement recovery duplicated or priced discovery")
	}
}

func TestPostgresUnpricedDiscoveryConcurrentFirstWriter(t *testing.T) {
	s, ctx := unpricedFixture(t)
	candidate := storageTestCandidate()
	share := unpricedShare(candidate)
	type result struct {
		disposition PutDisposition
		digest      [32]byte
		err         error
	}
	results := make(chan result, 4)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, disposition, err := s.PutUnpricedDiscoveryV1(ctx, candidate, share, 7, sharejournal.WholeShare, 0)
			results <- result{disposition, got.WorkDigest, err}
		}()
	}
	wg.Wait()
	close(results)
	var inserted, existing int
	var digest [32]byte
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if digest == ([32]byte{}) {
			digest = result.digest
		} else if digest != result.digest {
			t.Fatal("concurrent discovery changed work")
		}
		switch result.disposition {
		case PutDispositionInserted:
			inserted++
		case PutDispositionExisting:
			existing++
		default:
			t.Fatal(result.disposition)
		}
	}
	if inserted != 1 || existing != 3 {
		t.Fatalf("first writer results: %d inserted, %d existing", inserted, existing)
	}
	candidates, shares, discoveries, intents, credits := unpricedCounts(t, s, ctx)
	if candidates != 1 || shares != 1 || discoveries != 1 || intents != 0 || credits != 0 {
		t.Fatal("concurrent discovery duplicated or priced work")
	}
}

func TestPostgresUnpricedCatalogFingerprintDetectsDrift(t *testing.T) {
	s, ctx := unpricedFixture(t)
	baseline, err := sharejournal.CatalogFingerprint(ctx, s.pool)
	if err != nil || len(baseline) != 64 {
		t.Fatalf("catalog fingerprint unavailable: %s %v", baseline, err)
	}
	if baseline != sharejournal.ReviewedCatalogFingerprint {
		t.Fatalf("isolated migration differs from reviewed fingerprint: %s", baseline)
	}
	if err := sharejournal.VerifyCatalog(ctx, s.pool); err != nil {
		t.Fatalf("reviewed catalog rejected: %v", err)
	}
	if err := sharejournal.VerifyCatalog(context.Background(), s.pool); err == nil {
		t.Fatal("unbounded catalog verification accepted")
	}
	if err := sharejournal.VerifyCatalog(ctx, nil); err == nil {
		t.Fatal("missing catalog database accepted")
	}
	outer, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = outer.Exec(ctx, `SET LOCAL search_path=public`); err != nil {
		t.Fatal(err)
	}
	var beforePath, afterPath string
	if err = outer.QueryRow(ctx, `SHOW search_path`).Scan(&beforePath); err != nil {
		t.Fatal(err)
	}
	if _, err = sharejournal.CatalogFingerprint(ctx, outer); err != nil {
		t.Fatal(err)
	}
	if err = sharejournal.VerifyCatalog(ctx, outer); err != nil {
		t.Fatal(err)
	}
	if err = outer.QueryRow(ctx, `SHOW search_path`).Scan(&afterPath); err != nil || afterPath != beforePath {
		t.Fatalf("catalog check changed caller search_path: before=%q after=%q err=%v", beforePath, afterPath, err)
	}
	if err = outer.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("UNPRICED_CATALOG_FINGERPRINT=%s", baseline)
	candidate := storageTestCandidate()
	if _, _, err := s.PutUnpricedDiscoveryV1(ctx, candidate, unpricedShare(candidate), 7, sharejournal.WholeShare, 0); err != nil {
		t.Fatal(err)
	}
	afterData, err := sharejournal.CatalogFingerprint(ctx, s.pool)
	if err != nil || afterData != baseline {
		t.Fatalf("journal data changed structural fingerprint: %s %v", afterData, err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"disabled trigger", `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 DISABLE TRIGGER immutable_discovery`},
		{"disabled conflict trigger", `ALTER TABLE pool_share_journal_v1.unpriced_conflicts_v1 DISABLE TRIGGER immutable_unpriced_conflict`},
		{"weakened column", `ALTER TABLE pool_share_journal_v1.unpriced_discoveries_v1 ALTER COLUMN work_digest DROP NOT NULL`},
		{"missing foreign key", `ALTER TABLE pool_share_journal_v1.unpriced_conflicts_v1 DROP CONSTRAINT unpriced_conflicts_v1_candidate_id_fkey`},
		{"added table", `CREATE TABLE pool_share_journal_v1.unexpected (id int)`},
		{"missing schema", `DROP SCHEMA pool_share_journal_v1 CASCADE`},
		{"public grant", `GRANT SELECT ON pool_share_journal_v1.shares TO PUBLIC`},
		{"changed sequence", `ALTER SEQUENCE pool_share_journal_v1.shares_sequence_seq INCREMENT BY 2`},
		{"changed trigger function", `CREATE OR REPLACE FUNCTION pool_share_journal_v1.immutable_share() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'changed'; END; $$`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err = tx.Exec(ctx, tc.sql); err != nil {
				t.Fatal(err)
			}
			changed, err := sharejournal.CatalogFingerprint(ctx, tx)
			if err != nil || changed == baseline {
				t.Fatalf("catalog drift not detected: %s %v", changed, err)
			}
			if err := sharejournal.VerifyCatalog(ctx, tx); err == nil {
				t.Fatal("changed catalog passed verification")
			}
		})
	}
	afterRollback, err := sharejournal.CatalogFingerprint(ctx, s.pool)
	if err != nil || afterRollback != baseline {
		t.Fatalf("catalog rollback did not restore fingerprint: %s %v", afterRollback, err)
	}
}
