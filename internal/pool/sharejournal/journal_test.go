package sharejournal

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testJournal(t *testing.T) (*Journal, string) {
	t.Helper()
	raw := os.Getenv("POOL_PAYOUT_OUTBOX_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated test database not configured")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Port() == "55432" || !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "pool_payout_outbox_test_") {
		t.Fatal("refusing non-isolated test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var database string
	if err = pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil || database != strings.TrimPrefix(u.Path, "/") {
		t.Fatal("isolated database identity not verified")
	}
	if _, err = pool.Exec(ctx, `DROP SCHEMA IF EXISTS pool_share_journal_v1 CASCADE`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "20261117_durable_share_journal_v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	j, err := New(pool)
	if err != nil {
		t.Fatal(err)
	}
	return j, raw
}

func testShare(id byte, miner string, work uint64) Share {
	var s Share
	s.ID[31], s.Miner, s.Work = id, miner, work
	return s
}

func TestRejectInvalidShareBeforeDatabaseWrite(t *testing.T) {
	valid := testShare(1, "0x1111111111111111111111111111111111111111", 1)
	for _, tc := range []Share{
		{},
		{ID: valid.ID, Miner: valid.Miner, Work: 0},
		{ID: valid.ID, Miner: "0x0000000000000000000000000000000000000000", Work: 1},
		{ID: valid.ID, Miner: "not-an-address", Work: 1},
		{ID: valid.ID, Miner: valid.Miner, Work: 1, Sequence: 1},
	} {
		if _, err := normalize(tc); err == nil {
			t.Fatal("invalid share accepted")
		}
	}
}

func TestPostgresWindowAcrossPagesAndSequenceGaps(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := j.pool.Exec(ctx, `INSERT INTO pool_share_journal_v1.shares (sequence, share_id, miner_address, work)
 OVERRIDING SYSTEM VALUE SELECT n*2, decode(lpad(to_hex(n),64,'0'),'hex'), $1, 1 FROM generate_series(1,1100) n`,
		"0x1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		target uint64
		count  int
		first  int64
	}{
		{512, 512, 1178}, {650, 650, 902}, {2000, 1100, 2},
	} {
		window, err := j.Window(ctx, 2200, tc.target)
		if err != nil || len(window) != tc.count || window[0].Sequence != tc.first || window[len(window)-1].Sequence != 2200 {
			t.Fatalf("target %d: count %d, error %v", tc.target, len(window), err)
		}
	}
}

func TestPostgresJournalRetainsCutAcrossRestartAndRejectsConflicts(t *testing.T) {
	j, raw := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := testShare(1, "0x1111111111111111111111111111111111111111", 4)
	b := testShare(2, "0x2222222222222222222222222222222222222222", 6)
	if _, _, err := j.Append(context.Background(), a); err == nil {
		t.Fatal("unbounded share append accepted")
	}
	if _, err := j.Window(context.Background(), 1, 1); err == nil {
		t.Fatal("unbounded share window accepted")
	}
	seqA, created, err := j.Append(ctx, a)
	if err != nil || !created {
		t.Fatal("first append failed", err)
	}
	if seq, created, err := j.Append(ctx, a); err != nil || created || seq != seqA {
		t.Fatal("same share replay was not idempotent", err)
	}
	conflict := a
	conflict.Work++
	if _, _, err := j.Append(ctx, conflict); err == nil {
		t.Fatal("conflicting share identity accepted")
	}
	cut, created, err := j.Append(ctx, b)
	if err != nil || !created || cut <= seqA {
		t.Fatal("second append failed", err)
	}
	var candidate [32]byte
	candidate[31] = 10
	if bound, err := j.BindCut(ctx, candidate, &cut); err != nil || bound != cut {
		t.Fatal("candidate cut was not persisted", err)
	}
	if _, _, err = j.Append(ctx, testShare(3, a.Miner, 50)); err != nil {
		t.Fatal(err)
	}
	if bound, err := j.BindCut(ctx, candidate, nil); err != nil || bound != cut {
		t.Fatal("candidate cut moved after additional shares", err)
	}
	if _, err := j.BindCut(ctx, candidate, &seqA); err == nil {
		t.Fatal("candidate accepted a conflicting cut")
	}
	if _, err = j.pool.Exec(ctx, `UPDATE pool_share_journal_v1.shares SET work=1 WHERE sequence=$1`, cut); err == nil {
		t.Fatal("share journal allowed update")
	}
	if _, err = j.pool.Exec(ctx, `DELETE FROM pool_share_journal_v1.shares WHERE sequence=$1`, cut); err == nil {
		t.Fatal("share journal allowed deletion")
	}
	if _, err = j.pool.Exec(ctx, `TRUNCATE pool_share_journal_v1.shares CASCADE`); err == nil {
		t.Fatal("share journal allowed truncation")
	}
	if _, err = j.pool.Exec(ctx, `UPDATE pool_share_journal_v1.cuts SET share_sequence=$1 WHERE candidate_id=$2`, seqA, candidate[:]); err == nil {
		t.Fatal("share journal allowed a discovery cut update")
	}
	if _, err = j.pool.Exec(ctx, `TRUNCATE pool_share_journal_v1.cuts`); err == nil {
		t.Fatal("share journal allowed discovery cuts to be truncated")
	}
	j.pool.Close()
	pool, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	restarted, _ := New(pool)
	if bound, err := restarted.BindCut(ctx, candidate, nil); err != nil || bound != cut {
		t.Fatal("discovery cut was lost on restart", err)
	}
	window, err := restarted.Window(ctx, cut, 7)
	if err != nil || len(window) != 2 || window[0].ID != a.ID || window[1].ID != b.ID || window[0].Work != 4 || window[1].Work != 6 {
		t.Fatalf("restart or boundary lost original discovery cut: %+v err=%v", window, err)
	}
	if _, err := restarted.Window(ctx, cut+100, 7); err == nil {
		t.Fatal("unknown cut accepted")
	}
}

func TestPostgresEmptyDiscoveryCutNeverMoves(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var candidate [32]byte
	candidate[31] = 20
	if cut, err := j.BindCut(ctx, candidate, nil); err != nil || cut != 0 {
		t.Fatal("empty discovery cut not recorded", err)
	}
	seq, _, err := j.Append(ctx, testShare(1, "0x1111111111111111111111111111111111111111", 1))
	if err != nil {
		t.Fatal(err)
	}
	if cut, err := j.BindCut(ctx, candidate, nil); err != nil || cut != 0 {
		t.Fatal("empty discovery cut moved after a later share", err)
	}
	if _, err := j.BindCut(ctx, candidate, &seq); err == nil {
		t.Fatal("empty discovery cut was rebound")
	}
}

func TestPostgresCandidateShareAndCutCommitTogether(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	share := testShare(1, "0x1111111111111111111111111111111111111111", 8)
	var candidate [32]byte
	candidate[31] = 9
	sequence, created, err := j.AppendCandidate(ctx, share, candidate)
	if err != nil || !created || sequence <= 0 {
		t.Fatalf("candidate share not recorded: sequence=%d created=%t err=%v", sequence, created, err)
	}
	if _, _, err := j.Append(ctx, testShare(2, share.Miner, 3)); err != nil {
		t.Fatal(err)
	}
	if replay, created, err := j.AppendCandidate(ctx, share, candidate); err != nil || created || replay != sequence {
		t.Fatal("candidate retry did not preserve its original cut", err)
	}
	conflict := testShare(3, share.Miner, 5)
	if _, _, err := j.AppendCandidate(ctx, conflict, candidate); err == nil {
		t.Fatal("candidate accepted a different discovery share")
	}
	var count int
	if err := j.pool.QueryRow(ctx, `SELECT count(*) FROM pool_share_journal_v1.shares WHERE share_id=$1`, conflict.ID[:]).Scan(&count); err != nil || count != 0 {
		t.Fatal("conflicting discovery left a partially committed share", err)
	}
	if cut, err := j.BindCut(ctx, candidate, nil); err != nil || cut != sequence {
		t.Fatal("discovery cut changed after conflict", err)
	}
}

func TestPostgresCandidateWorkAndPolicyStayFrozenOnRetry(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := testShare(1, "0x1111111111111111111111111111111111111111", 4)
	b := testShare(2, "0x2222222222222222222222222222222222222222", 6)
	if _, _, err := j.Append(ctx, a); err != nil {
		t.Fatal(err)
	}
	var candidate [32]byte
	candidate[31] = 9
	got, err := j.CaptureWork(ctx, candidate, &b, 7, PartialShare)
	if err != nil || !got.Created || got.Total != 7 || got.Work[a.Miner] != 1 || got.Work[b.Miner] != 6 {
		t.Fatalf("incorrect initial candidate allocation: %+v err=%v", got, err)
	}
	if _, _, err := j.Append(ctx, testShare(3, a.Miner, 50)); err != nil {
		t.Fatal(err)
	}
	replay, err := j.CaptureWork(ctx, candidate, &b, 7, PartialShare)
	if err != nil || replay.Created || replay.Sequence != got.Sequence || replay.Total != 7 || replay.Work[a.Miner] != 1 || replay.Work[b.Miner] != 6 {
		t.Fatalf("retry changed candidate allocation: %+v err=%v", replay, err)
	}
	if _, err := j.CaptureWork(ctx, candidate, &b, 7, WholeShare); err == nil {
		t.Fatal("candidate accepted a different boundary policy")
	}
	if _, err := j.CaptureWork(ctx, candidate, &b, 8, PartialShare); err == nil {
		t.Fatal("candidate accepted a different target")
	}
	if _, err := j.pool.Exec(ctx, `UPDATE pool_share_journal_v1.allocation_policies SET target_work=8 WHERE candidate_id=$1`, candidate[:]); err == nil {
		t.Fatal("candidate allocation policy was mutable")
	}
	if _, err := j.pool.Exec(ctx, `TRUNCATE pool_share_journal_v1.allocation_policies`); err == nil {
		t.Fatal("candidate allocation policies could be truncated")
	}
}

func TestPostgresTransactionalCaptureCommitsOrRollsBackWithCandidate(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := j.pool.Exec(ctx, `CREATE TABLE pool_share_journal_v1.test_candidate (id int PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	first := testShare(1, "0x1111111111111111111111111111111111111111", 4)
	discovery := testShare(2, "0x2222222222222222222222222222222222222222", 6)
	if _, _, err := j.Append(ctx, first); err != nil {
		t.Fatal(err)
	}
	var candidate [32]byte
	candidate[31] = 9
	if _, err := CaptureWorkTx(ctx, nil, candidate, &discovery, 7, PartialShare); err == nil {
		t.Fatal("missing transaction accepted")
	}

	begin := func() *pgxpool.Tx {
		raw, err := j.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tx, ok := raw.(*pgxpool.Tx)
		if !ok {
			t.Fatal("expected top-level pool transaction")
		}
		return tx
	}
	count := func(query string, arg ...any) int {
		var n int
		if err := j.pool.QueryRow(ctx, query, arg...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	tx := begin()
	if _, err := tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.test_candidate VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	got, err := CaptureWorkTx(ctx, tx, candidate, &discovery, 7, PartialShare)
	if err != nil || !got.Created || got.Total != 7 || got.Work[first.Miner] != 1 || got.Work[discovery.Miner] != 6 {
		t.Fatalf("incorrect provisional work: %+v err=%v", got, err)
	}
	if count(`SELECT count(*) FROM pool_share_journal_v1.test_candidate`) != 0 || count(`SELECT count(*) FROM pool_share_journal_v1.shares WHERE share_id=$1`, discovery.ID[:]) != 0 {
		t.Fatal("uncommitted candidate or share became visible")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if count(`SELECT count(*) FROM pool_share_journal_v1.cuts WHERE candidate_id=$1`, candidate[:]) != 0 || count(`SELECT count(*) FROM pool_share_journal_v1.allocation_policies WHERE candidate_id=$1`, candidate[:]) != 0 || count(`SELECT count(*) FROM pool_share_journal_v1.shares WHERE share_id=$1`, discovery.ID[:]) != 0 {
		t.Fatal("rollback left candidate work or share behind")
	}

	tx = begin()
	if _, err := tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.test_candidate VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	committed, err := CaptureWorkTx(ctx, tx, candidate, &discovery, 7, PartialShare)
	if err != nil || !committed.Created || committed.Total != 7 {
		t.Fatalf("candidate capture failed: %+v %v", committed, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if count(`SELECT count(*) FROM pool_share_journal_v1.test_candidate WHERE id=1`) != 1 || count(`SELECT count(*) FROM pool_share_journal_v1.shares WHERE share_id=$1`, discovery.ID[:]) != 1 || count(`SELECT count(*) FROM pool_share_journal_v1.cuts WHERE candidate_id=$1`, candidate[:]) != 1 {
		t.Fatal("candidate and share were not committed together")
	}

	conflict := testShare(3, first.Miner, 5)
	tx = begin()
	if _, err := tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.test_candidate VALUES (2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureWorkTx(ctx, tx, candidate, &conflict, 7, PartialShare); err == nil {
		t.Fatal("conflicting candidate cut accepted")
	}
	if err := tx.Commit(ctx); !errors.Is(err, pgx.ErrTxClosed) {
		t.Fatalf("failed capture allowed outer commit: %v", err)
	}
	if count(`SELECT count(*) FROM pool_share_journal_v1.shares WHERE share_id=$1`, conflict.ID[:]) != 0 || count(`SELECT count(*) FROM pool_share_journal_v1.test_candidate WHERE id=2`) != 0 {
		t.Fatal("failed capture left a partial candidate or share")
	}
}

func TestPostgresTransactionalCaptureConnectionLoss(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := j.pool.Exec(ctx, `CREATE TABLE pool_share_journal_v1.test_candidate (id int PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	raw, err := j.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, ok := raw.(*pgxpool.Tx)
	if !ok {
		t.Fatal("expected top-level pool transaction")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.test_candidate VALUES (3)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Conn().Close(ctx); err != nil {
		t.Fatal(err)
	}
	var candidate [32]byte
	candidate[31] = 13
	share := testShare(3, "0x1111111111111111111111111111111111111111", 5)
	if _, err := CaptureWorkTx(ctx, tx, candidate, &share, 7, PartialShare); err == nil {
		t.Fatal("connection loss accepted")
	}
	if err := tx.Commit(ctx); !errors.Is(err, pgx.ErrTxClosed) {
		t.Fatalf("connection loss allowed commit: %v", err)
	}
	var candidates, shares int
	if err := j.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM pool_share_journal_v1.test_candidate), (SELECT count(*) FROM pool_share_journal_v1.shares)`).Scan(&candidates, &shares); err != nil {
		t.Fatal(err)
	}
	if candidates != 0 || shares != 0 {
		t.Fatal("connection loss left a partial candidate or share")
	}
}

func TestPostgresEmptyCandidateAllocationStaysEmpty(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var candidate [32]byte
	candidate[31] = 15
	initial, err := j.CaptureWork(ctx, candidate, nil, 7, WholeShare)
	if err != nil || initial.Sequence != 0 || initial.Total != 0 {
		t.Fatal("empty candidate cut was not recorded", err)
	}
	if _, _, err := j.Append(ctx, testShare(1, "0x1111111111111111111111111111111111111111", 10)); err != nil {
		t.Fatal(err)
	}
	replay, err := j.CaptureWork(ctx, candidate, nil, 7, WholeShare)
	if err != nil || replay.Sequence != 0 || replay.Total != 0 || len(replay.Work) != 0 {
		t.Fatal("empty candidate cut changed after later share", err)
	}
}

func TestPostgresJournalRefusesCorruptMinerIdentity(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	seq, _, err := j.Append(ctx, testShare(1, "0x1111111111111111111111111111111111111111", 1))
	if err != nil {
		t.Fatal(err)
	}
	// This simulated privileged corruption is confined to the disposable test
	// database; normal writes cannot bypass the append-only trigger.
	if _, err = j.pool.Exec(ctx, `ALTER TABLE pool_share_journal_v1.shares DISABLE TRIGGER immutable_share;
UPDATE pool_share_journal_v1.shares SET miner_address='0xgggggggggggggggggggggggggggggggggggggggg';
ALTER TABLE pool_share_journal_v1.shares ENABLE TRIGGER immutable_share`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Window(ctx, seq, 1); err == nil {
		t.Fatal("corrupt persisted miner identity accepted")
	}
}

func TestPostgresConcurrentAppendsHaveStableCut(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	sequences := make(chan int64, 16)
	for i := byte(1); i <= 16; i++ {
		wg.Add(1)
		go func(id byte) {
			defer wg.Done()
			seq, _, err := j.Append(ctx, testShare(id, "0x1111111111111111111111111111111111111111", 1))
			if err != nil {
				errs <- err
				return
			}
			sequences <- seq
		}(i)
	}
	wg.Wait()
	close(errs)
	close(sequences)
	for err := range errs {
		t.Fatal(err)
	}
	var cut int64
	for seq := range sequences {
		if seq > cut {
			cut = seq
		}
	}
	window, err := j.Window(ctx, cut, 16)
	if err != nil || len(window) != 16 {
		t.Fatalf("concurrent committed window count = %d, err=%v", len(window), err)
	}
	for i := 1; i < len(window); i++ {
		if window[i-1].Sequence >= window[i].Sequence {
			t.Fatal("share window is not in commit order")
		}
	}
}

func TestPostgresConcurrentDuplicateShareHasOneRecord(t *testing.T) {
	j, _ := testJournal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	share := testShare(1, "0x1111111111111111111111111111111111111111", 10)
	type result struct {
		sequence int64
		created  bool
		err      error
	}
	results := make(chan result, 12)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seq, created, err := j.Append(ctx, share)
			results <- result{seq, created, err}
		}()
	}
	wg.Wait()
	close(results)
	var sequence int64
	createdCount := 0
	for result := range results {
		if result.err != nil || result.sequence <= 0 {
			t.Fatalf("duplicate append failed: %+v", result)
		}
		if sequence == 0 {
			sequence = result.sequence
		} else if sequence != result.sequence {
			t.Fatal("duplicate share returned different sequences")
		}
		if result.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created %d records for one share", createdCount)
	}
}
