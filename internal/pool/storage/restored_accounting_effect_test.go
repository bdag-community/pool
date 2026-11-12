package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/blake2b"
)

const (
	restoredAccountingOptInEnv = "POOL_ACCEPTED_ACCOUNTING_RESTORED_TEST"
	restoredAccountingURLEnv   = "POOL_ACCEPTED_ACCOUNTING_RESTORED_DATABASE_URL"
	restoredAccountingHost     = "127.0.0.1"
	restoredAccountingPort     = "55433"
	restoredAccountingDatabase = "pool_submission_journal_test_restored"
)

func validateRestoredAccountingURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host != restoredAccountingHost+":"+restoredAccountingPort || parsed.Path != "/"+restoredAccountingDatabase || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return errors.New("restored accounting endpoint must be the explicit disposable 127.0.0.1:55433 restored database")
	}
	config, err := pgxpool.ParseConfig(raw)
	if err != nil || config.ConnConfig.Host != restoredAccountingHost || fmt.Sprint(config.ConnConfig.Port) != restoredAccountingPort || config.ConnConfig.Database != restoredAccountingDatabase {
		return errors.New("restored accounting endpoint did not resolve to the explicit disposable target")
	}
	return nil
}

func TestRestoredAccountingURLGuards(t *testing.T) {
	valid := "postgres://user:password@127.0.0.1:55433/" + restoredAccountingDatabase
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"missing_opt_in_is_not_a_target", ""},
		{"localhost", "postgres://user:password@localhost:55433/" + restoredAccountingDatabase},
		{"wrong_port", "postgres://user:password@127.0.0.1:5432/" + restoredAccountingDatabase},
		{"destructive_database", "postgres://user:password@127.0.0.1:55433/pool_submission_journal_test_contract"},
		{"live_database_name", "postgres://user:password@127.0.0.1:55433/bdagpool"},
		{"wrong_restored_name", "postgres://user:password@127.0.0.1:55433/pool_submission_journal_test_restored_copy"},
		{"host_override", valid + "?host=127.0.0.1"},
		{"port_override", valid + "?port=5432"},
		{"database_override", valid + "?dbname=bdagpool"},
		{"schema_override", valid + "?search_path=other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRestoredAccountingURL(tc.raw); err == nil {
				t.Fatal("unsafe restored endpoint was accepted")
			}
		})
	}
	if err := validateRestoredAccountingURL(valid); err != nil {
		t.Fatalf("valid endpoint rejected: %v", err)
	}
}

func restoredAccountingTestURL(t *testing.T) string {
	t.Helper()
	if os.Getenv(restoredAccountingOptInEnv) != "1" {
		t.Skip("restored accounting effect test requires explicit opt-in")
	}
	raw := os.Getenv(restoredAccountingURLEnv)
	if raw == "" {
		t.Fatal("restored accounting effect test URL is required when explicitly opted in")
	}
	if err := validateRestoredAccountingURL(raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

type restoredAccountingSnapshot struct {
	legacyRows  map[string][]string
	catalogRows map[string][]string
	sequences   map[string]restoredSequenceState
}

type restoredSequenceState struct {
	lastValue int64
	isCalled  bool
}

func TestRestoredAccountingPositiveEffectIsExactlyOnce(t *testing.T) {
	raw := restoredAccountingTestURL(t)
	ctx := context.Background()
	storage := NewPostgresStorage(raw)
	if err := storage.Connect(ctx); err != nil {
		t.Fatal("restored endpoint connection failed")
	}
	t.Cleanup(storage.Close)
	if err := storage.VerifySubmissionJournalV1(ctx, big.NewInt(1404), "node"); err != nil {
		t.Fatalf("journal verification: %v", err)
	}
	if err := storage.VerifyAcceptedAccountingV1(ctx); err != nil {
		t.Fatalf("accounting verification: %v", err)
	}

	before := captureRestoredAccountingSnapshot(t, storage)
	candidate, intent := uniqueRestoredCandidate(t, storage)
	if _, disposition, err := storage.PutCandidate(ctx, candidate, intent); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("put candidate disposition=%q err=%v", disposition, err)
	}
	leased, err := storage.ClaimCandidate(ctx, candidate.CandidateID, [16]byte{0x72}, 2*time.Hour)
	if err != nil || leased == nil {
		t.Fatalf("claim candidate=%#v err=%v", leased, err)
	}
	prepared, err := storage.PrepareAttempt(ctx, leased.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := storage.CompleteAttempt(ctx, leased.Lease, prepared.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal)
	if err != nil || !transition.FirstAccepted || transition.State != SubmissionStateAcceptedFinal {
		t.Fatalf("complete accepted candidate=%+v err=%v", transition, err)
	}

	const consumers = 8
	start := make(chan struct{})
	results := make(chan AcceptedAccountingApplyResultV1, consumers)
	errs := make(chan error, consumers)
	var wait sync.WaitGroup
	for i := 0; i < consumers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, applyErr := storage.ApplyAcceptedAccountingV1(ctx, candidate.CandidateID)
			results <- result
			errs <- applyErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for applyErr := range errs {
		if applyErr != nil {
			t.Fatal(applyErr)
		}
	}
	applied, alreadyApplied := 0, 0
	for result := range results {
		if result.Applied {
			applied++
		}
		if result.AlreadyApplied {
			alreadyApplied++
		}
	}
	if applied != 1 || alreadyApplied != consumers-1 {
		t.Fatalf("concurrent effect results applied=%d already_applied=%d", applied, alreadyApplied)
	}

	legacyHash := hex.EncodeToString(intent.LegacyBlockHash[:])
	assertRestoredAccountingEffect(t, storage, candidate, intent, legacyHash)
	storage.Close()
	reopened := NewPostgresStorage(raw)
	if err := reopened.Connect(ctx); err != nil {
		t.Fatal("reopen restored endpoint: ", err)
	}
	t.Cleanup(reopened.Close)
	replay, err := reopened.ApplyAcceptedAccountingV1(ctx, candidate.CandidateID)
	if err != nil || !replay.AlreadyApplied || replay.Applied {
		t.Fatalf("reopen replay=%+v err=%v", replay, err)
	}
	if err := reopened.VerifyAcceptedAccountingV1(ctx); err != nil {
		t.Fatalf("post-replay verification: %v", err)
	}
	assertRestoredAccountingEffect(t, reopened, candidate, intent, legacyHash)
	after := captureRestoredAccountingSnapshot(t, reopened)
	assertRestoredAccountingPreservation(t, before, after, legacyHash)
	assertRestoredAccountingSequenceDelta(t, before.sequences, after.sequences)
}

func uniqueRestoredCandidate(t *testing.T, storage *PostgresStorage) (FrozenCandidateV1, AcceptedAccountingIntentV1) {
	t.Helper()
	ctx := context.Background()
	base := storageTestCandidate()
	for i := 0; i < 256; i++ {
		candidate := base
		candidate.HeaderBytes[0] ^= byte(i + 1)
		candidate.SharePowHash[0] = byte(0x55 + i)
		first := blake2b.Sum256(candidate.HeaderBytes[:])
		candidate.ExpectedBlockHash = blake2b.Sum256(first[:])
		candidate.CandidateID = candidateIDForHashV1(candidate.ExpectedBlockHash)
		payload, err := encodeFrozenCandidateJournalV1(candidate)
		if err != nil {
			t.Fatal(err)
		}
		candidate.FrozenPayload = payload
		candidate.FrozenDigest = sha256.Sum256(payload)
		var candidateExists, blockExists bool
		legacyHash := hex.EncodeToString(candidate.SharePowHash[:])
		if err := storage.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM block_submission_candidates_v1 WHERE candidate_id=$1 OR expected_block_hash=$2), EXISTS (SELECT 1 FROM blocks WHERE hash=$3)`, candidate.CandidateID[:], candidate.ExpectedBlockHash[:], legacyHash).Scan(&candidateExists, &blockExists); err != nil {
			t.Fatal(err)
		}
		if !candidateExists && !blockExists {
			return candidate, storageTestAccountingIntent(candidate)
		}
	}
	t.Fatal("could not find a collision-free restored accounting candidate")
	return FrozenCandidateV1{}, AcceptedAccountingIntentV1{}
}

func assertRestoredAccountingEffect(t *testing.T, storage *PostgresStorage, candidate FrozenCandidateV1, intent AcceptedAccountingIntentV1, legacyHash string) {
	t.Helper()
	ctx := context.Background()
	var effects, blocks, credits, intents, lines, receipts, attempts int
	if err := storage.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM accepted_block_accounting_effects_v1 WHERE candidate_id=$1),
		(SELECT count(*) FROM blocks WHERE hash=$2),
		(SELECT count(*) FROM credits WHERE block_hash=$2),
		(SELECT count(*) FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1),
		(SELECT count(*) FROM accepted_block_accounting_credit_lines_v1 WHERE candidate_id=$1),
		(SELECT count(*) FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1),
		(SELECT count(*) FROM block_submission_attempts_v1 WHERE candidate_id=$1)`, candidate.CandidateID[:], legacyHash).Scan(&effects, &blocks, &credits, &intents, &lines, &receipts, &attempts); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || blocks != 1 || credits != 1 || intents != 1 || lines != 1 || receipts != 1 || attempts != 1 {
		t.Fatalf("effect rows effects=%d blocks=%d credits=%d intents=%d lines=%d receipts=%d attempts=%d", effects, blocks, credits, intents, lines, receipts, attempts)
	}
	var height int64
	var reward, fee, status, miner, amount string
	if err := storage.pool.QueryRow(ctx, `SELECT b.height,b.reward::text,b.fees::text,b.status,c.miner_address,c.amount::text FROM blocks b JOIN credits c ON c.block_hash=b.hash WHERE b.hash=$1`, legacyHash).Scan(&height, &reward, &fee, &status, &miner, &amount); err != nil {
		t.Fatal(err)
	}
	if height != int64(intent.Height) || reward != "1000" || fee != "0" || status != "PENDING" || miner != "miner" || amount != "1000" {
		t.Fatalf("materialized effect height=%d reward=%s fee=%s status=%s miner=%s amount=%s", height, reward, fee, status, miner, amount)
	}
}

func captureRestoredAccountingSnapshot(t *testing.T, storage *PostgresStorage) restoredAccountingSnapshot {
	t.Helper()
	legacyRows := map[string][]string{}
	for table, query := range map[string]string{
		"miners":            `SELECT row_to_json(t)::text FROM (SELECT * FROM miners ORDER BY address) AS t`,
		"blocks":            `SELECT row_to_json(t)::text FROM (SELECT * FROM blocks ORDER BY hash) AS t`,
		"block_submissions": `SELECT row_to_json(t)::text FROM (SELECT * FROM block_submissions ORDER BY id) AS t`,
		"credits":           `SELECT row_to_json(t)::text FROM (SELECT * FROM credits ORDER BY id) AS t`,
		"payouts":           `SELECT row_to_json(t)::text FROM (SELECT * FROM payouts ORDER BY id) AS t`,
	} {
		legacyRows[table] = queryTextRows(t, storage, query)
	}
	catalogRows := map[string][]string{}
	for name, query := range map[string]string{
		"columns":     `SELECT c.relname||':'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull::text||':'||COALESCE(pg_get_expr(ad.adbin,ad.adrelid),'') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname='public' ORDER BY c.relname,a.attnum`,
		"constraints": `SELECT conname||':'||conrelid::regclass::text||':'||pg_get_constraintdef(oid,true) FROM pg_constraint WHERE connamespace='public'::regnamespace ORDER BY conrelid::regclass::text,conname`,
		"indexes":     `SELECT schemaname||':'||tablename||':'||indexname||':'||indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname`,
		"triggers":    `SELECT tg.tgname||':'||tg.tgrelid::regclass::text||':'||p.proname||':'||tg.tgtype::text||':'||tg.tgenabled::text FROM pg_trigger tg JOIN pg_proc p ON p.oid=tg.tgfoid WHERE tg.tgrelid IN (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public') AND NOT tg.tgisinternal ORDER BY tg.tgrelid::regclass::text,tg.tgname`,
	} {
		catalogRows[name] = queryTextRows(t, storage, query)
	}
	return restoredAccountingSnapshot{legacyRows: legacyRows, catalogRows: catalogRows, sequences: captureRestoredSequences(t, storage)}
}

func queryTextRows(t *testing.T, storage *PostgresStorage, query string) []string {
	t.Helper()
	rows, err := storage.pool.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func captureRestoredSequences(t *testing.T, storage *PostgresStorage) map[string]restoredSequenceState {
	t.Helper()
	rows, err := storage.pool.Query(context.Background(), `SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='S' ORDER BY c.relname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	sequences := map[string]restoredSequenceState{}
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			t.Fatal(err)
		}
		var lastValue string
		var isCalled bool
		query := `SELECT last_value::text,is_called FROM ` + (pgx.Identifier{schema, name}).Sanitize()
		if err := storage.pool.QueryRow(context.Background(), query).Scan(&lastValue, &isCalled); err != nil {
			t.Fatal(err)
		}
		value, err := strconv.ParseInt(lastValue, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		sequences[schema+"."+name] = restoredSequenceState{lastValue: value, isCalled: isCalled}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return sequences
}

func assertRestoredAccountingPreservation(t *testing.T, before, after restoredAccountingSnapshot, legacyHash string) {
	t.Helper()
	if err := validateRestoredAccountingPreservation(before, after, legacyHash); err != nil {
		t.Fatal(err)
	}
}

func validateRestoredAccountingPreservation(before, after restoredAccountingSnapshot, legacyHash string) error {
	for _, table := range []string{"miners", "block_submissions", "payouts"} {
		if !equalStringMultisets(before.legacyRows[table], after.legacyRows[table]) {
			return fmt.Errorf("unrelated %s rows changed", table)
		}
	}
	for _, table := range []string{"blocks", "credits"} {
		extra := multisetDifference(before.legacyRows[table], after.legacyRows[table])
		missing := multisetDifference(after.legacyRows[table], before.legacyRows[table])
		if len(extra) != 1 || len(missing) != 0 || !strings.Contains(extra[0], legacyHash) {
			return fmt.Errorf("%s append delta=%v missing=%v", table, extra, missing)
		}
	}
	for name, rows := range before.catalogRows {
		if !equalStringMultisets(rows, after.catalogRows[name]) {
			return fmt.Errorf("catalog %s changed", name)
		}
	}
	return nil
}

func TestRestoredAccountingPreservationRejectsRemovedOrChangedRows(t *testing.T) {
	baseline := restoredAccountingSnapshot{
		legacyRows: map[string][]string{
			"miners":            {"miner"},
			"block_submissions": {"submission"},
			"payouts":           {"payout"},
			"blocks":            {"block-old", "block-keep"},
			"credits":           {"credit-old", "credit-keep"},
		},
		catalogRows: map[string][]string{},
	}
	removed := restoredAccountingSnapshot{
		legacyRows: map[string][]string{
			"miners":            {"miner"},
			"block_submissions": {"submission"},
			"payouts":           {"payout"},
			"blocks":            {"block-keep", "new-block-legacy-hash"},
			"credits":           {"credit-keep", "new-credit-legacy-hash"},
		},
		catalogRows: map[string][]string{},
	}
	if err := validateRestoredAccountingPreservation(baseline, removed, "legacy-hash"); err == nil {
		t.Fatal("preservation validator accepted a removed original row")
	}
	changed := restoredAccountingSnapshot{
		legacyRows: map[string][]string{
			"miners":            {"miner"},
			"block_submissions": {"submission"},
			"payouts":           {"payout"},
			"blocks":            {"block-changed", "block-keep", "new-block-legacy-hash"},
			"credits":           {"credit-changed", "credit-keep", "new-credit-legacy-hash"},
		},
		catalogRows: map[string][]string{},
	}
	if err := validateRestoredAccountingPreservation(baseline, changed, "legacy-hash"); err == nil {
		t.Fatal("preservation validator accepted a changed original row")
	}
}

func assertRestoredAccountingSequenceDelta(t *testing.T, before, after map[string]restoredSequenceState) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("sequence count changed: before=%d after=%d", len(before), len(after))
	}
	creditsSequence := "public.credits_id_seq"
	for name, prior := range before {
		current, ok := after[name]
		if !ok {
			t.Fatalf("sequence %s disappeared", name)
		}
		if name == creditsSequence {
			if current.lastValue != prior.lastValue+1 || !current.isCalled {
				t.Fatalf("credits sequence delta=%+v -> %+v", prior, current)
			}
		} else if current != prior {
			t.Fatalf("unrelated sequence %s changed: %+v -> %+v", name, prior, current)
		}
	}
	if _, ok := before[creditsSequence]; !ok {
		t.Fatal("credits sequence was not present in restored baseline")
	}
}

func equalStringMultisets(a, b []string) bool {
	return len(multisetDifference(a, b)) == 0 && len(multisetDifference(b, a)) == 0
}

func multisetDifference(before, after []string) []string {
	counts := make(map[string]int, len(before))
	for _, row := range before {
		counts[row]++
	}
	var extra []string
	for _, row := range after {
		if counts[row] > 0 {
			counts[row]--
		} else {
			extra = append(extra, row)
		}
	}
	sort.Strings(extra)
	return extra
}
