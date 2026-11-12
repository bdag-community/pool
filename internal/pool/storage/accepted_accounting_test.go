package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func acceptedAccountingJournal(t *testing.T) *PostgresStorage {
	t.Helper()
	return migratedJournal(t)
}

func acceptedAccountingReceipt(t *testing.T, s *PostgresStorage) (FrozenCandidateV1, AcceptedAccountingIntentV1) {
	t.Helper()
	candidate := storageTestCandidate()
	intent := storageTestAccountingIntent(candidate)
	if _, disposition, err := s.PutCandidate(context.Background(), candidate, intent); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("put candidate: %q %v", disposition, err)
	}
	leased, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{1}, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim candidate: %#v %v", leased, err)
	}
	prepared, err := s.PrepareAttempt(context.Background(), leased.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := s.CompleteAttempt(context.Background(), leased.Lease, prepared.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal)
	if err != nil || transition.State != SubmissionStateAcceptedFinal || !transition.FirstAccepted {
		t.Fatalf("complete accepted candidate: %+v %v", transition, err)
	}
	return candidate, intent
}

func wideAccountingIntentV2(t *testing.T, candidate FrozenCandidateV1) AcceptedAccountingIntentV2 {
	t.Helper()
	intent, err := SealAcceptedAccountingIntentV2(AcceptedAccountingIntentV2{
		CandidateID: candidate.CandidateID, LegacyBlockHash: candidate.SharePowHash, Height: candidate.Height.Uint64(),
		GrossReward: "260000000000000000000", PoolFeeFloatBits: math.Float64bits(2.5), FeeAmount: "6500000000000000000", Distributable: "253500000000000000000", TotalWork: 3,
		CreditLines: []AcceptedAccountingCreditLineV2{{MinerAddress: "miner-a", Work: 1, Amount: "84500000000000000000"}, {MinerAddress: "miner-b", Work: 2, Amount: "169000000000000000000"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func acceptedAccountingV2Receipt(t *testing.T, s *PostgresStorage) (FrozenCandidateV1, AcceptedAccountingIntentV2) {
	t.Helper()
	candidate := storageTestCandidate()
	intent := wideAccountingIntentV2(t, candidate)
	if _, disposition, err := s.PutCandidateV2(context.Background(), candidate, intent); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("PutCandidateV2 disposition=%q err=%v", disposition, err)
	}
	leased, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{2}, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim V2 candidate=%v err=%v", leased, err)
	}
	prepared, err := s.PrepareAttempt(context.Background(), leased.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	observation := acceptedObservation(candidate)
	observation.TemplateSeq = big.NewInt(11708) // Independent Core response counter.
	transition, err := s.CompleteAttempt(context.Background(), leased.Lease, prepared.AttemptID, observation, SubmitDecisionAcceptedFinal)
	if err != nil || !transition.FirstAccepted {
		t.Fatalf("complete V2 acceptance=%+v err=%v", transition, err)
	}
	var coreSequence string
	if err := s.pool.QueryRow(context.Background(), `SELECT core_template_seq::text FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&coreSequence); err != nil || coreSequence != "11708" {
		t.Fatalf("Core counter not retained: %q err=%v", coreSequence, err)
	}
	return candidate, intent
}

func TestAcceptedAccountingV2WideWeiMaterializesReplaysAndReadsUnpaid(t *testing.T) {
	s := acceptedAccountingJournal(t)
	applyWideAccountingMigration(t, s)
	candidate, _ := acceptedAccountingV2Receipt(t, s)
	first, err := s.ApplyAcceptedAccountingV2(context.Background(), candidate.CandidateID)
	if err != nil || !first.Applied {
		t.Fatalf("first V2 materialization=%+v err=%v", first, err)
	}
	second, err := s.ApplyAcceptedAccountingV2(context.Background(), candidate.CandidateID)
	if err != nil || !second.AlreadyApplied {
		t.Fatalf("V2 replay=%+v err=%v", second, err)
	}
	legacyHash := hex.EncodeToString(candidate.SharePowHash[:])
	credits, err := s.GetUnpaidCredits(context.Background(), legacyHash)
	if err != nil || len(credits) != 2 {
		t.Fatalf("wide unpaid credits=%+v err=%v", credits, err)
	}
	amounts := map[string]string{}
	for _, credit := range credits {
		amounts[credit.MinerAddress] = credit.Amount
	}
	if amounts["miner-a"] != "84500000000000000000" || amounts["miner-b"] != "169000000000000000000" {
		t.Fatalf("wide unreadable credit values=%+v", credits)
	}
	if err := s.VerifyAcceptedAccountingV2(context.Background()); err != nil {
		t.Fatalf("V2 verification: %v", err)
	}
	url := s.dbURL
	s.Close()
	reopened := NewPostgresStorage(url)
	if err := reopened.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	if replay, err := reopened.ApplyAcceptedAccountingV2(context.Background(), candidate.CandidateID); err != nil || !replay.AlreadyApplied {
		t.Fatalf("reopened V2 replay=%+v err=%v", replay, err)
	}
}

func TestAcceptedAccountingV2DispatchesHistoricalV1AndReplays(t *testing.T) {
	s := acceptedAccountingJournal(t)
	candidate, original := acceptedAccountingReceipt(t, s)
	applyWideAccountingMigration(t, s)
	first, err := s.ApplyAcceptedAccountingV2(context.Background(), candidate.CandidateID)
	if err != nil || !first.Applied {
		t.Fatalf("historical V1 through current API: %+v err=%v", first, err)
	}
	second, err := s.ApplyAcceptedAccountingV2(context.Background(), candidate.CandidateID)
	if err != nil || !second.AlreadyApplied {
		t.Fatalf("historical V1 replay: %+v err=%v", second, err)
	}
	var digest []byte
	if err := s.pool.QueryRow(context.Background(), `SELECT intent_digest FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(digest) != hex.EncodeToString(original.IntentDigest[:]) {
		t.Fatal("historical V1 intent digest changed")
	}
	if err := s.VerifyAcceptedAccountingV2(context.Background()); err != nil {
		t.Fatalf("historical V1 under widened catalog: %v", err)
	}
}

func TestAcceptedAccountingV2MissingIntentFailsClosed(t *testing.T) {
	s := acceptedAccountingJournal(t)
	applyWideAccountingMigration(t, s)
	candidate, _ := acceptedAccountingV2Receipt(t, s)
	for _, statement := range []string{
		`ALTER TABLE accepted_block_accounting_credit_lines_v1 DISABLE TRIGGER accepted_accounting_credit_lines_v1_immutable`,
		`ALTER TABLE accepted_block_accounting_intents_v1 DISABLE TRIGGER accepted_block_accounting_intents_v1_immutable`,
	} {
		if _, err := s.pool.Exec(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`DELETE FROM accepted_block_accounting_credit_lines_v1 WHERE candidate_id=$1`,
		`DELETE FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1`,
	} {
		if _, err := s.pool.Exec(context.Background(), statement, candidate.CandidateID[:]); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`ALTER TABLE accepted_block_accounting_credit_lines_v1 ENABLE TRIGGER accepted_accounting_credit_lines_v1_immutable`,
		`ALTER TABLE accepted_block_accounting_intents_v1 ENABLE TRIGGER accepted_block_accounting_intents_v1_immutable`,
	} {
		if _, err := s.pool.Exec(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.VerifyAcceptedAccountingV2(context.Background()); err == nil {
		t.Fatal("V2 verifier accepted an accepted receipt without its intent")
	}
	if _, err := s.ReconcileAcceptedAccountingV2(context.Background()); err == nil {
		t.Fatal("V2 reconciliation skipped an accepted receipt without its intent")
	}
}

func TestAcceptedAccountingV2PreexistingLegacyRowsPersistConflict(t *testing.T) {
	s := acceptedAccountingJournal(t)
	applyWideAccountingMigration(t, s)
	candidate, intent := acceptedAccountingV2Receipt(t, s)
	legacy := hex.EncodeToString(intent.LegacyBlockHash[:])
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO blocks(hash,height,reward,fees,status) VALUES($1,$2,$3,$4,'PENDING')`, legacy, int64(intent.Height), intent.GrossReward, intent.FeeAmount); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyAcceptedAccountingV2(context.Background(), candidate.CandidateID); err == nil {
		t.Fatal("V2 adopted pre-existing legacy rows")
	}
	var conflicts, effects int
	var state string
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM accepted_block_accounting_conflicts_v1),(SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT state FROM block_submission_state_v1 WHERE candidate_id=$1)`, candidate.CandidateID[:]).Scan(&conflicts, &effects, &state); err != nil {
		t.Fatal(err)
	}
	if conflicts != 1 || effects != 0 || state != SubmissionStateConflict {
		t.Fatalf("V2 conflict was not durable conflicts=%d effects=%d state=%q", conflicts, effects, state)
	}
	if err := s.VerifyAcceptedAccountingV2(context.Background()); err == nil {
		t.Fatal("V2 verifier ignored durable accounting conflict")
	}
}

func TestAcceptedAccountingV2ExpectedHashCollisionLatchesExistingCandidate(t *testing.T) {
	s := acceptedAccountingJournal(t)
	applyWideAccountingMigration(t, s)
	candidate := storageTestCandidate()
	existingID := candidate.CandidateID
	existingID[0] ^= 0xff
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO block_submission_candidates_v1(candidate_id,frozen_digest,frozen_payload,chain_id,expected_block_hash,header_bytes,extra_nonce2,nonce_bytes,share_pow_hash,parent_hash,height,template_seq,backend_id,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,clock_timestamp())`, existingID[:], candidate.FrozenDigest[:], candidate.FrozenPayload, candidate.ChainID.String(), candidate.ExpectedBlockHash[:], candidate.HeaderBytes[:], candidate.ExtraNonce2[:], candidate.NonceBytes[:], candidate.SharePowHash[:], candidate.ParentHash[:], candidate.Height.String(), candidate.TemplateSeq.String(), candidate.BackendID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO block_submission_state_v1(candidate_id,state,state_version,attempt_count,next_attempt_at,updated_at) VALUES($1,'pending',1,0,clock_timestamp(),clock_timestamp())`, existingID[:]); err != nil {
		t.Fatal(err)
	}
	if _, disposition, err := s.PutCandidateV2(context.Background(), candidate, wideAccountingIntentV2(t, candidate)); err != nil || disposition != PutDispositionConflict {
		t.Fatalf("V2 expected-hash collision disposition=%q err=%v", disposition, err)
	}
	var conflicts int
	var state string
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM block_submission_conflicts_v1),(SELECT state FROM block_submission_state_v1 WHERE candidate_id=$1)`, existingID[:]).Scan(&conflicts, &state); err != nil {
		t.Fatal(err)
	}
	if conflicts != 1 || state != SubmissionStateConflict {
		t.Fatalf("V2 expected-hash collision was not latched conflicts=%d state=%q", conflicts, state)
	}
}

func TestAcceptedAccountingLegacyNullabilityCompatibilityMatrix(t *testing.T) {
	cases := []struct {
		name             string
		statusNotNull    bool
		blockHashNotNull bool
		isPaidNotNull    bool
	}{
		{"all_nullable", false, false, false},
		{"status_not_null", true, false, false},
		{"block_hash_not_null", false, true, false},
		{"status_and_block_hash_not_null", true, true, false},
		{"is_paid_not_null", false, false, true},
		{"status_and_is_paid_not_null", true, false, true},
		{"block_hash_and_is_paid_not_null", false, true, true},
		{"testerd_strict_cohort", true, true, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := NewPostgresStorage(journalTestURL(t))
			if err := s.Connect(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.Close)
			resetJournalDatabase(t, s)
			applyJournalMigration(t, s)
			createLegacyAccountingTables(t, s)
			for _, column := range []struct {
				name    string
				ddl     string
				notNull bool
			}{
				{"blocks.status", `ALTER TABLE blocks ALTER COLUMN status DROP NOT NULL`, tc.statusNotNull},
				{"credits.block_hash", `ALTER TABLE credits ALTER COLUMN block_hash DROP NOT NULL`, tc.blockHashNotNull},
				{"credits.is_paid", `ALTER TABLE credits ALTER COLUMN is_paid DROP NOT NULL`, tc.isPaidNotNull},
			} {
				if !column.notNull {
					if _, err := s.pool.Exec(context.Background(), column.ddl); err != nil {
						t.Fatalf("set %s nullable: %v", column.name, err)
					}
				}
			}
			if _, err := s.pool.Exec(context.Background(), `INSERT INTO blocks(hash,height,reward,fees,status) VALUES ('legacy-block',9,100,2,'PENDING')`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(context.Background(), `INSERT INTO credits(block_hash,miner_address,amount,is_paid) VALUES ('legacy-block','miner',3,false)`); err != nil {
				t.Fatal(err)
			}
			before := legacyAccountingRows(t, s)
			applyAcceptedAccountingMigration(t, s)
			if err := s.VerifyAcceptedAccountingV1(context.Background()); err != nil {
				t.Fatalf("initial runtime verification: %v", err)
			}
			if _, err := s.pool.Exec(context.Background(), migrationSQL(t)); err != nil {
				t.Fatalf("accounting migration reapply: %v", err)
			}
			if err := s.VerifyAcceptedAccountingV1(context.Background()); err != nil {
				t.Fatalf("reapplied runtime verification: %v", err)
			}
			if after := legacyAccountingRows(t, s); !equalStrings(before, after) {
				t.Fatalf("legacy rows changed by migration/reapply: before=%v after=%v", before, after)
			}
		})
	}
}

func TestAcceptedAccountingMigrationInterruptionRollsBackAndReapplies(t *testing.T) {
	s := NewPostgresStorage(journalTestURL(t))
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	resetJournalDatabase(t, s)
	applyJournalMigration(t, s)
	createLegacyAccountingTables(t, s)
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO blocks(hash,height,reward,fees,status) VALUES ('legacy-block',9,100,2,'PENDING')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO credits(block_hash,miner_address,amount,is_paid) VALUES ('legacy-block','miner',3,false)`); err != nil {
		t.Fatal(err)
	}
	before := legacyAccountingRows(t, s)
	original := migrationSQL(t)
	interrupted := strings.Replace(original, "\nCOMMIT;", "\nDO $$ BEGIN RAISE EXCEPTION 'injected migration interruption'; END $$;\nCOMMIT;", 1)
	if _, err := s.pool.Exec(context.Background(), interrupted); err == nil {
		t.Fatal("interrupted accounting migration unexpectedly committed")
	}
	var installed int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind='r' AND relname IN ('accepted_block_accounting_intents_v1','accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1')`).Scan(&installed); err != nil {
		t.Fatal(err)
	}
	if installed != 0 {
		t.Fatalf("interrupted migration left %d accounting tables", installed)
	}
	if after := legacyAccountingRows(t, s); !equalStrings(before, after) {
		t.Fatalf("interrupted migration changed legacy rows: before=%v after=%v", before, after)
	}
	if _, err := s.pool.Exec(context.Background(), original); err != nil {
		t.Fatalf("reapply after interruption: %v", err)
	}
	if err := s.VerifyAcceptedAccountingV1(context.Background()); err != nil {
		t.Fatalf("verification after reapply: %v", err)
	}
}

func migrationSQL(t *testing.T) string {
	t.Helper()
	path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260902_accepted_receipt_accounting_v1.sql")
	migration, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(migration)
}

func legacyAccountingRows(t *testing.T, s *PostgresStorage) []string {
	t.Helper()
	rows := make([]string, 0, 3)
	for _, query := range []string{
		`SELECT COALESCE(string_agg(hash||'|'||height::text||'|'||reward::text||'|'||fees::text||'|'||status||'|'||created_at::text,'|' ORDER BY hash),'') FROM blocks`,
		`SELECT COALESCE(string_agg(block_hash||'|'||miner_address||'|'||amount::text||'|'||is_paid::text||'|'||created_at::text,'|' ORDER BY id),'') FROM credits`,
		`SELECT COALESCE(string_agg(tx_hash||'|'||amount::text||'|'||created_at::text,'|' ORDER BY id),'') FROM payouts`,
	} {
		var row string
		if err := s.pool.QueryRow(context.Background(), query).Scan(&row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

func TestAcceptedAccountingMaterializesExactlyOnce(t *testing.T) {
	s := acceptedAccountingJournal(t)
	candidate, intent := acceptedAccountingReceipt(t, s)
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO payouts(tx_hash,amount) VALUES ('unchanged-payout',7)`); err != nil {
		t.Fatal(err)
	}
	var payoutBefore string
	if err := s.pool.QueryRow(context.Background(), `SELECT ctid::text||'|'||xmin::text||'|'||tx_hash||'|'||amount::text FROM payouts`).Scan(&payoutBefore); err != nil {
		t.Fatal(err)
	}
	first, err := s.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID)
	if err != nil || !first.Applied || first.AlreadyApplied || first.CreditLines != int64(len(intent.CreditLines)) {
		t.Fatalf("first apply=%+v err=%v", first, err)
	}
	second, err := s.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID)
	if err != nil || second.Applied || !second.AlreadyApplied {
		t.Fatalf("second apply=%+v err=%v", second, err)
	}
	legacyKey := hex.EncodeToString(candidate.SharePowHash[:])
	var effects, blocks, credits int
	var height int64
	var reward, fee, miner, amount string
	if err := s.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM accepted_block_accounting_effects_v1),
		(SELECT count(*) FROM blocks WHERE hash=$1),
		(SELECT count(*) FROM credits WHERE block_hash=$1),
		b.height,b.reward::text,b.fees::text,c.miner_address,c.amount::text
		FROM blocks b JOIN credits c ON c.block_hash=b.hash WHERE b.hash=$1`, legacyKey).Scan(&effects, &blocks, &credits, &height, &reward, &fee, &miner, &amount); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || blocks != 1 || credits != 1 || height != int64(intent.Height) || reward != "1000" || fee != "0" || miner != "miner" || amount != "1000" {
		t.Fatalf("materialized accounting effects=%d blocks=%d credits=%d height=%d reward=%s fee=%s miner=%s amount=%s", effects, blocks, credits, height, reward, fee, miner, amount)
	}
	var payoutAfter string
	if err := s.pool.QueryRow(context.Background(), `SELECT ctid::text||'|'||xmin::text||'|'||tx_hash||'|'||amount::text FROM payouts`).Scan(&payoutAfter); err != nil {
		t.Fatal(err)
	}
	if payoutAfter != payoutBefore {
		t.Fatalf("payout row changed: before=%q after=%q", payoutBefore, payoutAfter)
	}
	if err := s.VerifyAcceptedAccountingV1(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedAccountingConcurrentConsumersSerialize(t *testing.T) {
	s := acceptedAccountingJournal(t)
	candidate, _ := acceptedAccountingReceipt(t, s)
	const consumers = 24
	start := make(chan struct{})
	results := make(chan AcceptedAccountingApplyResultV1, consumers)
	errs := make(chan error, consumers)
	var wg sync.WaitGroup
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := s.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var applied, already int
	for result := range results {
		if result.Applied {
			applied++
		}
		if result.AlreadyApplied {
			already++
		}
	}
	if applied != 1 || already != consumers-1 {
		t.Fatalf("concurrent results applied=%d already=%d", applied, already)
	}
	var effects, blocks, credits int
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT count(*) FROM blocks),(SELECT count(*) FROM credits)`).Scan(&effects, &blocks, &credits); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || blocks != 1 || credits != 1 {
		t.Fatalf("concurrent rows effects=%d blocks=%d credits=%d", effects, blocks, credits)
	}
}

func TestAcceptedAccountingCommitAcknowledgementLossIsIdempotent(t *testing.T) {
	s := acceptedAccountingJournal(t)
	candidate, _ := acceptedAccountingReceipt(t, s)
	acceptedAccountingFaultHookV1 = func(point string, after bool) error {
		if point == "accounting_effect_commit" && after {
			return errors.New("commit acknowledgement lost")
		}
		return nil
	}
	_, firstErr := s.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID)
	acceptedAccountingFaultHookV1 = nil
	if firstErr == nil {
		t.Fatal("post-commit acknowledgement loss was not surfaced")
	}
	result, err := s.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID)
	if err != nil || !result.AlreadyApplied {
		t.Fatalf("commit-loss retry=%+v err=%v", result, err)
	}
	var effects, blocks, credits int
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT count(*) FROM blocks),(SELECT count(*) FROM credits)`).Scan(&effects, &blocks, &credits); err != nil {
		t.Fatal(err)
	}
	if effects != 1 || blocks != 1 || credits != 1 {
		t.Fatalf("commit-loss rows effects=%d blocks=%d credits=%d", effects, blocks, credits)
	}
}

func TestAcceptedAccountingIntentFaultBoundariesAreAtomic(t *testing.T) {
	for _, point := range []string{"accounting_intent_insert", "accounting_credit_line_insert"} {
		for _, after := range []bool{false, true} {
			point, after := point, after
			t.Run(point+map[bool]string{false: "_before", true: "_after"}[after], func(t *testing.T) {
				s := migratedJournal(t)
				candidate := storageTestCandidate()
				intent := storageTestAccountingIntent(candidate)
				acceptedAccountingFaultHookV1 = func(gotPoint string, gotAfter bool) error {
					if gotPoint == point && gotAfter == after {
						return errors.New("injected accepted-accounting intent fault")
					}
					return nil
				}
				t.Cleanup(func() { acceptedAccountingFaultHookV1 = nil })
				if _, _, err := s.PutCandidate(context.Background(), candidate, intent); err == nil {
					t.Fatal("injected candidate/accounting intent fault was not surfaced")
				}
				acceptedAccountingFaultHookV1 = nil

				var candidates, states, intents, lines int
				if err := s.pool.QueryRow(context.Background(), `SELECT
					(SELECT count(*) FROM block_submission_candidates_v1),
					(SELECT count(*) FROM block_submission_state_v1),
					(SELECT count(*) FROM accepted_block_accounting_intents_v1),
					(SELECT count(*) FROM accepted_block_accounting_credit_lines_v1)`).Scan(&candidates, &states, &intents, &lines); err != nil {
					t.Fatal(err)
				}
				if candidates != 0 || states != 0 || intents != 0 || lines != 0 {
					t.Fatalf("intent fault crossed atomic rollback boundary: candidates=%d states=%d intents=%d lines=%d", candidates, states, intents, lines)
				}
				if _, disposition, err := s.PutCandidate(context.Background(), candidate, intent); err != nil || disposition != PutDispositionInserted {
					t.Fatalf("retry after intent fault: disposition=%q err=%v", disposition, err)
				}
			})
		}
	}
}

func TestAcceptedAccountingEffectFaultBoundariesSurviveRestartExactlyOnce(t *testing.T) {
	for _, point := range []string{"accepted_receipt_consume", "accounting_effect_insert", "legacy_block_insert", "legacy_credit_insert", "accounting_effect_commit"} {
		for _, after := range []bool{false, true} {
			point, after := point, after
			t.Run(point+map[bool]string{false: "_before", true: "_after"}[after], func(t *testing.T) {
				s := acceptedAccountingJournal(t)
				candidate, _ := acceptedAccountingReceipt(t, s)
				acceptedAccountingFaultHookV1 = func(gotPoint string, gotAfter bool) error {
					if gotPoint == point && gotAfter == after {
						return errors.New("injected accepted-accounting effect fault")
					}
					return nil
				}
				t.Cleanup(func() { acceptedAccountingFaultHookV1 = nil })
				if _, err := s.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID); err == nil {
					t.Fatal("injected accepted-accounting effect fault was not surfaced")
				}
				acceptedAccountingFaultHookV1 = nil
				s.Close()

				reopened := NewPostgresStorage(journalTestURL(t))
				if err := reopened.Connect(context.Background()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(reopened.Close)
				var effects, blocks, credits int
				if err := reopened.pool.QueryRow(context.Background(), `SELECT
					(SELECT count(*) FROM accepted_block_accounting_effects_v1),
					(SELECT count(*) FROM blocks),
					(SELECT count(*) FROM credits)`).Scan(&effects, &blocks, &credits); err != nil {
					t.Fatal(err)
				}
				committed := point == "accounting_effect_commit" && after
				want := 0
				if committed {
					want = 1
				}
				if effects != want || blocks != want || credits != want {
					t.Fatalf("post-restart boundary rows effects=%d blocks=%d credits=%d want=%d", effects, blocks, credits, want)
				}
				result, err := reopened.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID)
				if err != nil || result.AlreadyApplied != committed || result.Applied == committed {
					t.Fatalf("post-restart retry=%+v err=%v committed=%t", result, err, committed)
				}
				if err := reopened.pool.QueryRow(context.Background(), `SELECT
					(SELECT count(*) FROM accepted_block_accounting_effects_v1),
					(SELECT count(*) FROM blocks),
					(SELECT count(*) FROM credits)`).Scan(&effects, &blocks, &credits); err != nil {
					t.Fatal(err)
				}
				if effects != 1 || blocks != 1 || credits != 1 {
					t.Fatalf("retry did not converge exactly once: effects=%d blocks=%d credits=%d", effects, blocks, credits)
				}
			})
		}
	}
}

func TestAcceptedAccountingMissingIntentFailsReadinessWithoutLegacyEffects(t *testing.T) {
	s := acceptedAccountingJournal(t)
	candidate, _ := acceptedAccountingReceipt(t, s)
	for _, statement := range []string{
		`ALTER TABLE accepted_block_accounting_credit_lines_v1 DISABLE TRIGGER accepted_accounting_credit_lines_v1_immutable`,
		`ALTER TABLE accepted_block_accounting_intents_v1 DISABLE TRIGGER accepted_block_accounting_intents_v1_immutable`,
	} {
		if _, err := s.pool.Exec(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`DELETE FROM accepted_block_accounting_credit_lines_v1 WHERE candidate_id=$1`,
		`DELETE FROM accepted_block_accounting_intents_v1 WHERE candidate_id=$1`,
	} {
		if _, err := s.pool.Exec(context.Background(), statement, candidate.CandidateID[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.VerifyAcceptedAccountingV1(context.Background()); err == nil {
		t.Fatal("readiness verifier accepted an accepted receipt without its intent")
	}
	if _, err := s.ReconcileAcceptedAccountingV1(context.Background()); err == nil {
		t.Fatal("reconciliation reconstructed a missing accepted accounting intent")
	}
	var effects, blocks, credits int
	if err := s.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM accepted_block_accounting_effects_v1),
		(SELECT count(*) FROM blocks),
		(SELECT count(*) FROM credits)`).Scan(&effects, &blocks, &credits); err != nil {
		t.Fatal(err)
	}
	if effects != 0 || blocks != 0 || credits != 0 {
		t.Fatalf("missing intent created legacy effects: effects=%d blocks=%d credits=%d", effects, blocks, credits)
	}
}

func TestAcceptedAccountingVariantIntentLatchesConflict(t *testing.T) {
	s := acceptedAccountingJournal(t)
	candidate := storageTestCandidate()
	intent := storageTestAccountingIntent(candidate)
	if _, disposition, err := s.PutCandidate(context.Background(), candidate, intent); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("initial put: %q %v", disposition, err)
	}
	variant := intent
	variant.PoolFeeFloatBits = math.Float64bits(10)
	variant.FeeAmount = 100
	variant.Distributable = 900
	variant.CreditLines[0].Amount = 900
	variant, err := SealAcceptedAccountingIntentV1(variant)
	if err != nil {
		t.Fatal(err)
	}
	view, disposition, err := s.PutCandidate(context.Background(), candidate, variant)
	if err != nil || disposition != PutDispositionConflict || view.State != SubmissionStateConflict {
		t.Fatalf("variant put state=%q disposition=%q err=%v", view.State, disposition, err)
	}
	var conflicts, attempts int
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM accepted_block_accounting_conflicts_v1),(SELECT count(*) FROM block_submission_attempts_v1)`).Scan(&conflicts, &attempts); err != nil {
		t.Fatal(err)
	}
	if conflicts != 1 || attempts != 0 {
		t.Fatalf("variant conflict rows=%d attempts=%d", conflicts, attempts)
	}
}

func TestAcceptedAccountingRejectsPreexistingLegacyRows(t *testing.T) {
	s := acceptedAccountingJournal(t)
	candidate, intent := acceptedAccountingReceipt(t, s)
	legacyKey := hex.EncodeToString(intent.LegacyBlockHash[:])
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO blocks(hash,height,reward,fees,status) VALUES ($1,$2,$3,$4,'PENDING')`, legacyKey, int64(intent.Height), "1000", "0"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyAcceptedAccountingV1(context.Background(), candidate.CandidateID); err == nil {
		t.Fatal("pre-existing legacy block was adopted")
	}
	var conflicts, effects, credits int
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM accepted_block_accounting_conflicts_v1),(SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT count(*) FROM credits)`).Scan(&conflicts, &effects, &credits); err != nil {
		t.Fatal(err)
	}
	if conflicts != 1 || effects != 0 || credits != 0 {
		t.Fatalf("legacy conflict rows conflicts=%d effects=%d credits=%d", conflicts, effects, credits)
	}
}

func TestAcceptedAccountingMigrationReapplyDriftAndImmutability(t *testing.T) {
	s := migratedJournal(t)
	path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260902_accepted_receipt_accounting_v1.sql")
	migration, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), string(migration)); err != nil {
		t.Fatalf("accounting migration reapply: %v", err)
	}
	candidate := storageTestCandidate()
	intent := storageTestAccountingIntent(candidate)
	if _, _, err := s.PutCandidate(context.Background(), candidate, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), `UPDATE accepted_block_accounting_intents_v1 SET policy_version='changed' WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("immutable accounting intent accepted update")
	}
	if _, err := s.pool.Exec(context.Background(), `DROP TRIGGER accepted_block_accounting_intents_v1_immutable ON accepted_block_accounting_intents_v1`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), string(migration)); err == nil {
		t.Fatal("accounting migration silently repaired trigger drift")
	}
}

func TestAcceptedAccountingCatalogDriftFailsRuntimeAndReapply(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
	}{
		{"column_type", `ALTER TABLE accepted_block_accounting_conflicts_v1 ALTER COLUMN conflict_kind TYPE varchar(96)`},
		{"column_default", `ALTER TABLE accepted_block_accounting_intents_v1 ALTER COLUMN created_at SET DEFAULT now()`},
		{"column_nullability", `ALTER TABLE accepted_block_accounting_intents_v1 ALTER COLUMN intent_digest DROP NOT NULL`},
		{"constraint_definition", `ALTER TABLE accepted_block_accounting_credit_lines_v1 DROP CONSTRAINT accepted_accounting_credit_lines_v1_amount_ck; ALTER TABLE accepted_block_accounting_credit_lines_v1 ADD CONSTRAINT accepted_accounting_credit_lines_v1_amount_ck CHECK (amount BETWEEN 0 AND 18446744073709551614)`},
		{"index_definition", `DROP INDEX accepted_accounting_conflicts_v1_candidate_idx; CREATE INDEX accepted_accounting_conflicts_v1_candidate_idx ON accepted_block_accounting_conflicts_v1(observed_at,candidate_id)`},
		{"trigger_topology", `DROP TRIGGER accepted_block_accounting_intents_v1_immutable ON accepted_block_accounting_intents_v1; CREATE TRIGGER accepted_block_accounting_intents_v1_immutable BEFORE UPDATE OR DELETE ON accepted_block_accounting_effects_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1()`},
		{"function_body", `CREATE OR REPLACE FUNCTION reject_immutable_submission_row_v1() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`},
		{"legacy_column_type", `ALTER TABLE blocks ALTER COLUMN reward TYPE numeric(29,0)`},
		{"legacy_default", `ALTER TABLE blocks ALTER COLUMN status SET DEFAULT 'MATURE'`},
		{"legacy_height_nullability", `ALTER TABLE blocks ALTER COLUMN height DROP NOT NULL`},
		{"legacy_reward_nullability", `ALTER TABLE blocks ALTER COLUMN reward DROP NOT NULL`},
		{"legacy_fees_nullability", `ALTER TABLE blocks ALTER COLUMN fees DROP NOT NULL`},
		{"legacy_created_at_nullability", `ALTER TABLE blocks ALTER COLUMN created_at SET NOT NULL`},
		{"legacy_miner_nullability", `ALTER TABLE credits ALTER COLUMN miner_address DROP NOT NULL`},
		{"legacy_amount_nullability", `ALTER TABLE credits ALTER COLUMN amount DROP NOT NULL`},
		{"legacy_credit_created_at_nullability", `ALTER TABLE credits ALTER COLUMN created_at SET NOT NULL`},
		{"legacy_key", `ALTER TABLE credits DROP CONSTRAINT credits_block_hash_fkey`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := migratedJournal(t)
			if _, err := s.pool.Exec(context.Background(), tc.ddl); err != nil {
				t.Fatalf("inject %s drift: %v", tc.name, err)
			}
			if err := s.VerifyAcceptedAccountingV1(context.Background()); err == nil {
				t.Fatalf("runtime verifier accepted %s drift", tc.name)
			}
			path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260902_accepted_receipt_accounting_v1.sql")
			migration, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(context.Background(), string(migration)); err == nil {
				t.Fatalf("accounting migration silently repaired %s drift", tc.name)
			}
		})
	}
}

func TestAcceptedAccountingMigrationRejectsMissingTargetsAndPartialInstall(t *testing.T) {
	s := NewPostgresStorage(journalTestURL(t))
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260902_accepted_receipt_accounting_v1.sql")
	migration, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	resetJournalDatabase(t, s)
	applyJournalMigration(t, s)
	if _, err := s.pool.Exec(context.Background(), string(migration)); err == nil {
		t.Fatal("accounting migration accepted missing legacy target tables")
	}

	resetJournalDatabase(t, s)
	applyJournalMigration(t, s)
	createLegacyAccountingTables(t, s)
	if _, err := s.pool.Exec(context.Background(), `CREATE TABLE accepted_block_accounting_intents_v1(unrelated INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), string(migration)); err == nil {
		t.Fatal("accounting migration accepted a partial same-name installation")
	}
}
