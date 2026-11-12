package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/blake2b"
)

func TestSubmissionJournalSQLContract(t *testing.T) {
	databaseURL := journalTestURL(t)
	ctx := context.Background()
	s := NewPostgresStorage(databaseURL)
	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	resetJournalDatabase(t, s)
	schemaPath := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "schema.sql")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("fresh schema.sql: %v", err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, bigInt(1404), "node"); err != nil {
		t.Fatalf("fresh schema verification: %v", err)
	}
	if err := s.VerifyAcceptedAccountingV2(ctx); err != nil {
		t.Fatalf("fresh accounting verification: %v", err)
	}
	resetJournalDatabase(t, s)
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE miners(address TEXT PRIMARY KEY, joined_at TIMESTAMP, last_active TIMESTAMP);
		CREATE TABLE blocks(hash TEXT PRIMARY KEY, height BIGINT NOT NULL, reward NUMERIC(30,0) NOT NULL, fees NUMERIC(30,0) NOT NULL, status TEXT DEFAULT 'PENDING', created_at TIMESTAMP DEFAULT NOW());
		CREATE TABLE block_submissions(id SERIAL PRIMARY KEY, candidate_hash TEXT NOT NULL, node_block_hash TEXT, height BIGINT, backend TEXT, template_seq BIGINT, accepted BOOLEAN NOT NULL, outcome TEXT NOT NULL, message TEXT, created_at TIMESTAMP);
		CREATE TABLE credits(id SERIAL PRIMARY KEY, block_hash TEXT REFERENCES blocks(hash), miner_address TEXT NOT NULL, amount NUMERIC(30,0) NOT NULL, is_paid BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW());
		CREATE TABLE payouts(id SERIAL PRIMARY KEY, tx_hash TEXT UNIQUE NOT NULL, amount NUMERIC(30,0) NOT NULL, created_at TIMESTAMP);
		INSERT INTO miners VALUES ('miner', TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-02');
		INSERT INTO blocks VALUES ('legacy-block', 9, 100, 2, 'PENDING', TIMESTAMP '2026-01-01');
		INSERT INTO block_submissions(candidate_hash,node_block_hash,height,backend,template_seq,accepted,outcome,message,created_at) VALUES ('legacy-candidate','legacy-node',9,'legacy',1,false,'pending','unchanged',TIMESTAMP '2026-01-01');
		INSERT INTO credits(block_hash,miner_address,amount,is_paid,created_at) VALUES ('legacy-block','miner',3,false,TIMESTAMP '2026-01-01');
		INSERT INTO payouts(tx_hash,amount,created_at) VALUES ('legacy-tx',4,TIMESTAMP '2026-01-01')`); err != nil {
		t.Fatal(err)
	}
	before := legacyRows(t, s)
	migrationPath := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260901_block_submission_journal_v1.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("fresh migration: %v", err)
	}
	accountingMigrationPath := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260902_accepted_receipt_accounting_v1.sql")
	accountingMigration, err := os.ReadFile(accountingMigrationPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, string(accountingMigration)); err != nil {
		t.Fatalf("fresh accounting migration: %v", err)
	}
	if _, err := s.pool.Exec(ctx, string(accountingMigration)); err != nil {
		t.Fatalf("idempotent v1 accounting migration: %v", err)
	}
	applyWideAccountingMigration(t, s)
	if err := s.VerifySubmissionJournalV1(ctx, bigInt(1404), "node"); err != nil {
		t.Fatalf("fresh verification: %v", err)
	}
	if err := s.VerifyAcceptedAccountingV2(ctx); err != nil {
		t.Fatalf("fresh accounting verification: %v", err)
	}
	afterFirst := legacyRows(t, s)
	if !equalStrings(before, afterFirst) {
		t.Fatalf("legacy rows changed by migration: before=%v after=%v", before, afterFirst)
	}
	if _, err := s.pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}
	applyWideAccountingMigration(t, s)
	if afterSecond := legacyRows(t, s); !equalStrings(afterFirst, afterSecond) {
		t.Fatalf("legacy rows changed by reapplication: before=%v after=%v", afterFirst, afterSecond)
	}

	candidate := storageTestCandidate()
	accounting := storageTestAccountingIntent(candidate)
	view, disposition, err := s.PutCandidate(ctx, candidate, accounting)
	if err != nil || disposition != PutDispositionInserted || view.State != SubmissionStatePending {
		t.Fatalf("PutCandidate = state=%q disposition=%q err=%v", view.State, disposition, err)
	}
	if _, disposition, err := s.PutCandidate(ctx, candidate, accounting); err != nil || disposition != PutDispositionExisting {
		t.Fatalf("equal PutCandidate = disposition=%q err=%v", disposition, err)
	}
	if _, err := s.GetCandidate(ctx, candidate.CandidateID); err != nil {
		t.Fatalf("GetCandidate: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE block_submission_candidates_v1 SET backend_id='tampered' WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("candidate immutability trigger accepted update")
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM block_submission_candidates_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("candidate immutability trigger accepted delete")
	}

	owner := [16]byte{1}
	leased, err := s.ClaimCandidate(ctx, candidate.CandidateID, owner, 2*time.Second)
	if err != nil || leased == nil {
		t.Fatalf("ClaimCandidate = %#v err=%v", leased, err)
	}
	prepared, err := s.PrepareAttempt(ctx, leased.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatalf("PrepareAttempt: %v", err)
	}
	obs := acceptedObservation(candidate)
	transition, err := s.CompleteAttempt(ctx, leased.Lease, prepared.AttemptID, obs, SubmitDecisionAcceptedFinal)
	if err != nil || transition.State != SubmissionStateAcceptedFinal || !transition.FirstAccepted {
		t.Fatalf("accepted CompleteAttempt = %#v err=%v", transition, err)
	}
	var receipts, attempts int
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM accepted_block_submission_receipts_v1),(SELECT count(*) FROM block_submission_attempts_v1)`).Scan(&receipts, &attempts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || attempts != 1 {
		t.Fatalf("receipt/attempt counts = %d/%d, want 1/1", receipts, attempts)
	}
	if _, err := s.CompleteAttempt(ctx, leased.Lease, prepared.AttemptID, obs, SubmitDecisionAcceptedFinal); err == nil {
		t.Fatal("completion-once accepted duplicate")
	}
	if _, err := s.pool.Exec(ctx, `UPDATE block_submission_attempts_v1 SET request_body=request_body WHERE attempt_id=$1`, prepared.AttemptID[:]); err == nil {
		t.Fatal("completed attempt immutability trigger accepted update")
	}
	if _, err := s.pool.Exec(ctx, `UPDATE accepted_block_submission_receipts_v1 SET core_code=core_code WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("accepted receipt immutability trigger accepted update")
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM accepted_block_submission_receipts_v1 WHERE candidate_id=$1`, candidate.CandidateID[:]); err == nil {
		t.Fatal("accepted receipt immutability trigger accepted delete")
	}

	// An identity mismatch is evidence plus a durable conflict latch. It is
	// intentionally last because startup verification must then fail closed.
	mismatch := candidate
	mismatch.SharePowHash[0]++
	payload, err := encodeFrozenCandidateJournalV1(mismatch)
	if err != nil {
		t.Fatal(err)
	}
	mismatch.FrozenPayload = payload
	mismatch.FrozenDigest = sha256.Sum256(payload)
	view, disposition, err = s.PutCandidate(ctx, mismatch, storageTestAccountingIntent(mismatch))
	if err != nil || disposition != PutDispositionConflict || view.State != SubmissionStateConflict {
		t.Fatalf("mismatch PutCandidate = state=%q disposition=%q err=%v", view.State, disposition, err)
	}
	if err := s.VerifySubmissionJournalV1(ctx, bigInt(1404), "node"); err == nil {
		t.Fatal("verification ignored conflict latch")
	}
	source, err := os.ReadFile(filepath.Join(filepath.Dir(testSourceFile()), "submission_journal.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, legacyName := range []string{"block_submissions", "blocks", "credits", "payouts"} {
		if bytes.Contains(source, []byte(legacyName)) {
			t.Fatalf("journal query source names legacy table %q", legacyName)
		}
	}
}

func TestPrepareAttemptReplaysLatestPersistedRequestUnderNewLease(t *testing.T) {
	s := migratedJournal(t)
	candidate := storageTestCandidate()
	if _, disposition, err := s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate)); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("seed candidate: disposition=%q err=%v", disposition, err)
	}
	first, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{1}, 100*time.Millisecond)
	if err != nil || first == nil {
		t.Fatalf("first lease: %#v %v", first, err)
	}
	orphan, err := s.PrepareAttempt(context.Background(), first.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	oldBody := append([]byte(nil), orphan.RequestBody...)
	oldDigest := orphan.RequestSHA256
	time.Sleep(220 * time.Millisecond)
	if reclaimed, err := s.ReclaimExpiredLeases(context.Background()); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim = %d err=%v", reclaimed, err)
	}
	second, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{2}, time.Second)
	if err != nil || second == nil {
		t.Fatalf("second lease: %#v %v", second, err)
	}
	replay, err := s.PrepareAttempt(context.Background(), second.Lease, SubmitMethodLegacy)
	if err != nil {
		t.Fatalf("replay prepare: %v", err)
	}
	if replay.AttemptID == orphan.AttemptID || replay.LeaseToken == orphan.LeaseToken || replay.AttemptNo == orphan.AttemptNo {
		t.Fatal("replay reused old attempt identity")
	}
	if replay.Method != orphan.Method || !bytes.Equal(replay.RequestBody, oldBody) || replay.RequestSHA256 != oldDigest {
		t.Fatalf("replay changed persisted request: method=%q body_equal=%t digest_equal=%t", replay.Method, bytes.Equal(replay.RequestBody, oldBody), replay.RequestSHA256 == oldDigest)
	}
	var phase string
	if err := s.pool.QueryRow(context.Background(), `SELECT phase FROM block_submission_attempts_v1 WHERE attempt_id=$1`, orphan.AttemptID[:]).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != "prepared" {
		t.Fatalf("orphan phase=%q, want prepared", phase)
	}
}

func TestPrepareAttemptRejectsCorruptPersistedRequestAndLatchesConflict(t *testing.T) {
	s := migratedJournal(t)
	candidate := storageTestCandidate()
	if _, disposition, err := s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate)); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("seed candidate: disposition=%q err=%v", disposition, err)
	}
	first, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{3}, 100*time.Millisecond)
	if err != nil || first == nil {
		t.Fatalf("first lease: %#v %v", first, err)
	}
	corruptID := [16]byte{9}
	corruptBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"submitBlockHeaderV2","params":[]}`)
	corruptDigest := sha256.Sum256(corruptBody)
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO block_submission_attempts_v1 (attempt_id,candidate_id,attempt_no,lease_token,rpc_method,request_body,request_sha256,phase,prepared_at,decision) VALUES ($1,$2,1,$3,$4,$5,$6,'prepared',clock_timestamp(),'pending')`, corruptID[:], candidate.CandidateID[:], first.Lease.Token[:], SubmitMethodV2, corruptBody, corruptDigest[:]); err != nil {
		t.Fatalf("seed corrupt attempt: %v", err)
	}
	if _, err := s.pool.Exec(context.Background(), `UPDATE block_submission_state_v1 SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE candidate_id=$1`, candidate.CandidateID[:]); err != nil {
		t.Fatal(err)
	}
	if reclaimed, err := s.ReclaimExpiredLeases(context.Background()); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim = %d err=%v", reclaimed, err)
	}
	second, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{4}, time.Second)
	if err != nil || second == nil {
		t.Fatalf("second lease: %#v %v", second, err)
	}
	if _, err := s.PrepareAttempt(context.Background(), second.Lease, SubmitMethodV2); err == nil {
		t.Fatal("corrupt persisted request was accepted")
	}
	view, err := s.GetCandidate(context.Background(), candidate.CandidateID)
	if err != nil || view.State != SubmissionStateConflict {
		t.Fatalf("conflict state=%q err=%v", view.State, err)
	}
	var conflicts int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM block_submission_conflicts_v1`).Scan(&conflicts); err != nil {
		t.Fatal(err)
	}
	if conflicts != 1 {
		t.Fatalf("conflict rows=%d, want 1", conflicts)
	}
	if claimed, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{5}, time.Second); err != nil || claimed != nil {
		t.Fatalf("conflicted candidate remained claimable: %#v err=%v", claimed, err)
	}
}

func TestSubmissionJournalStaleCompletionIsEvidenceOnly(t *testing.T) {
	s := migratedJournal(t)
	candidate := storageTestCandidate()
	if _, _, err := s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate)); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{6}, 100*time.Millisecond)
	if err != nil || first == nil {
		t.Fatalf("first lease: %#v %v", first, err)
	}
	orphan, err := s.PrepareAttempt(context.Background(), first.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(220 * time.Millisecond)
	if _, err := s.ReclaimExpiredLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{7}, time.Second)
	if err != nil || second == nil {
		t.Fatalf("second lease: %#v %v", second, err)
	}
	current, err := s.PrepareAttempt(context.Background(), second.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := s.CompleteAttempt(context.Background(), first.Lease, orphan.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal)
	if err != nil {
		t.Fatal(err)
	}
	if stale.FirstAccepted || stale.State == SubmissionStateAcceptedFinal {
		t.Fatalf("stale completion authorized acceptance: %+v", stale)
	}
	accepted, err := s.CompleteAttempt(context.Background(), second.Lease, current.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal)
	if err != nil || !accepted.FirstAccepted || accepted.State != SubmissionStateAcceptedFinal {
		t.Fatalf("current completion: %+v err=%v", accepted, err)
	}
	var receipts int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM accepted_block_submission_receipts_v1`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("receipt count=%d, want 1", receipts)
	}
}

func TestSubmissionJournalConcurrentAcceptedExactlyOneReceipt(t *testing.T) {
	s := migratedJournal(t)
	candidate := storageTestCandidate()
	if _, _, err := s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate)); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{8}, 100*time.Millisecond)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	oldAttempt, err := s.PrepareAttempt(context.Background(), first.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(220 * time.Millisecond)
	if _, err := s.ReclaimExpiredLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{9}, time.Second)
	if err != nil || second == nil {
		t.Fatal(err)
	}
	newAttempt, err := s.PrepareAttempt(context.Background(), second.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan TransitionResultV1, 2)
	errs := make(chan error, 2)
	go func() {
		<-start
		result, err := s.CompleteAttempt(context.Background(), first.Lease, oldAttempt.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal)
		results <- result
		errs <- err
	}()
	go func() {
		<-start
		result, err := s.CompleteAttempt(context.Background(), second.Lease, newAttempt.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal)
		results <- result
		errs <- err
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var firstAccepted int
	for i := 0; i < 2; i++ {
		if (<-results).FirstAccepted {
			firstAccepted++
		}
	}
	if firstAccepted != 1 {
		t.Fatalf("FirstAccepted count=%d, want 1", firstAccepted)
	}
	var receipts int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM accepted_block_submission_receipts_v1`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("receipt count=%d, want 1", receipts)
	}
	var source []byte
	if err := s.pool.QueryRow(context.Background(), `SELECT source_attempt_id FROM accepted_block_submission_receipts_v1`).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(source, newAttempt.AttemptID[:]) {
		t.Fatalf("receipt source attempt=%x, want current=%x", source, newAttempt.AttemptID)
	}
}

func TestCandidateIDForHashV1ActualNULGoldenVector(t *testing.T) {
	// This is intentionally independent of the production helper's prefix.
	// A literal backslash followed by zero is a 60-byte negative vector, not a
	// v1 compatibility form.
	hash, err := hex.DecodeString("613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94c")
	if err != nil {
		t.Fatal(err)
	}
	preimage, err := hex.DecodeString("424441472d504f4f4c2d5355424d495353494f4e2d49442f763100613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94c")
	if err != nil || len(preimage) != 59 {
		t.Fatalf("invalid fixed NUL oracle: len=%d err=%v", len(preimage), err)
	}
	var blockHash [32]byte
	copy(blockHash[:], hash)
	got := candidateIDForHashV1(blockHash)
	if hex.EncodeToString(got[:]) != "66f8d64e0443382192672329b03a678e2f268fea6d2eee92319012649f954e17" || got != sha256.Sum256(preimage) {
		t.Fatalf("storage candidate id does not use the actual-NUL oracle: %x", got)
	}
	literal := append([]byte("BDAG-POOL-SUBMISSION-ID/v1\\0"), hash...)
	negative := sha256.Sum256(literal)
	if len(literal) != 60 || hex.EncodeToString(negative[:]) != "ef6be0d82b0aa96e495dce198a1db4d9e400cc93e1296154f86fc68ffc250994" || negative == got {
		t.Fatal("storage candidate identity accepted the literal-backslash negative vector")
	}
}

func acceptedObservation(c FrozenCandidateV1) RPCObservationV1 {
	accepted, final, retryable := true, true, false
	code, message := "accepted", "accepted"
	return RPCObservationV1{TransportClass: "http", Accepted: &accepted, Final: &final, Retryable: &retryable, Code: &code, Message: &message, BlockHash: append([]byte(nil), c.ExpectedBlockHash[:]...), Height: new(big.Int).Set(c.Height), TemplateSeq: new(big.Int).Set(c.TemplateSeq), ResponsePrefix: []byte(`{"result":"accepted"}`)}
}

func storageTestCandidate() FrozenCandidateV1 {
	var c FrozenCandidateV1
	for i := range c.HeaderBytes {
		c.HeaderBytes[i] = byte(i)
	}
	c.HeaderBytes[176] = 0x0a
	for i := 177; i < 181; i++ {
		c.HeaderBytes[i] = 0
	}
	binary.LittleEndian.PutUint64(c.HeaderBytes[140:148], 7)
	c.ChainID = bigInt(1404)
	c.Height = new(big.Int).SetUint64(binary.LittleEndian.Uint64(c.HeaderBytes[140:148]))
	c.TemplateSeq = new(big.Int).SetUint64(77)
	c.BackendID = "node"
	copy(c.ExtraNonce2[:], []byte{1, 2, 3, 4})
	copy(c.NonceBytes[:], c.HeaderBytes[181:185])
	copy(c.ParentHash[:], c.HeaderBytes[4:36])
	for i := range c.SharePowHash {
		c.SharePowHash[i] = 0x55
	}
	first := blake2b.Sum256(c.HeaderBytes[:])
	c.ExpectedBlockHash = blake2b.Sum256(first[:])
	c.CandidateID = candidateIDForHashV1(c.ExpectedBlockHash)
	c.FrozenPayload, _ = encodeFrozenCandidateJournalV1(c)
	c.FrozenDigest = sha256.Sum256(c.FrozenPayload)
	return c
}

func storageTestAccountingIntent(candidate FrozenCandidateV1) AcceptedAccountingIntentV1 {
	intent, err := SealAcceptedAccountingIntentV1(AcceptedAccountingIntentV1{
		CandidateID:      candidate.CandidateID,
		LegacyBlockHash:  candidate.SharePowHash,
		Height:           candidate.Height.Uint64(),
		GrossReward:      1000,
		PoolFeeFloatBits: 0,
		Distributable:    1000,
		TotalWork:        10,
		CreditLines: []AcceptedAccountingCreditLineV1{{
			MinerAddress: "miner",
			Work:         10,
			Amount:       1000,
		}},
	})
	if err != nil {
		panic(err)
	}
	return intent
}

func bigInt(v int64) *big.Int { return big.NewInt(v) }

func legacyRows(t *testing.T, s *PostgresStorage) []string {
	t.Helper()
	ctx := context.Background()
	queries := []string{
		`SELECT ctid::text||'|'||xmin::text||'|'||address||'|'||joined_at::text||'|'||last_active::text FROM miners`,
		`SELECT ctid::text||'|'||xmin::text||'|'||hash||'|'||height::text||'|'||reward::text||'|'||fees::text||'|'||status||'|'||created_at::text FROM blocks`,
		`SELECT ctid::text||'|'||xmin::text||'|'||candidate_hash||'|'||node_block_hash||'|'||height::text||'|'||backend||'|'||template_seq::text||'|'||accepted::text||'|'||outcome||'|'||message||'|'||created_at::text FROM block_submissions`,
		`SELECT ctid::text||'|'||xmin::text||'|'||block_hash||'|'||miner_address||'|'||amount::text||'|'||is_paid::text||'|'||created_at::text FROM credits`,
		`SELECT ctid::text||'|'||xmin::text||'|'||tx_hash||'|'||amount::text||'|'||created_at::text FROM payouts`,
	}
	rows := make([]string, 0, len(queries))
	for _, query := range queries {
		var row string
		if err := s.pool.QueryRow(ctx, query).Scan(&row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testSourceFile() string {
	_, file, _, _ := runtime.Caller(0)
	return file
}

func journalTestURL(t *testing.T) string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("POOL_SUBMISSION_JOURNAL_TEST_DATABASE_URL"))
	if raw == "" {
		t.Fatal("POOL_SUBMISSION_JOURNAL_TEST_DATABASE_URL is required; the SQL contract may not be skipped")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.Port() == "" {
		t.Fatalf("ambiguous disposable PostgreSQL endpoint %q", raw)
	}
	host := strings.ToLower(u.Hostname())
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		t.Fatalf("SQL contract endpoint is not isolated to the local disposable host: %q", host)
	}
	database := strings.TrimPrefix(u.Path, "/")
	database, err = url.PathUnescape(database)
	if err != nil || !strings.HasPrefix(database, "pool_submission_journal_test_") {
		t.Fatalf("database name must begin pool_submission_journal_test_: %q", database)
	}
	return raw
}

func resetJournalDatabase(t *testing.T, s *PostgresStorage) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatal(err)
	}
}

func applyJournalMigration(t *testing.T, s *PostgresStorage) {
	t.Helper()
	path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260901_block_submission_journal_v1.sql")
	migration, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), string(migration)); err != nil {
		t.Fatalf("journal migration: %v", err)
	}
}

func applyAcceptedAccountingMigration(t *testing.T, s *PostgresStorage) {
	t.Helper()
	path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260902_accepted_receipt_accounting_v1.sql")
	migration, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), string(migration)); err != nil {
		t.Fatalf("accepted accounting migration: %v", err)
	}
}

func applyWideAccountingMigration(t *testing.T, s *PostgresStorage) {
	t.Helper()
	path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260906_wide_reward_accounting_v2.sql")
	migration, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), string(migration)); err != nil {
		t.Fatalf("wide accounting migration: %v", err)
	}
}

func createLegacyAccountingTables(t *testing.T, s *PostgresStorage) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), `
		CREATE TABLE miners(address TEXT PRIMARY KEY,joined_at TIMESTAMP,last_active TIMESTAMP);
		CREATE TABLE blocks(hash TEXT PRIMARY KEY,height BIGINT NOT NULL,reward NUMERIC(30,0) NOT NULL CHECK (reward > 0),fees NUMERIC(30,0) NOT NULL CHECK (fees >= 0),status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','ORPHANED','MATURE','PAID')),created_at TIMESTAMP DEFAULT NOW());
		CREATE TABLE block_submissions(id SERIAL PRIMARY KEY,candidate_hash TEXT NOT NULL,node_block_hash TEXT,height BIGINT,backend TEXT,template_seq BIGINT,accepted BOOLEAN NOT NULL DEFAULT FALSE,outcome TEXT NOT NULL,message TEXT,created_at TIMESTAMP);
		CREATE TABLE credits(id SERIAL PRIMARY KEY,block_hash TEXT NOT NULL REFERENCES blocks(hash),miner_address TEXT NOT NULL,amount NUMERIC(30,0) NOT NULL CHECK (amount > 0),is_paid BOOLEAN NOT NULL DEFAULT FALSE,created_at TIMESTAMP DEFAULT NOW());
		CREATE TABLE payouts(id SERIAL PRIMARY KEY,tx_hash TEXT UNIQUE NOT NULL,amount NUMERIC(30,0) NOT NULL,created_at TIMESTAMP DEFAULT NOW());`); err != nil {
		t.Fatalf("legacy accounting tables: %v", err)
	}
}

func migratedJournal(t *testing.T) *PostgresStorage {
	t.Helper()
	s := NewPostgresStorage(journalTestURL(t))
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	resetJournalDatabase(t, s)
	applyJournalMigration(t, s)
	createLegacyAccountingTables(t, s)
	applyAcceptedAccountingMigration(t, s)
	if err := s.VerifySubmissionJournalV1(context.Background(), bigInt(1404), "node"); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyAcceptedAccountingV1(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSubmissionJournalCatalogDriftAndImmutableBoundaries(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
	}{
		{"column_type", `ALTER TABLE block_submission_candidates_v1 ALTER COLUMN backend_id TYPE varchar(64)`},
		{"column_default", `ALTER TABLE block_submission_state_v1 ALTER COLUMN updated_at SET DEFAULT now()`},
		{"column_nullability", `ALTER TABLE block_submission_candidates_v1 ALTER COLUMN backend_id DROP NOT NULL`},
		{"constraint", `ALTER TABLE block_submission_candidates_v1 DROP CONSTRAINT block_submission_candidates_v1_header_bytes_len_check`},
		{"index", `DROP INDEX block_submission_state_due_v1_idx`},
		{"trigger_binding", `DROP TRIGGER block_submission_state_v1_transition ON block_submission_state_v1; CREATE TRIGGER block_submission_state_v1_transition BEFORE UPDATE ON block_submission_state_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1()`},
		{"function_body", `CREATE OR REPLACE FUNCTION make_submission_retry_delay(attempts BIGINT) RETURNS interval LANGUAGE sql IMMUTABLE AS $$ SELECT interval '1 second' $$`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := migratedJournal(t)
			if _, err := s.pool.Exec(context.Background(), tc.ddl); err != nil {
				t.Fatalf("inject %s drift: %v", tc.name, err)
			}
			if err := s.VerifySubmissionJournalV1(context.Background(), bigInt(1404), "node"); err == nil {
				t.Fatalf("runtime verifier accepted %s drift", tc.name)
			}
			path := filepath.Join(filepath.Dir(testSourceFile()), "..", "..", "..", "migrations", "20260901_block_submission_journal_v1.sql")
			migration, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(context.Background(), string(migration)); err == nil {
				t.Fatalf("idempotent migration silently repaired %s drift", tc.name)
			}
		})
	}
}

func TestSubmissionJournalConcurrentPutAndLeaseFencing(t *testing.T) {
	s := migratedJournal(t)
	candidate := storageTestCandidate()
	const callers = 64
	type putResult struct {
		disposition PutDisposition
		err         error
	}
	start := make(chan struct{})
	results := make(chan putResult, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, disposition, err := s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate))
			results <- putResult{disposition: disposition, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	counts := map[PutDisposition]int{}
	for result := range results {
		if result.err != nil {
			t.Fatalf("exact concurrent PutCandidate: %v", result.err)
		}
		counts[result.disposition]++
	}
	if counts[PutDispositionInserted] != 1 || counts[PutDispositionExisting] != callers-1 || counts[PutDispositionConflict] != 0 {
		t.Fatalf("exact PutCandidate dispositions = %#v, want 1 inserted/%d existing/0 conflict", counts, callers-1)
	}

	resetJournalDatabase(t, s)
	applyJournalMigration(t, s)
	createLegacyAccountingTables(t, s)
	applyAcceptedAccountingMigration(t, s)
	variant := candidate
	variant.SharePowHash[0]++
	variant.FrozenPayload, _ = encodeFrozenCandidateJournalV1(variant)
	variant.FrozenDigest = sha256.Sum256(variant.FrozenPayload)
	start = make(chan struct{})
	results = make(chan putResult, callers)
	for i := 0; i < callers; i++ {
		candidateToPut := candidate
		if i%2 == 1 {
			candidateToPut = variant
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, disposition, err := s.PutCandidate(context.Background(), candidateToPut, storageTestAccountingIntent(candidateToPut))
			results <- putResult{disposition: disposition, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil && result.disposition != PutDispositionConflict {
			t.Fatalf("concurrent variant PutCandidate: disposition=%q err=%v", result.disposition, result.err)
		}
	}
	var candidateCount, conflictCount int
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM block_submission_candidates_v1),(SELECT count(*) FROM block_submission_conflicts_v1)`).Scan(&candidateCount, &conflictCount); err != nil {
		t.Fatal(err)
	}
	if candidateCount != 1 || conflictCount == 0 {
		t.Fatalf("concurrent variant rows = candidates %d conflicts %d, want one canonical row and a hard conflict", candidateCount, conflictCount)
	}
	if err := s.VerifySubmissionJournalV1(context.Background(), bigInt(1404), "node"); err == nil {
		t.Fatal("concurrent variant did not latch global conflict")
	}

	resetJournalDatabase(t, s)
	applyJournalMigration(t, s)
	createLegacyAccountingTables(t, s)
	applyAcceptedAccountingMigration(t, s)
	if _, disposition, err := s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate)); err != nil || disposition != PutDispositionInserted {
		t.Fatalf("seed candidate: disposition=%q err=%v", disposition, err)
	}
	const claimers = 32
	start = make(chan struct{})
	leases := make(chan *LeasedCandidateV1, claimers)
	errs := make(chan error, claimers)
	for i := 0; i < claimers; i++ {
		owner := [16]byte{byte(i + 1)}
		wg.Add(1)
		go func(owner [16]byte) {
			defer wg.Done()
			<-start
			lease, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, owner, 150*time.Millisecond)
			if err != nil {
				errs <- err
				return
			}
			if lease != nil {
				leases <- lease
			}
		}(owner)
	}
	close(start)
	wg.Wait()
	close(leases)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var firstLease *LeasedCandidateV1
	for lease := range leases {
		if firstLease != nil {
			t.Fatal("more than one concurrent claimant acquired a lease")
		}
		firstLease = lease
	}
	if firstLease == nil {
		t.Fatal("32 concurrent claimers acquired no lease")
	}
	if second, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{99}, time.Second); err != nil || second != nil {
		t.Fatalf("unexpired lease protection = %#v err=%v", second, err)
	}
	preparedStale, err := s.PrepareAttempt(context.Background(), firstLease.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatalf("prepare before expiry: %v", err)
	}
	time.Sleep(220 * time.Millisecond)
	if n, err := s.ReclaimExpiredLeases(context.Background()); err != nil || n != 1 {
		t.Fatalf("expiry reclaim = %d err=%v", n, err)
	}
	second, err := s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{99}, time.Second)
	if err != nil || second == nil {
		t.Fatalf("expiry reclaim claim = %#v err=%v", second, err)
	}
	if _, err := s.PrepareAttempt(context.Background(), firstLease.Lease, SubmitMethodV2); err == nil {
		t.Fatal("stale lease prepared an attempt")
	}
	prepared, err := s.PrepareAttempt(context.Background(), second.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	obs := acceptedObservation(candidate)
	stale, err := s.CompleteAttempt(context.Background(), firstLease.Lease, preparedStale.AttemptID, obs, SubmitDecisionAcceptedFinal)
	if err != nil || stale.State == SubmissionStateAcceptedFinal || stale.FirstAccepted {
		t.Fatalf("stale completion was not fenced: transition=%+v err=%v", stale, err)
	}
	transition, err := s.CompleteAttempt(context.Background(), second.Lease, prepared.AttemptID, obs, SubmitDecisionAcceptedFinal)
	if err != nil || transition.State != SubmissionStateAcceptedFinal || !transition.FirstAccepted {
		t.Fatalf("current accepted completion = %+v err=%v", transition, err)
	}
	s.Close()
	reopened := NewPostgresStorage(journalTestURL(t))
	if err := reopened.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	view, err := reopened.GetCandidate(context.Background(), candidate.CandidateID)
	if err != nil || view.State != SubmissionStateAcceptedFinal {
		t.Fatalf("reopened accepted state=%q err=%v", view.State, err)
	}
	var receipts int
	if err := reopened.pool.QueryRow(context.Background(), `SELECT count(*) FROM accepted_block_submission_receipts_v1`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("reopened receipt count=%d, want 1", receipts)
	}
	if final, err := reopened.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{8}, time.Second); err != nil || final != nil {
		t.Fatalf("final candidate was resubmittable: %#v err=%v", final, err)
	}
}

func TestSubmissionStorageFaultBoundariesBeforeAndAfter(t *testing.T) {
	cases := []struct {
		point string
		setup string
		want  string
	}{
		{"candidate_insert", "candidate", SubmissionStatePending},
		{"state_insert", "candidate", SubmissionStatePending},
		{"candidate_commit", "candidate", SubmissionStatePending},
		{"lease_update_commit", "lease", SubmissionStateInFlight},
		{"prepared_attempt_insert", "prepared", SubmissionStateInFlight},
		{"prepared_attempt_commit", "prepared", SubmissionStateInFlight},
		{"attempt_completion_update", "completion", SubmissionStateInFlight},
		{"state_update", "completion", SubmissionStateInFlight},
		{"accepted_receipt_insert", "completion", SubmissionStateInFlight},
		{"completion_commit", "completion", SubmissionStateAcceptedFinal},
	}
	for _, tc := range cases {
		for _, after := range []bool{false, true} {
			tc, after := tc, after
			t.Run(tc.point+map[bool]string{false: "_before", true: "_after"}[after], func(t *testing.T) {
				s := migratedJournal(t)
				candidate := storageTestCandidate()
				var lease *LeasedCandidateV1
				var prepared PreparedAttemptV1
				var err error
				if tc.setup != "candidate" {
					if _, disposition, err := s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate)); err != nil || disposition != PutDispositionInserted {
						t.Fatalf("seed candidate: %q %v", disposition, err)
					}
				}
				if tc.setup == "prepared" || tc.setup == "completion" {
					lease, err = s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{1}, time.Second)
					if err != nil || lease == nil {
						t.Fatalf("seed lease: %#v %v", lease, err)
					}
				}
				if tc.setup == "completion" {
					prepared, err = s.PrepareAttempt(context.Background(), lease.Lease, SubmitMethodV2)
					if err != nil {
						t.Fatalf("seed prepared attempt: %v", err)
					}
				}
				var calls int
				submissionFaultHookV1 = func(point string, gotAfter bool) error {
					if point == tc.point && gotAfter == after {
						calls++
						return errors.New("injected durable submission fault")
					}
					return nil
				}
				switch tc.setup {
				case "candidate":
					_, _, _ = s.PutCandidate(context.Background(), candidate, storageTestAccountingIntent(candidate))
				case "lease":
					_, _ = s.ClaimCandidate(context.Background(), candidate.CandidateID, [16]byte{2}, time.Second)
				case "prepared":
					_, _ = s.PrepareAttempt(context.Background(), lease.Lease, SubmitMethodV2)
				case "completion":
					_, _ = s.CompleteAttempt(context.Background(), lease.Lease, prepared.AttemptID, acceptedObservation(candidate), SubmitDecisionAcceptedFinal)
				}
				submissionFaultHookV1 = nil
				if calls != 1 {
					t.Fatalf("fault hook calls=%d for %s after=%t", calls, tc.point, after)
				}
				s.Close()
				reopened := NewPostgresStorage(journalTestURL(t))
				if err := reopened.Connect(context.Background()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(reopened.Close)
				var candidates, states, attempts, receipts, intents, creditLines int
				if err := reopened.pool.QueryRow(context.Background(), `SELECT
					(SELECT count(*) FROM block_submission_candidates_v1),
					(SELECT count(*) FROM block_submission_state_v1),
					(SELECT count(*) FROM block_submission_attempts_v1),
					(SELECT count(*) FROM accepted_block_submission_receipts_v1),
					(SELECT count(*) FROM accepted_block_accounting_intents_v1),
					(SELECT count(*) FROM accepted_block_accounting_credit_lines_v1)`).Scan(&candidates, &states, &attempts, &receipts, &intents, &creditLines); err != nil {
					t.Fatal(err)
				}
				if tc.setup == "candidate" && after && tc.point == "candidate_commit" {
					if candidates != 1 || states != 1 || intents != 1 || creditLines != 1 {
						t.Fatalf("post-commit candidate fault lost atomic candidate/accounting state: candidates=%d states=%d intents=%d lines=%d", candidates, states, intents, creditLines)
					}
				} else if tc.setup == "candidate" {
					if candidates != 0 || states != 0 || intents != 0 || creditLines != 0 {
						t.Fatalf("candidate fault crossed rollback boundary: candidates=%d states=%d intents=%d lines=%d", candidates, states, intents, creditLines)
					}
				}
				if tc.setup == "lease" {
					view, err := reopened.GetCandidate(context.Background(), candidate.CandidateID)
					if err != nil {
						t.Fatal(err)
					}
					want := SubmissionStatePending
					if after {
						want = SubmissionStateInFlight
					}
					if view.State != want {
						t.Fatalf("lease fault state=%q want %q", view.State, want)
					}
				}
				if tc.setup == "prepared" {
					view, err := reopened.GetCandidate(context.Background(), candidate.CandidateID)
					if err != nil {
						t.Fatal(err)
					}
					wantAttempts := 0
					if tc.point == "prepared_attempt_commit" && after {
						wantAttempts = 1
					}
					if view.State != SubmissionStateInFlight || attempts != wantAttempts {
						t.Fatalf("prepared fault state=%q attempts=%d want=%d after=%t", view.State, attempts, wantAttempts, after)
					}
				}
				if tc.setup == "completion" {
					view, err := reopened.GetCandidate(context.Background(), candidate.CandidateID)
					if err != nil {
						t.Fatal(err)
					}
					want := tc.want
					if tc.point != "completion_commit" || !after {
						want = SubmissionStateInFlight
					}
					if view.State != want {
						t.Fatalf("completion fault state=%q want %q attempts=%d receipts=%d", view.State, want, attempts, receipts)
					}
					if want == SubmissionStateAcceptedFinal && receipts != 1 {
						t.Fatalf("accepted completion lost atomic receipt: %d", receipts)
					}
				}
			})
		}
	}
}
