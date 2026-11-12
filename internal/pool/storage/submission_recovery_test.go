package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"
)

func recoveryFixture(t *testing.T) (*PostgresStorage, FrozenCandidateV1, RefreshedJobRecoveryProofV1) {
	return recoveryFixtureWithHook(t, nil)
}

func recoveryFixtureWithHook(t *testing.T, beforeConflict func(*PostgresStorage, FrozenCandidateV1)) (*PostgresStorage, FrozenCandidateV1, RefreshedJobRecoveryProofV1) {
	t.Helper()
	s := migratedJournal(t)
	applyWideAccountingMigration(t, s)
	ctx := context.Background()
	c := storageTestCandidate()
	if _, _, err := s.PutCandidateV2(ctx, c, wideAccountingIntentV2(t, c)); err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimCandidate(ctx, c.CandidateID, [16]byte{9}, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	attempt, err := s.PrepareAttempt(ctx, lease.Lease, SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	if beforeConflict != nil {
		beforeConflict(s, c)
	}
	duplicate := refreshedJobCandidate(t, c)
	// Reproduce the OLD implementation's race, without changing any runtime
	// checks: conflict commits before the already-in-flight acceptance returns.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertConflict(ctx, tx, c.CandidateID, "candidate_identity_mismatch", c.FrozenDigest[:], duplicate.FrozenDigest[:], duplicate.FrozenPayload, nil, differenceMask(c, duplicate)); err != nil {
		t.Fatal(err)
	}
	if err := latchCandidateConflict(ctx, tx, c.CandidateID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	observation := acceptedObservation(c)
	observation.TemplateSeq = big.NewInt(71)
	observation.HTTPStatus = new(int)
	*observation.HTTPStatus = 200
	observation.ResponsePrefix = []byte(`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"accepted","block_hash":"0x` + hex.EncodeToString(c.ExpectedBlockHash[:]) + `","height":7,"template_seq":71}}`)
	if _, err := s.CompleteAttempt(ctx, lease.Lease, attempt.AttemptID, observation, SubmitDecisionAcceptedFinal); err != nil {
		t.Fatal(err)
	}
	var conflict []byte
	if err := s.pool.QueryRow(ctx, `SELECT conflict_id FROM block_submission_conflicts_v1`).Scan(&conflict); err != nil {
		t.Fatal(err)
	}
	proof := RefreshedJobRecoveryProofV1{Version: "refreshed-job-recovery/v1", CandidateID: hex.EncodeToString(c.CandidateID[:]), ConflictID: hex.EncodeToString(conflict), AttemptID: hex.EncodeToString(attempt.AttemptID[:]), ChainID: "1404", BackendID: "node", BlockHash: hex.EncodeToString(c.ExpectedBlockHash[:]), HeaderHex: hex.EncodeToString(c.HeaderBytes[:]), NativeHeight: "7", CoreTemplateSeq: "71", CheckpointNumber: "99", CheckpointHash: hex.EncodeToString(make([]byte, 32)), CheckpointStateRoot: hex.EncodeToString(make([]byte, 32)), ObservedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), WitnessPeerIDs: []string{"peer-primary", "peer-witness"}, OperatorReference: "Francois authorized isolated recovery test"}
	return s, c, proof
}

func TestRefreshedJobRecoveryConcurrentApply(t *testing.T) {
	s, _, p := recoveryFixture(t)
	var wg sync.WaitGroup
	results := make(chan RefreshedJobRecoveryResultV1, 4)
	failures := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := s.RecoverRefreshedJobV1(context.Background(), p, true)
			if err != nil {
				failures <- err
			} else {
				results <- result
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	applied, replayed := 0, 0
	for result := range results {
		if result.Applied {
			applied++
		}
		if result.AlreadyApplied {
			replayed++
		}
	}
	if applied != 1 || replayed != 3 {
		t.Fatalf("concurrent recovery applied/replayed=%d/%d, want 1/3", applied, replayed)
	}
}

func TestRefreshedJobRecoveryRejectsAuditSchemaDrift(t *testing.T) {
	s, _, p := recoveryFixture(t)
	if _, err := s.RecoverRefreshedJobV1(context.Background(), p, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), `ALTER TABLE block_submission_job_recoveries_v1 DROP CONSTRAINT block_submission_job_recoveries_v1_proof_sha256_check; ALTER TABLE block_submission_job_recoveries_v1 ADD CONSTRAINT block_submission_job_recoveries_v1_proof_sha256_check CHECK(octet_length(proof_sha256)>0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecoverRefreshedJobV1(context.Background(), p, true); err == nil {
		t.Fatal("same-count constraint drift not detected")
	}
}

func TestRefreshedJobRecoveryCommitFaults(t *testing.T) {
	for _, where := range []struct {
		point string
		after bool
	}{{"recovery_receipt", false}, {"recovery_receipt", true}, {"recovery_commit", false}, {"recovery_commit", true}} {
		t.Run(fmt.Sprintf("%s-after-%t", where.point, where.after), func(t *testing.T) {
			s, c, p := recoveryFixture(t)
			ctx := context.Background()
			submissionFaultHookV1 = func(point string, after bool) error {
				if point == where.point && after == where.after {
					return errors.New("simulated recovery interruption")
				}
				return nil
			}
			t.Cleanup(func() { submissionFaultHookV1 = nil })
			if _, err := s.RecoverRefreshedJobV1(ctx, p, true); err == nil {
				t.Fatal("injected fault not exercised")
			}
			submissionFaultHookV1 = nil
			retried, err := s.RecoverRefreshedJobV1(ctx, p, true)
			if err != nil {
				t.Fatal(err)
			}
			if where.point == "recovery_commit" && where.after {
				if !retried.AlreadyApplied {
					t.Fatal("lost commit acknowledgement duplicated recovery")
				}
			} else if !retried.Applied {
				t.Fatal("failed transaction did not retry cleanly")
			}
			if applied, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil || !applied.Applied {
				t.Fatalf("post-fault accounting: %+v %v", applied, err)
			}
			if second, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil || !second.AlreadyApplied {
				t.Fatalf("post-fault repeated accounting: %+v %v", second, err)
			}
		})
	}
}

func TestRefreshedJobRecoveryIsAppendOnlyAndExactlyOnce(t *testing.T) {
	s, c, proof := recoveryFixture(t)
	ctx := context.Background()
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err == nil {
		t.Fatal("fixture did not reproduce latch")
	}
	var before string
	if err := s.pool.QueryRow(ctx, `SELECT md5(string_agg(row_to_json(t)::text,'|' ORDER BY conflict_id)) FROM block_submission_conflicts_v1 t`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	first, err := s.RecoverRefreshedJobV1(ctx, proof, false)
	if err != nil || !first.Prepared || first.Applied {
		t.Fatalf("dry run: %+v %v", first, err)
	}
	first, err = s.RecoverRefreshedJobV1(ctx, proof, true)
	if err != nil || !first.Applied {
		t.Fatalf("apply: %+v %v", first, err)
	}
	second, err := s.RecoverRefreshedJobV1(ctx, proof, true)
	if err != nil || !second.AlreadyApplied {
		t.Fatalf("retry: %+v %v", second, err)
	}
	if applied, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil || !applied.Applied {
		t.Fatalf("account: %+v %v", applied, err)
	}
	if again, err := s.ApplyAcceptedAccountingV2(ctx, c.CandidateID); err != nil || !again.AlreadyApplied {
		t.Fatalf("re-account: %+v %v", again, err)
	}
	var state, after string
	var conflicts, receipts, effects, resolutions int
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT state FROM block_submission_state_v1),(SELECT md5(string_agg(row_to_json(t)::text,'|' ORDER BY conflict_id)) FROM block_submission_conflicts_v1 t),(SELECT count(*) FROM block_submission_conflicts_v1),(SELECT count(*) FROM accepted_block_submission_receipts_v1),(SELECT count(*) FROM accepted_block_accounting_effects_v1),(SELECT count(*) FROM block_submission_job_recoveries_v1)`).Scan(&state, &after, &conflicts, &receipts, &effects, &resolutions); err != nil {
		t.Fatal(err)
	}
	if state != "conflict" || before != after || conflicts != 1 || receipts != 1 || effects != 1 || resolutions != 1 {
		t.Fatalf("non-additive or repeated recovery: %s %t %d/%d/%d/%d", state, before == after, conflicts, receipts, effects, resolutions)
	}
	// Only the verified receipt AND original effect can qualify the explicit
	// additive recovery. The original SQL conflict row remains immutable.
	if err := s.VerifySubmissionJournalV1(ctx, c.ChainID, c.BackendID); err != nil {
		t.Fatalf("fully qualified recovery did not verify: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE block_submission_job_recoveries_v1 SET operator_reference='altered'`); err == nil {
		t.Fatal("recovery audit row mutable")
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM block_submission_job_recoveries_v1`); err == nil {
		t.Fatal("recovery audit row deletable")
	}
	s.Close()
	reopened := NewPostgresStorage(s.dbURL)
	if err := reopened.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if again, err := reopened.RecoverRefreshedJobV1(ctx, proof, true); err != nil || !again.AlreadyApplied {
		t.Fatalf("restart recovery: %+v %v", again, err)
	}
}

func TestRefreshedJobRecoveryRejectsIncorrectEvidence(t *testing.T) {
	for _, change := range []string{"block", "header", "attempt", "witness", "sequence", "economic", "payload", "second-conflict"} {
		t.Run(change, func(t *testing.T) {
			s, c, p := recoveryFixture(t)
			ctx := context.Background()
			switch change {
			case "block":
				p.BlockHash = hex.EncodeToString(make([]byte, 32))
			case "header":
				p.HeaderHex = "00"
			case "attempt":
				p.AttemptID = hex.EncodeToString(make([]byte, 16))
			case "witness":
				p.WitnessPeerIDs = []string{"same", "same"}
			case "sequence":
				p.CoreTemplateSeq = "34"
			case "economic":
				tx, _ := s.pool.Begin(ctx)
				if err := insertAcceptedAccountingConflictTxV1(ctx, tx, c.CandidateID, "candidate_intent_mismatch", nil, nil, "different intent"); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			case "payload", "second-conflict":
				other := refreshedJobCandidate(t, c)
				other.SharePowHash[0] ^= 1
				other.FrozenPayload, _ = encodeFrozenCandidateJournalV1(other)
				other.FrozenDigest = sha256.Sum256(other.FrozenPayload)
				tx, _ := s.pool.Begin(ctx)
				if err := insertConflict(ctx, tx, c.CandidateID, "candidate_identity_mismatch", c.FrozenDigest[:], other.FrozenDigest[:], other.FrozenPayload, nil, differenceMask(c, other)); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.RecoverRefreshedJobV1(ctx, p, true); err == nil {
				t.Fatal("incorrect proof/contradiction accepted")
			}
			var receipts int
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM accepted_block_submission_receipts_v1`).Scan(&receipts); err != nil {
				t.Fatal(err)
			}
			if receipts != 0 {
				t.Fatal("rejected proof wrote receipt")
			}
		})
	}
}
