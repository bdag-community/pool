package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
)

// SQL planning only, on the already verified, isolated actual-DB restore.
// This cannot admit mining, alter an allowance in the running pool or write rows.
func TestActualCreditRecoveryGuardPlan(t *testing.T) {
	raw := os.Getenv("POOL_CREDIT_PLAN_DATABASE_URL")
	if raw == "" {
		t.Skip("explicit isolated actual database required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/pool_mining_repair_20261012" {
		t.Fatal("non-isolated plan database")
	}
	s := NewPostgresStorage(raw)
	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a := new(discoveryRecoveryAllowanceV1)
	err = s.pool.QueryRow(ctx, `SELECT r.conflict_id,to_jsonb(r)::text,to_jsonb(f)::text,to_jsonb(c)::text,to_jsonb(attempt)::text,to_jsonb(receipt)::text,to_jsonb(i)::text,(SELECT coalesce(jsonb_agg(to_jsonb(l) ORDER BY line_index),'[]'::jsonb)::text FROM accepted_block_accounting_credit_lines_v1 l WHERE l.candidate_id=r.candidate_id),to_jsonb(e)::text FROM block_submission_discovery_recoveries_v1 r JOIN accepted_block_accounting_conflicts_v1 f USING(conflict_id) JOIN block_submission_candidates_v1 c ON c.candidate_id=r.candidate_id JOIN block_submission_attempts_v1 attempt ON attempt.attempt_id=r.source_attempt_id JOIN accepted_block_submission_receipts_v1 receipt ON receipt.candidate_id=r.candidate_id JOIN accepted_block_accounting_intents_v1 i ON i.candidate_id=r.candidate_id JOIN accepted_block_accounting_effects_v1 e ON e.candidate_id=r.candidate_id`).Scan(&a.conflictID, &a.proof, &a.conflict, &a.candidate, &a.attempt, &a.receipt, &a.intent, &a.lines, &a.effect)
	if err != nil {
		t.Fatal(err)
	}
	s.discoveryRecovery.Store(a)
	query, args := s.accountingConflictLatchSQLV1(1)
	var plan json.RawMessage
	if err := s.pool.QueryRow(ctx, "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT "+query, args...).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	fmt.Println("RECOVERY_GUARD_PLAN=" + string(plan))
	// Index coverage alone is not enough: reject full scans of the large ledgers.
	var trees []map[string]any
	if err := json.Unmarshal(plan, &trees); err != nil {
		t.Fatal(err)
	}
	var walk func(map[string]any)
	walk = func(n map[string]any) {
		if n["Node Type"] == "Seq Scan" && (n["Relation Name"] == "accepted_block_accounting_intents_v1" || n["Relation Name"] == "accepted_block_accounting_credit_lines_v1" || n["Relation Name"] == "credits") {
			t.Errorf("full ledger scan: %s", n["Relation Name"])
		}
		if children, ok := n["Plans"].([]any); ok {
			for _, child := range children {
				walk(child.(map[string]any))
			}
		}
	}
	if !strings.Contains(string(plan), "credits_block_hash_idx") {
		t.Error("reviewed credit index not used")
	}
	walk(trees[0]["Plan"].(map[string]any))
}
