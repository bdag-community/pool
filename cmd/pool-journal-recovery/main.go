// Offline preparation tool. It is not part of the pool server or RPC surface.
package main

import (
	"asicPool/internal/pool/storage"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"time"
)

// Offline full-history verification may exceed two minutes on the Node 2 CPU.
// This only extends the maintenance budget, never skips a validation gate or
// changes a live submission deadline.
const offlineRecoveryTimeout = 10 * time.Minute

func main() {
	proofPath := flag.String("proof", "", "protected proof JSON file")
	apply := flag.Bool("apply", false, "explicitly append recovery and missing receipt")
	offline := flag.Bool("acknowledge-offline", false, "confirm the pool is stopped and backed up")
	account := flag.Bool("reconcile-accounting", false, "apply original immutable economic intent exactly once")
	verifyRuntime := flag.Bool("verify-runtime", false, "verify exact recovery-aware startup and report historical/unresolved conflicts")
	flag.Parse()
	if *proofPath == "" || (*apply && !*offline) || (*account && !*apply) {
		fmt.Fprintln(os.Stderr, "proof required; apply requires explicit offline acknowledgment; accounting requires apply")
		os.Exit(2)
	}
	dbURL := os.Getenv("POOL_RECOVERY_DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "protected POOL_RECOVERY_DATABASE_URL environment is required")
		os.Exit(2)
	}
	raw, err := os.ReadFile(*proofPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot read proof file")
		os.Exit(2)
	}
	var proof storage.RefreshedJobRecoveryProofV1
	if err := json.Unmarshal(raw, &proof); err != nil {
		fmt.Fprintln(os.Stderr, "invalid proof JSON")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), offlineRecoveryTimeout)
	defer cancel()
	db := storage.NewPostgresStorage(dbURL)
	if err := db.Connect(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "cannot connect to recovery database")
		os.Exit(1)
	}
	defer db.Close()
	result, err := db.RecoverRefreshedJobV1(ctx, proof, *apply)
	if err != nil {
		fmt.Fprintln(os.Stderr, "recovery rejected:", err)
		os.Exit(1)
	}
	output := map[string]any{"recovery": result, "liveMiningAuthorized": false, "originalConflictRowsUnchanged": true}
	if proof.Version == "discovery-cut-recovery/v1" && *apply {
		chain, ok := new(big.Int).SetString(proof.ChainID, 10)
		if !ok {
			fmt.Fprintln(os.Stderr, "invalid chain id")
			os.Exit(1)
		}
		if err := db.VerifySubmissionJournalV1(ctx, chain, proof.BackendID); err != nil {
			fmt.Fprintln(os.Stderr, "discovery runtime verification rejected:", err)
			os.Exit(1)
		}
	}
	if *account {
		raw, err := hex.DecodeString(proof.CandidateID)
		if err != nil || len(raw) != 32 {
			fmt.Fprintln(os.Stderr, "invalid candidate id")
			os.Exit(1)
		}
		var cid [32]byte
		copy(cid[:], raw)
		effect, err := db.ApplyAcceptedAccountingV2(ctx, cid)
		if err != nil {
			fmt.Fprintln(os.Stderr, "accounting rejected:", err)
			os.Exit(1)
		}
		output["accounting"] = effect
	}
	if *verifyRuntime {
		chainID, ok := new(big.Int).SetString(proof.ChainID, 10)
		if !ok {
			fmt.Fprintln(os.Stderr, "invalid chain id")
			os.Exit(1)
		}
		if err := db.VerifySubmissionJournalV1(ctx, chainID, proof.BackendID); err != nil {
			fmt.Fprintln(os.Stderr, "runtime verification rejected:", err)
			os.Exit(1)
		}
		metrics, err := db.ReadSubmissionMetrics(ctx)
		if err != nil || metrics.Conflict != metrics.RecoveredConflict || metrics.AccountingConflictCount != metrics.RecoveredAccountingConflict {
			fmt.Fprintln(os.Stderr, "runtime conflict metrics did not qualify")
			os.Exit(1)
		}
		output["runtimeGateVerified"] = true
		output["historicalConflictCount"] = metrics.Conflict
		output["verifiedRecoveryCount"] = metrics.RecoveredConflict
		output["unresolvedConflictCount"] = metrics.Conflict - metrics.RecoveredConflict
		output["historicalAccountingConflicts"] = metrics.AccountingConflictCount
		output["verifiedAccountingRecoveries"] = metrics.RecoveredAccountingConflict
	}
	json.NewEncoder(os.Stdout).Encode(output)
}
