// payment-admin performs explicit coordinator-reviewed schema/audit operations.
// It never loads a treasury private key, starts a worker or sends a transaction.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"asicPool/internal/pool/payments"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func readRequest(path string) (payments.LegacyRepair, error) {
	var q payments.LegacyRepair
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 8192 {
		return q, errors.New("request must be a small private regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) > 8192 {
		return q, errors.New("request read rejected")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&q); err != nil {
		return q, errors.New("invalid repair request")
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return q, errors.New("trailing repair request rejected")
	}
	return q, nil
}

func run(args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("explicit operation required")
	}
	if args[0] != "schema-check" && args[0] != "migrate" && args[0] != "repair-legacy" && args[0] != "preflight-legacy" {
		return errors.New("unknown admin operation")
	}
	if os.Getenv("POOL_PRIVATE_KEY") != "" {
		return errors.New("payment admin must not receive signing key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if os.Getenv("PG_URL") == "" {
		return errors.New("explicit database target required")
	}
	config, err := pgxpool.ParseConfig(os.Getenv("PG_URL"))
	if err != nil {
		return errors.New("database configuration rejected")
	}
	config.MaxConns = 2
	config.ConnConfig.RuntimeParams["application_name"] = "bdag-explicit-payment-admin"
	config.ConnConfig.RuntimeParams["lock_timeout"] = "250ms"
	config.ConnConfig.RuntimeParams["statement_timeout"] = "15s"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return errors.New("database unavailable")
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return errors.New("database not ready")
	}
	switch args[0] {
	case "schema-check":
		var absent bool
		if err = pool.QueryRow(ctx, `SELECT to_regnamespace('pool_payments_v1') IS NULL`).Scan(&absent); err != nil {
			return err
		}
		if absent {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "payment_namespace_absent", "namespace_absent": true})
		}
		j, _ := payments.NewReviewedPostgresJournal(pool)
		if err = j.Verify(ctx); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "reviewed_payment_namespace_verified", "catalog_fingerprint": payments.ReviewedSchemaFingerprint})
	case "migrate":
		fp, err := payments.ApplyMigration(ctx, pool)
		if err != nil {
			return err
		}
		if fp != payments.ReviewedSchemaFingerprint {
			return errors.New("created schema differs from reviewed artifact; do not replay")
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "reviewed_payment_namespace_created", "catalog_fingerprint": fp})
	}
	if len(args) != 2 {
		return errors.New("reviewed legacy request file required")
	}
	q, err := readRequest(args[1])
	if err != nil {
		return err
	}
	// Connection identity and the request's exact evidence digest are pinned by
	// the coordinator's host/container/hold/approval preflight, not CLI defaults.
	evmURL := os.Getenv("BDAG_EVM_HTTP_URL")
	nativeURL := os.Getenv("BDAG_NODE_RPC_URL")
	if !strings.HasPrefix(evmURL, "http://127.0.0.1:") || !strings.HasPrefix(nativeURL, "http://127.0.0.1:") {
		return errors.New("nonlocal repair RPC endpoints rejected")
	}
	evm, err := rpc.DialContext(ctx, evmURL)
	if err != nil {
		return err
	}
	defer evm.Close()
	opts := []rpc.ClientOption{}
	if user := os.Getenv("NODE_RPC_USER"); user != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + os.Getenv("NODE_RPC_PASS")))
		opts = append(opts, rpc.WithHeader("Authorization", "Basic "+auth))
	}
	native, err := rpc.DialOptions(ctx, nativeURL, opts...)
	if err != nil {
		return err
	}
	defer native.Close()
	network, err := payments.NewRPCNetwork(evm, q.Wallet)
	if err != nil {
		return err
	}
	gate := &payments.NativeGate{Native: native, EVM: evm, Wallet: q.Wallet}
	journal, _ := payments.NewReviewedPostgresJournal(pool)
	if args[0] == "preflight-legacy" {
		if err = journal.PreflightLegacyCredit(ctx, q, network, gate.Check); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "legacy_credit_preflight_passed", "credit_id": q.Missing.ID, "cash_rows_modified": false})
	}
	changed, err := journal.RepairLegacyCredit(ctx, q, network, gate.Check)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "legacy_credit_audit_reconciled", "newly_restored": changed, "credit_id": q.Missing.ID, "transactions_broadcast": false})
}

func failureCode(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return "database_" + pg.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline"
	}
	known := map[string]string{
		"native main-chain maturity height unavailable":          "native_height_unavailable",
		"native payout readiness is stale, incomplete or unsafe": "native_readiness",
		"native legacy repair readiness or maturity unavailable": "native_repair_readiness",
		"canonical nonce replacement body mismatch":              "replacement_body",
		"successful replacement receipt missing":                 "replacement_receipt",
		"replacement is not canonical":                           "replacement_canonical",
		"legacy credit identity changed before repair":           "credit_binding",
		"legacy payout row identity changed":                     "payout_binding",
		"payment catalog drift under wallet fence":               "catalog_drift",
	}
	if code, ok := known[err.Error()]; ok {
		return code
	}
	return "unclassified_rejection"
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "explicit payment administration failed; code="+failureCode(err)+"; preserve state and inspect bounded evidence")
		os.Exit(1)
	}
}
