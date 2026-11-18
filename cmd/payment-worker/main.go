// payment-worker is a separately supervised, explicitly approved sole payer.
// It never migrates/adopts a namespace or releases the predecessor's firewall.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"asicPool/internal/pool/payments"
	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
)

type scopedRewardGuard struct{ guard *storage.RewardCreditGuardV1 }

func (g scopedRewardGuard) VerifyScopedCredit(ctx context.Context, wallet payments.WalletID, credit payments.Credit) error {
	if wallet.ChainID != 1404 || g.guard == nil {
		return errors.New("scoped reward wallet unavailable")
	}
	return g.guard.Verify(ctx, credit.ID, wallet.Sender)
}

func signerFactory(a approval, reader func(string) ([]byte, error)) func(context.Context) (payments.Signer, error) {
	return func(ctx context.Context) (payments.Signer, error) {
		if !a.ActivationApproved || ctx.Err() != nil {
			return nil, errors.New("signer activation unavailable")
		}
		data, err := reader(a.KeyFile)
		if err != nil {
			return nil, errors.New("approved key input unavailable")
		}
		defer clear(data)
		fileHash := sha256.Sum256(data)
		if hex.EncodeToString(fileHash[:]) != a.KeyFileSHA256 {
			return nil, errors.New("approved key file changed")
		}
		value, count := "", 0
		addresses := make(map[string]bool)
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "POOL_PRIVATE_KEY=") {
				value, count = strings.TrimSpace(strings.TrimPrefix(line, "POOL_PRIVATE_KEY=")), count+1
			} else if strings.HasPrefix(line, "POOL_COINBASE_ADDRESS=") || strings.HasPrefix(line, "MINING_POOL_ADDRESS=") {
				name, raw, _ := strings.Cut(line, "=")
				if addresses[name] || !common.IsHexAddress(raw) || common.HexToAddress(raw) != a.Wallet.Sender {
					return nil, errors.New("wallet configuration address rejected")
				}
				addresses[name] = true
			} else if strings.TrimSpace(line) != "" && !strings.HasPrefix(strings.TrimSpace(line), "#") {
				return nil, errors.New("unapproved key file fields")
			}
		}
		valueHash := sha256.Sum256([]byte(value))
		if count != 1 || hex.EncodeToString(valueHash[:]) != a.KeyValueSHA256 || ctx.Err() != nil {
			return nil, errors.New("approved key identity unavailable")
		}
		return payments.NewLocalSigner(a.Wallet, value)
	}
}

func connectWorker(ctx context.Context, a approval, exclusive func(context.Context) error) (*payments.Worker, func(), error) {
	if err := exclusive(ctx); err != nil {
		return nil, nil, err
	}
	if os.Getenv("PG_URL") == "" {
		return nil, nil, errors.New("explicit database target required")
	}
	config, err := pgxpool.ParseConfig(os.Getenv("PG_URL"))
	if err != nil || config.ConnConfig.Host != "127.0.0.1" || config.ConnConfig.Port != 55432 || config.ConnConfig.Database != "bdagpool" || config.ConnConfig.User != "bdag_pool" {
		return nil, nil, errors.New("approved database target rejected")
	}
	config.MaxConns = 2
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	config.ConnConfig.RuntimeParams["application_name"] = "bdag-supervised-payment-worker"
	config.ConnConfig.RuntimeParams["lock_timeout"] = "250ms"
	config.ConnConfig.RuntimeParams["statement_timeout"] = "15s"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, nil, errors.New("payment database unavailable")
	}
	evm, err := rpc.DialContext(ctx, a.EVMURL)
	if err != nil {
		pool.Close()
		return nil, nil, errors.New("payment EVM unavailable")
	}
	options := []rpc.ClientOption{}
	if user := os.Getenv("NODE_RPC_USER"); user != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + os.Getenv("NODE_RPC_PASS")))
		options = append(options, rpc.WithHeader("Authorization", "Basic "+auth))
	}
	native, err := rpc.DialOptions(ctx, a.NativeURL, options...)
	if err != nil {
		evm.Close()
		pool.Close()
		return nil, nil, errors.New("payment native RPC unavailable")
	}
	cleanup := func() { native.Close(); evm.Close(); pool.Close() }
	network, err := payments.NewRPCNetwork(evm, a.Wallet)
	if err != nil {
		cleanup()
		return nil, nil, errors.New("payment network rejected")
	}
	journal, err := payments.NewReviewedPostgresJournal(pool)
	if err != nil {
		cleanup()
		return nil, nil, errors.New("payment journal rejected")
	}
	canonical := storage.RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: payments.NativePayoutMaturity}
	creditGuard, err := storage.NewRewardCreditGuardV1(pool, "node", canonical)
	if err != nil {
		cleanup()
		return nil, nil, errors.New("reward credit guard rejected")
	}
	scopedJournal, err := payments.NewWalletScopedJournal(journal, scopedRewardGuard{guard: creditGuard})
	if err != nil {
		cleanup()
		return nil, nil, errors.New("wallet-scoped payment journal rejected")
	}
	guard := func(c context.Context) error {
		if err := exclusive(c); err != nil {
			return err
		}
		return a.Baseline.Check(c, network)
	}
	worker := &payments.Worker{Wallet: a.Wallet, Journal: scopedJournal,
		Network:   payments.NonceFloorNetwork{Network: network, Floor: a.Baseline.NonceFloor},
		Gate:      &payments.NativeGate{Native: native, EVM: evm, Wallet: a.Wallet},
		Exclusive: guard, SignerFactory: func(c context.Context) (payments.Signer, error) {
			if err := guard(c); err != nil {
				return nil, err
			}
			return signerFactory(a, privateRead)(c)
		}}
	return worker, cleanup, nil
}

func authorityState(err error) string {
	switch {
	case errors.Is(err, errLegacyAlive):
		return "awaiting_legacy_shutdown"
	case errors.Is(err, errInactive):
		return "awaiting_explicit_activation"
	case err != nil:
		return "authority_unverified"
	default:
		return "authority_verified_not_signing"
	}
}

func writeStatus(directory string, result map[string]any) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".payer-status-")
	if err != nil {
		return errors.New("payer status unavailable")
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(append(data, '\n')); err != nil {
		file.Close()
		return errors.New("payer status write failed")
	}
	if err = file.Close(); err != nil {
		return errors.New("payer status close failed")
	}
	if os.Rename(file.Name(), filepath.Join(directory, "status.json")) != nil {
		return errors.New("payer status commit failed")
	}
	return nil
}

func run(ctx context.Context, args []string, output io.Writer) error {
	return runWithHost(ctx, args, output, localHost{})
}

// Tests inject evidence only through this unexported function. The executable
// has no host-inspection override flag, environment knob, or fallback path.
func runWithHost(ctx context.Context, args []string, output io.Writer, host hostInspector) error {
	if len(args) != 3 || (args[0] != "check" && args[0] != "run") {
		return errors.New("explicit mode approval path and digest required")
	}
	if _, present := os.LookupEnv("POOL_PRIVATE_KEY"); present {
		return errors.New("worker must not inherit a signing key")
	}
	expected := strings.TrimPrefix(args[2], "sha256:")
	a, err := readApproval(args[1], expected)
	if err != nil {
		return err
	}
	exclusive := func(c context.Context) error {
		now, err := readApproval(args[1], expected)
		if err != nil {
			return err
		}
		return authorityCheck(c, now, host)
	}
	if args[0] == "check" {
		bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		err = exclusive(bounded)
		// This mode never constructs a network, journal, signer or key reader.
		return json.NewEncoder(output).Encode(map[string]any{"state": authorityState(err),
			"key_loaded": false, "send_attempted": false, "production_ready": false})
	}
	lock, err := lockRuntime(a.StatusDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	var worker *payments.Worker
	var cleanup func()
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()
	previous := ""
	for ctx.Err() == nil {
		bounded, cancel := context.WithTimeout(ctx, 45*time.Second)
		result := payments.Result{}
		err = exclusive(bounded)
		if err != nil {
			result.State = authorityState(err)
		} else {
			if worker == nil {
				worker, cleanup, err = connectWorker(bounded, a, exclusive)
			}
			if err == nil {
				result, err = worker.Step(bounded)
			} else {
				result.State = "awaiting_runtime_dependencies"
			}
		}
		cancel()
		if result.State == "" {
			result.State = "payment_operation_held"
		}
		status := map[string]any{"at": time.Now().UTC().Format(time.RFC3339),
			"state": result.State, "operation_error": err != nil,
			"activation_approved": a.ActivationApproved,
			"credit_id":           result.CreditID, "newly_confirmed": result.NewlyConfirmed}
		status["zero_credits_satisfied"] = result.ZeroSatisfied
		if statusErr := writeStatus(a.StatusDir, status); statusErr != nil {
			return statusErr
		}
		// Raw RPC/SQL/key errors are never written to the supervisor journal.
		signature := result.State
		if err != nil {
			signature += ":error"
		}
		if signature != previous {
			if err := json.NewEncoder(output).Encode(status); err != nil {
				return err
			}
			previous = signature
		}
		delay := 10 * time.Second
		if err != nil || result.State == "awaiting_funds" || result.State == "no_eligible_credit" {
			delay = 30 * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		// Deliberately omit err.Error(): external errors can contain credentials.
		_, _ = io.WriteString(os.Stderr, "payment worker rejected configuration or operation; preserve state\n")
		os.Exit(1)
	}
}
