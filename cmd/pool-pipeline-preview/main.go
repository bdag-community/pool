// pool-pipeline-preview checks the next fenced payout action without loading a
// private key or permitting a transaction broadcast.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"

	"asicPool/internal/pool/payments"
	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
)

type noSigner struct{}

func (noSigner) Sign(context.Context, payments.Credit, uint64, *big.Int) ([]byte, error) {
	return nil, errors.New("preview cannot sign")
}

type noBroadcast struct{ *payments.RPCNetwork }

func (noBroadcast) Broadcast(context.Context, []byte) error {
	return errors.New("preview cannot broadcast")
}

type rewardGuard struct{ inner *storage.RewardCreditGuardV1 }

func (g rewardGuard) VerifyScopedCredit(ctx context.Context, wallet payments.WalletID, credit payments.Credit) error {
	return g.inner.Verify(ctx, credit.ID, wallet.Sender)
}

func run() error {
	address := os.Getenv("POOL_COINBASE_ADDRESS")
	if !common.IsHexAddress(address) || common.HexToAddress(address) == (common.Address{}) || os.Getenv("PG_URL") == "" || os.Getenv("NODE_RPC_URL") == "" || os.Getenv("EVM_RPC_URL") == "" {
		return errors.New("preview configuration incomplete")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(os.Getenv("PG_URL"))
	if err != nil {
		return errors.New("preview database configuration invalid")
	}
	config.MaxConns = 3
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return errors.New("preview database unavailable")
	}
	defer pool.Close()
	auth := base64.StdEncoding.EncodeToString([]byte(os.Getenv("NODE_RPC_USER") + ":" + os.Getenv("NODE_RPC_PASS")))
	native, err := rpc.DialOptions(ctx, os.Getenv("NODE_RPC_URL"), rpc.WithHeader("Authorization", "Basic "+auth))
	if err != nil {
		return errors.New("preview native RPC unavailable")
	}
	defer native.Close()
	evm, err := rpc.DialContext(ctx, os.Getenv("EVM_RPC_URL"))
	if err != nil {
		return errors.New("preview EVM RPC unavailable")
	}
	defer evm.Close()
	wallet := payments.WalletID{ChainID: 1404, Sender: common.HexToAddress(address)}
	network, err := payments.NewRPCNetwork(evm, wallet)
	if err != nil {
		return err
	}
	base, err := payments.NewReviewedPostgresJournal(pool)
	if err != nil {
		return err
	}
	if err = base.Verify(ctx); err != nil {
		return err
	}
	verifier := storage.RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: payments.NativePayoutMaturity, RecoveryDirectory: os.Getenv("POOL_REWARD_RECOVERY_DIR")}
	guard, err := storage.NewRewardCreditGuardV1(pool, "node", verifier)
	if err != nil {
		return err
	}
	journal, err := payments.NewWalletScopedJournal(base, rewardGuard{guard})
	if err != nil {
		return err
	}
	gate := &payments.NativeGate{Native: native, EVM: evm, Wallet: wallet}
	maxHeight, eligible, err := gate.Check(ctx)
	if err != nil || !eligible {
		return errors.New("preview chain readiness unavailable")
	}
	ready := func(ctx context.Context) error {
		next, ok, err := gate.Check(ctx)
		if err != nil || !ok || next < maxHeight {
			return errors.New("preview chain readiness changed")
		}
		return nil
	}
	engine := payments.Engine{Wallet: wallet, Journal: journal, Network: noBroadcast{network}, Signer: noSigner{}, Ready: ready, RequireFreshWalletHistory: true, EnablePipeline: true, PreviewPipeline: true}
	result, err := engine.Step(ctx, maxHeight)
	if err != nil {
		return err
	}
	fmt.Printf("pipeline preview: state=%s credit=%d\n", result.State, result.CreditID)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
