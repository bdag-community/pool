package server

import (
	"context"
	"encoding/base64"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"asicPool/internal/pool/payments"
	"asicPool/internal/pool/sharejournal"
	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

type rewardRuntime struct {
	store            *storage.PostgresStorage
	chainID          *big.Int
	backend          string
	wallet           common.Address
	accounting       *durableShareAccounting
	collector        *rewardEvidenceCollector
	allocator        *rewardAllocator
	ipc, native, evm *rpc.Client
	ctx              context.Context
	cancel           context.CancelFunc
}

// newRewardRuntime is enabled only when the dedicated node socket is
// configured. It never installs a signer in the Stratum process.
func newRewardRuntime(cfg Config, pg *storage.PostgresStorage, chainID *big.Int, backend string) (*rewardRuntime, error) {
	if strings.TrimSpace(cfg.RewardIPCPath) == "" {
		return nil, nil
	}
	if !filepath.IsAbs(cfg.RewardIPCPath) || filepath.Clean(cfg.RewardIPCPath) != cfg.RewardIPCPath || cfg.RPCURL == "" || cfg.RewardEVMRPCURL == "" || chainID == nil || chainID.Cmp(big.NewInt(1404)) != 0 || strings.TrimSpace(cfg.PoolPrivateKey) != "" {
		return nil, errors.New("durable reward configuration is incomplete")
	}
	wallet, _, err := configuredPoolCoinbaseAddress()
	if err != nil {
		return nil, err
	}
	target := cfg.PPLNSTargetWork
	if target <= 0 {
		target = 1000
	}
	targetWork, err := acceptedShareWork(target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, _, err = pg.NextMatureRewardV1(ctx, 0); err != nil {
		return nil, err
	}
	ipc, err := rpc.DialIPC(ctx, cfg.RewardIPCPath)
	if err != nil {
		return nil, errors.New("reward IPC unavailable")
	}
	options := []rpc.ClientOption{}
	if cfg.RPCUser != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(cfg.RPCUser + ":" + cfg.RPCPassword))
		options = append(options, rpc.WithHeader("Authorization", "Basic "+auth))
	}
	native, err := rpc.DialOptions(ctx, cfg.RPCURL, options...)
	if err != nil {
		ipc.Close()
		return nil, errors.New("reward native RPC unavailable")
	}
	evm, err := rpc.DialContext(ctx, cfg.RewardEVMRPCURL)
	if err != nil {
		ipc.Close()
		native.Close()
		return nil, errors.New("reward EVM RPC unavailable")
	}
	var actualChain hexutil.Uint64
	if err = evm.CallContext(ctx, &actualChain, "eth_chainId"); err != nil || uint64(actualChain) != 1404 {
		ipc.Close()
		native.Close()
		evm.Close()
		return nil, errors.New("reward EVM chain identity unavailable")
	}
	life, stop := context.WithCancel(context.Background())
	verifier := storage.RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: payments.NativePayoutMaturity, RecoveryDirectory: os.Getenv("POOL_REWARD_RECOVERY_DIR")}
	gate := &payments.NativeGate{Native: native, EVM: evm, Wallet: payments.WalletID{ChainID: 1404, Sender: wallet}}
	return &rewardRuntime{store: pg, chainID: new(big.Int).Set(chainID), backend: backend, wallet: wallet,
		accounting: &durableShareAccounting{journal: pg, target: targetWork, policy: sharejournal.PartialShare},
		collector:  &rewardEvidenceCollector{node: ipc, store: pg, wallet: wallet},
		allocator:  &rewardAllocator{store: pg, gate: gate, verifier: verifier, wallet: wallet, chainID: new(big.Int).Set(chainID), backend: backend, bestEffort: os.Getenv("POOL_ACCOUNTING_BEST_EFFORT") != "0"},
		ipc:        ipc, native: native, evm: evm, ctx: life, cancel: stop}, nil
}

func (r *rewardRuntime) verifyAndReconcile(ctx context.Context) error {
	if r == nil {
		return errors.New("reward runtime unavailable")
	}
	return r.store.PrepareRewardStartupV1(ctx, r.chainID, r.backend, r.wallet)
}

func (r *rewardRuntime) start() {
	go r.collector.run(r.ctx)
	go r.allocator.run(r.ctx)
}

func (r *rewardRuntime) close() {
	if r == nil {
		return
	}
	r.cancel()
	r.ipc.Close()
	r.native.Close()
	r.evm.Close()
}
