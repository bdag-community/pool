// pool-payer runs a single wallet-scoped payment worker for a newly created
// pool wallet. It never adopts old credits or prints private key material.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"asicPool/internal/pool/payments"
	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
)

func readWallet(path string) ([]byte, common.Address, string, error) {
	var zero common.Address
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, zero, "", errors.New("wallet path must be absolute")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil || parent != filepath.Dir(path) {
		return nil, zero, "", errors.New("wallet directory is unavailable or symlinked")
	}
	dir, err := os.Stat(parent)
	if err != nil || !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
		return nil, zero, "", errors.New("wallet directory must be private")
	}
	owner, ok := dir.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Geteuid()) {
		return nil, zero, "", errors.New("wallet directory owner differs")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, zero, "", errors.New("wallet file unavailable")
	}
	file := os.NewFile(uintptr(fd), "pool-wallet")
	defer file.Close()
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || stat.Size < 1 || stat.Size > 16384 {
		return nil, zero, "", errors.New("wallet file permissions rejected")
	}
	data, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil || len(data) > 16384 {
		return nil, zero, "", errors.New("wallet file could not be read")
	}
	var keyText string
	var address common.Address
	seen := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok || seen[name] {
			clear(data)
			return nil, zero, "", errors.New("wallet file fields invalid")
		}
		seen[name] = true
		switch name {
		case "POOL_PRIVATE_KEY":
			keyText = value
		case "POOL_COINBASE_ADDRESS", "MINING_POOL_ADDRESS":
			if !common.IsHexAddress(value) || common.HexToAddress(value) == zero {
				clear(data)
				return nil, zero, "", errors.New("wallet address invalid")
			}
			if address != zero && address != common.HexToAddress(value) {
				clear(data)
				return nil, zero, "", errors.New("wallet addresses differ")
			}
			address = common.HexToAddress(value)
		default:
			clear(data)
			return nil, zero, "", errors.New("unexpected wallet file field")
		}
	}
	key, err := crypto.HexToECDSA(strings.TrimPrefix(keyText, "0x"))
	if err != nil || address == zero || !seen["POOL_COINBASE_ADDRESS"] || !seen["MINING_POOL_ADDRESS"] || crypto.PubkeyToAddress(key.PublicKey) != address {
		clear(data)
		return nil, zero, "", errors.New("wallet key and address differ")
	}
	key.D.SetInt64(0)
	return data, address, keyText, nil
}

type rewardGuard struct{ inner *storage.RewardCreditGuardV1 }

func (g rewardGuard) VerifyScopedCredit(ctx context.Context, wallet payments.WalletID, credit payments.Credit) error {
	if g.inner == nil || wallet.ChainID != 1404 {
		return errors.New("reward credit guard unavailable")
	}
	return g.inner.Verify(ctx, credit.ID, wallet.Sender)
}

func run(ctx context.Context) error {
	walletPath := os.Getenv("POOL_WALLET_ENV_FILE")
	data, address, _, err := readWallet(walletPath)
	if err != nil {
		return err
	}
	walletDigest := sha256.Sum256(data)
	clear(data)
	lockPath := filepath.Join(filepath.Dir(walletPath), ".pool-payer.lock")
	fd, err := unix.Open(lockPath, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return errors.New("wallet payer lock unavailable")
	}
	lock := os.NewFile(uintptr(fd), "pool-payer-lock")
	defer lock.Close()
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		return errors.New("wallet payer lock is already held or invalid")
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	exclusive := func(c context.Context) error {
		if c.Err() != nil {
			return c.Err()
		}
		var held unix.Stat_t
		var current unix.Stat_t
		if unix.Fstat(fd, &held) != nil || unix.Lstat(lockPath, &current) != nil || held.Ino != current.Ino || held.Dev != current.Dev || held.Nlink != 1 {
			return errors.New("wallet payer lock changed")
		}
		return nil
	}
	pgURL, nativeURL, evmURL := os.Getenv("PG_URL"), os.Getenv("NODE_RPC_URL"), os.Getenv("EVM_RPC_URL")
	if pgURL == "" || nativeURL == "" || evmURL == "" {
		return errors.New("pool database and native/EVM RPCs are required")
	}
	config, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return errors.New("pool database configuration invalid")
	}
	config.MaxConns = 3
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return errors.New("pool database unavailable")
	}
	defer pool.Close()
	options := []rpc.ClientOption{}
	if user := os.Getenv("NODE_RPC_USER"); user != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + os.Getenv("NODE_RPC_PASS")))
		options = append(options, rpc.WithHeader("Authorization", "Basic "+auth))
	}
	native, err := rpc.DialOptions(ctx, nativeURL, options...)
	if err != nil {
		return errors.New("native RPC unavailable")
	}
	defer native.Close()
	evm, err := rpc.DialContext(ctx, evmURL)
	if err != nil {
		return errors.New("EVM RPC unavailable")
	}
	defer evm.Close()
	wallet := payments.WalletID{ChainID: 1404, Sender: address}
	network, err := payments.NewRPCNetwork(evm, wallet)
	if err != nil {
		return err
	}
	base, err := payments.NewReviewedPostgresJournal(pool)
	if err != nil {
		return err
	}
	canonical := storage.RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: payments.NativePayoutMaturity, RecoveryDirectory: os.Getenv("POOL_REWARD_RECOVERY_DIR")}
	creditGuard, err := storage.NewRewardCreditGuardV1(pool, "node", canonical)
	if err != nil {
		return err
	}
	journal, err := payments.NewWalletScopedJournal(base, rewardGuard{creditGuard})
	if err != nil {
		return err
	}
	signerFactory := func(c context.Context) (payments.Signer, error) {
		if err := exclusive(c); err != nil {
			return nil, err
		}
		content, sender, key, err := readWallet(walletPath)
		if err != nil {
			return nil, err
		}
		defer clear(content)
		if sha256.Sum256(content) != walletDigest || sender != address {
			return nil, errors.New("pool wallet changed")
		}
		return payments.NewLocalSigner(wallet, key)
	}
	worker := &payments.Worker{Wallet: wallet, Journal: journal, Network: network, Gate: &payments.NativeGate{Native: native, EVM: evm, Wallet: wallet}, Exclusive: exclusive, SignerFactory: signerFactory, RequireFreshWalletHistory: true, EnablePipeline: true}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
			result, stepErr := worker.Step(attempt)
			cancel()
			if stepErr != nil {
				log.Printf("pool payout held: %v", stepErr)
			} else if result.State != "no_eligible_credit" {
				log.Printf("pool payout state=%s", result.State)
			}
		}
	}
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--wallet-address" {
		data, address, _, err := readWallet(os.Getenv("POOL_WALLET_ENV_FILE"))
		clear(data)
		if err != nil {
			log.Printf("pool wallet validation failed: %v", err)
			os.Exit(1)
		}
		fmt.Println(address.Hex())
		return
	}
	if len(os.Args) != 1 {
		log.Print("unexpected payer arguments")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Printf("pool payer stopped: %v", err)
		os.Exit(1)
	}
}
