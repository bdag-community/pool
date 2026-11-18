package payments

// These adapters deliberately do not load an environment file, migrate SQL or
// start a worker. Their caller must first prove exclusive signing authority and
// supply the independently checked readiness function and reviewed journal.

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type LocalSigner struct {
	wallet WalletID
	key    *ecdsa.PrivateKey
}

func validWallet(w WalletID) bool { return w.ChainID == 1404 && w.Sender != (common.Address{}) }

// NewLocalSigner never includes supplied key material in an error or log.
func NewLocalSigner(w WalletID, privateKeyHex string) (*LocalSigner, error) {
	if !validWallet(w) {
		return nil, errors.New("invalid approved payout wallet")
	}
	key, err := crypto.HexToECDSA(strings.TrimPrefix(privateKeyHex, "0x"))
	if err != nil {
		return nil, errors.New("invalid payout signing key encoding")
	}
	if crypto.PubkeyToAddress(key.PublicKey) != w.Sender {
		clear(key.D.Bits())
		key.D.SetInt64(0)
		return nil, errors.New("payout key does not match approved sender")
	}
	return &LocalSigner{wallet: w, key: key}, nil
}

// Close drops this operation's retained key and clears its scalar storage.
// This is best-effort lifetime reduction, not a guarantee against runtime copies.
func (s *LocalSigner) Close() {
	if s != nil && s.key != nil {
		clear(s.key.D.Bits())
		s.key.D.SetInt64(0)
		s.key = nil
	}
}

func (s *LocalSigner) Sign(ctx context.Context, c Credit, nonce uint64, gasPrice *big.Int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.key == nil || !validWallet(s.wallet) {
		return nil, errors.New("payout signer is not configured")
	}
	if err := validateCredit(c); err != nil {
		return nil, err
	}
	if gasPrice == nil || gasPrice.Sign() <= 0 || gasPrice.BitLen() > 256 {
		return nil, errors.New("invalid payout gas price")
	}
	tx, err := types.SignTx(types.NewTransaction(nonce, c.Recipient, new(big.Int).Set(c.Amount), 21000, new(big.Int).Set(gasPrice), nil),
		types.LatestSignerForChainID(new(big.Int).SetUint64(s.wallet.ChainID)), s.key)
	if err != nil {
		return nil, errors.New("payout transaction signing failed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return tx.MarshalBinary()
}

// RPCNetwork borrows its RPC connection. The creator owns connection lifetime
// and endpoint/authentication qualification. Calls always use caller deadlines;
// there is no hidden background sender, nonce allocator or retry loop.
type RPCNetwork struct {
	wallet WalletID
	rpc    *rpc.Client
	eth    *ethclient.Client
}

func NewRPCNetwork(c *rpc.Client, w WalletID) (*RPCNetwork, error) {
	if c == nil || !validWallet(w) {
		return nil, errors.New("invalid payout RPC configuration")
	}
	return &RPCNetwork{wallet: w, rpc: c, eth: ethclient.NewClient(c)}, nil
}

func (n *RPCNetwork) ChainID(ctx context.Context) (*big.Int, error) { return n.eth.ChainID(ctx) }
func (n *RPCNetwork) Balance(ctx context.Context, a common.Address) (*big.Int, error) {
	return n.eth.BalanceAt(ctx, a, nil)
}
func (n *RPCNetwork) GasPrice(ctx context.Context) (*big.Int, error) {
	return n.eth.SuggestGasPrice(ctx)
}
func (n *RPCNetwork) PendingNonce(ctx context.Context, a common.Address) (uint64, error) {
	return n.eth.PendingNonceAt(ctx, a)
}
func (n *RPCNetwork) MinedNonce(ctx context.Context, a common.Address) (uint64, error) {
	return n.eth.NonceAt(ctx, a, nil)
}
func (n *RPCNetwork) Header(ctx context.Context, number *big.Int) (*types.Header, error) {
	return n.eth.HeaderByNumber(ctx, number)
}
func (n *RPCNetwork) Receipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	r, err := n.eth.TransactionReceipt(ctx, hash)
	if errors.Is(err, ethereum.NotFound) {
		return nil, nil
	}
	return r, err
}

// Transaction is read-only and used by audited legacy reconciliation only.
func (n *RPCNetwork) Transaction(ctx context.Context, hash common.Hash) (*types.Transaction, error) {
	tx, _, err := n.eth.TransactionByHash(ctx, hash)
	if errors.Is(err, ethereum.NotFound) {
		return nil, nil
	}
	return tx, err
}

// PendingTransactionV1 allows a pipelined payer to bind a mempool nonce to the
// exact durable signed envelope, rather than trusting a nonce count alone.
func (n *RPCNetwork) PendingTransactionV1(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	tx, pending, err := n.eth.TransactionByHash(ctx, hash)
	if errors.Is(err, ethereum.NotFound) {
		return nil, false, nil
	}
	return tx, pending, err
}

func (n *RPCNetwork) Broadcast(ctx context.Context, raw []byte) error {
	if len(raw) == 0 || len(raw) > 4096 {
		return errors.New("invalid payout broadcast envelope size")
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(raw); err != nil {
		return errors.New("invalid payout broadcast encoding")
	}
	if tx.Type() != types.LegacyTxType || tx.ChainId().Cmp(new(big.Int).SetUint64(n.wallet.ChainID)) != 0 || tx.To() == nil || *tx.To() == (common.Address{}) || tx.Value().Sign() <= 0 || tx.Gas() != 21000 || tx.GasPrice().Sign() <= 0 || len(tx.Data()) != 0 {
		return errors.New("payout broadcast violates transfer contract")
	}
	sender, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), &tx)
	if err != nil || sender != n.wallet.Sender {
		return errors.New("payout broadcast sender mismatch")
	}
	chain, err := n.ChainID(ctx)
	if err != nil || chain == nil || chain.Cmp(tx.ChainId()) != 0 {
		return errors.New("payout broadcast RPC chain identity not verified")
	}
	var returned common.Hash
	if err := n.rpc.CallContext(ctx, &returned, "eth_sendRawTransaction", hexutil.Bytes(raw)); err != nil {
		// Ambiguous network errors leave the committed intent pending. A later
		// Step checks its receipt and can only resend these exact stored bytes.
		return err
	}
	if returned != tx.Hash() {
		return errors.New("payout RPC returned a different transaction hash")
	}
	return nil
}

var _ Signer = (*LocalSigner)(nil)
var _ Network = (*RPCNetwork)(nil)
