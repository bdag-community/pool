package payments

// A new outbox does not erase a wallet's earlier confirmed nonce history.
// This read-only bootstrap proof is independent of credit/payment adoption:
// it never changes an old payout or marks a credit paid.

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type BaselineNetwork interface {
	Network
	Transaction(context.Context, common.Hash) (*types.Transaction, error)
}

// LegacyBaseline must come from explicit reviewed deployment evidence, not
// whichever receipt/nonce an RPC happens to return on first boot.
type LegacyBaseline struct {
	Wallet      WalletID
	NonceFloor  uint64
	Hash        common.Hash
	Recipient   common.Address
	Amount      *big.Int
	BlockNumber uint64
	BlockHash   common.Hash
}

func (b LegacyBaseline) Check(ctx context.Context, network BaselineNetwork) error {
	if _, bounded := ctx.Deadline(); !bounded {
		return errors.New("legacy baseline requires a bounded operation")
	}
	if !validWallet(b.Wallet) || b.NonceFloor == 0 || b.Hash == (common.Hash{}) || b.Recipient == (common.Address{}) || b.Amount == nil || b.Amount.Sign() <= 0 || b.Amount.BitLen() > 256 || b.BlockHash == (common.Hash{}) || network == nil {
		return errors.New("reviewed legacy baseline incomplete")
	}
	chain, err := network.ChainID(ctx)
	if err != nil || chain == nil || chain.Cmp(new(big.Int).SetUint64(b.Wallet.ChainID)) != 0 {
		return errors.New("legacy baseline chain identity unavailable")
	}
	tx, err := network.Transaction(ctx, b.Hash)
	if err != nil || tx == nil || tx.Hash() != b.Hash || tx.Type() != types.LegacyTxType || tx.ChainId().Cmp(chain) != 0 || tx.Nonce() != b.NonceFloor-1 || tx.To() == nil || *tx.To() != b.Recipient || tx.Value().Cmp(b.Amount) != 0 || tx.Gas() != 21000 || tx.GasPrice().Sign() <= 0 || len(tx.Data()) != 0 {
		return errors.New("legacy baseline transaction binding rejected")
	}
	sender, err := types.Sender(types.LatestSignerForChainID(chain), tx)
	if err != nil || sender != b.Wallet.Sender {
		return errors.New("legacy baseline sender rejected")
	}
	receipt, err := network.Receipt(ctx, b.Hash)
	if err != nil || receipt == nil || receipt.TxHash != b.Hash || receipt.Status != types.ReceiptStatusSuccessful || receipt.BlockNumber == nil || !receipt.BlockNumber.IsUint64() || receipt.BlockNumber.Uint64() != b.BlockNumber || receipt.BlockHash != b.BlockHash {
		return errors.New("legacy baseline successful receipt unavailable")
	}
	checkBlock := func() error {
		header, err := network.Header(ctx, new(big.Int).SetUint64(b.BlockNumber))
		if err != nil || header == nil || header.Number == nil || !header.Number.IsUint64() || header.Number.Uint64() != b.BlockNumber || header.Hash() != b.BlockHash {
			return errors.New("legacy baseline canonical anchor changed")
		}
		return nil
	}
	if err = checkBlock(); err != nil {
		return err
	}
	head, err := network.Header(ctx, nil)
	if err != nil || head == nil || head.Number == nil || !head.Number.IsUint64() || head.Number.Uint64() < b.BlockNumber || head.Number.Uint64()-b.BlockNumber < MinimumConfirmations-1 {
		return errors.New("legacy baseline confirmation depth unavailable")
	}
	mined, err := network.MinedNonce(ctx, b.Wallet.Sender)
	if err != nil || mined < b.NonceFloor {
		return errors.New("canonical nonce regressed behind legacy baseline")
	}
	pending, err := network.PendingNonce(ctx, b.Wallet.Sender)
	if err != nil || pending < mined {
		return errors.New("pending nonce regressed behind canonical baseline")
	}
	if err = checkBlock(); err != nil {
		return err
	}
	return ctx.Err()
}

// NonceFloorNetwork enforces the baseline at the actual allocation/send
// boundary too. An earlier successful Check cannot authorize a later regressed
// RPC nonce or an old durable envelope below the reviewed floor.
type NonceFloorNetwork struct {
	Network
	Floor uint64
}

func (n NonceFloorNetwork) PendingNonce(ctx context.Context, wallet common.Address) (uint64, error) {
	value, err := n.Network.PendingNonce(ctx, wallet)
	if err != nil {
		return 0, err
	}
	if n.Floor == 0 || value < n.Floor {
		return 0, errors.New("pending nonce below reviewed legacy floor")
	}
	return value, nil
}

func (n NonceFloorNetwork) MinedNonce(ctx context.Context, wallet common.Address) (uint64, error) {
	value, err := n.Network.MinedNonce(ctx, wallet)
	if err != nil {
		return 0, err
	}
	if n.Floor == 0 || value < n.Floor {
		return 0, errors.New("mined nonce below reviewed legacy floor")
	}
	return value, nil
}

func (n NonceFloorNetwork) Broadcast(ctx context.Context, raw []byte) error {
	var tx types.Transaction
	if n.Floor == 0 || len(raw) > 4096 || tx.UnmarshalBinary(raw) != nil || tx.Nonce() < n.Floor {
		return errors.New("broadcast envelope below reviewed legacy floor")
	}
	return n.Network.Broadcast(ctx, raw)
}
