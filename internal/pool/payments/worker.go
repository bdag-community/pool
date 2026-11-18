package payments

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// ReviewedJournal adds mandatory catalog verification before loading a key.
type ReviewedJournal interface {
	Journal
	Verify(context.Context) error
}

// Worker is a single bounded lifecycle operation, not a background goroutine.
// The external supervisor owns cadence, cancellation, evidence and exclusive
// predecessor shutdown. No signer is created until all startup gates pass.
type Worker struct {
	Wallet        WalletID
	Journal       ReviewedJournal
	Network       Network
	Gate          *NativeGate
	Exclusive     func(context.Context) error
	SignerFactory func(context.Context) (Signer, error)
	// Only enable after proving this wallet has no pre-journal payment history.
	RequireFreshWalletHistory bool
	EnablePipeline            bool
}

func (w *Worker) Step(ctx context.Context) (Result, error) {
	if w == nil || !validWallet(w.Wallet) || w.Journal == nil || w.Network == nil || w.Gate == nil || w.Gate.Wallet != w.Wallet || w.Exclusive == nil || w.SignerFactory == nil {
		return Result{}, errors.New("incomplete guarded payout worker")
	}
	if err := w.Exclusive(ctx); err != nil {
		return Result{State: "awaiting_exclusive_signer"}, err
	}
	max, eligible, err := w.Gate.Check(ctx)
	if err != nil {
		return Result{State: "awaiting_readiness"}, err
	}
	if !eligible {
		return Result{State: "awaiting_native_maturity"}, nil
	}
	if err := w.Journal.Verify(ctx); err != nil {
		return Result{State: "awaiting_reviewed_catalog"}, err
	}
	// A late predecessor resurrection, lost maturity evidence or native rollback
	// must stop key use and transmission, even after the first gate succeeded.
	ready := func(c context.Context) error {
		if err := w.Exclusive(c); err != nil {
			return err
		}
		next, eligible, err := w.Gate.Check(c)
		if err != nil {
			return err
		}
		if !eligible || next < max {
			return errors.New("native payout maturity regressed during operation")
		}
		return nil
	}
	if err := ready(ctx); err != nil {
		return Result{State: "awaiting_readiness"}, err
	}
	signer, err := w.SignerFactory(ctx)
	if closer, ok := signer.(interface{ Close() }); ok {
		defer closer.Close()
	}
	if err != nil || signer == nil {
		return Result{State: "awaiting_approved_signer"}, errors.New("approved payout signer unavailable")
	}
	e := Engine{Wallet: w.Wallet, Journal: w.Journal, Network: guardedNetwork{Network: w.Network, ready: ready},
		Signer: guardedSigner{signer: signer, ready: ready}, Ready: ready,
		RequireFreshWalletHistory: w.RequireFreshWalletHistory, EnablePipeline: w.EnablePipeline}
	return e.Step(ctx, max)
}

type guardedSigner struct {
	signer Signer
	ready  func(context.Context) error
}

func (s guardedSigner) Sign(ctx context.Context, c Credit, n uint64, g *big.Int) ([]byte, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	return s.signer.Sign(ctx, c, n, g)
}

type guardedNetwork struct {
	Network
	ready func(context.Context) error
}

func (n guardedNetwork) Broadcast(ctx context.Context, raw []byte) error {
	if err := n.ready(ctx); err != nil {
		return err
	}
	return n.Network.Broadcast(ctx, raw)
}

func (n guardedNetwork) PendingTransactionV1(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	reader, ok := n.Network.(pipelinePendingNetworkV1)
	if !ok {
		return nil, false, errors.New("pending transaction evidence unavailable")
	}
	return reader.PendingTransactionV1(ctx, hash)
}
