// Package payments owns durable cash-payment execution, not reward allocation.
// Production integration must supply a verified PostgreSQL journal and hold its
// wallet fence across each Step. A broadcast response is never a paid credit.
package payments

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const MinimumConfirmations uint64 = 12

var ErrWalletBusy = errors.New("payout wallet already fenced by another worker")

type WalletID struct {
	ChainID uint64
	Sender  common.Address
}

type Credit struct {
	ID        int64
	BlockHash string
	Recipient common.Address
	Amount    *big.Int // Exact existing credit Wei; no reward-policy conversion.
}

type Intent struct {
	Wallet    WalletID
	Credit    Credit
	Nonce     uint64
	GasPrice  *big.Int
	Raw       []byte // Durable signed bytes. Never a private key.
	Hash      common.Hash
	CreatedAt time.Time
	Held      bool
}

// Journal's fence must be a live database-session fence, not a file/lease whose
// expiry could authorize a competing nonce allocator while this worker runs.
type Journal interface {
	Acquire(context.Context, WalletID) (Fence, error)
}

type Fence interface {
	Active(context.Context) (*Intent, error)
	NextCredit(context.Context, uint64) (*Credit, error)
	SatisfyZeroCredits(context.Context, uint64) (int, error)
	LastConfirmedNonce(context.Context) (*uint64, error)
	// Save must commit the complete immutable signed intent before returning.
	// It must enforce the reviewed active window and uniqueness of credit,
	// hash and wallet/nonce. Ambiguous commit must return an error, not a guess.
	Save(context.Context, Intent) (*Intent, error)
	Hold(context.Context, common.Hash, string) error
	// Confirm must atomically verify the credit binding, mark the credit paid,
	// record the canonical receipt, insert its legacy payout and close the
	// intent. Idempotent replay returns newlyConfirmed=false.
	Confirm(context.Context, Intent, *types.Receipt, *types.Header) (newlyConfirmed bool, err error)
	Close(context.Context) error
	History(context.Context) (HistoryBatch, error)
	AdvanceHistory(context.Context, *HistoryCheckpoint, HistoryCheckpoint) error
	HoldWallet(context.Context, string) error
}

type Network interface {
	ChainID(context.Context) (*big.Int, error)
	Balance(context.Context, common.Address) (*big.Int, error)
	GasPrice(context.Context) (*big.Int, error)
	PendingNonce(context.Context, common.Address) (uint64, error)
	MinedNonce(context.Context, common.Address) (uint64, error)
	// Receipt returns nil,nil for not-found, never a fabricated receipt.
	Receipt(context.Context, common.Hash) (*types.Receipt, error)
	Header(context.Context, *big.Int) (*types.Header, error) // nil number = head.
	Broadcast(context.Context, []byte) error
}

type Signer interface {
	Sign(context.Context, Credit, uint64, *big.Int) ([]byte, error)
}

type Engine struct {
	Wallet  WalletID
	Journal Journal
	Network Network
	Signer  Signer
	// Ready must independently verify current canonical native/EVM readiness.
	// Missing readiness is fail-closed, including confirmation processing.
	Ready func(context.Context) error
	Now   func() time.Time
	// RequireFreshWalletHistory applies only to a separately proven new wallet.
	// A restored or incomplete journal must not authorize a second payment.
	RequireFreshWalletHistory bool
	// EnablePipeline permits multiple durable, canonically included but not
	// yet fully confirmed intents. It requires the reviewed pipeline schema.
	EnablePipeline bool
	// PreviewPipeline validates the next action without signing, broadcasting,
	// recording a payment intent, or marking a credit paid.
	PreviewPipeline bool
}

type Result struct {
	State          string
	CreditID       int64
	Hash           common.Hash
	NewlyConfirmed bool
	Reason         string
	ZeroSatisfied  int
}

func copyCredit(c Credit) Credit {
	if c.Amount != nil {
		c.Amount = new(big.Int).Set(c.Amount)
	}
	return c
}

func validateCredit(c Credit) error {
	block, err := hex.DecodeString(c.BlockHash)
	if err != nil || len(block) != 32 || c.ID <= 0 || c.Recipient == (common.Address{}) || c.Amount == nil || c.Amount.Sign() <= 0 || len(c.Amount.String()) > 30 {
		return errors.New("invalid positive cash-credit identity or amount")
	}
	return nil
}

// ValidateIntent binds every spend-bearing byte to the existing credit. Tests
// and production adapters must call this before any network broadcast.
func ValidateIntent(p Intent) error {
	if err := validateCredit(p.Credit); err != nil {
		return err
	}
	if p.Wallet.ChainID != 1404 || p.Wallet.Sender == (common.Address{}) || p.CreatedAt.IsZero() || p.GasPrice == nil || p.GasPrice.Sign() <= 0 || len(p.Raw) == 0 || len(p.Raw) > 4096 {
		return errors.New("invalid payout wallet, timestamp, gas or signed envelope")
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(p.Raw); err != nil {
		return errors.New("invalid signed payout transaction encoding")
	}
	if tx.Type() != types.LegacyTxType || tx.ChainId().Cmp(new(big.Int).SetUint64(p.Wallet.ChainID)) != 0 || tx.Nonce() != p.Nonce || tx.To() == nil || *tx.To() != p.Credit.Recipient || tx.Value().Cmp(p.Credit.Amount) != 0 || tx.Gas() != 21000 || tx.GasPrice().Cmp(p.GasPrice) != 0 || len(tx.Data()) != 0 || tx.Hash() != p.Hash {
		return errors.New("signed payout differs from frozen credit or nonce/gas identity")
	}
	sender, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), &tx)
	if err != nil || sender != p.Wallet.Sender {
		return errors.New("signed payout sender differs from approved wallet")
	}
	return nil
}

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// Step does at most one payment transition. Restart/timeout recovery loads the
// same intent; it never turns an ambiguous broadcast into a fresh nonce/spend.
// Existing confirmed history must pass a bounded canonical audit before any
// further signing, sending or payment confirmation. No legacy fallback is safe.
func (e *Engine) Step(ctx context.Context, maxEligibleHeight uint64) (Result, error) {
	if e == nil || e.Wallet.ChainID != 1404 || e.Wallet.Sender == (common.Address{}) || e.Journal == nil || e.Network == nil || e.Signer == nil || e.Ready == nil {
		return Result{}, errors.New("incomplete fail-closed payout engine configuration")
	}
	if err := e.Ready(ctx); err != nil {
		return Result{State: "awaiting_readiness"}, err
	}
	chainID, err := e.Network.ChainID(ctx)
	if err != nil || chainID == nil || chainID.Cmp(new(big.Int).SetUint64(e.Wallet.ChainID)) != 0 {
		return Result{}, errors.New("payout RPC chain identity not verified")
	}
	fence, err := e.Journal.Acquire(ctx, e.Wallet)
	if err != nil {
		return Result{State: "wallet_fenced"}, err
	}
	if fence == nil {
		return Result{}, errors.New("payout wallet fence missing")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = fence.Close(cleanup)
	}()
	anchor, historyReady, err := e.auditHistory(ctx, fence)
	if err != nil || !historyReady {
		return Result{State: "awaiting_history_audit"}, err
	}
	if e.EnablePipeline {
		pipeline, ok := fence.(pipelineFenceV1)
		if !ok || !e.RequireFreshWalletHistory {
			return Result{}, errors.New("reviewed pipeline fence and complete wallet history are required")
		}
		return e.pipelineStepV1(ctx, pipeline, anchor, maxEligibleHeight)
	}
	p, err := fence.Active(ctx)
	if err != nil {
		return Result{}, err
	}
	if p == nil {
		if err := e.Ready(ctx); err != nil {
			return Result{State: "awaiting_readiness"}, err
		}
		zeros, err := fence.SatisfyZeroCredits(ctx, maxEligibleHeight)
		if err != nil {
			return Result{State: "credit_integrity_held"}, err
		}
		if zeros > 0 {
			return Result{State: "zero_credits_satisfied", ZeroSatisfied: zeros}, nil
		}
		credit, err := fence.NextCredit(ctx, maxEligibleHeight)
		if err != nil || credit == nil {
			return Result{State: "no_eligible_credit"}, err
		}
		if err := validateCredit(*credit); err != nil {
			return Result{}, err
		}
		gasPrice, err := e.Network.GasPrice(ctx)
		if err != nil || gasPrice == nil || gasPrice.Sign() <= 0 {
			return Result{}, errors.New("positive payout gas price unavailable")
		}
		balance, err := e.Network.Balance(ctx, e.Wallet.Sender)
		if err != nil || balance == nil {
			return Result{}, errors.New("payout balance unavailable")
		}
		required := new(big.Int).Add(credit.Amount, new(big.Int).Mul(gasPrice, big.NewInt(21000)))
		if balance.Cmp(required) < 0 {
			return Result{State: "awaiting_funds", CreditID: credit.ID}, nil
		}
		nonce, err := e.Network.PendingNonce(ctx, e.Wallet.Sender)
		if err != nil {
			return Result{}, err
		}
		minedNonce, err := e.Network.MinedNonce(ctx, e.Wallet.Sender)
		if err != nil || nonce < minedNonce {
			return Result{State: "awaiting_nonce_evidence"}, errors.New("pending payout nonce is behind canonical mined nonce")
		}
		last, err := fence.LastConfirmedNonce(ctx)
		if err != nil {
			return Result{}, err
		}
		if last != nil && nonce <= *last {
			return Result{State: "awaiting_nonce_evidence"}, errors.New("RPC pending nonce regressed behind confirmed outbox")
		}
		if e.RequireFreshWalletHistory {
			expected := uint64(0)
			if last != nil {
				if *last == math.MaxUint64 {
					return Result{State: "awaiting_nonce_evidence"}, errors.New("confirmed payout nonce exhausted")
				}
				expected = *last + 1
			}
			if minedNonce != expected || nonce != expected {
				return Result{State: "awaiting_nonce_evidence"}, errors.New("fresh wallet nonce differs from complete confirmed payout history")
			}
		}
		if err := e.recheckHistoryAnchor(ctx, fence, anchor); err != nil {
			return Result{State: "awaiting_history_audit"}, err
		}
		raw, err := e.Signer.Sign(ctx, copyCredit(*credit), nonce, new(big.Int).Set(gasPrice))
		if err != nil {
			return Result{}, err
		}
		var tx types.Transaction
		if err := tx.UnmarshalBinary(raw); err != nil {
			return Result{}, errors.New("signer returned invalid transaction")
		}
		proposal := Intent{Wallet: e.Wallet, Credit: copyCredit(*credit), Nonce: nonce, GasPrice: new(big.Int).Set(gasPrice), Raw: append([]byte(nil), raw...), Hash: tx.Hash(), CreatedAt: e.now()}
		if err := ValidateIntent(proposal); err != nil {
			return Result{}, err
		}
		// No network send has happened. Only committed returned bytes may leave.
		p, err = fence.Save(ctx, proposal)
		if err != nil {
			return Result{State: "intent_commit_unproven", CreditID: credit.ID}, err
		}
		if p == nil || p.Hash != proposal.Hash || p.Credit.ID != proposal.Credit.ID || p.Credit.BlockHash != proposal.Credit.BlockHash {
			return Result{}, errors.New("committed payout differs from prepared credit identity")
		}
	}
	if p == nil || p.Wallet != e.Wallet {
		return Result{}, errors.New("durable intent wallet mismatch")
	}
	if err := ValidateIntent(*p); err != nil {
		return Result{}, err
	}
	result := Result{State: "awaiting_confirmation", CreditID: p.Credit.ID, Hash: p.Hash}
	receipt, err := e.Network.Receipt(ctx, p.Hash)
	if err != nil {
		return result, err
	}
	if receipt != nil {
		if receipt.TxHash != p.Hash || receipt.BlockNumber == nil || receipt.BlockNumber.Sign() < 0 || !receipt.BlockNumber.IsUint64() {
			return result, errors.New("receipt does not bind durable payout")
		}
		canonical, err := e.Network.Header(ctx, receipt.BlockNumber)
		if err != nil || canonical == nil || canonical.Number == nil || canonical.Number.Cmp(receipt.BlockNumber) != 0 {
			return result, errors.New("canonical payout block unavailable")
		}
		if canonical.Hash() != receipt.BlockHash {
			result.Reason = "orphaned_receipt_not_a_payment"
			return result, nil
		}
		if receipt.Status != types.ReceiptStatusSuccessful {
			result.State = "held"
			return result, fence.Hold(ctx, p.Hash, "canonical transaction reverted; credit remains unpaid")
		}
		head, err := e.Network.Header(ctx, nil)
		if err != nil || head == nil || head.Number == nil || !head.Number.IsUint64() {
			return result, errors.New("current canonical payout head unavailable")
		}
		if head.Number.Cmp(receipt.BlockNumber) < 0 || head.Number.Uint64()-receipt.BlockNumber.Uint64() < MinimumConfirmations-1 {
			return result, nil
		}
		// Revalidate the receipt block after reading head, before the atomic commit.
		again, err := e.Network.Header(ctx, receipt.BlockNumber)
		if err != nil || again == nil || again.Hash() != canonical.Hash() {
			return result, errors.New("canonical payout block changed during confirmation")
		}
		if err := e.recheckHistoryAnchor(ctx, fence, anchor); err != nil {
			return Result{State: "awaiting_history_audit"}, err
		}
		if err := e.Ready(ctx); err != nil {
			return Result{State: "awaiting_readiness"}, err
		}
		newly, err := fence.Confirm(ctx, *p, receipt, head)
		if err != nil {
			result.State = "confirmation_commit_unproven"
			return result, err
		}
		result.State, result.NewlyConfirmed = "confirmed", newly
		return result, err
	}
	if p.Held {
		result.State = "held"
		return result, nil // May self-heal only from exact canonical proof above.
	}
	minedNonce, err := e.Network.MinedNonce(ctx, e.Wallet.Sender)
	if err != nil {
		return result, err
	}
	if minedNonce > p.Nonce {
		result.Reason = "nonce_consumed_but_matching_receipt_missing"
		if e.now().Sub(p.CreatedAt) >= 2*time.Minute {
			result.State = "held"
			return result, fence.Hold(ctx, p.Hash, result.Reason)
		}
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := e.recheckHistoryAnchor(ctx, fence, anchor); err != nil {
		return Result{State: "awaiting_history_audit"}, err
	}
	if err := e.Network.Broadcast(ctx, append([]byte(nil), p.Raw...)); err != nil {
		// Even a timeout can mean accepted. The intent remains the same.
		return result, fmt.Errorf("durable payout broadcast ambiguous: %w", err)
	}
	return result, nil // Broadcast success still cannot mark the credit paid.
}
