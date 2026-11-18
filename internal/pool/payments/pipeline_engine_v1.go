package payments

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const pipelineWindowLimitV1 = 16

type pipelineFenceV1 interface {
	Fence
	PendingWindowV1(context.Context) ([]Intent, error)
}

type pipelinePendingNetworkV1 interface {
	PendingTransactionV1(context.Context, common.Hash) (*types.Transaction, bool, error)
}

// pipelineStepV1 keeps the same wallet fence, canonical history checks, and
// 12-confirmation payment rule as the serial engine. Once every earlier
// transaction is included canonically, it may send the next durable nonce.
func (e *Engine) pipelineStepV1(ctx context.Context, fence pipelineFenceV1, anchor *HistoryCheckpoint, maxEligibleHeight uint64) (Result, error) {
	intents, err := fence.PendingWindowV1(ctx)
	if err != nil {
		return Result{State: "awaiting_pipeline_evidence"}, err
	}
	last, err := fence.LastConfirmedNonce(ctx)
	if err != nil {
		return Result{}, err
	}
	head, err := e.Network.Header(ctx, nil)
	if err != nil || head == nil || head.Number == nil || !head.Number.IsUint64() {
		return Result{State: "awaiting_pipeline_evidence"}, errors.New("canonical payout head unavailable")
	}
	type includedReceipt struct {
		receipt *types.Receipt
		header  *types.Header
	}
	observed := make([]pipelinePendingV1, len(intents))
	evidence := make([]includedReceipt, len(intents))
	for i, intent := range intents {
		if intent.Wallet != e.Wallet || ValidateIntent(intent) != nil {
			return Result{}, errors.New("pipeline intent identity invalid")
		}
		observed[i] = pipelinePendingV1{CreditID: intent.Credit.ID, Nonce: intent.Nonce, Hash: intent.Hash, Held: intent.Held}
		r, receiptErr := e.Network.Receipt(ctx, intent.Hash)
		if receiptErr != nil {
			return Result{State: "awaiting_pipeline_evidence"}, receiptErr
		}
		if r == nil {
			continue
		}
		if r.TxHash != intent.Hash || r.BlockNumber == nil || !r.BlockNumber.IsUint64() {
			return Result{}, errors.New("pipeline receipt does not bind intent")
		}
		canonical, headerErr := e.Network.Header(ctx, r.BlockNumber)
		if headerErr != nil || canonical == nil || canonical.Number == nil || canonical.Number.Cmp(r.BlockNumber) != 0 {
			return Result{State: "awaiting_pipeline_evidence"}, errors.New("pipeline receipt block unavailable")
		}
		if canonical.Hash() != r.BlockHash {
			continue
		}
		if head.Number.Cmp(r.BlockNumber) < 0 {
			return Result{State: "awaiting_pipeline_evidence"}, errors.New("pipeline receipt is ahead of current head")
		}
		if r.Status != types.ReceiptStatusSuccessful {
			if err = fence.Hold(ctx, intent.Hash, "canonical transaction reverted; credit remains unpaid"); err != nil {
				return Result{State: "held"}, err
			}
			return Result{State: "held", CreditID: intent.Credit.ID, Hash: intent.Hash}, errors.New("canonical pipeline payout reverted")
		}
		observed[i].Included, observed[i].Canonical, observed[i].Successful = true, true, true
		observed[i].Confirmations = head.Number.Uint64() - r.BlockNumber.Uint64() + 1
		evidence[i] = includedReceipt{receipt: r, header: canonical}
	}
	mined, err := e.Network.MinedNonce(ctx, e.Wallet.Sender)
	if err != nil {
		return Result{State: "awaiting_nonce_evidence"}, err
	}
	pending, err := e.Network.PendingNonce(ctx, e.Wallet.Sender)
	if err != nil {
		return Result{State: "awaiting_nonce_evidence"}, err
	}
	verifyPending := func() error {
		if pending == mined {
			return nil
		}
		reader, ok := e.Network.(pipelinePendingNetworkV1)
		if !ok {
			return errors.New("pending payout transaction evidence unavailable")
		}
		for _, intent := range intents {
			if intent.Nonce < mined || intent.Nonce >= pending {
				continue
			}
			transaction, isPending, lookupErr := reader.PendingTransactionV1(ctx, intent.Hash)
			if lookupErr != nil || !isPending || transaction == nil {
				return errors.New("pending payout transaction is not proven")
			}
			raw, marshalErr := transaction.MarshalBinary()
			if marshalErr != nil || !bytes.Equal(raw, intent.Raw) {
				return errors.New("pending payout transaction differs from durable bytes")
			}
		}
		return nil
	}
	if err = verifyPending(); err != nil {
		return Result{State: "awaiting_nonce_evidence"}, err
	}
	decision, err := planPipelineV1(last, mined, pending, observed, pipelineWindowLimitV1)
	if err != nil {
		return Result{State: "awaiting_nonce_evidence"}, err
	}
	recheck := func() error {
		if err := e.Ready(ctx); err != nil {
			return err
		}
		if err := e.recheckHistoryAnchor(ctx, fence, anchor); err != nil {
			return err
		}
		for _, proof := range evidence {
			if proof.receipt == nil {
				continue
			}
			header, err := e.Network.Header(ctx, proof.receipt.BlockNumber)
			if err != nil || header == nil || header.Hash() != proof.header.Hash() {
				return errors.New("pipeline receipt block changed during verification")
			}
		}
		return nil
	}
	switch decision.Action {
	case pipelineWaitV1:
		if e.PreviewPipeline {
			return Result{State: "pipeline_preview_wait"}, nil
		}
		return Result{State: "awaiting_confirmation"}, nil
	case pipelineConfirmV1:
		intent, proof := intents[decision.Index], evidence[decision.Index]
		if err = recheck(); err != nil {
			return Result{State: "awaiting_pipeline_evidence"}, err
		}
		latest, headErr := e.Network.Header(ctx, nil)
		if headErr != nil || latest == nil || latest.Number == nil || !latest.Number.IsUint64() || latest.Number.Cmp(proof.receipt.BlockNumber) < 0 || latest.Number.Uint64()-proof.receipt.BlockNumber.Uint64() < MinimumConfirmations-1 {
			return Result{State: "awaiting_confirmation"}, errors.New("payout confirmation depth changed")
		}
		if e.PreviewPipeline {
			return Result{State: "pipeline_preview_confirm", CreditID: intent.Credit.ID, Hash: intent.Hash}, nil
		}
		newly, err := fence.Confirm(ctx, intent, proof.receipt, latest)
		if err != nil {
			return Result{State: "confirmation_commit_unproven", CreditID: intent.Credit.ID, Hash: intent.Hash}, err
		}
		return Result{State: "confirmed", CreditID: intent.Credit.ID, Hash: intent.Hash, NewlyConfirmed: newly}, nil
	case pipelineBroadcastV1:
		intent := intents[decision.Index]
		if err = recheck(); err != nil {
			return Result{State: "awaiting_pipeline_evidence"}, err
		}
		currentMined, minedErr := e.Network.MinedNonce(ctx, e.Wallet.Sender)
		currentPending, pendingErr := e.Network.PendingNonce(ctx, e.Wallet.Sender)
		if minedErr != nil || pendingErr != nil || currentMined != mined || currentPending != pending || verifyPending() != nil {
			return Result{State: "awaiting_nonce_evidence"}, errors.New("payout nonce window changed before broadcast")
		}
		if e.PreviewPipeline {
			return Result{State: "pipeline_preview_broadcast", CreditID: intent.Credit.ID, Hash: intent.Hash}, nil
		}
		if err = e.Network.Broadcast(ctx, append([]byte(nil), intent.Raw...)); err != nil {
			return Result{State: "awaiting_confirmation", CreditID: intent.Credit.ID, Hash: intent.Hash}, fmt.Errorf("durable payout broadcast ambiguous: %w", err)
		}
		return Result{State: "awaiting_confirmation", CreditID: intent.Credit.ID, Hash: intent.Hash}, nil
	case pipelinePrepareV1:
		return e.preparePipelineIntentV1(ctx, fence, decision.Nonce, maxEligibleHeight, recheck)
	default:
		return Result{}, errors.New("unsupported payout pipeline action")
	}
}

func (e *Engine) preparePipelineIntentV1(ctx context.Context, fence pipelineFenceV1, nonce, maxHeight uint64, recheck func() error) (Result, error) {
	zeros, err := fence.SatisfyZeroCredits(ctx, maxHeight)
	if err != nil {
		return Result{State: "credit_integrity_held"}, err
	}
	if zeros > 0 {
		return Result{State: "zero_credits_satisfied", ZeroSatisfied: zeros}, nil
	}
	credit, err := fence.NextCredit(ctx, maxHeight)
	if err != nil || credit == nil {
		return Result{State: "no_eligible_credit"}, err
	}
	if err = validateCredit(*credit); err != nil {
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
	if err = recheck(); err != nil {
		return Result{State: "awaiting_pipeline_evidence"}, err
	}
	mined, err := e.Network.MinedNonce(ctx, e.Wallet.Sender)
	if err != nil || mined != nonce {
		return Result{State: "awaiting_nonce_evidence"}, errors.New("canonical payout nonce changed before signing")
	}
	pending, err := e.Network.PendingNonce(ctx, e.Wallet.Sender)
	if err != nil || pending != nonce {
		return Result{State: "awaiting_nonce_evidence"}, errors.New("pending payout nonce changed before signing")
	}
	if e.PreviewPipeline {
		return Result{State: "pipeline_preview_prepare", CreditID: credit.ID}, nil
	}
	raw, err := e.Signer.Sign(ctx, copyCredit(*credit), nonce, new(big.Int).Set(gasPrice))
	if err != nil {
		return Result{}, err
	}
	var signed types.Transaction
	if err = signed.UnmarshalBinary(raw); err != nil {
		return Result{}, errors.New("signer returned invalid transaction")
	}
	proposal := Intent{Wallet: e.Wallet, Credit: copyCredit(*credit), Nonce: nonce, GasPrice: new(big.Int).Set(gasPrice), Raw: append([]byte(nil), raw...), Hash: signed.Hash(), CreatedAt: e.now()}
	if err = ValidateIntent(proposal); err != nil {
		return Result{}, err
	}
	intent, err := fence.Save(ctx, proposal)
	if err != nil {
		return Result{State: "intent_commit_unproven", CreditID: credit.ID}, err
	}
	if intent == nil || intent.Hash != proposal.Hash || !sameCredit(intent.Credit, proposal.Credit) || intent.Wallet != proposal.Wallet {
		return Result{}, errors.New("committed payout differs from prepared credit")
	}
	if err = recheck(); err != nil {
		return Result{State: "awaiting_pipeline_evidence", CreditID: credit.ID, Hash: intent.Hash}, err
	}
	mined, err = e.Network.MinedNonce(ctx, e.Wallet.Sender)
	if err != nil || mined != nonce {
		return Result{State: "awaiting_nonce_evidence", CreditID: credit.ID, Hash: intent.Hash}, errors.New("canonical payout nonce changed before broadcast")
	}
	pending, err = e.Network.PendingNonce(ctx, e.Wallet.Sender)
	if err != nil || pending != nonce {
		return Result{State: "awaiting_nonce_evidence", CreditID: credit.ID, Hash: intent.Hash}, errors.New("pending payout nonce changed before broadcast")
	}
	if err = e.Network.Broadcast(ctx, append([]byte(nil), intent.Raw...)); err != nil {
		return Result{State: "awaiting_confirmation", CreditID: credit.ID, Hash: intent.Hash}, fmt.Errorf("durable payout broadcast ambiguous: %w", err)
	}
	return Result{State: "awaiting_confirmation", CreditID: credit.ID, Hash: intent.Hash}, nil
}
