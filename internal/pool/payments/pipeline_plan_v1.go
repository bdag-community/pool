package payments

import (
	"errors"
	"math"

	"github.com/ethereum/go-ethereum/common"
)

// pipelinePlanV1 is a pure decision over independently verified, durable
// intents. It never signs, sends, changes a credit, or relaxes confirmation
// depth. Runtime integration must hold the wallet fence while observing and
// executing the selected action.
type pipelinePendingV1 struct {
	CreditID      int64
	Nonce         uint64
	Hash          common.Hash
	Held          bool
	Included      bool
	Canonical     bool
	Successful    bool
	Confirmations uint64
}

type pipelineActionV1 uint8

const (
	pipelineWaitV1 pipelineActionV1 = iota
	pipelinePrepareV1
	pipelineConfirmV1
	pipelineBroadcastV1
)

type pipelineDecisionV1 struct {
	Action pipelineActionV1
	Index  int
	Nonce  uint64
}

// planPipelineV1 requires a complete contiguous pending window after the last
// confirmed nonce. A later transaction is prepared only after all earlier
// intents have canonical successful receipts and the node's mined and pending
// nonces exactly match the durable window. After a reorganisation, any
// unmined suffix is recovered only by resending its original saved bytes.
func planPipelineV1(lastConfirmed *uint64, mined, pending uint64, items []pipelinePendingV1, limit int) (pipelineDecisionV1, error) {
	wait := pipelineDecisionV1{Action: pipelineWaitV1, Index: -1}
	if limit < 1 || len(items) > limit || pending < mined {
		return wait, errors.New("invalid payout pipeline bounds or nonce ordering")
	}
	base := uint64(0)
	if lastConfirmed != nil {
		if *lastConfirmed == math.MaxUint64 {
			return wait, errors.New("confirmed payout nonce exhausted")
		}
		base = *lastConfirmed + 1
	}
	if len(items) > 0 && uint64(len(items)) > math.MaxUint64-base {
		return wait, errors.New("pending payout nonce exhausted")
	}
	next := base + uint64(len(items))
	if mined < base || mined > next || pending > next {
		return wait, errors.New("wallet nonce differs from complete payout window")
	}
	missing := -1
	held := false
	creditIDs := make(map[int64]struct{}, len(items))
	hashes := make(map[common.Hash]struct{}, len(items))
	for i, item := range items {
		if item.CreditID <= 0 || item.Hash == (common.Hash{}) || item.Nonce != base+uint64(i) {
			return wait, errors.New("payout intent identity or nonce sequence invalid")
		}
		if _, exists := creditIDs[item.CreditID]; exists {
			return wait, errors.New("duplicate credit in payout window")
		}
		if _, exists := hashes[item.Hash]; exists {
			return wait, errors.New("duplicate transaction in payout window")
		}
		creditIDs[item.CreditID], hashes[item.Hash] = struct{}{}, struct{}{}
		if !item.Included && item.Canonical {
			return wait, errors.New("missing payout receipt cannot be canonical")
		}
		if item.Held {
			held = true
		}
		if !item.Included || !item.Canonical {
			if missing < 0 {
				missing = i
			}
			if item.Confirmations != 0 {
				return wait, errors.New("unproven receipt has confirmations")
			}
			continue
		}
		if missing >= 0 {
			return wait, errors.New("canonical payout follows an unresolved earlier nonce")
		}
		if item.Confirmations == 0 {
			return wait, errors.New("canonical receipt has no confirmations")
		}
		if !item.Successful {
			return wait, errors.New("canonical payout transaction failed")
		}
	}
	expectedMined := next
	if missing >= 0 {
		expectedMined = items[missing].Nonce
	}
	if mined != expectedMined {
		return wait, errors.New("receipt window differs from canonical mined nonce")
	}
	if missing < 0 && pending != next {
		return wait, errors.New("included payout window differs from pending nonce")
	}
	if len(items) > 0 && items[0].Included && items[0].Canonical && items[0].Successful && items[0].Confirmations >= MinimumConfirmations {
		return pipelineDecisionV1{Action: pipelineConfirmV1, Index: 0, Nonce: items[0].Nonce}, nil
	}
	if held {
		return wait, nil
	}
	if missing >= 0 {
		if pending < next {
			index := int(pending - base)
			if items[index].Held {
				return wait, nil
			}
			return pipelineDecisionV1{Action: pipelineBroadcastV1, Index: index, Nonce: pending}, nil
		}
		return wait, nil
	}
	if len(items) == limit {
		return wait, nil
	}
	if next == math.MaxUint64 {
		return wait, errors.New("payout nonce cannot advance further")
	}
	return pipelineDecisionV1{Action: pipelinePrepareV1, Index: -1, Nonce: next}, nil
}
