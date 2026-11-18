package payments

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestPipelinePlansOnlyFencedCanonicalPayments(t *testing.T) {
	last := uint64(4)
	makeItem := func(n uint64) pipelinePendingV1 {
		return pipelinePendingV1{CreditID: int64(n + 1), Nonce: n, Hash: common.BigToHash(new(big.Int).SetUint64(n + 1)), Included: true, Canonical: true, Successful: true, Confirmations: 1}
	}
	a, b := makeItem(5), makeItem(6)
	check := func(name string, mined, pending uint64, items []pipelinePendingV1, limit int, action pipelineActionV1, nonce uint64, wantErr bool) {
		t.Helper()
		got, err := planPipelineV1(&last, mined, pending, items, limit)
		if (err != nil) != wantErr || (!wantErr && (got.Action != action || (action != pipelineWaitV1 && got.Nonce != nonce))) {
			t.Fatalf("%s: decision=%+v error=%v", name, got, err)
		}
	}
	check("fresh window", 5, 5, nil, 12, pipelinePrepareV1, 5, false)
	check("included window", 7, 7, []pipelinePendingV1{a, b}, 12, pipelinePrepareV1, 7, false)
	full := []pipelinePendingV1{a, b}
	check("window cap", 7, 7, full, 2, pipelineWaitV1, 0, false)
	confirmed := a
	confirmed.Confirmations = MinimumConfirmations
	check("confirm oldest first", 7, 7, []pipelinePendingV1{confirmed, b}, 12, pipelineConfirmV1, 5, false)
	missing := b
	missing.Included, missing.Canonical, missing.Confirmations = false, false, 0
	check("resend saved intent", 6, 6, []pipelinePendingV1{a, missing}, 12, pipelineBroadcastV1, 6, false)
	check("already in mempool", 6, 7, []pipelinePendingV1{a, missing}, 12, pipelineWaitV1, 0, false)
	check("unknown nonce", 7, 8, []pipelinePendingV1{a, b}, 12, pipelineWaitV1, 0, true)
	check("inconsistent mined nonce", 6, 7, []pipelinePendingV1{a, b}, 12, pipelineWaitV1, 0, true)
	gap := b
	gap.Nonce++
	check("journal gap", 7, 7, []pipelinePendingV1{a, gap}, 12, pipelineWaitV1, 0, true)
	dup := b
	dup.CreditID = a.CreditID
	check("duplicate credit", 7, 7, []pipelinePendingV1{a, dup}, 12, pipelineWaitV1, 0, true)
	failed := b
	failed.Successful = false
	check("reverted payout", 7, 7, []pipelinePendingV1{a, failed}, 12, pipelineWaitV1, 0, true)
	missingFirst := a
	missingFirst.Included, missingFirst.Canonical, missingFirst.Confirmations = false, false, 0
	check("canonical receipt after missing nonce", 5, 7, []pipelinePendingV1{missingFirst, b}, 12, pipelineWaitV1, 0, true)
	check("reorg resends missing suffix in order", 5, 6, []pipelinePendingV1{missingFirst, missing}, 12, pipelineBroadcastV1, 6, false)
	missingFirst.Held = true
	check("held missing intent stops later sends", 5, 6, []pipelinePendingV1{missingFirst, missing}, 12, pipelineWaitV1, 0, false)
	hold := b
	hold.Held = true
	check("held payout stops new sends", 7, 7, []pipelinePendingV1{a, hold}, 12, pipelineWaitV1, 0, false)
}
