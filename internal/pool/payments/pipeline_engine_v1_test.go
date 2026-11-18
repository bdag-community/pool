package payments

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type pipelineStoreTest struct {
	*fakeStore
	credits []Credit
	intents []Intent
	paidIDs map[int64]bool
}

func (s *pipelineStoreTest) Acquire(context.Context, WalletID) (Fence, error) { return s, nil }
func (s *pipelineStoreTest) PendingWindowV1(context.Context) ([]Intent, error) {
	result := make([]Intent, len(s.intents))
	copy(result, s.intents)
	return result, nil
}
func (s *pipelineStoreTest) NextCredit(context.Context, uint64) (*Credit, error) {
	for _, c := range s.credits {
		if s.paidIDs[c.ID] {
			continue
		}
		found := false
		for _, p := range s.intents {
			found = found || p.Credit.ID == c.ID
		}
		if !found {
			copy := copyCredit(c)
			return &copy, nil
		}
	}
	return nil, nil
}
func (s *pipelineStoreTest) Save(_ context.Context, intent Intent) (*Intent, error) {
	s.saves++
	s.intents = append(s.intents, *cloneIntent(&intent))
	return cloneIntent(&intent), nil
}
func (s *pipelineStoreTest) Confirm(_ context.Context, intent Intent, _ *types.Receipt, _ *types.Header) (bool, error) {
	if len(s.intents) == 0 || s.intents[0].Hash != intent.Hash {
		return false, errors.New("confirmation is not oldest")
	}
	s.paidIDs[intent.Credit.ID] = true
	s.intents = s.intents[1:]
	nonce := intent.Nonce
	s.last = &nonce
	s.confirms++
	return true, nil
}

type pipelineNetworkTest struct{ *fakeNetwork }

type reviewedPipelineStoreTest struct{ *pipelineStoreTest }

func (s reviewedPipelineStoreTest) Verify(context.Context) error { return nil }

type pipelineNonceDriftTest struct {
	*pipelineNetworkTest
	reads int
	drift bool
}

func (n *pipelineNonceDriftTest) PendingNonce(ctx context.Context, address common.Address) (uint64, error) {
	n.reads++
	if n.drift && n.reads >= 3 {
		return 1, nil
	}
	return n.pipelineNetworkTest.PendingNonce(ctx, address)
}

func (n *pipelineNetworkTest) PendingTransactionV1(_ context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	for _, raw := range n.broadcasts {
		var tx types.Transaction
		if tx.UnmarshalBinary(raw) == nil && tx.Hash() == hash {
			return &tx, true, nil
		}
	}
	return nil, false, nil
}

func pipelineFixture(t *testing.T) (fixture, *pipelineStoreTest, *pipelineNetworkTest) {
	t.Helper()
	f := newFixture(t)
	second := copyCredit(f.store.credit)
	second.ID++
	store := &pipelineStoreTest{fakeStore: f.store, credits: []Credit{f.store.credit, second}, paidIDs: make(map[int64]bool)}
	network := &pipelineNetworkTest{fakeNetwork: f.net}
	network.canonical = &types.Header{Number: big.NewInt(20)}
	network.head = &types.Header{Number: big.NewInt(20)}
	network.receipts = make(map[common.Hash]*types.Receipt)
	f.engine.Journal, f.engine.Network = store, network
	f.engine.RequireFreshWalletHistory, f.engine.EnablePipeline = true, true
	return f, store, network
}

func pipelineMine(t *testing.T, n *pipelineNetworkTest, p Intent) {
	t.Helper()
	n.receipts[p.Hash] = &types.Receipt{TxHash: p.Hash, BlockNumber: big.NewInt(20), BlockHash: n.canonical.Hash(), Status: types.ReceiptStatusSuccessful}
	n.mined, n.pending = p.Nonce+1, p.Nonce+1
}

func TestPipelinePaysOnlyAfterConfirmationAndRecoversReorg(t *testing.T) {
	ctx := context.Background()
	f, store, network := pipelineFixture(t)
	if _, err := f.engine.Step(ctx, 100); err != nil || len(store.intents) != 1 || f.signer.calls != 1 {
		t.Fatalf("first intent not durably sent: %v", err)
	}
	first := *cloneIntent(&store.intents[0])
	network.pending = 1
	if _, err := f.engine.Step(ctx, 100); err != nil || f.signer.calls != 1 || len(store.paidIDs) != 0 {
		t.Fatalf("unmined intent allowed a second signing: %v", err)
	}
	pipelineMine(t, network, first)
	if _, err := f.engine.Step(ctx, 100); err != nil || len(store.intents) != 2 || f.signer.calls != 2 || len(store.paidIDs) != 0 {
		t.Fatalf("canonical inclusion did not permit next nonce: %v", err)
	}
	second := *cloneIntent(&store.intents[1])
	pipelineMine(t, network, second)
	network.pending = 3 // No durable intent exists for this additional nonce.
	if _, err := f.engine.Step(ctx, 100); err == nil || f.signer.calls != 2 || len(store.paidIDs) != 0 {
		t.Fatal("unknown pending nonce was accepted")
	}
	network.pending = 2
	network.head = &types.Header{Number: big.NewInt(31)}
	if _, err := f.engine.Step(ctx, 100); err != nil || !store.paidIDs[first.Credit.ID] || store.paidIDs[second.Credit.ID] {
		t.Fatalf("oldest confirmation was not atomic: %v", err)
	}
	if _, err := f.engine.Step(ctx, 100); err != nil || !store.paidIDs[second.Credit.ID] {
		t.Fatalf("second confirmed payment not applied: %v", err)
	}

	f, store, network = pipelineFixture(t)
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	first = *cloneIntent(&store.intents[0])
	pipelineMine(t, network, first)
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	second = *cloneIntent(&store.intents[1])
	network.receipts = make(map[common.Hash]*types.Receipt)
	network.mined, network.pending = 0, 0
	if _, err := f.engine.Step(ctx, 100); err != nil || !bytes.Equal(network.broadcasts[len(network.broadcasts)-1], first.Raw) {
		t.Fatalf("reorg did not resend exact first transaction: %v", err)
	}
	network.pending = 1
	if _, err := f.engine.Step(ctx, 100); err != nil || !bytes.Equal(network.broadcasts[len(network.broadcasts)-1], second.Raw) {
		t.Fatalf("reorg did not resend exact second transaction: %v", err)
	}
	if f.signer.calls != 2 || len(store.paidIDs) != 0 {
		t.Fatal("reorg created a new payment or marked a credit paid")
	}
}

func TestPipelineHoldsSignedIntentWhenNonceOrCanonicalityChanges(t *testing.T) {
	ctx := context.Background()
	f, store, network := pipelineFixture(t)
	drift := &pipelineNonceDriftTest{pipelineNetworkTest: network, drift: true}
	f.engine.Network = drift
	if _, err := f.engine.Step(ctx, 100); err == nil || len(store.intents) != 1 || len(network.broadcasts) != 0 || f.signer.calls != 1 {
		t.Fatalf("changed nonce was broadcast or intent lost: %v", err)
	}
	saved := *cloneIntent(&store.intents[0])
	drift.drift = false
	if _, err := f.engine.Step(ctx, 100); err != nil || len(network.broadcasts) != 1 || !bytes.Equal(network.broadcasts[0], saved.Raw) || f.signer.calls != 1 {
		t.Fatalf("saved intent was not safely resumed: %v", err)
	}

	f, store, network = pipelineFixture(t)
	if _, err := f.engine.Step(ctx, 100); err != nil {
		t.Fatal(err)
	}
	pipelineMine(t, network, store.intents[0])
	network.headerReads, network.orphanOnRecheck = 0, true
	if _, err := f.engine.Step(ctx, 100); err == nil || f.signer.calls != 1 || len(store.intents) != 1 {
		t.Fatal("changed canonical payout block permitted another signature")
	}
}

func TestPipelinePreviewNeverSignsSendsOrConfirms(t *testing.T) {
	ctx := context.Background()
	f, store, network := pipelineFixture(t)
	f.engine.PreviewPipeline = true
	if result, err := f.engine.Step(ctx, 100); err != nil || result.State != "pipeline_preview_prepare" || f.signer.calls != 0 || store.saves != 0 || len(network.broadcasts) != 0 {
		t.Fatalf("preview prepare had an effect: %+v %v", result, err)
	}
	f.engine.PreviewPipeline = false
	if _, err := f.engine.Step(ctx, 100); err != nil || len(store.intents) != 1 {
		t.Fatalf("prepare fixture intent: %v", err)
	}
	f.engine.PreviewPipeline = true
	if result, err := f.engine.Step(ctx, 100); err != nil || result.State != "pipeline_preview_broadcast" || len(network.broadcasts) != 1 {
		t.Fatalf("preview broadcast had an effect: %+v %v", result, err)
	}
	pipelineMine(t, network, store.intents[0])
	network.head = &types.Header{Number: big.NewInt(31)}
	if result, err := f.engine.Step(ctx, 100); err != nil || result.State != "pipeline_preview_confirm" || len(store.paidIDs) != 0 {
		t.Fatalf("preview confirmation had an effect: %+v %v", result, err)
	}
}

func TestPipelineWorkerVerifiesPendingBytesBeforeNextNonce(t *testing.T) {
	ctx := context.Background()
	f, store, network := pipelineFixture(t)
	gate, _ := gateFixture(f)
	worker := Worker{Wallet: f.engine.Wallet, Journal: reviewedPipelineStoreTest{store}, Network: network, Gate: gate, Exclusive: func(context.Context) error { return nil }, SignerFactory: func(context.Context) (Signer, error) { return f.signer, nil }, RequireFreshWalletHistory: true, EnablePipeline: true}
	if _, err := worker.Step(ctx); err != nil || len(store.intents) != 1 {
		t.Fatalf("worker first intent: %v", err)
	}
	network.pending = 1
	if _, err := worker.Step(ctx); err != nil || f.signer.calls != 1 {
		t.Fatalf("worker pending envelope verification: %v", err)
	}
	pipelineMine(t, network, store.intents[0])
	if _, err := worker.Step(ctx); err != nil || len(store.intents) != 2 || f.signer.calls != 2 {
		t.Fatalf("worker did not advance after inclusion: %v", err)
	}
}
