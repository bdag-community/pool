package payments

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type fixture struct {
	engine *Engine
	store  *fakeStore
	net    *fakeNetwork
	signer *fakeSigner
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	// Public test key only. No production key or endpoint is used in this suite.
	key, err := crypto.HexToECDSA(strings.Repeat("0", 63) + "1")
	if err != nil {
		t.Fatal(err)
	}
	wallet := WalletID{ChainID: 1404, Sender: crypto.PubkeyToAddress(key.PublicKey)}
	credit := Credit{ID: 7, BlockHash: strings.Repeat("ab", 32), Recipient: common.HexToAddress("0x2222222222222222222222222222222222222222"), Amount: big.NewInt(100)}
	store := &fakeStore{credit: credit}
	net := &fakeNetwork{chain: big.NewInt(1404), balance: big.NewInt(1_000_000), gasPrice: big.NewInt(1)}
	signer := &fakeSigner{}
	signer.sign = func(c Credit, nonce uint64, gas *big.Int) ([]byte, error) {
		tx := types.NewTransaction(nonce, c.Recipient, c.Amount, 21000, gas, nil)
		signed, err := types.SignTx(tx, types.LatestSignerForChainID(big.NewInt(1404)), key)
		if err != nil {
			return nil, err
		}
		return signed.MarshalBinary()
	}
	e := &Engine{Wallet: wallet, Journal: store, Network: net, Signer: signer,
		Ready: func(context.Context) error { return nil }, Now: func() time.Time { return time.Unix(1000, 0) }}
	return fixture{e, store, net, signer}
}

func TestFreshWalletNonceMustMatchCompleteJournalBeforeSigning(t *testing.T) {
	for _, tc := range []struct {
		name           string
		last           *uint64
		mined, pending uint64
		wantSign       bool
	}{
		{name: "new wallet", wantSign: true},
		{name: "unknown pending transaction", pending: 1},
		{name: "missing journal history", mined: 3, pending: 3},
		{name: "next confirmed nonce", last: noncePtr(4), mined: 5, pending: 5, wantSign: true},
		{name: "unrecorded pending nonce", last: noncePtr(4), mined: 5, pending: 6},
		{name: "canonical regression", last: noncePtr(4), mined: 4, pending: 5},
		{name: "nonce overflow", last: noncePtr(^uint64(0)), mined: ^uint64(0), pending: ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.engine.RequireFreshWalletHistory = true
			f.store.last, f.net.mined, f.net.pending = tc.last, tc.mined, tc.pending
			result, err := f.engine.Step(context.Background(), 100)
			if tc.wantSign {
				if err != nil || f.signer.calls != 1 || f.store.saves != 1 {
					t.Fatalf("expected one durable intent: result=%+v err=%v", result, err)
				}
			} else if err == nil || f.signer.calls != 0 || f.store.saves != 0 || len(f.net.broadcasts) != 0 {
				t.Fatalf("unexpected signing or broadcast: result=%+v err=%v", result, err)
			}
		})
	}
}

func noncePtr(n uint64) *uint64 { return &n }

func cloneIntent(p *Intent) *Intent {
	if p == nil {
		return nil
	}
	c := *p
	c.Credit = copyCredit(p.Credit)
	c.GasPrice = new(big.Int).Set(p.GasPrice)
	c.Raw = append([]byte(nil), p.Raw...)
	return &c
}

type fakeStore struct {
	credit                                             Credit
	active                                             *Intent
	paid                                               bool
	last                                               *uint64
	saves, confirms, holds                             int
	busy, saveAmbiguous, confirmAmbiguous, corruptSave bool
	history                                            HistoryBatch
	walletHeld                                         bool
	historyCommitAmbiguous                             bool
	zeroCount                                          int
	zeroErr                                            error
}

func (s *fakeStore) SatisfyZeroCredits(context.Context, uint64) (int, error) {
	if s.zeroErr != nil {
		return 0, s.zeroErr
	}
	n := s.zeroCount
	if n > ZeroCreditBatchLimit {
		n = ZeroCreditBatchLimit
	}
	s.zeroCount -= n
	return n, nil
}

func (s *fakeStore) Acquire(context.Context, WalletID) (Fence, error) {
	if s.walletHeld {
		return nil, errors.New("wallet integrity held")
	}
	if s.busy {
		return nil, ErrWalletBusy
	}
	return s, nil
}
func (s *fakeStore) Active(context.Context) (*Intent, error) { return cloneIntent(s.active), nil }
func (s *fakeStore) NextCredit(context.Context, uint64) (*Credit, error) {
	if s.paid {
		return nil, nil
	}
	c := copyCredit(s.credit)
	return &c, nil
}
func (s *fakeStore) LastConfirmedNonce(context.Context) (*uint64, error) { return s.last, nil }
func (s *fakeStore) Save(_ context.Context, p Intent) (*Intent, error) {
	s.saves++
	s.active = cloneIntent(&p)
	if s.saveAmbiguous {
		s.saveAmbiguous = false
		return nil, errors.New("commit acknowledgement lost")
	}
	result := cloneIntent(s.active)
	if s.corruptSave {
		result.Credit.ID++
	}
	return result, nil
}
func (s *fakeStore) Hold(context.Context, common.Hash, string) error {
	s.holds++
	s.active.Held = true
	return nil
}
func (s *fakeStore) Confirm(_ context.Context, p Intent, _ *types.Receipt, _ *types.Header) (bool, error) {
	if s.paid {
		return false, nil
	}
	s.paid = true
	s.confirms++
	s.active = nil
	n := p.Nonce
	s.last = &n
	if s.confirmAmbiguous {
		return false, errors.New("confirmation commit acknowledgement lost")
	}
	return true, nil
}
func (s *fakeStore) Close(context.Context) error { return nil }

func (s *fakeStore) History(context.Context) (HistoryBatch, error) { return s.history, nil }
func (s *fakeStore) AdvanceHistory(_ context.Context, _ *HistoryCheckpoint, c HistoryCheckpoint) error {
	s.history.Checkpoint = &c
	s.history.Payments = nil
	s.history.More = false
	if s.historyCommitAmbiguous {
		s.historyCommitAmbiguous = false
		return errors.New("checkpoint acknowledgement lost")
	}
	return nil
}
func (s *fakeStore) HoldWallet(context.Context, string) error { s.walletHeld = true; return nil }

type fakeSigner struct {
	calls int
	sign  func(Credit, uint64, *big.Int) ([]byte, error)
}

func (s *fakeSigner) Sign(_ context.Context, c Credit, n uint64, g *big.Int) ([]byte, error) {
	s.calls++
	return s.sign(c, n, g)
}

type fakeNetwork struct {
	chain, balance, gasPrice *big.Int
	pending, mined           uint64
	receipt                  *types.Receipt
	receipts                 map[common.Hash]*types.Receipt
	canonical, head          *types.Header
	broadcasts               [][]byte
	broadcastErr             error
	orphanOnRecheck          bool
	headerReads              int
}

func (n *fakeNetwork) ChainID(context.Context) (*big.Int, error) { return n.chain, nil }
func (n *fakeNetwork) Balance(context.Context, common.Address) (*big.Int, error) {
	return n.balance, nil
}
func (n *fakeNetwork) GasPrice(context.Context) (*big.Int, error) { return n.gasPrice, nil }
func (n *fakeNetwork) PendingNonce(context.Context, common.Address) (uint64, error) {
	return n.pending, nil
}
func (n *fakeNetwork) MinedNonce(context.Context, common.Address) (uint64, error) {
	return n.mined, nil
}
func (n *fakeNetwork) Receipt(_ context.Context, hash common.Hash) (*types.Receipt, error) {
	if r := n.receipts[hash]; r != nil {
		return r, nil
	}
	if n.receipt != nil && n.receipt.TxHash == hash {
		return n.receipt, nil
	}
	return nil, nil
}
func (n *fakeNetwork) Header(_ context.Context, number *big.Int) (*types.Header, error) {
	if number == nil {
		return n.head, nil
	}
	n.headerReads++
	if n.orphanOnRecheck && n.headerReads%2 == 0 {
		return &types.Header{Number: number, Extra: []byte("changed")}, nil
	}
	return n.canonical, nil
}
func (n *fakeNetwork) Broadcast(_ context.Context, raw []byte) error {
	n.broadcasts = append(n.broadcasts, append([]byte(nil), raw...))
	return n.broadcastErr
}

func (f fixture) accepted(confirmations uint64) {
	f.net.canonical = &types.Header{Number: big.NewInt(20)}
	f.net.receipt = &types.Receipt{TxHash: f.store.active.Hash, BlockNumber: big.NewInt(20), BlockHash: f.net.canonical.Hash(), Status: types.ReceiptStatusSuccessful}
	f.net.head = &types.Header{Number: new(big.Int).SetUint64(20 + confirmations - 1)}
}

func TestBroadcastNeverMeansPaidAndRetriesKeepSameNonceAndBytes(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		r, err := f.engine.Step(ctx, 100)
		if err != nil || r.State != "awaiting_confirmation" {
			t.Fatalf("step: %+v %v", r, err)
		}
	}
	if f.store.paid || f.store.confirms != 0 || f.store.saves != 1 || f.signer.calls != 1 || len(f.net.broadcasts) != 3 {
		t.Fatal("broadcast was paid or regenerated")
	}
	for _, raw := range f.net.broadcasts {
		if !bytes.Equal(raw, f.net.broadcasts[0]) {
			t.Fatal("ambiguous retry changed signed bytes")
		}
	}
}

func TestRestartAfterAmbiguousIntentCommitUsesPersistedPayload(t *testing.T) {
	f := newFixture(t)
	f.store.saveAmbiguous = true
	if _, err := f.engine.Step(context.Background(), 100); err == nil {
		t.Fatal("ambiguous commit accepted")
	}
	if len(f.net.broadcasts) != 0 || f.store.active == nil {
		t.Fatal("sent before proven persistence")
	}
	restarted := *f.engine
	if _, err := restarted.Step(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if f.signer.calls != 1 || f.store.saves != 1 || len(f.net.broadcasts) != 1 {
		t.Fatal("restart created another payment")
	}
}

func TestTimedOutBroadcastCanLaterConfirmWithoutAnotherPayment(t *testing.T) {
	f := newFixture(t)
	f.net.broadcastErr = context.DeadlineExceeded
	if _, err := f.engine.Step(context.Background(), 100); err == nil {
		t.Fatal("timeout hidden")
	}
	f.accepted(MinimumConfirmations)
	r, err := f.engine.Step(context.Background(), 100)
	if err != nil || !r.NewlyConfirmed || !f.store.paid || len(f.net.broadcasts) != 1 {
		t.Fatalf("confirmation: %+v %v", r, err)
	}
	if _, err := f.engine.Step(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if f.store.confirms != 1 || f.signer.calls != 1 {
		t.Fatal("confirmed credit paid twice")
	}
}

func TestCanonicalDepthAndReorgChecksAreRequired(t *testing.T) {
	for _, mode := range []string{"too-young", "orphaned", "recheck-changed", "reverted"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			_, err := f.engine.Step(context.Background(), 100)
			if err != nil {
				t.Fatal(err)
			}
			f.accepted(MinimumConfirmations)
			switch mode {
			case "too-young":
				f.net.head.Number.Sub(f.net.head.Number, big.NewInt(1))
			case "orphaned":
				f.net.receipt.BlockHash = common.HexToHash("0xab")
			case "recheck-changed":
				f.net.orphanOnRecheck = true
			case "reverted":
				f.net.receipt.Status = types.ReceiptStatusFailed
			}
			_, _ = f.engine.Step(context.Background(), 100)
			if f.store.paid || f.store.confirms != 0 || len(f.net.broadcasts) != 1 {
				t.Fatal("unconfirmed/reorg/reverted credit paid or rebroadcast")
			}
		})
	}
}

func TestConsumedNonceNeverAllocatesAnotherAndCanHealFromExactReceipt(t *testing.T) {
	f := newFixture(t)
	_, err := f.engine.Step(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	f.net.mined = 1
	f.engine.Now = func() time.Time { return time.Unix(1201, 0) }
	r, err := f.engine.Step(context.Background(), 100)
	if err != nil || r.State != "held" || !f.store.active.Held || len(f.net.broadcasts) != 1 {
		t.Fatalf("missing receipt: %+v %v", r, err)
	}
	_, _ = f.engine.Step(context.Background(), 100)
	if f.signer.calls != 1 || len(f.net.broadcasts) != 1 {
		t.Fatal("held payment allocated a new spend")
	}
	f.accepted(MinimumConfirmations)
	r, err = f.engine.Step(context.Background(), 100)
	if err != nil || !r.NewlyConfirmed {
		t.Fatalf("exact receipt did not heal: %+v %v", r, err)
	}
}

func TestIdentityReadinessBalanceAndFenceFailuresCannotSend(t *testing.T) {
	for _, mode := range []string{"wrong-chain", "not-ready", "no-readiness", "no-funds", "busy", "regressed-nonce", "pending-behind-mined", "corrupt-save"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			switch mode {
			case "wrong-chain":
				f.net.chain = big.NewInt(1)
			case "not-ready":
				f.engine.Ready = func(context.Context) error { return errors.New("not current") }
			case "no-readiness":
				f.engine.Ready = nil
			case "no-funds":
				f.net.balance = big.NewInt(0)
			case "busy":
				f.store.busy = true
			case "regressed-nonce":
				n := uint64(0)
				f.store.last = &n
			case "pending-behind-mined":
				f.net.mined = 1
			case "corrupt-save":
				f.store.corruptSave = true
			}
			_, _ = f.engine.Step(context.Background(), 100)
			if len(f.net.broadcasts) != 0 || f.store.paid {
				t.Fatal("failed gate sent or paid money")
			}
		})
	}
}

func TestConfirmationCommitAmbiguityIsNotReportedAsPaid(t *testing.T) {
	f := newFixture(t)
	_, _ = f.engine.Step(context.Background(), 100)
	f.accepted(MinimumConfirmations)
	f.store.confirmAmbiguous = true
	r, err := f.engine.Step(context.Background(), 100)
	if err == nil || r.NewlyConfirmed || r.State == "confirmed" {
		t.Fatal("unproven commit reported confirmed")
	}
	// The committed journal state is authoritative after a lost acknowledgement.
	_, _ = f.engine.Step(context.Background(), 100)
	if f.store.confirms != 1 || len(f.net.broadcasts) != 1 {
		t.Fatal("commit ambiguity duplicated spend")
	}
}

func TestSignedEnvelopeCannotChangeExistingEntitlement(t *testing.T) {
	f := newFixture(t)
	_, _ = f.engine.Step(context.Background(), 100)
	for _, mutate := range []func(*Intent){
		func(p *Intent) { p.Wallet.ChainID = 1 }, func(p *Intent) { p.Wallet.Sender = common.HexToAddress("0x33") },
		func(p *Intent) { p.Credit.Recipient = common.HexToAddress("0x44") }, func(p *Intent) { p.Credit.Amount = big.NewInt(99) },
		func(p *Intent) { p.Nonce++ }, func(p *Intent) { p.GasPrice = big.NewInt(2) }, func(p *Intent) { p.Raw[0] ^= 1 },
	} {
		p := cloneIntent(f.store.active)
		mutate(p)
		if ValidateIntent(*p) == nil {
			t.Fatal("changed cash entitlement accepted")
		}
	}
}
