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
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// Only public key=1 test fixtures and an in-process RPC transport are used.
func runtimeSigner(t *testing.T) (*LocalSigner, fixture) {
	t.Helper()
	f := newFixture(t)
	s, err := NewLocalSigner(f.engine.Wallet, strings.Repeat("0", 63)+"1")
	if err != nil {
		t.Fatal(err)
	}
	return s, f
}

func TestLocalSignerPinsCreditSenderAndChain(t *testing.T) {
	s, f := runtimeSigner(t)
	raw, err := s.Sign(context.Background(), f.store.credit, 19, big.NewInt(2))
	if err != nil {
		t.Fatal(err)
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	p := Intent{Wallet: f.engine.Wallet, Credit: f.store.credit, Nonce: 19, GasPrice: big.NewInt(2), Raw: raw, Hash: tx.Hash(), CreatedAt: time.Unix(1000, 0)}
	if err := ValidateIntent(p); err != nil {
		t.Fatal(err)
	}
	for _, w := range []WalletID{{1405, f.engine.Wallet.Sender}, {1404, common.Address{}}, {1404, f.store.credit.Recipient}} {
		if _, err := NewLocalSigner(w, strings.Repeat("0", 63)+"1"); err == nil {
			t.Fatal("accepted incorrect sender or chain")
		}
	}
	secret := "malformed-key-must-never-appear-in-error"
	if _, err := NewLocalSigner(f.engine.Wallet, secret); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("key encoding error leaked input or succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Sign(ctx, f.store.credit, 0, big.NewInt(1)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored")
	}
	for _, g := range []*big.Int{nil, big.NewInt(0), big.NewInt(-1), new(big.Int).Lsh(big.NewInt(1), 257)} {
		if _, err := s.Sign(context.Background(), f.store.credit, 0, g); err == nil {
			t.Fatal("invalid gas accepted")
		}
	}
	c := copyCredit(f.store.credit)
	c.Amount = big.NewInt(0)
	if _, err := s.Sign(context.Background(), c, 0, big.NewInt(1)); err == nil {
		t.Fatal("zero credit signed")
	}
}

type adapterRPC struct {
	chain                   hexutil.Uint64
	sent                    [][]byte
	wrongHash, receiptError bool
	tags                    []string
	receipt                 *types.Receipt
}

func (a *adapterRPC) ChainId() hexutil.Uint64 { return a.chain }
func (a *adapterRPC) GetBalance(common.Address, string) *hexutil.Big {
	return (*hexutil.Big)(big.NewInt(1_000_000))
}
func (a *adapterRPC) GasPrice() *hexutil.Big { return (*hexutil.Big)(big.NewInt(2)) }
func (a *adapterRPC) GetTransactionCount(_ common.Address, tag string) hexutil.Uint64 {
	a.tags = append(a.tags, tag)
	if tag == "pending" {
		return 11
	}
	return 10
}
func (a *adapterRPC) GetTransactionReceipt(common.Hash) (*types.Receipt, error) {
	if a.receiptError {
		return nil, errors.New("receipt service unavailable")
	}
	return a.receipt, nil
}
func (a *adapterRPC) GetBlockByNumber(number string, _ bool) *types.Header {
	height := big.NewInt(50)
	if number != "latest" {
		height = big.NewInt(20)
	}
	return &types.Header{Number: height, Difficulty: big.NewInt(1), Extra: []byte{}}
}
func (a *adapterRPC) SendRawTransaction(raw hexutil.Bytes) (common.Hash, error) {
	a.sent = append(a.sent, append([]byte(nil), raw...))
	var tx types.Transaction
	if err := tx.UnmarshalBinary(raw); err != nil {
		return common.Hash{}, err
	}
	if a.wrongHash {
		return common.Hash{}, nil
	}
	return tx.Hash(), nil
}

func adapterNetwork(t *testing.T, f fixture) (*RPCNetwork, *adapterRPC) {
	t.Helper()
	a := &adapterRPC{chain: 1404}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", a); err != nil {
		t.Fatal(err)
	}
	c := rpc.DialInProc(server)
	t.Cleanup(func() { c.Close(); server.Stop() })
	n, err := NewRPCNetwork(c, f.engine.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	return n, a
}

func TestRPCAdapterReadSemanticsAndNotFound(t *testing.T) {
	_, f := runtimeSigner(t)
	n, a := adapterNetwork(t, f)
	ctx := context.Background()
	if v, err := n.Balance(ctx, f.engine.Wallet.Sender); err != nil || v.Cmp(big.NewInt(1_000_000)) != 0 {
		t.Fatal(v, err)
	}
	if v, err := n.GasPrice(ctx); err != nil || v.Cmp(big.NewInt(2)) != 0 {
		t.Fatal(v, err)
	}
	if v, err := n.PendingNonce(ctx, f.engine.Wallet.Sender); err != nil || v != 11 {
		t.Fatal(v, err)
	}
	if v, err := n.MinedNonce(ctx, f.engine.Wallet.Sender); err != nil || v != 10 {
		t.Fatal(v, err)
	}
	if strings.Join(a.tags, ",") != "pending,latest" {
		t.Fatal(a.tags)
	}
	if v, err := n.Receipt(ctx, common.Hash{}); v != nil || err != nil {
		t.Fatal(v, err)
	}
	a.receiptError = true
	if _, err := n.Receipt(ctx, common.Hash{}); err == nil {
		t.Fatal("RPC receipt error became not found")
	}
	for _, number := range []*big.Int{nil, big.NewInt(20)} {
		h, err := n.Header(ctx, number)
		if err != nil {
			t.Fatal(err)
		}
		want := int64(20)
		if number == nil {
			want = 50
		}
		if h.Number.Int64() != want {
			t.Fatal(h.Number)
		}
	}
}

func TestRPCBroadcastPinsHashSenderAndChain(t *testing.T) {
	s, f := runtimeSigner(t)
	n, a := adapterNetwork(t, f)
	ctx := context.Background()
	raw, err := s.Sign(ctx, f.store.credit, 10, big.NewInt(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Broadcast(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if len(a.sent) != 1 || !bytes.Equal(a.sent[0], raw) {
		t.Fatal("broadcast bytes changed")
	}
	a.wrongHash = true
	if err := n.Broadcast(ctx, raw); err == nil {
		t.Fatal("different returned hash accepted")
	}
	a.chain = 1405
	if err := n.Broadcast(ctx, raw); err == nil || len(a.sent) != 2 {
		t.Fatal("chain drift broadcast")
	}
	a.chain = 1404
	wrong := *n
	wrong.wallet.Sender = f.store.credit.Recipient
	if err := wrong.Broadcast(ctx, raw); err == nil || len(a.sent) != 2 {
		t.Fatal("foreign sender broadcast")
	}
	for _, bad := range [][]byte{nil, {1, 2, 3}, make([]byte, 4097)} {
		if err := n.Broadcast(ctx, bad); err == nil {
			t.Fatal("invalid raw accepted")
		}
	}
}

func TestRuntimeAdaptersWithEngineDoNotPayAtBroadcast(t *testing.T) {
	s, f := runtimeSigner(t)
	n, a := adapterNetwork(t, f)
	f.engine.Network = n
	f.engine.Signer = s
	for i := 0; i < 2; i++ {
		r, err := f.engine.Step(context.Background(), 100)
		if err != nil {
			t.Fatal(err)
		}
		if r.State != "awaiting_confirmation" || f.store.paid {
			t.Fatal(r)
		}
	}
	if len(a.sent) != 2 || !bytes.Equal(a.sent[0], a.sent[1]) || f.store.saves != 1 {
		t.Fatal("restart path did not reuse durable bytes")
	}
}
