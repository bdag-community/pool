package payments

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type gateRPC struct {
	health     map[string]any
	height     uint64
	heightWire any
	chain      string
	sync       any
	fail       map[string]error
	calls      []string
}

func (r *gateRPC) CallContext(_ context.Context, out interface{}, method string, _ ...interface{}) error {
	r.calls = append(r.calls, method)
	if err := r.fail[method]; err != nil {
		return err
	}
	var value any
	switch method {
	case "getTemplateHealth":
		value = r.health
	case "getMainChainHeight":
		value = r.height
		if r.heightWire != nil {
			value = r.heightWire
		}
	case "eth_syncing":
		value = r.sync
	case "eth_chainId":
		value = r.chain
	default:
		return errors.New("unexpected RPC method; no height fallback permitted")
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
func gateFixture(f fixture) (*NativeGate, *gateRPC) {
	r := &gateRPC{height: 5000, chain: "0x57c", sync: false, fail: map[string]error{}, health: map[string]any{
		"checked_at_unix_ms": int64(1000000), "enabled": true, "started": true, "shutdown": false,
		"mineable_now": true, "submit_ready": true, "get_block_template_ready": true, "p2p_current": true,
		"p2p_fresh_consensus_peer_count": 2, "canonical_mining_backend_ready": true,
		"canonical_mining_bypass_active": false, "template_coinbase_address": f.engine.Wallet.Sender.Hex(), "template_age_ms": 1000}}
	return &NativeGate{Native: r, EVM: r, Wallet: f.engine.Wallet, Now: func() time.Time { return time.Unix(1000, 0) }}, r
}

func TestNativePayoutMaturityHasNoDAGOrEVMHeightFallback(t *testing.T) {
	f := newFixture(t)
	g, r := gateFixture(f)
	max, eligible, err := g.Check(context.Background())
	if err != nil || !eligible || max != 905 {
		t.Fatal(max, eligible, err)
	}
	r.height = 4094
	max, eligible, err = g.Check(context.Background())
	if err != nil || eligible || max != 0 {
		t.Fatal("premature payout maturity", max, eligible, err)
	}
	r.height = 5000
	r.fail["getMainChainHeight"] = errors.New("height unavailable")
	if _, _, err = g.Check(context.Background()); err == nil {
		t.Fatal("unknown native maturity accepted")
	}
	for _, m := range r.calls {
		if m == "getBlockCount" || m == "eth_blockNumber" || m == "getBlockTemplate" {
			t.Fatal("wrong maturity coordinate", m)
		}
	}
}

func TestNativeMainHeightAcceptsLiveDecimalStringWithoutRelaxingCoordinate(t *testing.T) {
	for _, wire := range []any{"5000", uint64(5000)} {
		f := newFixture(t)
		g, r := gateFixture(f)
		r.heightWire = wire
		max, eligible, err := g.Check(context.Background())
		if err != nil || !eligible || max != 905 {
			t.Fatalf("native wire form %T: %d %t %v", wire, max, eligible, err)
		}
	}
	for _, wire := range []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`"-1"`), json.RawMessage(`-1`), json.RawMessage(`1.5`), json.RawMessage(`"1e4"`), json.RawMessage(`"0x1388"`), json.RawMessage(`"18446744073709551616"`), json.RawMessage(`true`)} {
		if _, err := nativeMainHeight(wire); err == nil {
			t.Fatalf("unsafe native height accepted: %s", wire)
		}
	}
}

func TestNativeGateRejectsMissingUnsafeStaleAndWrongWalletEvidence(t *testing.T) {
	for _, mode := range []string{"missing-bypass", "bypass", "not-current", "one-peer", "stale", "future", "old-template", "wrong-wallet", "wrong-chain", "evm-syncing", "unknown-evm"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			g, r := gateFixture(f)
			switch mode {
			case "missing-bypass":
				delete(r.health, "canonical_mining_bypass_active")
			case "bypass":
				r.health["canonical_mining_bypass_active"] = true
			case "not-current":
				r.health["p2p_current"] = false
			case "one-peer":
				r.health["p2p_fresh_consensus_peer_count"] = 1
			case "stale":
				r.health["checked_at_unix_ms"] = int64(980000)
			case "future":
				r.health["checked_at_unix_ms"] = int64(1002000)
			case "old-template":
				r.health["template_age_ms"] = 15001
			case "wrong-wallet":
				r.health["template_coinbase_address"] = f.store.credit.Recipient.Hex()
			case "wrong-chain":
				r.chain = "0x1"
			case "evm-syncing":
				r.sync = map[string]any{"currentBlock": "0x1"}
			case "unknown-evm":
				r.sync = nil
			}
			if _, _, err := g.Check(context.Background()); err == nil {
				t.Fatal("unsafe or incomplete gate accepted")
			}
		})
	}
}

type reviewedFake struct {
	*fakeStore
	verifyErr error
}

type closingSigner struct {
	*fakeSigner
	closed int
}

func (s *closingSigner) Close() { s.closed++ }

func TestWorkerClosesOperationSignerOnSuccessAndFactoryError(t *testing.T) {
	for _, fail := range []bool{false, true} {
		f := newFixture(t)
		f.store.paid = true
		gate, _ := gateFixture(f)
		signer := &closingSigner{fakeSigner: f.signer}
		w := Worker{Wallet: f.engine.Wallet, Journal: reviewedFake{fakeStore: f.store}, Network: f.net,
			Gate: gate, Exclusive: func(context.Context) error { return nil },
			SignerFactory: func(context.Context) (Signer, error) {
				if fail {
					return signer, errors.New("partial factory failure")
				}
				return signer, nil
			}}
		_, err := w.Step(context.Background())
		if (err != nil) != fail || signer.closed != 1 || signer.calls != 0 {
			t.Fatal("operation signer retained or unexpectedly used", fail, err, signer.closed)
		}
	}
}

func (j reviewedFake) Verify(context.Context) error { return j.verifyErr }

func TestWorkerChecksAuthorityCatalogAndReadinessBeforeLoadingKey(t *testing.T) {
	for _, mode := range []string{"old-payer-alive", "catalog-drift", "native-down", "wrong-chain", "wrong-wallet", "immature", "missing-authority"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			g, r := gateFixture(f)
			j := reviewedFake{fakeStore: f.store}
			loads := 0
			w := Worker{Wallet: f.engine.Wallet, Journal: j, Network: f.net, Gate: g, Exclusive: func(context.Context) error { return nil }, SignerFactory: func(context.Context) (Signer, error) { loads++; return f.signer, nil }}
			switch mode {
			case "old-payer-alive":
				w.Exclusive = func(context.Context) error { return errors.New("predecessor PID nonzero") }
			case "catalog-drift":
				j.verifyErr = errors.New("unreviewed schema")
				w.Journal = j
			case "native-down":
				r.fail["getTemplateHealth"] = errors.New("timeout")
			case "wrong-chain":
				r.chain = "0x1"
			case "wrong-wallet":
				g.Wallet.Sender = f.store.credit.Recipient
			case "immature":
				r.height = 4000
			case "missing-authority":
				w.Exclusive = nil
			}
			_, _ = w.Step(context.Background())
			if loads != 0 || f.signer.calls != 0 || len(f.net.broadcasts) != 0 {
				t.Fatal("failed gate loaded signer or sent money")
			}
		})
	}
}

func TestWorkerGuardsLateAuthorityLossAndUsesDurableEngine(t *testing.T) {
	for _, mode := range []string{"normal", "late-authority-loss"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			g, _ := gateFixture(f)
			live := false
			w := Worker{Wallet: f.engine.Wallet, Journal: reviewedFake{fakeStore: f.store}, Network: f.net, Gate: g,
				Exclusive: func(context.Context) error {
					if live {
						return errors.New("predecessor unexpectedly running")
					}
					return nil
				},
				SignerFactory: func(context.Context) (Signer, error) {
					if mode == "late-authority-loss" {
						live = true
					}
					return f.signer, nil
				}}
			r, err := w.Step(context.Background())
			if mode == "late-authority-loss" {
				if err == nil || f.signer.calls != 0 || len(f.net.broadcasts) != 0 {
					t.Fatal("late authority loss spent money")
				}
				return
			}
			if err != nil || r.State != "awaiting_confirmation" || f.store.paid || f.store.saves != 1 {
				t.Fatal(r, err)
			}
			if _, err = w.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.signer.calls != 1 || len(f.net.broadcasts) != 2 {
				t.Fatal("worker bypassed durable retry contract")
			}
		})
	}
}

func TestPostgresWorkerIntegrationPreservesCreditAndConfirmation(t *testing.T) {
	f := newSQLFixture(t)
	g, _ := gateFixture(f.fixture)
	j, err := NewReviewedPostgresJournal(f.pool)
	if err != nil {
		t.Fatal(err)
	}
	if f.fingerprint != ReviewedSchemaFingerprint {
		t.Fatal("compiled reviewed fingerprint differs from actual migration")
	}
	w := Worker{Wallet: f.engine.Wallet, Journal: j, Network: f.net, Gate: g, Exclusive: func(context.Context) error { return nil }, SignerFactory: func(context.Context) (Signer, error) { return f.signer, nil }}
	r, err := w.Step(context.Background())
	if err != nil || r.State != "awaiting_confirmation" {
		t.Fatal(r, err)
	}
	paid, state, count := f.state(t)
	if paid || state != "pending" || count != 0 {
		t.Fatal("worker paid at broadcast")
	}
	f.accepted(t)
	r, err = w.Step(context.Background())
	if err != nil || !r.NewlyConfirmed {
		t.Fatal(r, err)
	}
	paid, state, count = f.state(t)
	if !paid || state != "confirmed" || count != 1 {
		t.Fatal("worker confirmation was not atomic")
	}
}

type loseAuthorityBeforeSend struct {
	Network
	lose func()
}

func (n loseAuthorityBeforeSend) Receipt(ctx context.Context, h common.Hash) (*types.Receipt, error) {
	n.lose()
	return n.Network.Receipt(ctx, h)
}

func TestWorkerLateAuthorityLossAfterCommitCannotBroadcast(t *testing.T) {
	f := newFixture(t)
	g, _ := gateFixture(f)
	lost := false
	w := Worker{Wallet: f.engine.Wallet, Journal: reviewedFake{fakeStore: f.store}, Gate: g,
		Network: loseAuthorityBeforeSend{Network: f.net, lose: func() { lost = true }},
		Exclusive: func(context.Context) error {
			if lost {
				return errors.New("old payer resurrected")
			}
			return nil
		},
		SignerFactory: func(context.Context) (Signer, error) { return f.signer, nil }}
	if _, err := w.Step(context.Background()); err == nil {
		t.Fatal("late loss hidden")
	}
	if f.signer.calls != 1 || f.store.saves != 1 || len(f.net.broadcasts) != 0 {
		t.Fatal("unfenced late broadcast")
	}
	lost = false
	w.Network = f.net
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.signer.calls != 1 || len(f.net.broadcasts) != 1 {
		t.Fatal("authority recovery generated another spend")
	}
}
