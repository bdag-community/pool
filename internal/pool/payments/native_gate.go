package payments

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// NativeCaller is implemented by rpc.Client. Endpoint identity/authentication
// remains a deployment invariant, not a caller-supplied readiness override.
type NativeCaller interface {
	CallContext(context.Context, interface{}, string, ...interface{}) error
}

const NativePayoutMaturity uint64 = 4096

type NativeGate struct {
	Native NativeCaller
	EVM    NativeCaller
	Wallet WalletID
	Now    func() time.Time
}

type nativeHealth struct {
	CheckedAt   int64   `json:"checked_at_unix_ms"`
	Enabled     *bool   `json:"enabled"`
	Started     *bool   `json:"started"`
	Shutdown    *bool   `json:"shutdown"`
	Mineable    *bool   `json:"mineable_now"`
	Submit      *bool   `json:"submit_ready"`
	Template    *bool   `json:"get_block_template_ready"`
	Current     *bool   `json:"p2p_current"`
	FreshPeers  *uint64 `json:"p2p_fresh_consensus_peer_count"`
	Canonical   *bool   `json:"canonical_mining_backend_ready"`
	Bypass      *bool   `json:"canonical_mining_bypass_active"`
	Coinbase    string  `json:"template_coinbase_address"`
	TemplateAge *uint64 `json:"template_age_ms"`
}

func explicit(v *bool, want bool) bool { return v != nil && *v == want }

func nativeMainHeight(raw json.RawMessage) (uint64, error) {
	value := strings.TrimSpace(string(raw))
	if strings.HasPrefix(value, `"`) {
		if err := json.Unmarshal(raw, &value); err != nil {
			return 0, errors.New("invalid native main-chain height encoding")
		}
	}
	if value == "" {
		return 0, errors.New("missing native main-chain height")
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, errors.New("native maturity height must be unsigned decimal")
		}
	}
	height, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("native maturity height exceeds uint64")
	}
	return height, nil
}

// Check keeps native main-chain height (the credit coordinate) separate from
// DAG block count and EVM head height. It never substitutes either count or a
// template/log/cache fallback for a failed maturity RPC.
func (g *NativeGate) Check(ctx context.Context) (maxEligible uint64, eligible bool, err error) {
	if g == nil || g.Native == nil || g.EVM == nil || !validWallet(g.Wallet) {
		return 0, false, errors.New("incomplete canonical payout readiness gate")
	}
	var h nativeHealth
	if err = g.Native.CallContext(ctx, &h, "getTemplateHealth"); err != nil {
		return 0, false, errors.New("native payout readiness unavailable")
	}
	now := time.Now()
	if g.Now != nil {
		now = g.Now()
	}
	age := now.UnixMilli() - h.CheckedAt
	if h.CheckedAt <= 0 || age < -1000 || age > 15000 || !explicit(h.Enabled, true) || !explicit(h.Started, true) || !explicit(h.Shutdown, false) || !explicit(h.Mineable, true) || !explicit(h.Submit, true) || !explicit(h.Template, true) || !explicit(h.Current, true) || !explicit(h.Canonical, true) || !explicit(h.Bypass, false) || h.FreshPeers == nil || *h.FreshPeers < 2 || h.TemplateAge == nil || *h.TemplateAge > 15000 {
		return 0, false, errors.New("native payout readiness is stale, incomplete or unsafe")
	}
	if !common.IsHexAddress(h.Coinbase) || common.HexToAddress(h.Coinbase) != g.Wallet.Sender {
		return 0, false, errors.New("native reward wallet differs from approved payer")
	}
	var chain hexutil.Uint64
	if err = g.EVM.CallContext(ctx, &chain, "eth_chainId"); err != nil || uint64(chain) != g.Wallet.ChainID {
		return 0, false, errors.New("EVM payout chain identity not verified")
	}
	var syncing json.RawMessage
	if err = g.EVM.CallContext(ctx, &syncing, "eth_syncing"); err != nil || string(syncing) != "false" {
		return 0, false, errors.New("EVM payout backend syncing or unknown")
	}
	var wire json.RawMessage
	if err = g.Native.CallContext(ctx, &wire, "getMainChainHeight"); err != nil {
		return 0, false, errors.New("native main-chain maturity height unavailable")
	}
	height, err := nativeMainHeight(wire)
	if err != nil {
		return 0, false, err
	}
	if height < NativePayoutMaturity-1 {
		return 0, false, nil
	}
	return height - NativePayoutMaturity + 1, true, nil
}
