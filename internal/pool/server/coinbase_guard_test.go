package server

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

const testPoolCoinbase = "0xD5F8DF0A60bC1Ff4636250b2a2dff319Bf79B1f5"

func TestMain(m *testing.M) {
	normalized := common.HexToAddress(testPoolCoinbase).Hex()
	poolAddress = normalized
	rememberVerifiedTemplateCoinbase(normalized)
	os.Exit(m.Run())
}

func withPoolCoinbase(t *testing.T, address string) {
	t.Helper()
	oldPoolAddress := poolAddress
	lastVerifiedTemplateCoinbaseMu.Lock()
	oldVerified := lastVerifiedTemplateCoinbase
	lastVerifiedTemplateCoinbaseMu.Unlock()

	normalized, err := normalizePoolCoinbaseAddress(address)
	if err != nil {
		t.Fatalf("test coinbase invalid: %v", err)
	}
	poolAddress = normalized
	rememberVerifiedTemplateCoinbase(normalized)

	t.Cleanup(func() {
		poolAddress = oldPoolAddress
		lastVerifiedTemplateCoinbaseMu.Lock()
		lastVerifiedTemplateCoinbase = oldVerified
		lastVerifiedTemplateCoinbaseMu.Unlock()
	})
}

func TestConfigurePoolCoinbaseAddressUsesExplicitAddress(t *testing.T) {
	withPoolCoinbase(t, testPoolCoinbase)
	poolAddress = ""

	if err := configurePoolCoinbaseAddress(Config{PoolCoinbaseAddress: strings.ToLower(testPoolCoinbase)}); err != nil {
		t.Fatalf("configurePoolCoinbaseAddress() error = %v", err)
	}
	if got := poolAddress; got != common.HexToAddress(testPoolCoinbase).Hex() {
		t.Fatalf("poolAddress = %s, want %s", got, common.HexToAddress(testPoolCoinbase).Hex())
	}
}

func TestConfigurePoolCoinbaseAddressRejectsMissingAddress(t *testing.T) {
	oldPoolAddress := poolAddress
	poolAddress = ""
	t.Cleanup(func() { poolAddress = oldPoolAddress })

	if err := configurePoolCoinbaseAddress(Config{}); err == nil {
		t.Fatal("configurePoolCoinbaseAddress() succeeded without an explicit address or private key")
	}
}

func TestConfigurePoolCoinbaseAddressValidatesSigningKey(t *testing.T) {
	withPoolCoinbase(t, testPoolCoinbase)
	const key = "0000000000000000000000000000000000000000000000000000000000000001"
	const correspondingAddress = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
	for _, tc := range []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"matching key and address", Config{PoolPrivateKey: key, PoolCoinbaseAddress: correspondingAddress}, false},
		{"key only", Config{PoolPrivateKey: key}, false},
		{"different address", Config{PoolPrivateKey: key, PoolCoinbaseAddress: testPoolCoinbase}, true},
		{"malformed key", Config{PoolPrivateKey: "invalid", PoolCoinbaseAddress: correspondingAddress}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := poolAddress
			err := configurePoolCoinbaseAddress(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("configuration error = %v, want error %t", err, tc.wantErr)
			}
			if tc.wantErr && poolAddress != before {
				t.Fatal("invalid configuration changed the pool address")
			}
			if !tc.wantErr && poolAddress != common.HexToAddress(correspondingAddress).Hex() {
				t.Fatalf("pool address = %s, want signing-key address", poolAddress)
			}
		})
	}
}

func TestUnjournaledPayoutCannotBeEnabledByWalletConfiguration(t *testing.T) {
	for _, tc := range []struct {
		cfg     Config
		wantErr bool
	}{
		{Config{}, false},
		{Config{PoolPrivateKey: "key"}, false},
		{Config{WalletRPCURL: "http://127.0.0.1:8545"}, false},
		{Config{PoolPrivateKey: "key", WalletRPCURL: "http://127.0.0.1:8545"}, true},
	} {
		if err := rejectUnjournaledPayout(tc.cfg); (err != nil) != tc.wantErr {
			t.Fatalf("legacy payout guard error = %v, want error %t", err, tc.wantErr)
		}
	}
}

func TestBuildNotifyFillsMissingTemplateCoinbaseFromConfiguredPoolAddress(t *testing.T) {
	withPoolCoinbase(t, testPoolCoinbase)

	tpl := testCleanRefreshBlockTemplate(strings.Repeat("4", 64), 100)
	tpl.CoinbaseAddress = ""

	_, job, err := buildNotify("job-missing-coinbase", tpl, true)
	if err != nil {
		t.Fatalf("buildNotify() error = %v", err)
	}
	want := common.HexToAddress(testPoolCoinbase).Bytes()
	if !bytes.Equal(job.CoinbaseAddress, want) {
		t.Fatalf("job coinbase = %x, want %x", job.CoinbaseAddress, want)
	}
	if got := tpl.CoinbaseAddress; got != common.HexToAddress(testPoolCoinbase).Hex() {
		t.Fatalf("template coinbase = %s, want %s", got, common.HexToAddress(testPoolCoinbase).Hex())
	}
}

func TestBuildNotifyRejectsZeroTemplateCoinbase(t *testing.T) {
	withPoolCoinbase(t, testPoolCoinbase)

	tpl := testCleanRefreshBlockTemplate(strings.Repeat("4", 64), 100)
	tpl.CoinbaseAddress = "0x0000000000000000000000000000000000000000"

	if _, _, err := buildNotify("job-zero-coinbase", tpl, true); err == nil {
		t.Fatal("buildNotify() succeeded with a zero template coinbase")
	}
}

func TestBuildNotifyRejectsMismatchedTemplateCoinbase(t *testing.T) {
	withPoolCoinbase(t, testPoolCoinbase)

	tpl := testCleanRefreshBlockTemplate(strings.Repeat("4", 64), 100)
	tpl.CoinbaseAddress = "0x1111111111111111111111111111111111111111"

	if _, _, err := buildNotify("job-mismatch-coinbase", tpl, true); err == nil {
		t.Fatal("buildNotify() succeeded with a mismatched template coinbase")
	}
}

func TestSubmittedHeaderCoinbaseMustMatchConfiguredPoolAddress(t *testing.T) {
	withPoolCoinbase(t, testPoolCoinbase)

	header := make([]byte, 144+9+32)
	if err := validateSubmittedHeaderCoinbase("0x" + common.Bytes2Hex(header)); err == nil {
		t.Fatal("validateSubmittedHeaderCoinbase() succeeded with zero coinbase bytes")
	}

	copy(header[148:168], common.HexToAddress(testPoolCoinbase).Bytes())
	if err := validateSubmittedHeaderCoinbase(common.Bytes2Hex(header)); err != nil {
		t.Fatalf("validateSubmittedHeaderCoinbase() error = %v", err)
	}

	copy(header[148:168], common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes())
	if err := validateSubmittedHeaderCoinbase(common.Bytes2Hex(header)); err == nil {
		t.Fatal("validateSubmittedHeaderCoinbase() succeeded with mismatched coinbase bytes")
	}
}
