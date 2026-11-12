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
