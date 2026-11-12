package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"asicPool/internal/pool/storage"
)

func testMainnetChainIdentityV1() chainIdentityV1 {
	return chainIdentityV1{
		Schema:             chainIdentitySchemaV1,
		Network:            "mainnet",
		NativeNetworkMagic: "0xb4c3dce8",
		NativeGenesisHash:  "0xd62590d28f5c32b6d46839075b3173c3d4c178482f8cd1b0e424bd3feeaed11c",
		EVMChainID:         "1404",
		EVMGenesisHash:     "0x3fb19ea409ac7399ad7b988b88e0d436722c967137e51e834ccefd7611a592ef",
	}
}

func testTestnetChainIdentityV1() chainIdentityV1 {
	return chainIdentityV1{
		Schema:             chainIdentitySchemaV1,
		Network:            "testnet",
		NativeNetworkMagic: "0x35e0c424",
		NativeGenesisHash:  "0x1111111111111111111111111111111111111111111111111111111111111111",
		EVMChainID:         "1043",
		EVMGenesisHash:     "0x2222222222222222222222222222222222222222222222222222222222222222",
	}
}

func marshalChainIdentityResultV1(t *testing.T, identity chainIdentityV1) string {
	t.Helper()
	result := map[string]string{
		"schema":               identity.Schema,
		"network":              identity.Network,
		"native_network_magic": identity.NativeNetworkMagic,
		"native_genesis_hash":  identity.NativeGenesisHash,
		"evm_chain_id":         identity.EVMChainID,
		"evm_genesis_hash":     identity.EVMGenesisHash,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func chainIdentityServerV1(t *testing.T, response func(call int32) string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			JSONRPC string `json:"jsonrpc"`
			ID      int    `json:"id"`
			Method  string `json:"method"`
			Params  []any  `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if request.JSONRPC != "2.0" || request.ID != 1 || request.Method != "getChainIdentity" || len(request.Params) != 0 {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(response(calls.Add(1))))
	}))
	return server, &calls
}

func TestDecodeChainIdentityGoldenFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/get-chain-identity-v1.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	got, err := decodeChainIdentityResultV1(raw)
	if err != nil {
		t.Fatalf("decodeChainIdentityResultV1() error = %v", err)
	}
	if got != testMainnetChainIdentityV1() {
		t.Fatalf("fixture identity = %#v", got)
	}
}

func TestVerifyChainIdentityPinsStrictCoreResult(t *testing.T) {
	result := marshalChainIdentityResultV1(t, testMainnetChainIdentityV1())
	server, calls := chainIdentityServerV1(t, func(int32) string {
		return `{"jsonrpc":"2.0","id":1,"result":` + result + `,"error":null}`
	})
	defer server.Close()

	router, err := newRPCRouterFromConfig(Config{RPCURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	router.backend.gbtClient = server.Client()
	if ready, reason := router.backendReadyForSubmitRecovery(router.backend); ready || reason != "chain-identity-unverified" {
		t.Fatalf("unverified readiness = %v/%q", ready, reason)
	}
	if err := router.verifyChainIdentity(context.Background(), router.backend); err != nil {
		t.Fatalf("verifyChainIdentity() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("getChainIdentity calls = %d, want 1", calls.Load())
	}
	if ready, reason := router.backendReadyForSubmitRecovery(router.backend); !ready || reason != "ok" {
		t.Fatalf("verified submit readiness = %v/%q", ready, reason)
	}
	if reason := router.templateFetchPauseReason(router.backend); reason != "" {
		t.Fatalf("verified template pause reason = %q", reason)
	}
	if err := validateSubmissionChainIDAgainstCore(router, big.NewInt(1404)); err != nil {
		t.Fatalf("matching submission chain ID error = %v", err)
	}
	if err := validateSubmissionChainIDAgainstCore(router, big.NewInt(1043)); err == nil {
		t.Fatal("mismatched submission chain ID was accepted")
	}
}

func TestVerifyChainIdentityRejectsMethodMissing(t *testing.T) {
	server, _ := chainIdentityServerV1(t, func(int32) string {
		return `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`
	})
	defer server.Close()
	router, err := newRPCRouterFromConfig(Config{RPCURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	router.backend.gbtClient = server.Client()
	if err := router.verifyChainIdentity(context.Background(), router.backend); err == nil {
		t.Fatal("verifyChainIdentity() accepted a missing method")
	}
	if ready, _ := router.chainIdentityReadiness(router.backend); ready {
		t.Fatal("backend remained ready after missing identity method")
	}
}

func TestDecodeChainIdentityRejectsMalformedResults(t *testing.T) {
	valid := marshalChainIdentityResultV1(t, testMainnetChainIdentityV1())
	tests := map[string]string{
		"unknown field":   strings.TrimSuffix(valid, "}") + `,"extra":"forbidden"}`,
		"missing field":   `{"schema":"bdag.chain-identity.v1","network":"mainnet","native_network_magic":"0xb4c3dce8","native_genesis_hash":"0xd62590d28f5c32b6d46839075b3173c3d4c178482f8cd1b0e424bd3feeaed11c","evm_chain_id":"1404"}`,
		"duplicate field": strings.TrimSuffix(valid, "}") + `,"network":"testnet"}`,
		"wrong schema":    strings.Replace(valid, chainIdentitySchemaV1, "bdag.chain-identity.v2", 1),
		"bad network":     strings.Replace(valid, `"network":"mainnet"`, `"network":"unknown"`, 1),
		"bad magic":       strings.Replace(valid, "0xb4c3dce8", "0xB4C3DCE8", 1),
		"bad native hash": strings.Replace(valid, "0xd62590", "0xD62590", 1),
		"bad EVM ID":      strings.Replace(valid, `"evm_chain_id":"1404"`, `"evm_chain_id":"01404"`, 1),
		"zero EVM hash":   strings.Replace(valid, testMainnetChainIdentityV1().EVMGenesisHash, "0x"+strings.Repeat("0", 64), 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeChainIdentityResultV1([]byte(raw)); err == nil {
				t.Fatalf("decodeChainIdentityResultV1() accepted %s", raw)
			}
		})
	}
}

func TestValidateChainIdentityAcceptsAllContractNetworks(t *testing.T) {
	for network, mapping := range chainIdentityNetworkMappingV1 {
		t.Run(network, func(t *testing.T) {
			identity := chainIdentityV1{
				Schema:             chainIdentitySchemaV1,
				Network:            network,
				NativeNetworkMagic: mapping.nativeMagic,
				NativeGenesisHash:  "0x" + strings.Repeat("1", 64),
				EVMChainID:         mapping.evmChainID,
				EVMGenesisHash:     "0x" + strings.Repeat("2", 64),
			}
			if err := validateChainIdentityV1(identity); err != nil {
				t.Fatalf("validateChainIdentityV1() error = %v", err)
			}
		})
	}
}

func TestVerifyChainIdentityPermanentlyBlocksEndpointIdentityChange(t *testing.T) {
	mainnet := marshalChainIdentityResultV1(t, testMainnetChainIdentityV1())
	testnet := marshalChainIdentityResultV1(t, testTestnetChainIdentityV1())
	server, calls := chainIdentityServerV1(t, func(call int32) string {
		result := mainnet
		if call == 2 {
			result = testnet
		}
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":%s,"error":null}`, result)
	})
	defer server.Close()

	router, err := newRPCRouterFromConfig(Config{RPCURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	router.backend.gbtClient = server.Client()
	if err := router.verifyChainIdentity(context.Background(), router.backend); err != nil {
		t.Fatalf("initial verifyChainIdentity() error = %v", err)
	}
	if err := router.verifyChainIdentity(context.Background(), router.backend); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("changed identity error = %v", err)
	}
	if ready, reason := router.chainIdentityReadiness(router.backend); ready || reason != "chain-identity-changed" {
		t.Fatalf("changed identity readiness = %v/%q", ready, reason)
	}
	if err := router.verifyChainIdentity(context.Background(), router.backend); err == nil {
		t.Fatal("fatal identity change became retryable")
	}
	if calls.Load() != 2 {
		t.Fatalf("fatal identity state made another request; calls = %d", calls.Load())
	}
}

func TestDecodeChainIdentityRPCEnvelopeRejectsUnknownAndTrailingData(t *testing.T) {
	result := marshalChainIdentityResultV1(t, testMainnetChainIdentityV1())
	tests := []string{
		`{"jsonrpc":"2.0","id":1,"result":` + result + `,"trace":"forbidden"}`,
		`{"jsonrpc":"2.0","id":1,"result":` + result + `} {}`,
		`{"jsonrpc":"1.0","id":1,"result":` + result + `}`,
		`{"jsonrpc":"2.0","id":2,"result":` + result + `}`,
	}
	for _, raw := range tests {
		if _, err := decodeChainIdentityRPCResponseV1([]byte(raw)); err == nil {
			t.Fatalf("decodeChainIdentityRPCResponseV1() accepted %s", raw)
		}
	}
}

func TestSubmitHTTPIsBlockedUntilChainIdentityIsVerified(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	backend := &rpcBackend{url: server.URL, submitClient: server.Client()}
	router := &rpcRouter{backend: backend, chainIdentityEnforced: true}
	oldRouter := activeRPCRouter
	activeRPCRouter = router
	t.Cleanup(func() { activeRPCRouter = oldRouter })

	attempt := PreparedAttemptV1{
		Method:      storage.SubmitMethodV2,
		RequestBody: []byte(`{"jsonrpc":"2.0","id":1,"method":"submitBlockHeaderV2","params":["00",0]}`),
	}
	attempt.CandidateID[0] = 1
	attempt.AttemptID[0] = 1
	attempt.LeaseToken[0] = 1
	attempt.RequestSHA256 = sha256BytesV1(attempt.RequestBody)
	observation, err := sendPreparedAttemptV1(context.Background(), attempt)
	if err == nil || !strings.Contains(err.Error(), "chain-identity-unverified") {
		t.Fatalf("unverified submit error = %v", err)
	}
	if observation.TransportClass != "chain-identity" {
		t.Fatalf("transport class = %q", observation.TransportClass)
	}
	if requests.Load() != 0 {
		t.Fatalf("unverified submit made %d HTTP requests", requests.Load())
	}
	if !bytes.Equal(attempt.RequestBody, []byte(`{"jsonrpc":"2.0","id":1,"method":"submitBlockHeaderV2","params":["00",0]}`)) {
		t.Fatal("submit guard mutated prepared request bytes")
	}
}
