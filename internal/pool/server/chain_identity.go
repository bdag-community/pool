package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	chainIdentitySchemaV1        = "bdag.chain-identity.v1"
	maxChainIdentityResponseSize = 64 * 1024
)

var (
	lowerHex32V1 = regexp.MustCompile(`^0x[0-9a-f]{64}$`)
	lowerMagicV1 = regexp.MustCompile(`^0x[0-9a-f]{8}$`)
	decimalIDV1  = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

	chainIdentityNetworkMappingV1 = map[string]struct {
		nativeMagic string
		evmChainID  string
	}{
		"mainnet":          {nativeMagic: "0xb4c3dce8", evmChainID: "1404"},
		"mainnet-isolated": {nativeMagic: "0xb4c3dce8", evmChainID: "1404"},
		"testnet":          {nativeMagic: "0x35e0c424", evmChainID: "1043"},
		"privnet":          {nativeMagic: "0xf1eb0001", evmChainID: "1048"},
		"mixnet":           {nativeMagic: "0xc459b247", evmChainID: "1046"},
		"devnet":           {nativeMagic: "0x80808080", evmChainID: "8008"},
	}
)

type chainIdentityV1 struct {
	Schema             string
	Network            string
	NativeNetworkMagic string
	NativeGenesisHash  string
	EVMChainID         string
	EVMGenesisHash     string
}

func fetchChainIdentityFromBackend(ctx context.Context, backend *rpcBackend) (chainIdentityV1, error) {
	if backend == nil {
		return chainIdentityV1{}, errors.New("no RPC backend configured")
	}
	requestBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"getChainIdentity","params":[]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, backend.url, bytes.NewReader(requestBody))
	if err != nil {
		return chainIdentityV1{}, fmt.Errorf("build getChainIdentity request: %w", err)
	}
	req.Header.Set("Authorization", backend.authorizationHeader())
	req.Header.Set("Content-Type", "application/json")
	client := backend.gbtClient
	if client == nil {
		client = gbtHTTPClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return chainIdentityV1{}, fmt.Errorf("getChainIdentity transport: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxChainIdentityResponseSize+1))
	if err != nil {
		return chainIdentityV1{}, fmt.Errorf("read getChainIdentity response: %w", err)
	}
	if len(body) > maxChainIdentityResponseSize {
		return chainIdentityV1{}, errors.New("getChainIdentity response exceeds size limit")
	}
	if resp.StatusCode != http.StatusOK {
		return chainIdentityV1{}, fmt.Errorf("getChainIdentity HTTP status %d", resp.StatusCode)
	}
	return decodeChainIdentityRPCResponseV1(body)
}

func decodeChainIdentityRPCResponseV1(body []byte) (chainIdentityV1, error) {
	envelope, err := decodeUniqueObjectV1(body)
	if err != nil {
		return chainIdentityV1{}, fmt.Errorf("decode getChainIdentity RPC envelope: %w", err)
	}
	for field := range envelope {
		switch field {
		case "jsonrpc", "id", "result", "error":
		default:
			return chainIdentityV1{}, fmt.Errorf("getChainIdentity RPC envelope has unknown field %q", field)
		}
	}
	jsonRPC, err := rawStringV1(envelope["jsonrpc"], "jsonrpc")
	if err != nil || jsonRPC != "2.0" {
		return chainIdentityV1{}, errors.New("getChainIdentity response has invalid jsonrpc version")
	}
	if id, ok := envelope["id"]; !ok || !bytes.Equal(bytes.TrimSpace(id), []byte("1")) {
		return chainIdentityV1{}, errors.New("getChainIdentity response has invalid request id")
	}
	if rpcError, ok := envelope["error"]; ok && !bytes.Equal(bytes.TrimSpace(rpcError), []byte("null")) {
		return chainIdentityV1{}, errors.New("getChainIdentity returned an RPC error")
	}
	result, ok := envelope["result"]
	if !ok || bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return chainIdentityV1{}, errors.New("getChainIdentity returned no result")
	}
	return decodeChainIdentityResultV1(result)
}

func decodeChainIdentityResultV1(raw []byte) (chainIdentityV1, error) {
	fields, err := decodeUniqueObjectV1(raw)
	if err != nil {
		return chainIdentityV1{}, fmt.Errorf("decode getChainIdentity result: %w", err)
	}
	expected := []string{
		"schema",
		"network",
		"native_network_magic",
		"native_genesis_hash",
		"evm_chain_id",
		"evm_genesis_hash",
	}
	if len(fields) != len(expected) {
		return chainIdentityV1{}, fmt.Errorf("getChainIdentity result must contain exactly %d fields", len(expected))
	}
	values := make(map[string]string, len(expected))
	for _, field := range expected {
		value, err := rawStringV1(fields[field], field)
		if err != nil {
			return chainIdentityV1{}, fmt.Errorf("getChainIdentity result: %w", err)
		}
		values[field] = value
	}
	identity := chainIdentityV1{
		Schema:             values["schema"],
		Network:            values["network"],
		NativeNetworkMagic: values["native_network_magic"],
		NativeGenesisHash:  values["native_genesis_hash"],
		EVMChainID:         values["evm_chain_id"],
		EVMGenesisHash:     values["evm_genesis_hash"],
	}
	if err := validateChainIdentityV1(identity); err != nil {
		return chainIdentityV1{}, err
	}
	return identity, nil
}

func validateChainIdentityV1(identity chainIdentityV1) error {
	if identity.Schema != chainIdentitySchemaV1 {
		return fmt.Errorf("getChainIdentity schema is not %s", chainIdentitySchemaV1)
	}
	mapping, ok := chainIdentityNetworkMappingV1[identity.Network]
	if !ok {
		return fmt.Errorf("getChainIdentity network is unsupported")
	}
	if !lowerMagicV1.MatchString(identity.NativeNetworkMagic) || identity.NativeNetworkMagic != mapping.nativeMagic {
		return fmt.Errorf("getChainIdentity native network magic is invalid for network")
	}
	if !lowerHex32V1.MatchString(identity.NativeGenesisHash) || isZeroHexHashV1(identity.NativeGenesisHash) {
		return fmt.Errorf("getChainIdentity native genesis hash is invalid")
	}
	if !decimalIDV1.MatchString(identity.EVMChainID) || identity.EVMChainID != mapping.evmChainID {
		return fmt.Errorf("getChainIdentity EVM chain ID is invalid for network")
	}
	if parsed, ok := new(big.Int).SetString(identity.EVMChainID, 10); !ok || parsed.Sign() < 0 {
		return fmt.Errorf("getChainIdentity EVM chain ID is invalid")
	}
	if !lowerHex32V1.MatchString(identity.EVMGenesisHash) || isZeroHexHashV1(identity.EVMGenesisHash) {
		return fmt.Errorf("getChainIdentity EVM genesis hash is invalid")
	}
	return nil
}

func isZeroHexHashV1(value string) bool {
	return strings.Trim(strings.TrimPrefix(value, "0x"), "0") == ""
}

func (r *rpcRouter) verifyChainIdentity(ctx context.Context, backend *rpcBackend) error {
	if r == nil || backend == nil || backend != r.selectedBackend() {
		return errors.New("chain identity verification requires the selected Core backend")
	}
	backend.mu.Lock()
	if backend.state.chainIdentityFatal {
		reason := backend.state.chainIdentityError
		backend.mu.Unlock()
		if reason == "" {
			reason = "endpoint chain identity previously changed"
		}
		return errors.New(reason)
	}
	backend.state.chainIdentityVerified = false
	backend.state.chainIdentityError = "verification in progress"
	backend.mu.Unlock()

	identity, err := fetchChainIdentityFromBackend(ctx, backend)
	if err != nil {
		r.recordChainIdentityError(backend, err, false)
		return err
	}

	backend.mu.Lock()
	if backend.state.chainIdentityPinned && backend.state.chainIdentity != identity {
		backend.state.chainIdentityVerified = false
		backend.state.chainIdentityFatal = true
		backend.state.chainIdentityError = "endpoint chain identity changed after initial verification"
		backend.state.healthy = false
		backend.state.score = 0
		backend.state.reason = "chain-identity-changed"
		backend.mu.Unlock()
		return errors.New("endpoint chain identity changed after initial verification")
	}
	backend.state.chainIdentity = identity
	backend.state.chainIdentityPinned = true
	backend.state.chainIdentityVerified = true
	backend.state.chainIdentityError = ""
	backend.state.lastChainIdentityAt = time.Now()
	if backend.state.reason == "chain-identity-unverified" {
		// Identity transport/method recovery only re-opens the normal readiness
		// path. A template or node-health sample will immediately establish the
		// substantive backend health state.
		backend.state.healthy = true
		backend.state.score = 100
		backend.state.reason = "not-yet-measured"
	}
	if backend.state.healthySince.IsZero() {
		backend.state.healthySince = backend.state.lastChainIdentityAt
	}
	backend.mu.Unlock()
	return nil
}

func (r *rpcRouter) recordChainIdentityError(backend *rpcBackend, err error, fatal bool) {
	if r == nil || backend == nil || err == nil {
		return
	}
	backend.mu.Lock()
	backend.state.chainIdentityVerified = false
	backend.state.chainIdentityFatal = backend.state.chainIdentityFatal || fatal
	backend.state.chainIdentityError = err.Error()
	backend.state.healthy = false
	backend.state.score = 0
	if backend.state.chainIdentityFatal {
		backend.state.reason = "chain-identity-changed"
	} else {
		backend.state.reason = "chain-identity-unverified"
	}
	backend.mu.Unlock()
}

func (r *rpcRouter) chainIdentityReadiness(backend *rpcBackend) (bool, string) {
	if r == nil || backend == nil || backend != r.selectedBackend() {
		return false, "chain-identity-no-backend"
	}
	// All production routers are constructed by newRPCRouterFromConfig with
	// enforcement enabled. The zero value exists only for narrow in-package
	// synthetic routers used to test the submit transport in isolation.
	if !r.chainIdentityEnforced {
		return true, "ok"
	}
	state := backend.snapshotState()
	if state.chainIdentityFatal {
		return false, "chain-identity-changed"
	}
	if !state.chainIdentityVerified || !state.chainIdentityPinned {
		return false, "chain-identity-unverified"
	}
	return true, "ok"
}

func verifySelectedCoreChainIdentity(r *rpcRouter) error {
	if r == nil {
		return errors.New("Core RPC router is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.verifyChainIdentity(ctx, r.selectedBackend()); err != nil {
		return fmt.Errorf("Core chain identity preflight failed: %w", err)
	}
	return nil
}

func validateSubmissionChainIDAgainstCore(r *rpcRouter, submissionChainID *big.Int) error {
	if r == nil || submissionChainID == nil || submissionChainID.Sign() < 0 {
		return errors.New("cannot compare submission chain ID without verified Core identity")
	}
	backend := r.selectedBackend()
	if ready, reason := r.chainIdentityReadiness(backend); !ready {
		return fmt.Errorf("cannot compare submission chain ID: %s", reason)
	}
	state := backend.snapshotState()
	if submissionChainID.String() != state.chainIdentity.EVMChainID {
		return fmt.Errorf("POOL_SUBMISSION_CHAIN_ID does not match the verified Core EVM chain ID")
	}
	return nil
}

func runIdentityVerifiedTemplateSubscription(backend *rpcBackend) error {
	if activeRPCRouter == nil {
		return errors.New("Core RPC router is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := activeRPCRouter.verifyChainIdentity(ctx, backend); err != nil {
		return fmt.Errorf("Core chain identity reconnect verification failed: %w", err)
	}
	return runTemplateSubscription(backend)
}
