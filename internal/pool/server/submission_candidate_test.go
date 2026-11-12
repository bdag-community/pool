package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"

	"asicPool/internal/pool/storage"
)

func TestCoreBlockHashV1GoldenVector(t *testing.T) {
	header := make([]byte, 185)
	for i := range header {
		header[i] = byte(i)
	}
	got, err := coreBlockHashV1(header)
	if err != nil {
		t.Fatal(err)
	}
	if gotHex := hex.EncodeToString(got[:]); gotHex != "613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94c" {
		t.Fatalf("Core hash = %s", gotHex)
	}
	preimage, err := hex.DecodeString("424441472d504f4f4c2d5355424d495353494f4e2d49442f763100613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94c")
	if err != nil || len(preimage) != 59 || !bytes.Equal(preimage[:27], candidateIDPrefixV1) {
		t.Fatalf("authoritative candidate preimage is not the required 59 bytes: %x", preimage)
	}
	idBytes := candidateIDV1(got)
	if id := hex.EncodeToString(idBytes[:]); id != "66f8d64e0443382192672329b03a678e2f268fea6d2eee92319012649f954e17" {
		t.Fatalf("candidate id = %s", id)
	}
	if sha256.Sum256(preimage) != idBytes {
		t.Fatal("candidate id oracle disagrees with fixed actual-NUL preimage")
	}
	literal := append([]byte("BDAG-POOL-SUBMISSION-ID/v1\\0"), got[:]...)
	literalDigest := sha256.Sum256(literal)
	if len(literal) != 60 || literalDigest == idBytes {
		t.Fatal("literal backslash-zero was accepted as v1 identity")
	}
	if hex.EncodeToString(literalDigest[:]) != "ef6be0d82b0aa96e495dce198a1db4d9e400cc93e1296154f86fc68ffc250994" {
		t.Fatalf("literal backslash-zero negative vector = %x", literalDigest)
	}
}

func TestBuildSubmitRequestV1ExactBytes(t *testing.T) {
	var c FrozenCandidateV1
	for i := range c.HeaderBytes {
		c.HeaderBytes[i] = byte(i)
	}
	c.ExtraNonce2 = [4]byte{0, 0, 0, 42}
	got, err := buildSubmitRequestV1(c, storage.SubmitMethodV2)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"jsonrpc":"2.0","id":1,"method":"submitBlockHeaderV2","params":["000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8",42]}`
	if string(got) != want {
		t.Fatalf("request = %s\nwant %s", got, want)
	}
}

func TestDecodeSubmitResponseV1RejectsAmbiguity(t *testing.T) {
	valid := `{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94c","height":7,"template_seq":9}}`
	if _, err := decodeSubmitResponseV1([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"result":null}`,
		`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"accepted":true}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"00","height":7,"template_seq":9}} trailing`,
		`{"jsonrpc":"2.0","id":2,"result":{}}`,
		`{"jsonrpc":"2.0","id":1,"result":true}`,
		`{"jsonrpc":"2.0","id":1,"result":[]}`,
		`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94","height":1.0,"template_seq":9}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"accepted":true,"final":true,"retryable":false,"code":"accepted","message":"ok","block_hash":"613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94","height":7,"template_seq":9},"error":{"code":-1,"message":"also error"}}`,
	} {
		if _, err := decodeSubmitResponseV1([]byte(body)); err == nil {
			t.Fatalf("accepted ambiguous response %q", body)
		}
	}
}

func TestClassifySubmitDecisionV1Table(t *testing.T) {
	var c FrozenCandidateV1
	hashBytes, _ := hex.DecodeString("613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94c")
	copy(c.ExpectedBlockHash[:], hashBytes)
	c.Height = big.NewInt(7)
	c.TemplateSeq = big.NewInt(9)
	matching := func(code string, accepted, final, retryable bool, height int64) RPCObservationV1 {
		msg := "message"
		return RPCObservationV1{Accepted: &accepted, Final: &final, Retryable: &retryable, Code: &code, Message: &msg, BlockHash: append([]byte(nil), c.ExpectedBlockHash[:]...), Height: big.NewInt(height), TemplateSeq: big.NewInt(9)}
	}
	for _, tc := range []struct {
		code                       string
		accepted, final, retryable bool
		height                     int64
		want                       SubmitDecisionV1
	}{
		{"accepted", true, true, false, 7, storage.SubmitDecisionAcceptedFinal},
		{"already_accepted", true, true, false, 0, storage.SubmitDecisionAcceptedFinal},
		{"already_rejected", false, true, false, 7, storage.SubmitDecisionRejectedFinal},
		{"stale_parent", false, true, false, 7, storage.SubmitDecisionRejectedFinal},
		{"queued", false, false, true, 7, storage.SubmitDecisionPending},
		{"duplicate", true, true, false, 7, storage.SubmitDecisionPending},
		{"unknown", false, true, false, 7, storage.SubmitDecisionPending},
	} {
		got := classifySubmitDecisionV1(c, matching(tc.code, tc.accepted, tc.final, tc.retryable, tc.height))
		if got != tc.want {
			t.Errorf("%s: got %s want %s", tc.code, got, tc.want)
		}
	}
	bad := matching("accepted", true, true, false, 7)
	bad.BlockHash = bytes.Repeat([]byte{1}, 32)
	if got := classifySubmitDecisionV1(c, bad); got != storage.SubmitDecisionConflict {
		t.Fatalf("hash mismatch decision=%s", got)
	}
	bad = matching("accepted", true, true, false, 7)
	c.TemplateSeq = big.NewInt(126)
	bad.TemplateSeq = big.NewInt(11708)
	if got := classifySubmitDecisionV1(c, bad); got != storage.SubmitDecisionAcceptedFinal {
		t.Fatalf("independent Core counter rejected: decision=%s", got)
	}
	bad.Height = big.NewInt(8)
	if got := classifySubmitDecisionV1(c, bad); got != storage.SubmitDecisionConflict {
		t.Fatalf("height mismatch decision=%s", got)
	}
	bad.Height = big.NewInt(7)
	bad.TemplateSeq = big.NewInt(-1)
	if got := classifySubmitDecisionV1(c, bad); got != storage.SubmitDecisionConflict {
		t.Fatalf("invalid Core counter decision=%s", got)
	}
	for _, code := range []string{"missing_trie", "nonce_too_low", "node_syncing", "p2p_not_current", "miner_disabled", "internal_error", "template_seq_unknown"} {
		if got := classifySubmitDecisionV1(c, matching(code, false, false, true, 7)); got != storage.SubmitDecisionPending {
			t.Errorf("%s flags: got %s want pending", code, got)
		}
	}
}

func TestDecodeSubmitResponseV1StrictFieldMatrix(t *testing.T) {
	base := map[string]string{
		"accepted": "true", "final": "true", "retryable": "false", "code": `"accepted"`,
		"message": `"ok"`, "block_hash": `"613e7fbbf3d18afbca82c5a06fbbd2be6c87eed6765695c1efba3851e265a94"`,
		"height": "7", "template_seq": "9",
	}
	for field, invalid := range map[string]string{
		"accepted": `"true"`, "final": "1", "retryable": "null", "code": "1",
		"message": "false", "block_hash": "null", "height": `"7"`, "template_seq": "1.0",
	} {
		fields := make(map[string]string, len(base))
		for k, v := range base {
			fields[k] = v
		}
		fields[field] = invalid
		body := strictResponseObjectV1(fields)
		if _, err := decodeSubmitResponseV1([]byte(body)); err == nil {
			t.Errorf("field %s invalid encoding was accepted", field)
		}
	}
	for _, field := range []string{"accepted", "final", "retryable", "code", "message", "block_hash", "height", "template_seq"} {
		fields := make(map[string]string, len(base))
		for k, v := range base {
			fields[k] = v
		}
		delete(fields, field)
		if _, err := decodeSubmitResponseV1([]byte(strictResponseObjectV1(fields))); err == nil {
			t.Errorf("missing field %s was accepted", field)
		}
	}
}

func TestDecodeSubmitResponseV1StrictMethodNotFoundMatrix(t *testing.T) {
	valid := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`)
	observation, err := decodeSubmitResponseV1(valid)
	if err == nil || !observation.MethodNotFound || observation.Code == nil || *observation.Code != "-32601" {
		t.Fatalf("strict method-not-found response = %+v err=%v", observation, err)
	}
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"method not found"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"method not found"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"},"extra":true}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"code":-32601,"message":"method not found"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":true}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"accepted":false},"error":{"code":-32601,"message":"method not found"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}} trailing`,
	} {
		observation, err := decodeSubmitResponseV1([]byte(body))
		if err == nil || observation.MethodNotFound {
			t.Fatalf("malformed method-not-found response accepted: body=%q observation=%+v err=%v", body, observation, err)
		}
	}
}

func strictResponseObjectV1(fields map[string]string) string {
	keys := []string{"accepted", "final", "retryable", "code", "message", "block_hash", "height", "template_seq"}
	var b bytes.Buffer
	b.WriteString(`{"jsonrpc":"2.0","id":1,"result":{`)
	first := true
	for _, key := range keys {
		value, ok := fields[key]
		if !ok {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(`"` + key + `":` + value)
	}
	b.WriteString(`}}`)
	return b.String()
}
