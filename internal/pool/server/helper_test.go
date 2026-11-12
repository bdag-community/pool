package server

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var rawParamsBenchSink []string
var duplicateStringKeyBenchSink string
var duplicateStructKeyBenchSink shareNonceKey
var submitAcceptedPayloadBenchSink []byte
var submitAcceptedPayloadLenBenchSink int
var merkleRootBenchSink [32]byte
var extraNonce2BenchSink uint64

func TestNormalizeHexStripsCaseInsensitivePrefix(t *testing.T) {
	tests := map[string]string{
		" ABCDEF1234 ": "abcdef1234",
		"0xABCDEF1234": "abcdef1234",
		"0XABCDEF1234": "abcdef1234",
		"0x":           "",
		"0X":           "",
	}
	for input, want := range tests {
		if got := normalizeHex(input); got != want {
			t.Fatalf("normalizeHex(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestWriteJSONLineSetsAndClearsWriteDeadline(t *testing.T) {
	var writes bytes.Buffer
	conn := &deadlineRecordingConn{}
	c := &client{
		addr: "miner-1",
		conn: conn,
		rw:   bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
	}

	if err := writeJSONLine(c, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("writeJSONLine returned error: %v", err)
	}
	if got, want := writes.String(), "{\"ok\":true}\n"; got != want {
		t.Fatalf("written line = %q, want %q", got, want)
	}
	if len(conn.writeDeadlines) != 2 {
		t.Fatalf("SetWriteDeadline calls = %d, want set and clear", len(conn.writeDeadlines))
	}
	if conn.writeDeadlines[0].IsZero() {
		t.Fatal("first write deadline was not set")
	}
	if !conn.writeDeadlines[1].IsZero() {
		t.Fatalf("second write deadline = %v, want cleared zero deadline", conn.writeDeadlines[1])
	}
}

func TestWriteJSONLineWithTimeoutUsesShortDeadline(t *testing.T) {
	var writes bytes.Buffer
	conn := &deadlineRecordingConn{}
	c := &client{
		addr: "miner-1",
		conn: conn,
		rw:   bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
	}

	start := time.Now()
	if err := writeJSONLineWithTimeout(c, []byte(`{"ok":true}`), 250*time.Millisecond); err != nil {
		t.Fatalf("writeJSONLineWithTimeout returned error: %v", err)
	}
	if len(conn.writeDeadlines) != 2 {
		t.Fatalf("SetWriteDeadline calls = %d, want set and clear", len(conn.writeDeadlines))
	}
	if conn.writeDeadlines[0].Before(start) || conn.writeDeadlines[0].After(start.Add(time.Second)) {
		t.Fatalf("short write deadline = %v, want near start", conn.writeDeadlines[0])
	}
}

func TestClassifyStratumDisconnectSeparatesNoRequestEOF(t *testing.T) {
	if got := classifyStratumDisconnect(nil, false); got != "no-request-eof" {
		t.Fatalf("disconnect reason = %q, want no-request-eof", got)
	}
	if got := classifyStratumDisconnect(io.ErrUnexpectedEOF, false); got != "no-request-read-error" {
		t.Fatalf("disconnect reason = %q, want no-request-read-error", got)
	}
	if got := classifyStratumDisconnect(nil, true); got != "eof" {
		t.Fatalf("disconnect reason = %q, want eof", got)
	}
	if got := classifyStratumDisconnect(io.ErrUnexpectedEOF, true); got != "read-error" {
		t.Fatalf("disconnect reason = %q, want read-error", got)
	}
}

func TestWriteJSONLineClosesClientOnWriteError(t *testing.T) {
	conn := &deadlineRecordingConn{}
	c := &client{
		addr: "miner-1",
		conn: conn,
		rw:   bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(errorWriter{})),
	}

	if err := writeJSONLine(c, []byte(`{"ok":true}`)); err == nil {
		t.Fatal("writeJSONLine returned nil error")
	}
	if !c.closed {
		t.Fatal("client was not marked closed after write error")
	}
	if !conn.closed {
		t.Fatal("connection was not closed after write error")
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("forced write failure")
}

type deadlineRecordingConn struct {
	writeDeadlines []time.Time
	closed         bool
}

func (c *deadlineRecordingConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (c *deadlineRecordingConn) Write(p []byte) (int, error)     { return len(p), nil }
func (c *deadlineRecordingConn) Close() error                    { c.closed = true; return nil }
func (c *deadlineRecordingConn) LocalAddr() net.Addr             { return testAddr("local") }
func (c *deadlineRecordingConn) RemoteAddr() net.Addr            { return testAddr("remote") }
func (c *deadlineRecordingConn) SetDeadline(time.Time) error     { return nil }
func (c *deadlineRecordingConn) SetReadDeadline(time.Time) error { return nil }
func (c *deadlineRecordingConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadlines = append(c.writeDeadlines, t)
	return nil
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func TestMaxInflightRequestsPerClient(t *testing.T) {
	if got := maxInflightRequestsPerClient(nil); got != defaultMaxInflightRequestsPerClient {
		t.Fatalf("nil config max in-flight = %d, want %d", got, defaultMaxInflightRequestsPerClient)
	}
	if got := maxInflightRequestsPerClient(&Config{}); got != defaultMaxInflightRequestsPerClient {
		t.Fatalf("default max in-flight = %d, want %d", got, defaultMaxInflightRequestsPerClient)
	}
	if got := maxInflightRequestsPerClient(&Config{MaxInflightRequestsPerClient: -1}); got != defaultMaxInflightRequestsPerClient {
		t.Fatalf("negative max in-flight = %d, want %d", got, defaultMaxInflightRequestsPerClient)
	}
	if got := maxInflightRequestsPerClient(&Config{MaxInflightRequestsPerClient: 3}); got != 3 {
		t.Fatalf("configured max in-flight = %d, want 3", got)
	}
}

func TestDispatchClientRequestBoundsInflightPerClient(t *testing.T) {
	inflight := make(chan struct{}, 2)
	release := make(chan struct{})
	started := make(chan struct{}, 3)
	var current atomic.Int32
	var maxSeen atomic.Int32
	handler := func(*client, *stratumRequest) {
		now := current.Add(1)
		for {
			max := maxSeen.Load()
			if now <= max || maxSeen.CompareAndSwap(max, now) {
				break
			}
		}
		started <- struct{}{}
		<-release
		current.Add(-1)
	}

	dispatchClientRequestWithHandler(&client{}, &stratumRequest{Method: "mining.submit"}, inflight, handler)
	dispatchClientRequestWithHandler(&client{}, &stratumRequest{Method: "mining.submit"}, inflight, handler)
	waitForStartedRequests(t, started, 2)

	thirdDispatched := make(chan struct{})
	go func() {
		dispatchClientRequestWithHandler(&client{}, &stratumRequest{Method: "mining.submit"}, inflight, handler)
		close(thirdDispatched)
	}()

	select {
	case <-thirdDispatched:
		t.Fatal("third request dispatched before an in-flight slot was released")
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	waitForStartedRequests(t, started, 1)
	select {
	case <-thirdDispatched:
	case <-time.After(time.Second):
		t.Fatal("third request did not dispatch after an in-flight slot was released")
	}
	if got := maxSeen.Load(); got > 2 {
		t.Fatalf("max concurrent handlers = %d, want at most 2", got)
	}
}

func TestDispatchClientRequestRunsSubmitConcurrentlyWhenOrderingEnabled(t *testing.T) {
	inflight := make(chan struct{}, 4)
	c := &client{
		cfg: &Config{SubmitOrderingEnabled: true},
	}
	releaseFirst := make(chan struct{})
	started := make(chan string, 2)
	completed := make(chan string, 2)
	handler := func(_ *client, req *stratumRequest) {
		id, _ := req.ID.(string)
		started <- id
		if id == "first" {
			<-releaseFirst
		}
		completed <- id
	}

	dispatchClientRequestWithHandler(c, &stratumRequest{ID: "first", Method: "mining.submit"}, inflight, handler)
	dispatchClientRequestWithHandler(c, &stratumRequest{ID: "second", Method: "mining.submit"}, inflight, handler)

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case got := <-started:
			seen[got] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for submit %d to start", i+1)
		}
	}
	if !seen["first"] || !seen["second"] {
		t.Fatalf("started submits = %#v, want first and second", seen)
	}

	close(releaseFirst)
	select {
	case got := <-completed:
		if got != "second" && got != "first" {
			t.Fatalf("unexpected completed request = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first completion")
	}
	select {
	case got := <-completed:
		if got != "second" && got != "first" {
			t.Fatalf("unexpected completed request = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second completion")
	}
}

func waitForStartedRequests(t *testing.T, started <-chan struct{}, want int) {
	t.Helper()
	for i := 0; i < want; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for started request %d of %d", i+1, want)
		}
	}
}

func TestRawParamsToStrings(t *testing.T) {
	raw := []json.RawMessage{
		[]byte(`"0x05518E03e148C56e426ff9e1CBdB962B4FC5250A"`),
		[]byte(`"123-abcdef"`),
		[]byte(`42`),
		[]byte(`"escaped\nvalue"`),
	}

	got := rawParamsToStrings(raw)
	want := []string{
		"0x05518E03e148C56e426ff9e1CBdB962B4FC5250A",
		"123-abcdef",
		"42",
		"escaped\nvalue",
	}
	if len(got) != len(want) {
		t.Fatalf("param count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("param %d mismatch: got %q want %q", i, got[i], want[i])
		}
	}
}

func BenchmarkRawParamsToStringsSubmit(b *testing.B) {
	raw := []json.RawMessage{
		[]byte(`"0x05518E03e148C56e426ff9e1CBdB962B4FC5250A"`),
		[]byte(`"6650000-bc29702af8250b21"`),
		[]byte(`"00000001"`),
		[]byte(`"6920c9a0"`),
		[]byte(`"1f4a2b3c"`),
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rawParamsBenchSink = rawParamsToStrings(raw)
	}
}

func BenchmarkSubmitJobStaticFieldPreparation(b *testing.B) {
	job := &JobData{
		ParentHash: "bc29702af8250b21f3d7bf71dc98d80b6367eef2e1bd76a1ce38a449aee8e278",
		NBits:      0x1d0251a8,
	}
	header := make([]byte, 80)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		prevBytes, _ := hex.DecodeString(job.ParentHash)
		copy(header[4:36], prevBytes)
		target := bitsToTarget(job.NBits)
		if target.Sign() <= 0 {
			b.Fatal("invalid target")
		}
	}
}

func BenchmarkSubmitJobCachedStaticFieldPreparation(b *testing.B) {
	parentBytes, err := hex.DecodeString("bc29702af8250b21f3d7bf71dc98d80b6367eef2e1bd76a1ce38a449aee8e278")
	if err != nil {
		b.Fatal(err)
	}
	job := &JobData{
		ParentHash:    "bc29702af8250b21f3d7bf71dc98d80b6367eef2e1bd76a1ce38a449aee8e278",
		ParentBytes:   parentBytes,
		NBits:         0x1d0251a8,
		NetworkTarget: bitsToTarget(0x1d0251a8),
	}
	header := make([]byte, 80)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if len(job.ParentBytes) == 32 {
			copy(header[4:36], job.ParentBytes)
		}
		target := job.NetworkTarget
		if target == nil {
			target = bitsToTarget(job.NBits)
		}
		if target.Sign() <= 0 {
			b.Fatal("invalid target")
		}
	}
}

func TestShareNonceKey(t *testing.T) {
	nonceBytes, err := hex.DecodeString("1f4a2b3c")
	if err != nil {
		t.Fatal(err)
	}
	en2Bytes, err := hex.DecodeString("00000001")
	if err != nil {
		t.Fatal(err)
	}
	ntimeBytes, err := hex.DecodeString("6920c9a0")
	if err != nil {
		t.Fatal(err)
	}

	key := newShareNonceKey(nonceBytes, en2Bytes, ntimeBytes)
	if key.nonce != 0x1f4a2b3c || key.extranonce2 != 1 || key.ntime != 0x6920c9a0 {
		t.Fatalf("unexpected key: %+v", key)
	}
	if got := key.String(); got != "1f4a2b3c-00000001-6920c9a0" {
		t.Fatalf("unexpected key string: %s", got)
	}
}

func TestSubmitAcceptedResult(t *testing.T) {
	header := []byte{0x01, 0x02}
	result := []byte{0x0a, 0x0b}

	if got := submitAcceptedResult(&Config{}, header, result); got != true {
		t.Fatalf("compact result = %#v, want true", got)
	}

	got := submitAcceptedResult(&Config{SubmitDebugResultEnabled: true}, header, result)
	items, ok := got.([]interface{})
	if !ok || len(items) != 3 {
		t.Fatalf("debug result = %#v, want 3-item array", got)
	}
	if items[0] != true || items[1] != "0102" || items[2] != "0a0b" {
		t.Fatalf("debug result = %#v", got)
	}
}

func TestAppendCompactSubmitAcceptedMatchesMarshal(t *testing.T) {
	for _, id := range []interface{}{nil, float64(1), float64(1.5), "abc", "", true, int(7), json.Number("9")} {
		got, err := appendCompactSubmitAccepted(nil, id)
		if err != nil {
			t.Fatalf("appendCompactSubmitAccepted(%#v) error: %v", id, err)
		}
		want, err := json.Marshal(stratumResponse{ID: id, Result: true})
		if err != nil {
			t.Fatalf("marshal reference(%#v) error: %v", id, err)
		}
		if string(got) != string(want) {
			t.Fatalf("appendCompactSubmitAccepted(%#v) = %s, want %s", id, got, want)
		}
	}
}

func TestFixedHexUint32MatchesSprintf(t *testing.T) {
	for _, v := range []uint32{
		0,
		1,
		0x0f,
		0x10,
		0x1234abcd,
		0x80000000,
		0xffffffff,
	} {
		got := fixedHexUint32(v)
		want := fmt.Sprintf("%08x", v)
		if got != want {
			t.Fatalf("fixedHexUint32(%#x) = %q, want %q", v, got, want)
		}
	}
}

func BenchmarkDuplicateShareStringKey(b *testing.B) {
	nonce := "1f4a2b3c"
	extranonce2 := "00000001"
	ntime := "6920c9a0"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		duplicateStringKeyBenchSink = fmt.Sprintf("%s-%s-%s", nonce, extranonce2, ntime)
	}
}

func BenchmarkDuplicateShareStructKey(b *testing.B) {
	nonceBytes, _ := hex.DecodeString("1f4a2b3c")
	en2Bytes, _ := hex.DecodeString("00000001")
	ntimeBytes, _ := hex.DecodeString("6920c9a0")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		duplicateStructKeyBenchSink = newShareNonceKey(nonceBytes, en2Bytes, ntimeBytes)
	}
}

func BenchmarkFixedHexUint32Sprintf(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		duplicateStringKeyBenchSink = fmt.Sprintf("%08x", uint32(i))
	}
}

func BenchmarkFixedHexUint32Fastpath(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		duplicateStringKeyBenchSink = fixedHexUint32(uint32(i))
	}
}

func BenchmarkSubmitAcceptedDebugResultPayload(b *testing.B) {
	header := make([]byte, 80)
	result := make([]byte, 32)
	binaryHeaderFixture(header)
	binaryHeaderFixture(result)
	cfg := &Config{SubmitDebugResultEnabled: true}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(stratumResponse{ID: 1, Result: submitAcceptedResult(cfg, header, result)})
		if err != nil {
			b.Fatal(err)
		}
		submitAcceptedPayloadBenchSink = payload
	}
}

func BenchmarkSubmitAcceptedCompactResultPayload(b *testing.B) {
	header := make([]byte, 80)
	result := make([]byte, 32)
	binaryHeaderFixture(header)
	binaryHeaderFixture(result)
	cfg := &Config{}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(stratumResponse{ID: 1, Result: submitAcceptedResult(cfg, header, result)})
		if err != nil {
			b.Fatal(err)
		}
		submitAcceptedPayloadBenchSink = payload
	}
}

func BenchmarkSubmitAcceptedCompactResultFastPath(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var scratch [128]byte
		payload, err := appendCompactSubmitAccepted(scratch[:0], float64(1))
		if err != nil {
			b.Fatal(err)
		}
		submitAcceptedPayloadLenBenchSink += len(payload)
	}
}

func BenchmarkDummyMerkleRootAppendStyle(b *testing.B) {
	en1 := []byte{1, 2, 3, 4}
	en2 := []byte{5, 6, 7, 8}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		merkleRootBenchSink = dummyMerkleRootAppendStyle(en1, en2)
	}
}

func BenchmarkDummyMerkleRootStack(b *testing.B) {
	en1 := []byte{1, 2, 3, 4}
	en2 := []byte{5, 6, 7, 8}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		merkleRootBenchSink = dummyMerkleRoot(en1, en2)
	}
}

func BenchmarkExtraNonce2ParseUint(b *testing.B) {
	raw := "0000002a"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, err := strconv.ParseUint(raw, 16, 64)
		if err != nil {
			b.Fatal(err)
		}
		extraNonce2BenchSink = v
	}
}

func BenchmarkExtraNonce2BigEndian(b *testing.B) {
	raw, _ := hex.DecodeString("0000002a")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		extraNonce2BenchSink = uint64(binary.BigEndian.Uint32(raw))
	}
}

func BenchmarkBlockCandidateDoubleScryptCheck(b *testing.B) {
	header := make([]byte, 80)
	binaryHeaderFixture(header)
	target := new(big.Int).Lsh(big.NewInt(1), 256)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := scryptHash(header)
		hashInt := HashToBig(&result)
		if hashInt.Cmp(target) > 0 {
			b.Fatal("unexpected first check failure")
		}
		checkHash := scryptHash(header)
		checkInt := HashToBig(&checkHash)
		if checkInt.Cmp(target) > 0 {
			b.Fatal("unexpected second check failure")
		}
	}
}

func BenchmarkBlockCandidateSingleScryptCheck(b *testing.B) {
	header := make([]byte, 80)
	binaryHeaderFixture(header)
	target := new(big.Int).Lsh(big.NewInt(1), 256)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := scryptHash(header)
		hashInt := HashToBig(&result)
		if hashInt.Cmp(target) > 0 {
			b.Fatal("unexpected check failure")
		}
	}
}

func binaryHeaderFixture(header []byte) {
	for i := range header {
		header[i] = byte(i*31 + 7)
	}
}

func dummyMerkleRootAppendStyle(extranonce1, extranonce2 []byte) [32]byte {
	buf := make([]byte, 0, len(extranonce1)+len(extranonce2))
	buf = append(buf, extranonce1...)
	buf = append(buf, extranonce2...)

	h1 := sha256.Sum256(buf)
	h2 := sha256.Sum256(h1[:])
	return h2
}
