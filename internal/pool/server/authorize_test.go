package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestValidateUserEVMAddress(t *testing.T) {
	tests := []struct {
		name       string
		username   string
		wantValid  bool
		wantReason string
	}{
		{name: "valid lower", username: "0x05518e03e148c56e426ff9e1cbdb962b4fc5250a", wantValid: true},
		{name: "valid mixed", username: "0x05518E03e148C56e426ff9e1CBdB962B4FC5250A", wantValid: true},
		{name: "zero address", username: "0x0000000000000000000000000000000000000000", wantReason: "zero address cannot receive pool payouts"},
		{name: "empty", username: "", wantReason: "empty username"},
		{name: "missing prefix", username: "05518E03e148C56e426ff9e1CBdB962B4FC5250A", wantReason: "username should be a valid evm address (start with 0x...)"},
		{name: "invalid hex", username: "0x05518E03e148C56e426ff9e1CBdB962B4FC5250Z", wantReason: "username should be a valid evm address"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValid, gotReason := validateUser(tt.username)
			if gotValid != tt.wantValid {
				t.Fatalf("valid = %v, want %v", gotValid, tt.wantValid)
			}
			if gotReason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", gotReason, tt.wantReason)
			}
		})
	}
}

func TestAuthorizedWorkerRemainsStableForConnection(t *testing.T) {
	c := &client{}
	first := "0x05518e03e148c56e426ff9e1cbdb962b4fc5250a"
	other := "0x1111111111111111111111111111111111111111"
	if got, ok := bindAuthorizedWorker(c, first); !ok || got != first {
		t.Fatal("first payout identity was not bound")
	}
	if got, ok := bindAuthorizedWorker(c, strings.ToUpper(first)); !ok || got != first {
		t.Fatal("equivalent address changed the bound identity")
	}
	if got, ok := bindAuthorizedWorker(c, other); ok || got != first || c.username != first {
		t.Fatal("connection accepted a changed payout identity")
	}
}

func TestConcurrentAuthorizationsBindOnlyOnePayoutIdentity(t *testing.T) {
	c := &client{}
	addresses := []string{"0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222"}
	var wg sync.WaitGroup
	results := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(value string) {
			defer wg.Done()
			if bound, ok := bindAuthorizedWorker(c, value); ok {
				results <- bound
			}
		}(addresses[i%len(addresses)])
	}
	wg.Wait()
	close(results)
	for bound := range results {
		if bound != c.username {
			t.Fatal("one connection accepted multiple payout identities")
		}
	}
}

func TestAuthorizeRejectsChangingPayoutAddress(t *testing.T) {
	old := authorizationsTotal
	authorizationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_authorizations_total"}, []string{"result"})
	t.Cleanup(func() { authorizationsTotal = old })
	var out bytes.Buffer
	c := &client{addr: "test-miner", authorized: true, username: "0x1111111111111111111111111111111111111111", rw: bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&out))}
	req := &stratumRequest{ID: 1, Params: []json.RawMessage{json.RawMessage(`"0x2222222222222222222222222222222222222222"`)}}
	handleAuthorize(c, req)
	var response stratumResponse
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	payload, ok := response.Error.([]any)
	if !ok || len(payload) != 2 || payload[0] != float64(24) || c.username != "0x1111111111111111111111111111111111111111" {
		t.Fatal("changing payout identity was not rejected for the connection")
	}
}
