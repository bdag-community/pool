package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSubmitPipelineParamsMapsStratumFields(t *testing.T) {
	req := &stratumRequest{
		Params: []json.RawMessage{
			json.RawMessage(`"worker.1"`),
			json.RawMessage(`"job-7"`),
			json.RawMessage(`"00000001"`),
			json.RawMessage(`"65f00000"`),
			json.RawMessage(`"00000002"`),
		},
	}
	pipeline := newSubmitPipeline(nil, req, time.Unix(100, 0))

	params, ok := pipeline.params()
	if !ok {
		t.Fatal("params() returned ok=false, want true")
	}
	if params.worker != "worker.1" ||
		params.jobID != "job-7" ||
		params.extranonce2 != "00000001" ||
		params.ntime != "65f00000" ||
		params.nonce != "00000002" {
		t.Fatalf("params() = %#v, want mapped submit fields", params)
	}
}

func TestHandleSubmitRejectsInvalidParamsThroughPipeline(t *testing.T) {
	var writes bytes.Buffer
	c := &client{
		addr:       "miner-test",
		authorized: true,
		sessionID:  "session-1",
		rw:         bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(&writes)),
	}
	req := &stratumRequest{
		ID:     "submit-1",
		Method: "mining.submit",
		Params: []json.RawMessage{json.RawMessage(`"worker.1"`)},
	}

	if err := handleSubmit(c, req); err != nil {
		t.Fatalf("handleSubmit returned error: %v", err)
	}

	got := writes.String()
	if !strings.Contains(got, `"id":"submit-1"`) ||
		!strings.Contains(got, `"error":[-32602,"invalid params"]`) {
		t.Fatalf("response = %q, want invalid params error", got)
	}
}
