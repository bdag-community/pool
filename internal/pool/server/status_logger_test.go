package server

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPoolStatusLogEventJSONShape(t *testing.T) {
	now := time.Unix(1000, 123).UTC()
	payload := newPoolStatusLogEvent(now, poolJobHealthSnapshot{
		GeneratedAtUnixMs:     now.UnixMilli(),
		Status:                "ok",
		ReasonCode:            poolJobHealthOK,
		ActiveConnections:     2,
		AuthorizedConnections: 2,
		ReadyConnections:      2,
		CurrentTemplateSeq:    42,
		CurrentParent:         "abc123",
	})
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal status event: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal status event: %v", err)
	}
	if decoded["event"] != poolStatusLogEventName {
		t.Fatalf("event = %v, want %s", decoded["event"], poolStatusLogEventName)
	}
	if decoded["schema"] != "bdag.pool.status.v1" {
		t.Fatalf("schema = %v", decoded["schema"])
	}
	status, ok := decoded["pool_status"].(map[string]any)
	if !ok {
		t.Fatalf("pool_status object missing: %#v", decoded["pool_status"])
	}
	if status["status"] != "ok" || status["reason_code"] != poolJobHealthOK {
		t.Fatalf("unexpected pool status payload: %#v", status)
	}
}

func TestPoolStatusLoggerLoopEmitsOnTicksAndStops(t *testing.T) {
	ticks := make(chan time.Time)
	stop := make(chan struct{})
	done := make(chan struct{})
	emitted := make(chan time.Time, 1)

	go func() {
		defer close(done)
		runPoolStatusLoggerLoop(ticks, stop, func(now time.Time) {
			emitted <- now
		})
	}()

	now := time.Unix(2000, 0)
	ticks <- now
	select {
	case got := <-emitted:
		if !got.Equal(now) {
			t.Fatalf("emitted time = %v, want %v", got, now)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for status emit")
	}

	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("logger loop did not stop")
	}
}

func TestPoolStatusLogIntervalCanDisable(t *testing.T) {
	if got := poolStatusLogInterval(0); got != 0 {
		t.Fatalf("zero interval = %s, want disabled", got)
	}
	if got := poolStatusLogInterval(-1); got != 0 {
		t.Fatalf("negative interval = %s, want disabled", got)
	}
	if got := poolStatusLogInterval(30); got != 30*time.Second {
		t.Fatalf("interval = %s, want 30s", got)
	}
}
