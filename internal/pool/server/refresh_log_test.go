package server

import (
	"testing"
	"time"
)

func resetRefreshLogStateForTest() {
	refreshLogMu.Lock()
	defer refreshLogMu.Unlock()
	refreshLogState = map[string]templateErrorThrottleState{}
}

func TestRefreshLogKeyGroupsDynamicReasons(t *testing.T) {
	cases := map[string]string{
		"ws tip change to abc123":    "ws tip change",
		"poll tip change to def456":  "poll tip change",
		"head change to 789abc":      "head change",
		"block submission processed": "block submission processed",
		"custom refresh reason":      "custom refresh reason",
		"":                           "template refresh",
	}
	for input, expected := range cases {
		if got := refreshLogKey(input); got != expected {
			t.Fatalf("refreshLogKey(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestShouldLogRefreshThrottlesRoutineRefreshes(t *testing.T) {
	resetRefreshLogStateForTest()
	start := time.Unix(100, 0)
	interval := 5 * time.Second

	if ok, suppressed := shouldLogRefresh("ws tip change to aaa", start, interval); !ok || suppressed != 0 {
		t.Fatalf("first refresh should log without suppressed count, got ok=%v suppressed=%d", ok, suppressed)
	}
	if ok, suppressed := shouldLogRefresh("ws tip change to bbb", start.Add(time.Second), interval); ok || suppressed != 0 {
		t.Fatalf("second refresh within interval should be suppressed, got ok=%v suppressed=%d", ok, suppressed)
	}
	if ok, suppressed := shouldLogRefresh("ws tip change to ccc", start.Add(6*time.Second), interval); !ok || suppressed != 1 {
		t.Fatalf("refresh after interval should log with one suppressed item, got ok=%v suppressed=%d", ok, suppressed)
	}
}

func TestShouldLogRefreshKeepsErrorsImmediate(t *testing.T) {
	resetRefreshLogStateForTest()
	start := time.Unix(100, 0)
	interval := time.Hour

	if ok, suppressed := shouldLogRefresh("block submit error", start, interval); !ok || suppressed != 0 {
		t.Fatalf("refresh errors should log immediately, got ok=%v suppressed=%d", ok, suppressed)
	}
	if ok, suppressed := shouldLogRefresh("block submit error", start.Add(time.Second), interval); !ok || suppressed != 0 {
		t.Fatalf("repeated refresh errors should not be throttled, got ok=%v suppressed=%d", ok, suppressed)
	}
}
