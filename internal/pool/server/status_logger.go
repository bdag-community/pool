package server

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const poolStatusLogEventName = "pool_status"

type poolStatusLogEvent struct {
	Event      string                `json:"event"`
	Schema     string                `json:"schema"`
	Timestamp  string                `json:"timestamp"`
	PoolStatus poolJobHealthSnapshot `json:"pool_status"`
}

func startPoolStatusLogger(intervalSeconds int, stop <-chan struct{}) {
	interval := poolStatusLogInterval(intervalSeconds)
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		emitPoolStatusLog(time.Now())
		runPoolStatusLoggerLoop(ticker.C, stop, emitPoolStatusLog)
	}()
}

func runPoolStatusLoggerLoop(ticks <-chan time.Time, stop <-chan struct{}, emit func(time.Time)) {
	if emit == nil {
		return
	}
	for {
		select {
		case <-stop:
			return
		case now, ok := <-ticks:
			if !ok {
				return
			}
			emit(now)
		}
	}
}

func emitPoolStatusLog(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	payload := newPoolStatusLogEvent(now, snapshotPoolJobHealth(now, false))
	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, `{"event":"%s","schema":"bdag.pool.status.v1","timestamp":"%s","error":"marshal failed: %s"}`+"\n",
			poolStatusLogEventName, now.UTC().Format(time.RFC3339Nano), err)
		return
	}
	fmt.Fprintln(os.Stderr, string(data))
}

func newPoolStatusLogEvent(now time.Time, snapshot poolJobHealthSnapshot) poolStatusLogEvent {
	if now.IsZero() {
		now = time.Now()
	}
	return poolStatusLogEvent{
		Event:      poolStatusLogEventName,
		Schema:     "bdag.pool.status.v1",
		Timestamp:  now.UTC().Format(time.RFC3339Nano),
		PoolStatus: snapshot,
	}
}

func poolStatusLogInterval(intervalSeconds int) time.Duration {
	if intervalSeconds <= 0 {
		return 0
	}
	return time.Duration(intervalSeconds) * time.Second
}
