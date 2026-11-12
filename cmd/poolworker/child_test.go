package main

import (
	"testing"
	"time"
)

// stop() and a supervisor selecting on exited() both observe the child's exit.
// A single-value channel would let one consume it and block the other forever,
// so the exit must be signalled by closing a channel.
func TestStopAndExitedBothObserveExit(t *testing.T) {
	c := newChild("/bin/sh", []string{"-c", "sleep 30"})
	if err := c.start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	observed := make(chan struct{})
	go func() {
		<-c.exited()
		close(observed)
	}()

	done := make(chan struct{})
	go func() {
		c.stop(5 * time.Second)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stop() blocked; the exit signal was consumed by another receiver")
	}
	select {
	case <-observed:
	case <-time.After(2 * time.Second):
		t.Fatal("a concurrent watcher never saw the exit")
	}
}

// A child that ignores SIGTERM must still be reaped, or a wedged pool would
// keep its stratum port bound and block the replacement.
func TestStopEscalatesToKillWhenSigtermIgnored(t *testing.T) {
	c := newChild("/bin/sh", []string{"-c", "trap '' TERM; sleep 30"})
	if err := c.start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	start := time.Now()
	c.stop(500 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("stop took %s; SIGKILL escalation did not fire", elapsed)
	}
	if c.cmd != nil {
		t.Fatal("child not cleared after stop")
	}
}

// stop() after the child has already exited must return, not hang.
func TestStopAfterSpontaneousExit(t *testing.T) {
	c := newChild("/bin/sh", []string{"-c", "exit 3"})
	if err := c.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-c.exited()
	if c.exitError() == nil {
		t.Fatal("expected a non-zero exit to surface as an error")
	}

	done := make(chan struct{})
	go func() { c.stop(time.Second); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop() hung on an already-exited child")
	}
}

func TestStartRejectsDoubleStart(t *testing.T) {
	c := newChild("/bin/sh", []string{"-c", "sleep 5"})
	if err := c.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.stop(time.Second)

	if err := c.start(); err == nil {
		t.Fatal("starting a second child must fail")
	}
}
