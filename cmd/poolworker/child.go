package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// child supervises the mining-pool process.
//
// The pool installs a SIGTERM handler that closes its stratum listener and then
// drops client connections, so a signalled stop lets ASICs reconnect on their
// own schedule instead of hanging on a dead socket. That is why we always try
// SIGTERM first and only escalate to SIGKILL after a grace period — an external
// `docker restart` cannot make that distinction.
type child struct {
	binary string
	args   []string

	mu sync.Mutex
	// cmd is nil when no child is running.
	cmd *exec.Cmd
	// done is closed once the child has exited. A closed channel rather than a
	// value-carrying one, so stop() and the supervisor's select can both observe
	// the exit; a single-value channel would let one of them consume it and
	// block the other forever.
	done chan struct{}
	// exitErr is valid only after done is closed.
	exitErr error
}

func newChild(binary string, args []string) *child {
	return &child{binary: binary, args: args}
}

func (c *child) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd != nil {
		return errors.New("child already running")
	}

	cmd := exec.Command(c.binary, c.args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Own process group, so a SIGKILL escalation reaps the whole tree rather
	// than orphaning anything the pool spawned.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", c.binary, err)
	}

	done := make(chan struct{})
	c.cmd, c.done, c.exitErr = cmd, done, nil

	go func() {
		err := cmd.Wait()
		c.mu.Lock()
		c.exitErr = err
		c.mu.Unlock()
		close(done)
	}()

	log.Printf("[poolworker] child started binary=%s pid=%d", c.binary, cmd.Process.Pid)
	return nil
}

// exited returns a channel closed when the current child exits. It returns nil
// when no child is running, which blocks forever in a select — the correct
// behaviour for a supervisor with nothing to supervise.
func (c *child) exited() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *child) exitError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exitErr
}

// stop signals the child and waits up to grace for it to drain, escalating to
// SIGKILL on the process group if it does not. Safe to call after the child has
// already exited on its own.
func (c *child) stop(grace time.Duration) {
	c.mu.Lock()
	cmd, done := c.cmd, c.done
	c.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid

	select {
	case <-done:
		// Already exited; nothing to signal.
	default:
		log.Printf("[poolworker] stopping child pid=%d (SIGTERM, grace=%s)", pid, grace)
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			log.Printf("[poolworker] SIGTERM failed pid=%d: %v", pid, err)
		}
		select {
		case <-done:
			log.Printf("[poolworker] child drained and exited pid=%d", pid)
		case <-time.After(grace):
			log.Printf("[poolworker] child did not drain within %s; SIGKILL process group pid=%d", grace, pid)
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			<-done
		}
	}

	c.mu.Lock()
	c.cmd = nil
	c.mu.Unlock()
}
