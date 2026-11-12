package main

import (
	"testing"
	"time"
)

func TestOfflineVerificationAllowsFullHistoryOnSlowHost(t *testing.T) {
	// The real stopped Node 2 database exceeded two minutes during strict
	// history verification. Allow it to finish without skipping any checks.
	if offlineRecoveryTimeout != 10*time.Minute {
		t.Fatalf("offline verification budget = %s, want bounded 10m", offlineRecoveryTimeout)
	}
}
