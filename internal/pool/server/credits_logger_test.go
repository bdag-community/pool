package server

import (
	"testing"
	"time"
)

func resetCreditDetailLogState() {
	creditLogMu.Lock()
	defer creditLogMu.Unlock()
	creditDetailLogs = creditDetailLogState{}
}

func resetDatabaseSuccessLogState() {
	creditLogMu.Lock()
	defer creditLogMu.Unlock()
	databaseSuccessLog = creditDetailLogState{}
}

func TestShouldLogCreditDetailsThrottles(t *testing.T) {
	resetCreditDetailLogState()
	now := time.Unix(100, 0)
	interval := time.Minute

	shouldLog, suppressed := shouldLogCreditDetails(now, interval)
	if !shouldLog || suppressed != 0 {
		t.Fatalf("first credit detail log = %v/%d, want true/0", shouldLog, suppressed)
	}
	if shouldLog, _ := shouldLogCreditDetails(now.Add(time.Second), interval); shouldLog {
		t.Fatal("second credit detail log should be suppressed inside interval")
	}
	if shouldLog, _ := shouldLogCreditDetails(now.Add(2*time.Second), interval); shouldLog {
		t.Fatal("third credit detail log should be suppressed inside interval")
	}

	shouldLog, suppressed = shouldLogCreditDetails(now.Add(interval+time.Second), interval)
	if !shouldLog || suppressed != 2 {
		t.Fatalf("later credit detail log = %v/%d, want true/2", shouldLog, suppressed)
	}
}

func TestShouldLogCreditDetailsCanBeDisabled(t *testing.T) {
	resetCreditDetailLogState()
	if shouldLog, suppressed := shouldLogCreditDetails(time.Unix(100, 0), 0); shouldLog || suppressed != 0 {
		t.Fatalf("disabled credit detail log = %v/%d, want false/0", shouldLog, suppressed)
	}
}

func TestShouldLogDatabaseSuccessThrottles(t *testing.T) {
	resetDatabaseSuccessLogState()
	now := time.Unix(200, 0)
	interval := time.Minute

	shouldLog, suppressed := shouldLogDatabaseSuccess(now, interval)
	if !shouldLog || suppressed != 0 {
		t.Fatalf("first database success log = %v/%d, want true/0", shouldLog, suppressed)
	}
	if shouldLog, _ := shouldLogDatabaseSuccess(now.Add(time.Second), interval); shouldLog {
		t.Fatal("second database success log should be suppressed inside interval")
	}
	if shouldLog, _ := shouldLogDatabaseSuccess(now.Add(2*time.Second), interval); shouldLog {
		t.Fatal("third database success log should be suppressed inside interval")
	}

	shouldLog, suppressed = shouldLogDatabaseSuccess(now.Add(interval+time.Second), interval)
	if !shouldLog || suppressed != 2 {
		t.Fatalf("later database success log = %v/%d, want true/2", shouldLog, suppressed)
	}
}

func TestShouldLogDatabaseSuccessCanBeDisabled(t *testing.T) {
	resetDatabaseSuccessLogState()
	if shouldLog, suppressed := shouldLogDatabaseSuccess(time.Unix(200, 0), 0); shouldLog || suppressed != 0 {
		t.Fatalf("disabled database success log = %v/%d, want false/0", shouldLog, suppressed)
	}
}
