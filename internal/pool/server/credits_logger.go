package server

import (
	"asicPool/internal/pool/pplns"
	"asicPool/internal/pool/storage"
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const defaultCreditDetailLogInterval = 5 * time.Minute
const defaultDatabaseSuccessLogInterval = time.Minute

type creditDetailLogState struct {
	lastLog    time.Time
	suppressed int
}

var (
	creditDetailLogs   creditDetailLogState
	databaseSuccessLog creditDetailLogState
	creditLogMu        sync.Mutex
)

func logCredits(db storage.Storage, snapshot map[string]uint64, totalWork uint64, reward uint64, blockHash string, blockHeight uint64, maturityDepth int, payoutDepth int, poolFeePct float64) {
	// Calculate Pool Fee
	feeAmount := uint64(float64(reward) * (poolFeePct / 100.0))
	distributable := reward - feeAmount

	// Persist Block (if DB is connected)
	if db != nil {
		ctx := context.Background()
		if err := db.AddBlock(ctx, blockHash, blockHeight, reward, feeAmount); err != nil {
			log.Printf("❌ DB AddBlock Failed: %v", err)
		}
	}

	credits := pplns.CalculateCredits(snapshot, totalWork, distributable)

	// Persist Credits
	if db != nil {
		ctx := context.Background()
		if err := db.AddCredits(ctx, blockHash, credits); err != nil {
			log.Printf("❌ DB AddCredits Failed: %v", err)
		} else {
			if shouldLog, suppressed := shouldLogDatabaseSuccess(time.Now(), databaseSuccessLogInterval()); shouldLog {
				log.Printf("✅ Block & Credits saved to Database suppressed=%d", suppressed)
			}
		}
	}

	matureHeight := blockHeight + uint64(maturityDepth)
	payoutUnlockHeight := blockHeight + uint64(payoutDepth)
	shouldDetail, suppressedDetails := shouldLogCreditDetails(time.Now(), creditDetailLogInterval())

	log.Printf(
		"💰 CREDIT SUMMARY block=%s height=%d totalWork=%d accounts=%d reward=%d poolFeePct=%.2f fee=%d distributable=%d matureHeight=%d payoutUnlockHeight=%d suppressedDetailBlocks=%d",
		blockHash,
		blockHeight,
		totalWork,
		len(snapshot),
		reward,
		poolFeePct,
		feeAmount,
		distributable,
		matureHeight,
		payoutUnlockHeight,
		suppressedDetails,
	)
	if !shouldDetail {
		return
	}

	log.Println("--- BLOCK CREDITS (PENDING) ---")
	log.Printf("   (Credits mature at height %d, Payout unlock at height %d)", matureHeight, payoutUnlockHeight)

	for acc, credit := range credits {
		log.Printf("  Miner %s: %d (%.4f%%)", acc, credit, float64(credit)/float64(distributable)*100.0)
	}
	log.Println("-----------------------------")
}

func creditDetailLogInterval() time.Duration {
	raw := os.Getenv("POOL_CREDIT_DETAIL_LOG_INTERVAL_SECONDS")
	if raw == "" {
		return defaultCreditDetailLogInterval
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return defaultCreditDetailLogInterval
	}
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func databaseSuccessLogInterval() time.Duration {
	raw := os.Getenv("POOL_DB_SUCCESS_LOG_INTERVAL_SECONDS")
	if raw == "" {
		return defaultDatabaseSuccessLogInterval
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return defaultDatabaseSuccessLogInterval
	}
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func shouldLogCreditDetails(now time.Time, interval time.Duration) (bool, int) {
	if interval <= 0 {
		return false, 0
	}
	if now.IsZero() {
		now = time.Now()
	}

	creditLogMu.Lock()
	defer creditLogMu.Unlock()

	creditDetailLogs.suppressed++
	if creditDetailLogs.lastLog.IsZero() || now.Sub(creditDetailLogs.lastLog) >= interval {
		suppressed := creditDetailLogs.suppressed - 1
		creditDetailLogs.suppressed = 0
		creditDetailLogs.lastLog = now
		return true, suppressed
	}
	return false, 0
}

func shouldLogDatabaseSuccess(now time.Time, interval time.Duration) (bool, int) {
	if interval <= 0 {
		return false, 0
	}
	if now.IsZero() {
		now = time.Now()
	}

	creditLogMu.Lock()
	defer creditLogMu.Unlock()

	databaseSuccessLog.suppressed++
	if databaseSuccessLog.lastLog.IsZero() || now.Sub(databaseSuccessLog.lastLog) >= interval {
		suppressed := databaseSuccessLog.suppressed - 1
		databaseSuccessLog.suppressed = 0
		databaseSuccessLog.lastLog = now
		return true, suppressed
	}
	return false, 0
}
