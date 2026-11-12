package server

import (
	"context"
	"log"
	"math/big"
	"time"

	"asicPool/internal/pool/storage"
	"asicPool/internal/pool/wallet"
)

// blockLifecycleManager runs in the background to check for block confirmations (Mature & Paid)
func blockLifecycleManager(db storage.Storage, walletClient wallet.Client, maturityDepth int, payoutDepth int) {
	log.Printf("Starting Block Lifecycle Manager (Maturity=%d, Payout=%d)", maturityDepth, payoutDepth)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if db == nil {
			continue
		}

		// 1. Get Current Chain Height
		currentHeight, err := getLifecycleChainHeight()
		if err != nil {
			log.Printf("Maturity: Failed to get chain tip: %v", err)
			continue
		}
		ctx := context.Background()

		// 2. Handle PENDING -> MATURE
		pendingMaxHeight, pendingEligible := maxEligibleBlockHeight(currentHeight, maturityDepth)
		if pendingEligible {
			pending, err := db.GetPendingBlocks(ctx, pendingMaxHeight)
			if err == nil {
				maturedCount := 0
				var firstMatureHash, lastMatureHash string
				var minConfirmations, maxConfirmations uint64
				for _, b := range pending {
					if currentHeight < b.Height {
						continue
					}
					confirmations := currentHeight - b.Height + 1
					if confirmations >= uint64(maturityDepth) {
						if err := db.UpdateBlockStatus(ctx, b.Hash, "MATURE"); err != nil {
							log.Printf("Maturity: Failed to update block %s: %v", b.Hash, err)
						} else {
							blocksMatureTotal.Inc()
							maturedCount++
							if firstMatureHash == "" {
								firstMatureHash = b.Hash
								minConfirmations = confirmations
							}
							lastMatureHash = b.Hash
							if confirmations < minConfirmations {
								minConfirmations = confirmations
							}
							if confirmations > maxConfirmations {
								maxConfirmations = confirmations
							}
						}
					}
				}
				if maturedCount > 0 {
					log.Printf(
						"✅ Maturity: marked %d blocks MATURE currentHeight=%d first=%s last=%s minConfs=%d maxConfs=%d",
						maturedCount,
						currentHeight,
						firstMatureHash,
						lastMatureHash,
						minConfirmations,
						maxConfirmations,
					)
				}
			}
		}

		// 3. Handle MATURE -> PAID
		// Only proceed if wallet is available
		if walletClient == nil {
			continue
		}

		payoutMaxHeight, payoutEligible := maxEligibleBlockHeight(currentHeight, payoutDepth)
		if !payoutEligible {
			continue
		}
		mature, err := db.GetMatureBlocks(ctx, payoutMaxHeight)
		if err == nil {
			for _, b := range mature {
				if currentHeight < b.Height {
					continue
				}
				confirmations := currentHeight - b.Height + 1
				if confirmations >= uint64(payoutDepth) {
					// Fetch credits for this block
					credits, err := db.GetUnpaidCredits(ctx, b.Hash)
					if err != nil {
						log.Printf("Payout: Failed to get credits for block %s: %v", b.Hash, err)
						continue
					}

					if len(credits) == 0 {
						// All paid or none exist, mark block as PAID
						if err := db.UpdateBlockStatus(ctx, b.Hash, "PAID"); err == nil {
							log.Printf("💰 Block %s fully PAID", b.Hash)
						}
						continue
					}

					log.Printf("💸 Processing Payouts for Block %s (%d credits)", b.Hash, len(credits))

					// 1. Proactive Balance Check
					totalReq := new(big.Int)
					validCredits := true
					for _, c := range credits {
						amount, ok := new(big.Int).SetString(c.Amount, 10)
						if !ok || amount.Sign() < 0 {
							log.Printf("Payout: invalid stored credit %d", c.ID)
							validCredits = false
							break
						}
						totalReq.Add(totalReq, amount)
					}
					if !validCredits {
						continue
					}

					balance, err := walletClient.GetBalance(ctx)
					if err != nil {
						log.Printf("Payout: Failed to get wallet balance: %v", err)
						continue
					}

					if balance.Cmp(totalReq) < 0 {
						log.Printf("⚠️ [Payout Paused] Insufficient Wallet Balance. Have %s wei, Need %s wei. Waiting...", balance.String(), totalReq.String())
						continue
					}

					allPaid := true
					for _, c := range credits {
						// Amount is already in Wei (stored by handler.go)
						amountWei, ok := new(big.Int).SetString(c.Amount, 10)
						if !ok || amountWei.Sign() < 0 {
							log.Printf("Payout: invalid stored credit %d", c.ID)
							allPaid = false
							break
						}
						amountWei = new(big.Int).Set(amountWei)
						// No extra scaling needed.

						txID, err := walletClient.SendTransaction(ctx, c.MinerAddress, amountWei)
						if err != nil {
							payoutFailures.Inc()
							log.Printf("❌ Payout Failed for %s: %v", c.MinerAddress, err)
							allPaid = false
							log.Printf("⚠️ Payout loop aborted to prevent mempool overflow. Will retry in next tick.")
							break
						}

						payoutsTotal.Inc()
						payoutAmountWei.Add(0) // metric is observability only; never a payment value
						log.Printf("  Sent %s to %s (Tx: %s)", amountWei.String(), c.MinerAddress, txID)
						if err := db.MarkCreditPaid(ctx, c.ID, txID, amountWei, c.MinerAddress); err != nil {
							payoutFailures.Inc()
							log.Printf("❌ Payout DB update failed for credit=%d miner=%s tx=%s: %v", c.ID, c.MinerAddress, txID, err)
							allPaid = false
							log.Printf("⚠️ Payout loop aborted to prevent marking block paid with unrecorded credit state. Will retry in next tick.")
							break
						}
					}

					if !allPaid {
						// If we aborted or failed, stop processing subsequent blocks in this tick too
						break
					}

					if allPaid {
						if err := db.UpdateBlockStatus(ctx, b.Hash, "PAID"); err != nil {
							log.Printf("Payout: Failed to update block status %s: %v", b.Hash, err)
						} else {
							blocksPaidTotal.Inc()
							log.Printf("💰 Block %s marked as PAID", b.Hash)
						}
					}
				}
			}
		}
	}
}

func maxEligibleBlockHeight(currentHeight uint64, requiredConfirmations int) (uint64, bool) {
	if requiredConfirmations <= 1 {
		return currentHeight, true
	}
	required := uint64(requiredConfirmations)
	if currentHeight < required-1 {
		return 0, false
	}
	return currentHeight - required + 1, true
}
