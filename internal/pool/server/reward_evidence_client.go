package server

import (
	"bytes"
	"context"
	"errors"
	"log"
	"math/big"
	"time"

	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
)

type rewardEvidenceRPC interface {
	CallContext(context.Context, interface{}, string, ...interface{}) error
}

type rewardEvidenceSink interface {
	RecordRewardObservationV1(context.Context, [32]byte, common.Address, storage.RewardObservationV1) (bool, error)
}

type rewardEvidenceStore interface {
	rewardEvidenceSink
	NextRewardObservationCandidateExcludingV1(context.Context, [][32]byte) (*storage.FrozenCandidateV1, error)
}

type rewardEvidenceCollector struct {
	node   rewardEvidenceRPC
	store  rewardEvidenceStore
	wallet common.Address
	held   map[[32]byte]rewardEvidenceHold
}

type rewardEvidenceHold struct {
	until    time.Time
	failures uint
}

// captureNext can be called by a bounded background worker. The candidate
// remains pending if the node or database cannot complete the observation.
func (c *rewardEvidenceCollector) captureNext(ctx context.Context) (bool, error) {
	if c == nil || c.node == nil || c.store == nil || c.wallet == (common.Address{}) {
		return false, errors.New("reward evidence collector unavailable")
	}
	excluded := make([][32]byte, 0, len(c.held))
	now := time.Now()
	for id, hold := range c.held {
		if now.Before(hold.until) {
			excluded = append(excluded, id)
		}
	}
	candidate, err := c.store.NextRewardObservationCandidateExcludingV1(ctx, excluded)
	if err != nil {
		return false, err
	}
	if candidate == nil {
		return false, nil
	}
	if err = captureRewardEvidence(ctx, c.node, c.store, *candidate, c.wallet); err != nil {
		if c.held == nil {
			c.held = make(map[[32]byte]rewardEvidenceHold)
		}
		hold := c.held[candidate.CandidateID]
		if hold.failures < 8 {
			hold.failures++
		}
		hold.until = time.Now().Add(5 * time.Second * time.Duration(1<<(hold.failures-1)))
		c.held[candidate.CandidateID] = hold
		return false, err
	}
	delete(c.held, candidate.CandidateID)
	return true, nil
}

// run is a single bounded collector lane. Accepted candidates remain in the
// database until observation succeeds, allowing a later restart to retry.
func (c *rewardEvidenceCollector) run(ctx context.Context) {
	if ctx == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastLog time.Time
	var suppressed uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := c.captureNext(attempt)
			cancel()
			if err != nil && ctx.Err() == nil {
				if lastLog.IsZero() || time.Since(lastLog) >= 30*time.Second {
					log.Printf("pool reward observation pending: %v (suppressed=%d)", err, suppressed)
					lastLog, suppressed = time.Now(), 0
				} else {
					suppressed++
				}
			}
		}
	}
}

// rewardEvidence is an observed replay, not payout authorization. The native
// and EVM identities must be rechecked for maturity and canonicality before
// allocating or paying any of these amounts.
type rewardEvidence = storage.RewardObservationV1

func readRewardEvidence(ctx context.Context, node rewardEvidenceRPC, candidate storage.FrozenCandidateV1, wallet common.Address) (rewardEvidence, error) {
	var result rewardEvidence
	if ctx == nil || ctx.Err() != nil || node == nil || wallet == (common.Address{}) || candidate.ChainID == nil || candidate.ChainID.Cmp(big.NewInt(1404)) != 0 || candidate.Height == nil || !candidate.Height.IsUint64() || !bytes.Equal(candidate.HeaderBytes[148:168], wallet[:]) {
		return result, errors.New("reward evidence request identity invalid")
	}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("reward evidence requires a deadline")
	}
	if err := node.CallContext(ctx, &result, "bdag_getPoolRewardEvidence", common.Hash(candidate.ExpectedBlockHash), wallet); err != nil {
		return rewardEvidence{}, err
	}
	if result.NativeBlockHash != common.Hash(candidate.ExpectedBlockHash) || result.NativeHeight != candidate.Height.Uint64() || result.NativeOrder == 0 || result.EVMBlockHash == (common.Hash{}) || result.EVMStateRoot == (common.Hash{}) || result.EVMBlockNumber == 0 || result.Wallet != wallet {
		return rewardEvidence{}, errors.New("reward evidence does not match candidate")
	}
	total := new(big.Int)
	for _, raw := range []string{result.SubsidyWei, result.OtherFeesWei, result.SelfPaidFeesWei} {
		value, ok := new(big.Int).SetString(raw, 10)
		if !ok || value.Sign() < 0 || value.String() != raw || len(raw) > 30 {
			return rewardEvidence{}, errors.New("reward evidence amount is invalid")
		}
		total.Add(total, value)
	}
	if len(total.String()) > 30 {
		return rewardEvidence{}, errors.New("reward evidence total is outside accounting range")
	}
	return result, nil
}

// captureRewardEvidence stores a matched node observation without creating any
// credit. The caller is responsible for authenticating the local IPC endpoint
// and retrying accepted candidates after restart.
func captureRewardEvidence(ctx context.Context, node rewardEvidenceRPC, sink rewardEvidenceSink, candidate storage.FrozenCandidateV1, wallet common.Address) error {
	if sink == nil {
		return errors.New("reward evidence storage unavailable")
	}
	observation, err := readRewardEvidence(ctx, node, candidate, wallet)
	if err != nil {
		return err
	}
	_, err = sink.RecordRewardObservationV1(ctx, candidate.CandidateID, wallet, observation)
	return err
}
