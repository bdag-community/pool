package sharejournal

import "errors"

type BoundaryPolicy string

const (
	// WholeShare matches the existing pool's strict FIFO eviction behavior.
	WholeShare BoundaryPolicy = "whole-share"
	// PartialShare credits the fraction of boundary work inside the window.
	PartialShare BoundaryPolicy = "partial-share"
)

// AllocateWindow groups a chronological durable cut by payout address.
// The caller must explicitly select the financial policy; there is no default.
func AllocateWindow(window []Share, target uint64, policy BoundaryPolicy) (map[string]uint64, uint64, error) {
	if target == 0 || (policy != WholeShare && policy != PartialShare) {
		return nil, 0, errors.New("explicit share-window boundary policy and work target are required")
	}
	seen := make(map[[32]byte]bool, len(window))
	var previous int64
	for _, share := range window {
		if share.Sequence <= previous || seen[share.ID] {
			return nil, 0, errors.New("share window identity or ordering is invalid")
		}
		check := share
		check.Sequence = 0
		normalized, err := normalize(check)
		if err != nil || normalized.Miner != share.Miner {
			return nil, 0, errors.New("share window contains invalid miner or work")
		}
		seen[share.ID] = true
		previous = share.Sequence
	}
	result := make(map[string]uint64)
	var total uint64
	for i := len(window) - 1; i >= 0; i-- {
		share := window[i]
		remaining := target - total
		if share.Work > remaining {
			if policy == PartialShare && remaining > 0 {
				result[share.Miner] += remaining
				total += remaining
			}
			break
		}
		result[share.Miner] += share.Work
		total += share.Work
		if total == target {
			break
		}
	}
	return result, total, nil
}
