package server

import (
	"context"
	"errors"
	"math"

	"asicPool/internal/pool/pplns"
	"asicPool/internal/pool/sharejournal"
	"asicPool/internal/pool/storage"
)

// This path is installed only when startup has qualified its schema, reward
// source and reconciler. The existing live server does not install it.
type durableShareJournal interface {
	AppendAcceptedShareV1(context.Context, sharejournal.Share) (int64, bool, error)
	PutUnpricedDiscoveryV1(context.Context, storage.FrozenCandidateV1, *sharejournal.Share, uint64, sharejournal.BoundaryPolicy, float64) (storage.UnpricedDiscoveryV1, storage.PutDisposition, error)
}

type durableShareAccounting struct {
	journal durableShareJournal
	target  uint64
	policy  sharejournal.BoundaryPolicy
}

func acceptedShareWork(difficulty float64) (uint64, error) {
	if math.IsNaN(difficulty) || math.IsInf(difficulty, 0) || difficulty <= 0 {
		return 0, errors.New("invalid accepted share difficulty")
	}
	scaled := difficulty * pplns.PPLNS_SCALE
	if math.IsInf(scaled, 0) || scaled < 1 || scaled >= math.Exp2(64) {
		return 0, errors.New("accepted share work is outside the representable range")
	}
	return uint64(scaled), nil
}
