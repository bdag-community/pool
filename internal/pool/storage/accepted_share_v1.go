package storage

import (
	"context"
	"errors"

	"asicPool/internal/pool/sharejournal"
)

// AppendAcceptedShareV1 records a share after the submitter has checked its
// proof, target, and authorized miner. The returned sequence is durable only
// when err is nil; a failed commit acknowledgement must be retried with the
// same share identity and inputs.
func (s *PostgresStorage) AppendAcceptedShareV1(ctx context.Context, share sharejournal.Share) (int64, bool, error) {
	if s == nil || s.pool == nil {
		return 0, false, errors.New("accepted share database unavailable")
	}
	journal, err := sharejournal.New(s.pool)
	if err != nil {
		return 0, false, err
	}
	return journal.Append(ctx, share)
}
