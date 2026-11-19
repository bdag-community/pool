// Package sharejournal records accepted work independently of allocation
// policy. It is not wired into the pool submit path or production migrations.
package sharejournal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxWindowRows = 100000

var ErrShareCutMissing = errors.New("accepted-share cut not found")

// The transaction-scoped lock serializes inserts across all cooperating pool
// processes so sequence order is also commit order.
const appendLock int64 = 4702976340189971

type Share struct {
	ID       [32]byte
	Miner    string
	Work     uint64
	Sequence int64
}

type Journal struct{ pool *pgxpool.Pool }

func bounded(ctx context.Context) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	_, ok := ctx.Deadline()
	return ok
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func New(pool *pgxpool.Pool) (*Journal, error) {
	if pool == nil {
		return nil, errors.New("share journal database is required")
	}
	return &Journal{pool: pool}, nil
}

func normalize(s Share) (Share, error) {
	if s.ID == ([32]byte{}) || s.Work == 0 || s.Sequence != 0 || !common.IsHexAddress(s.Miner) {
		return Share{}, errors.New("invalid accepted-share identity, miner, work or sequence")
	}
	address := common.HexToAddress(s.Miner)
	if address == (common.Address{}) {
		return Share{}, errors.New("zero miner address is not payable")
	}
	s.Miner = address.Hex()
	return s, nil
}

// Append commits before reporting success. A byte-identical retry returns the
// original sequence; reuse of an ID for different work or an owner fails.
func (j *Journal) Append(ctx context.Context, input Share) (sequence int64, created bool, err error) {
	s, err := normalize(input)
	if err != nil || j == nil || j.pool == nil || !bounded(ctx) {
		return 0, false, errors.New("invalid share or unavailable journal")
	}
	tx, err := j.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, appendLock); err != nil {
		return 0, false, err
	}
	sequence, created, err = appendTx(ctx, tx, s)
	if err != nil {
		return 0, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("accepted-share commit unproven: %w", err)
	}
	return sequence, created, nil
}

func appendTx(ctx context.Context, tx pgx.Tx, s Share) (sequence int64, created bool, err error) {
	work := strconv.FormatUint(s.Work, 10)
	err = tx.QueryRow(ctx, `INSERT INTO pool_share_journal_v1.shares(share_id,miner_address,work)
VALUES($1,$2,$3::numeric) ON CONFLICT(share_id) DO NOTHING RETURNING sequence`, s.ID[:], s.Miner, work).Scan(&sequence)
	switch {
	case err == nil:
		created = true
	case errors.Is(err, pgx.ErrNoRows):
		var id []byte
		var miner, storedWork string
		err = tx.QueryRow(ctx, `SELECT sequence,share_id,miner_address,work::text FROM pool_share_journal_v1.shares WHERE share_id=$1`, s.ID[:]).Scan(&sequence, &id, &miner, &storedWork)
		if err != nil || !bytes.Equal(id, s.ID[:]) || miner != s.Miner || storedWork != work {
			return 0, false, errors.New("accepted-share replay conflicts with committed identity")
		}
	default:
		return 0, false, err
	}
	if sequence <= 0 {
		return 0, false, errors.New("invalid accepted-share sequence")
	}
	return sequence, created, nil
}

// AppendCandidate atomically commits a valid discovery share and its fixed
// candidate cut. A conflicting retry rolls back both effects.
func (j *Journal) AppendCandidate(ctx context.Context, input Share, candidate [32]byte) (int64, bool, error) {
	s, err := normalize(input)
	if err != nil || j == nil || j.pool == nil || !bounded(ctx) || candidate == ([32]byte{}) {
		return 0, false, errors.New("invalid discovery share or candidate")
	}
	tx, err := j.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, appendLock); err != nil {
		return 0, false, err
	}
	sequence, created, err := appendTx(ctx, tx, s)
	if err != nil {
		return 0, false, err
	}
	if _, err = bindCutTx(ctx, tx, candidate, &sequence); err != nil {
		return 0, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("discovery share and cut commit unproven: %w", err)
	}
	return sequence, created, nil
}

// BindCut fixes a candidate's discovery boundary. A positive sequence must be
// a previously committed share. A nil sequence captures the latest share under
// the append lock, including an empty journal as sequence zero. Once bound,
// a retry can never move the cut forward to include later shares.
func (j *Journal) BindCut(ctx context.Context, candidate [32]byte, requested *int64) (int64, error) {
	if j == nil || j.pool == nil || !bounded(ctx) || candidate == ([32]byte{}) || (requested != nil && *requested <= 0) {
		return 0, errors.New("invalid discovery cut request")
	}
	tx, err := j.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, appendLock); err != nil {
		return 0, err
	}
	sequence, err := bindCutTx(ctx, tx, candidate, requested)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("discovery cut commit unproven: %w", err)
	}
	return sequence, nil
}

func bindCutTx(ctx context.Context, tx pgx.Tx, candidate [32]byte, requested *int64) (int64, error) {
	var sequence int64
	err := tx.QueryRow(ctx, `SELECT COALESCE(share_sequence,0) FROM pool_share_journal_v1.cuts WHERE candidate_id=$1`, candidate[:]).Scan(&sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		if requested == nil {
			if err = tx.QueryRow(ctx, `SELECT COALESCE(MAX(sequence),0) FROM pool_share_journal_v1.shares`).Scan(&sequence); err != nil {
				return 0, err
			}
		} else {
			sequence = *requested
		}
		if _, err = tx.Exec(ctx, `INSERT INTO pool_share_journal_v1.cuts(candidate_id,share_sequence) VALUES($1,NULLIF($2::bigint,0))`, candidate[:], sequence); err != nil {
			return 0, fmt.Errorf("discovery cut could not be bound: %w", err)
		}
	} else if err != nil {
		return 0, err
	} else if requested != nil && sequence != *requested {
		return 0, errors.New("discovery candidate already has a different cut")
	}
	return sequence, nil
}

// Window returns complete individual shares in chronological order, including
// the share crossing the requested work boundary. The caller must apply its
// separately approved whole-share or partial-share allocation policy. A saved
// cut sequence always selects the same immutable history after restart.
func (j *Journal) Window(ctx context.Context, cut int64, target uint64) ([]Share, error) {
	if j == nil || j.pool == nil || cut <= 0 || target == 0 || !bounded(ctx) {
		return nil, errors.New("invalid accepted-share window request")
	}
	tx, err := j.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	out, err := windowTx(ctx, tx, cut, target)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// ReadWindowTx reads a saved cut in the caller's transaction without changing
// or committing it. The caller is responsible for database identity, schema
// verification and a consistent read snapshot.
func ReadWindowTx(ctx context.Context, tx pgx.Tx, cut int64, target uint64) ([]Share, error) {
	return windowTx(ctx, tx, cut, target)
}

// windowTx also sees shares inserted by its caller's transaction. It does not
// commit, roll back, or relax the caller's transaction isolation.
func windowTx(ctx context.Context, tx pgx.Tx, cut int64, target uint64) ([]Share, error) {
	if tx == nil || !bounded(ctx) || cut <= 0 || target == 0 {
		return nil, errors.New("invalid transactional share window request")
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_share_journal_v1.shares WHERE sequence=$1)`, cut).Scan(&exists); err != nil {
		return nil, err
	} else if !exists {
		return nil, ErrShareCutMissing
	}
	var out []Share
	remaining, cursor := target, cut
	const pageSize = 512
	for cursor > 0 {
		limit := pageSize
		if left := maxWindowRows + 1 - len(out); left < limit {
			limit = left
		}
		rows, err := tx.Query(ctx, `SELECT sequence,share_id,miner_address,work::text
 FROM pool_share_journal_v1.shares WHERE sequence <= $1 ORDER BY sequence DESC LIMIT $2`, cursor, limit)
		if err != nil {
			return nil, err
		}
		count, complete := 0, false
		for rows.Next() {
			var s Share
			var id []byte
			var work string
			if err = rows.Scan(&s.Sequence, &id, &s.Miner, &work); err != nil {
				break
			}
			if len(id) != len(s.ID) {
				err = errors.New("invalid persisted share identity")
				break
			}
			copy(s.ID[:], id)
			s.Work, err = strconv.ParseUint(work, 10, 64)
			if err != nil || s.Work == 0 || s.Sequence <= 0 {
				err = errors.New("invalid persisted share work or sequence")
				break
			}
			checked := s
			checked.Sequence = 0
			checked, err = normalize(checked)
			if err != nil || checked.Miner != s.Miner {
				err = errors.New("invalid persisted share or miner identity")
				break
			}
			out = append(out, s)
			if len(out) > maxWindowRows {
				err = errors.New("accepted-share window exceeds bounded row limit")
				break
			}
			count++
			cursor = s.Sequence - 1
			if s.Work >= remaining {
				complete = true
				break
			}
			remaining -= s.Work
		}
		rows.Close()
		if err == nil {
			err = rows.Err()
		}
		if err != nil {
			return nil, err
		}
		if complete || count < limit {
			break
		}
	}
	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out, nil
}
