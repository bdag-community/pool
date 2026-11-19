package storage

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage defines the interface for persistence
type Storage interface {
	Connect(ctx context.Context) error
	Close()
	AddBlock(ctx context.Context, hash string, height uint64, reward, fees uint64) error
	RecordBlockSubmission(ctx context.Context, submission BlockSubmission) error
	AddCredits(ctx context.Context, blockHash string, credits map[string]uint64) error
	AddMiner(ctx context.Context, address string) error
	// GetMatureBlocks fetches MATURE blocks at or below maxHeight.
	GetMatureBlocks(ctx context.Context, maxHeight uint64) ([]Block, error)
	// GetPendingBlocks fetches PENDING blocks at or below maxHeight.
	GetPendingBlocks(ctx context.Context, maxHeight uint64) ([]Block, error)
	UpdateBlockStatus(ctx context.Context, hash string, status string) error

	GetUnpaidCredits(ctx context.Context, blockHash string) ([]Credit, error)
	MarkCreditPaid(ctx context.Context, creditID int, txHash string, amountWei *big.Int, minerAddr string) error
}

type Block struct {
	Hash   string
	Height uint64
	Status string
}

type BlockSubmission struct {
	CandidateHash string
	NodeBlockHash string
	Height        int64
	BackendName   string
	TemplateSeq   int64
	Accepted      bool
	Outcome       string
	Message       string
}

type Credit struct {
	ID           int
	MinerAddress string
	// Amount is canonical unsigned decimal Wei.  Numeric(30,0) must never be
	// scanned through uint64: V2 credits can legitimately exceed it.
	Amount string
}

// PostgresStorage implements Storage using pgx
type PostgresStorage struct {
	dbURL                  string
	pool                   *pgxpool.Pool
	jobRecovery            atomic.Pointer[jobRecoveryAllowanceV1]
	discoveryRecovery      atomic.Pointer[discoveryRecoveryAllowanceV1]
	unpricedSubmissionHold atomic.Pointer[[16]byte]
}

func NewPostgresStorage(url string) *PostgresStorage {
	return &PostgresStorage{
		dbURL: url,
	}
}

func (s *PostgresStorage) Connect(ctx context.Context) error {
	s.jobRecovery.Store(nil)
	s.discoveryRecovery.Store(nil)
	s.unpricedSubmissionHold.Store(nil)
	var err error
	s.pool, err = pgxpool.New(ctx, s.dbURL)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("unable to ping database: %w", err)
	}

	log.Println("✅ Connected to PostgreSQL")
	return nil
}

func (s *PostgresStorage) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *PostgresStorage) AddBlock(ctx context.Context, hash string, height uint64, reward, fees uint64) error {
	query := `
		INSERT INTO blocks (hash, height, reward, fees, status) 
		VALUES ($1, $2, $3, $4, 'PENDING')
		ON CONFLICT (hash) DO NOTHING
	`
	_, err := s.pool.Exec(ctx, query, hash, height, reward, fees)
	if err != nil {
		return fmt.Errorf("AddBlock error: %w", err)
	}
	return nil
}

func (s *PostgresStorage) RecordBlockSubmission(ctx context.Context, submission BlockSubmission) error {
	query, args := buildRecordBlockSubmissionInsert(submission)
	_, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("RecordBlockSubmission error: %w", err)
	}
	return nil
}

func buildRecordBlockSubmissionInsert(submission BlockSubmission) (string, []any) {
	return `INSERT INTO block_submissions (` +
			`candidate_hash, node_block_hash, height, backend, template_seq, accepted, outcome, message` +
			`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		[]any{
			submission.CandidateHash,
			submission.NodeBlockHash,
			submission.Height,
			submission.BackendName,
			submission.TemplateSeq,
			submission.Accepted,
			submission.Outcome,
			submission.Message,
		}
}

func (s *PostgresStorage) GetMatureBlocks(ctx context.Context, maxHeight uint64) ([]Block, error) {
	return s.getBlocksByStatusUpToHeight(ctx, "MATURE", maxHeight)
}

func (s *PostgresStorage) GetPendingBlocks(ctx context.Context, maxHeight uint64) ([]Block, error) {
	return s.getBlocksByStatusUpToHeight(ctx, "PENDING", maxHeight)
}

func (s *PostgresStorage) getBlocksByStatusUpToHeight(ctx context.Context, status string, maxHeight uint64) ([]Block, error) {
	query := `SELECT hash, height, status FROM blocks WHERE status = $1 AND height <= $2`
	rows, err := s.pool.Query(ctx, query, status, maxHeight)
	if err != nil {
		return nil, err
	}
	return scanBlocks(rows)
}

func scanBlocks(rows pgx.Rows) ([]Block, error) {
	defer rows.Close()
	var blocks []Block
	for rows.Next() {
		var b Block
		if err := rows.Scan(&b.Hash, &b.Height, &b.Status); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return blocks, nil
}

func (s *PostgresStorage) UpdateBlockStatus(ctx context.Context, hash string, status string) error {
	query := `UPDATE blocks SET status = $1 WHERE hash = $2`
	_, err := s.pool.Exec(ctx, query, status, hash)
	return err
}

func (s *PostgresStorage) AddCredits(ctx context.Context, blockHash string, credits map[string]uint64) error {
	if len(credits) == 0 {
		return nil
	}

	// Use a transaction for bulk insert
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	query, args := buildAddCreditsInsert(blockHash, credits)
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to insert credits for block %s: %w", blockHash, err)
	}

	return tx.Commit(ctx)
}

func buildAddCreditsInsert(blockHash string, credits map[string]uint64) (string, []any) {
	miners := make([]string, 0, len(credits))
	for miner := range credits {
		miners = append(miners, miner)
	}
	sort.Strings(miners)

	args := make([]any, 1, 1+len(miners)*2)
	args[0] = blockHash
	var query strings.Builder
	query.Grow(64 + len(miners)*18)
	query.WriteString(`INSERT INTO credits (block_hash, miner_address, amount) VALUES `)

	for i, miner := range miners {
		if i > 0 {
			query.WriteByte(',')
		}

		base := i*2 + 2
		query.WriteString("($1,$")
		query.WriteString(strconv.Itoa(base))
		query.WriteString(",$")
		query.WriteString(strconv.Itoa(base + 1))
		query.WriteByte(')')

		args = append(args, miner, credits[miner])
	}

	return query.String(), args
}

func (s *PostgresStorage) AddMiner(ctx context.Context, address string) error {
	query := `
		INSERT INTO miners (address, last_active) 
		VALUES ($1, NOW())
		ON CONFLICT (address) 
		DO UPDATE SET last_active = NOW()
	`
	_, err := s.pool.Exec(ctx, query, address)
	if err != nil {
		return fmt.Errorf("AddMiner error: %w", err)
	}
	return nil
}

func (s *PostgresStorage) GetUnpaidCredits(ctx context.Context, blockHash string) ([]Credit, error) {
	query := `SELECT id, miner_address, amount::text FROM credits WHERE block_hash = $1 AND is_paid = FALSE`
	rows, err := s.pool.Query(ctx, query, blockHash)
	if err != nil {
		return nil, err
	}
	return scanCredits(rows)
}

func scanCredits(rows pgx.Rows) ([]Credit, error) {
	defer rows.Close()
	var credits []Credit
	for rows.Next() {
		var c Credit
		if err := rows.Scan(&c.ID, &c.MinerAddress, &c.Amount); err != nil {
			return nil, err
		}
		if _, err := canonicalWei(c.Amount); err != nil {
			return nil, fmt.Errorf("invalid stored credit %d: %w", c.ID, err)
		}
		credits = append(credits, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return credits, nil
}

func (s *PostgresStorage) MarkCreditPaid(ctx context.Context, creditID int, txHash string, amountWei *big.Int, minerAddr string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin MarkCreditPaid transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := markCreditPaidInTx(ctx, tx, creditID, txHash, amountWei, minerAddr); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit MarkCreditPaid transaction: %w", err)
	}
	return nil
}

type creditPaidExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func markCreditPaidInTx(ctx context.Context, tx creditPaidExecer, creditID int, txHash string, amountWei *big.Int, minerAddr string) error {
	query := `UPDATE credits SET is_paid = TRUE WHERE id = $1`
	if _, err := tx.Exec(ctx, query, creditID); err != nil {
		return fmt.Errorf("failed to update credit status: %w", err)
	}

	insertSimple := `
		INSERT INTO payouts (tx_hash, amount, created_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (tx_hash) DO NOTHING
	`
	if _, err := tx.Exec(ctx, insertSimple, txHash, amountWei.String()); err != nil {
		return fmt.Errorf("failed to record payout: %w", err)
	}
	return nil
}
