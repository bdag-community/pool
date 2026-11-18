package payments

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var MigrationSQL string

// Qualified against the exact fsync-enabled PostgreSQL15 migration artifact.
// There is deliberately no environment/configuration override for this value.
const LegacyReviewedSchemaFingerprint = "000be7584d3bd5ab8563c06de766f5a1d781db9edae90baf8e1fd1ad3d3ea7e2"
const ReviewedSchemaFingerprint = "9544d04d9018fb21f7c7332a438ad7bf1a40681a5fbbcb3c25ff7fb08015eed1"

func NewReviewedPostgresJournal(pool *pgxpool.Pool) (*PostgresJournal, error) {
	return NewPostgresJournal(pool, ReviewedSchemaFingerprint)
}

type catalogQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Begin(context.Context) (pgx.Tx, error)
}

// CatalogFingerprint contains structure, not changing table/sequence contents.
// The expected value MUST come from the reviewed isolated migration artifact,
// compiled into the runtime integration, never from a live DB metadata row.
func CatalogFingerprint(ctx context.Context, q catalogQuerier) (string, error) {
	tx, err := q.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL search_path=pg_catalog`); err != nil {
		return "", err
	}
	var descriptor []byte
	err = tx.QueryRow(ctx, `SELECT json_build_object(
 'owner_ok',(SELECT nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) FROM pg_namespace WHERE nspname='pool_payments_v1'),
 'relations',(SELECT json_agg(x ORDER BY x.name) FROM (SELECT c.relname name,c.relkind::text kind,c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) owner_ok FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_payments_v1') x),
 'columns',(SELECT json_agg(x ORDER BY x.tbl,x.pos) FROM (SELECT c.relname tbl,a.attnum pos,a.attname name,format_type(a.atttypid,a.atttypmod) typ,a.attnotnull nn,COALESCE(pg_get_expr(d.adbin,d.adrelid),'') def FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE n.nspname='pool_payments_v1' AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped) x),
 'constraints',(SELECT json_agg(x ORDER BY x.tbl,x.name) FROM (SELECT c.relname tbl,k.conname name,pg_get_constraintdef(k.oid,true) def,k.convalidated valid FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_payments_v1') x),
 'indexes',(SELECT json_agg(x ORDER BY x.indexname) FROM (SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='pool_payments_v1') x),
 'triggers',(SELECT json_agg(x ORDER BY x.tbl,x.name) FROM (SELECT c.relname tbl,t.tgname name,t.tgenabled::text enabled,pg_get_triggerdef(t.oid,true) def FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_payments_v1' AND NOT t.tgisinternal) x),
 'functions',(SELECT json_agg(x ORDER BY x.name) FROM (SELECT p.proname name,p.prosrc body,p.provolatile::text volatility,p.prosecdef security_definer,p.proconfig config,pg_get_function_identity_arguments(p.oid) args,pg_get_function_result(p.oid) result,p.proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) owner_ok FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pool_payments_v1') x),
 'sequences',(SELECT json_agg(x ORDER BY x.name) FROM (SELECT c.relname name,format_type(s.seqtypid,NULL) typ,s.seqstart,s.seqincrement,s.seqmax,s.seqmin,s.seqcache,s.seqcycle FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_payments_v1') x)
)::text`).Scan(&descriptor)
	if err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	sum := sha256.Sum256(descriptor)
	return hex.EncodeToString(sum[:]), nil
}

// ApplyMigration is an explicit coordinator operation, not a startup action.
// Existing namespaces are never overwritten or silently adopted.
func ApplyMigration(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT to_regnamespace('pool_payments_v1') IS NOT NULL`).Scan(&exists); err != nil {
		return "", err
	}
	if exists {
		return "", errors.New("existing payment namespace requires reviewed verification, not migration replay")
	}
	if _, err = tx.Exec(ctx, MigrationSQL); err != nil {
		return "", err
	}
	fingerprint, err := CatalogFingerprint(ctx, tx)
	if err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return fingerprint, nil
}

type PostgresJournal struct {
	pool        *pgxpool.Pool
	fingerprint string
	// Test-only fault boundary, deliberately not exposed by constructor/config.
	beforePayoutInsert  func() error
	beforeHistoryCommit func() error
	beforeLegacyAudit   func() error
	beforeZeroAudit     func() error
}

func NewPostgresJournal(pool *pgxpool.Pool, reviewedFingerprint string) (*PostgresJournal, error) {
	b, err := hex.DecodeString(reviewedFingerprint)
	if pool == nil || err != nil || len(b) != 32 {
		return nil, errors.New("reviewed payment catalog fingerprint is mandatory")
	}
	return &PostgresJournal{pool: pool, fingerprint: reviewedFingerprint}, nil
}

func (s *PostgresJournal) Verify(ctx context.Context) error {
	actual, err := CatalogFingerprint(ctx, s.pool)
	if err != nil {
		return err
	}
	if actual != s.fingerprint {
		return errors.New("payment catalog drift; payouts held")
	}
	return nil
}

func walletLockKey(w WalletID) int64 {
	h := fnv.New64a()
	h.Write([]byte("BDAG-PAYOUT-WALLET/v1"))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], w.ChainID)
	h.Write(n[:])
	h.Write(w.Sender[:])
	return int64(h.Sum64())
}

type postgresFence struct {
	journal     *PostgresJournal
	conn        *pgxpool.Conn
	wallet      WalletID
	key         int64
	maxEligible *uint64
}

func (s *PostgresJournal) Acquire(ctx context.Context, w WalletID) (Fence, error) {
	if w.ChainID != 1404 || w.Sender == (common.Address{}) {
		return nil, errors.New("unqualified payout wallet")
	}
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	f := &postgresFence{journal: s, conn: c, wallet: w, key: walletLockKey(w)}
	var locked bool
	if err = c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, f.key).Scan(&locked); err != nil || !locked {
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Conn().Close(cleanup)
			cancel()
		}
		c.Release()
		if err != nil {
			return nil, err
		}
		return nil, ErrWalletBusy
	}
	ok := false
	defer func() {
		if !ok {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = f.Close(cleanup)
		}
	}()
	actual, err := CatalogFingerprint(ctx, c)
	if err != nil {
		return nil, err
	}
	if actual != s.fingerprint {
		return nil, errors.New("payment catalog drift under wallet fence")
	}
	_, err = c.Exec(ctx, `INSERT INTO pool_payments_v1.wallet_state(chain_id,sender) VALUES($1::numeric,$2) ON CONFLICT DO NOTHING`, strconv.FormatUint(w.ChainID, 10), w.Sender[:])
	if err != nil {
		return nil, err
	}
	var held *string
	if err = c.QueryRow(ctx, `SELECT hold_reason FROM pool_payments_v1.wallet_state WHERE chain_id=$1::numeric AND sender=$2`, strconv.FormatUint(w.ChainID, 10), w.Sender[:]).Scan(&held); err != nil {
		return nil, err
	}
	if held != nil {
		return nil, errors.New("wallet-level payment integrity hold")
	}
	ok = true
	return f, nil
}

func (f *postgresFence) Close(ctx context.Context) error {
	if f.conn == nil {
		return nil
	}
	c := f.conn
	f.conn = nil
	var unlocked bool
	err := c.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, f.key).Scan(&unlocked)
	if err != nil || !unlocked {
		_ = c.Conn().Close(ctx)
	}
	c.Release()
	return err
}

func (f *postgresFence) live() error {
	if f.conn == nil {
		return errors.New("payment wallet fence closed")
	}
	return nil
}

const intentColumns = `credit_id,chain_id::text,sender,credit_block_hash,recipient,amount::text,nonce::text,gas_price::text,raw_tx,tx_hash,created_at,state`

func readIntent(row pgx.Row) (*Intent, string, error) {
	var p Intent
	var chain, amount, nonce, gas, state string
	var sender, to, hash []byte
	err := row.Scan(&p.Credit.ID, &chain, &sender, &p.Credit.BlockHash, &to, &amount, &nonce, &gas, &p.Raw, &hash, &p.CreatedAt, &state)
	if err != nil {
		return nil, "", err
	}
	if len(sender) != 20 || len(to) != 20 || len(hash) != 32 {
		return nil, "", errors.New("payment fixed identity width drift")
	}
	p.Wallet.ChainID, err = strconv.ParseUint(chain, 10, 64)
	if err != nil {
		return nil, "", err
	}
	p.Nonce, err = strconv.ParseUint(nonce, 10, 64)
	if err != nil {
		return nil, "", err
	}
	p.Wallet.Sender = common.BytesToAddress(sender)
	p.Credit.Recipient = common.BytesToAddress(to)
	p.Hash = common.BytesToHash(hash)
	var ok bool
	p.Credit.Amount, ok = new(big.Int).SetString(amount, 10)
	if !ok {
		return nil, "", errors.New("invalid stored payment amount")
	}
	p.GasPrice, ok = new(big.Int).SetString(gas, 10)
	if !ok {
		return nil, "", errors.New("invalid stored payment gas price")
	}
	p.Held = state == "held"
	if err = ValidateIntent(p); err != nil {
		return nil, "", err
	}
	return &p, state, nil
}

func (f *postgresFence) Active(ctx context.Context) (*Intent, error) {
	if err := f.live(); err != nil {
		return nil, err
	}
	p, _, err := readIntent(f.conn.QueryRow(ctx, `SELECT `+intentColumns+` FROM pool_payments_v1.intents AS i WHERE chain_id=$1::numeric AND sender=$2 AND state<>'confirmed' ORDER BY i.nonce LIMIT 1`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:]))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

func (f *postgresFence) PendingWindowV1(ctx context.Context) ([]Intent, error) {
	if err := f.live(); err != nil {
		return nil, err
	}
	rows, err := f.conn.Query(ctx, `SELECT `+intentColumns+` FROM pool_payments_v1.intents AS i WHERE chain_id=$1::numeric AND sender=$2 AND state<>'confirmed' ORDER BY i.nonce LIMIT $3`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:], pipelineWindowLimitV1+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Intent
	for rows.Next() {
		intent, _, scanErr := readIntent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, *intent)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(result) > pipelineWindowLimitV1 {
		return nil, errors.New("payout pipeline exceeds reviewed window")
	}
	return result, nil
}

func (f *postgresFence) NextCredit(ctx context.Context, max uint64) (*Credit, error) {
	if err := f.live(); err != nil {
		return nil, err
	}
	f.maxEligible = &max
	var c Credit
	var address, amount string
	err := f.conn.QueryRow(ctx, `SELECT c.id,c.block_hash,c.miner_address,c.amount::text FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash WHERE c.is_paid=FALSE AND c.amount>0 AND b.status='MATURE' AND b.height<=$1::numeric AND NOT EXISTS(SELECT 1 FROM pool_payments_v1.intents p WHERE p.credit_id=c.id) ORDER BY b.height,c.id LIMIT 1`, strconv.FormatUint(max, 10)).Scan(&c.ID, &c.BlockHash, &address, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !common.IsHexAddress(address) {
		return nil, errors.New("invalid stored payout recipient")
	}
	c.Recipient = common.HexToAddress(address)
	var ok bool
	c.Amount, ok = new(big.Int).SetString(amount, 10)
	if !ok {
		return nil, errors.New("invalid stored payout amount")
	}
	var zeroSatisfied bool
	if err = f.conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_payments_v1.events WHERE credit_id=$1 AND kind='zero_satisfied')`, c.ID).Scan(&zeroSatisfied); err != nil {
		return nil, err
	}
	if zeroSatisfied {
		return nil, errors.New("positive credit has prior zero satisfaction; review required")
	}
	return &c, validateCredit(c)
}

func (f *postgresFence) LastConfirmedNonce(ctx context.Context) (*uint64, error) {
	if err := f.live(); err != nil {
		return nil, err
	}
	var raw *string
	err := f.conn.QueryRow(ctx, `SELECT max(nonce)::text FROM pool_payments_v1.intents WHERE chain_id=$1::numeric AND sender=$2 AND state='confirmed'`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:]).Scan(&raw)
	if err != nil || raw == nil {
		return nil, err
	}
	n, err := strconv.ParseUint(*raw, 10, 64)
	return &n, err
}

func sameCredit(a, b Credit) bool {
	return a.ID == b.ID && a.BlockHash == b.BlockHash && a.Recipient == b.Recipient && a.Amount != nil && b.Amount != nil && a.Amount.Cmp(b.Amount) == 0
}

func (f *postgresFence) Save(ctx context.Context, p Intent) (*Intent, error) {
	if err := f.live(); err != nil {
		return nil, err
	}
	if p.Wallet != f.wallet || f.maxEligible == nil || p.Held {
		return nil, errors.New("payment maturity/wallet selection not fenced")
	}
	if err := ValidateIntent(p); err != nil {
		return nil, err
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var c Credit
	var recipient, amount string
	err = tx.QueryRow(ctx, `SELECT c.id,c.block_hash,c.miner_address,c.amount::text FROM public.credits c JOIN public.blocks b ON b.hash=c.block_hash WHERE c.id=$1 AND c.is_paid=FALSE AND b.status='MATURE' AND b.height<=$2::numeric FOR UPDATE OF c,b`, p.Credit.ID, strconv.FormatUint(*f.maxEligible, 10)).Scan(&c.ID, &c.BlockHash, &recipient, &amount)
	if err != nil {
		return nil, err
	}
	if !common.IsHexAddress(recipient) {
		return nil, errors.New("stored credit recipient drift")
	}
	c.Recipient = common.HexToAddress(recipient)
	var ok bool
	c.Amount, ok = new(big.Int).SetString(amount, 10)
	if !ok || !sameCredit(c, p.Credit) {
		return nil, errors.New("payment differs from original eligible credit")
	}
	var zeroSatisfied bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_payments_v1.events WHERE credit_id=$1 AND kind='zero_satisfied')`, p.Credit.ID).Scan(&zeroSatisfied); err != nil {
		return nil, err
	}
	if zeroSatisfied {
		return nil, errors.New("credit already satisfied as zero; amount or paid flag drift requires review")
	}
	var prior bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.payouts WHERE lower(tx_hash)=lower($1))`, p.Hash.Hex()).Scan(&prior); err != nil {
		return nil, err
	}
	if prior {
		return nil, errors.New("legacy payout hash cannot pay a different credit")
	}
	existing, state, err := readIntent(tx.QueryRow(ctx, `SELECT `+intentColumns+` FROM pool_payments_v1.intents WHERE credit_id=$1 FOR UPDATE`, p.Credit.ID))
	if err == nil {
		if state == "confirmed" || existing.Hash != p.Hash || existing.Wallet != p.Wallet || !sameCredit(existing.Credit, p.Credit) {
			return nil, errors.New("immutable payment intent conflict")
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	err = tx.QueryRow(ctx, `INSERT INTO pool_payments_v1.intents(credit_id,chain_id,sender,credit_block_hash,recipient,amount,nonce,gas_price,raw_tx,tx_hash) VALUES($1,$2::numeric,$3,$4,$5,$6::numeric,$7::numeric,$8::numeric,$9,$10) RETURNING created_at`, p.Credit.ID, strconv.FormatUint(p.Wallet.ChainID, 10), p.Wallet.Sender[:], p.Credit.BlockHash, p.Credit.Recipient[:], p.Credit.Amount.String(), strconv.FormatUint(p.Nonce, 10), p.GasPrice.String(), p.Raw, p.Hash[:]).Scan(&p.CreatedAt)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES($1,'prepared',$2)`, p.Credit.ID, p.Hash.Hex()); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &p, nil
}

func (f *postgresFence) Hold(ctx context.Context, hash common.Hash, reason string) error {
	if err := f.live(); err != nil {
		return err
	}
	if len(reason) == 0 || len(reason) > 2048 {
		return errors.New("invalid payout hold reason")
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id int64
	var state string
	var prior *string
	err = tx.QueryRow(ctx, `SELECT credit_id,state,hold_reason FROM pool_payments_v1.intents WHERE tx_hash=$1 AND chain_id=$2::numeric AND sender=$3 FOR UPDATE`, hash[:], strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:]).Scan(&id, &state, &prior)
	if err != nil {
		return err
	}
	if state == "confirmed" {
		return errors.New("confirmed payment cannot be rewritten as held")
	}
	if state == "held" && prior != nil && *prior == reason {
		return tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `UPDATE pool_payments_v1.intents SET state='held',hold_reason=$2 WHERE credit_id=$1`, id, reason); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES($1,'held',$2)`, id, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (f *postgresFence) Confirm(ctx context.Context, p Intent, r *types.Receipt, head *types.Header) (bool, error) {
	if err := f.live(); err != nil {
		return false, err
	}
	if err := ValidateIntent(p); err != nil {
		return false, err
	}
	if p.Wallet != f.wallet || r == nil || head == nil || r.TxHash != p.Hash || r.Status != types.ReceiptStatusSuccessful || r.BlockNumber == nil || head.Number == nil || !r.BlockNumber.IsUint64() || !head.Number.IsUint64() || head.Number.Cmp(r.BlockNumber) < 0 || head.Number.Uint64()-r.BlockNumber.Uint64() < MinimumConfirmations-1 || r.BlockHash == (common.Hash{}) {
		return false, errors.New("canonical successful payout confirmation is mandatory")
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	stored, state, err := readIntent(tx.QueryRow(ctx, `SELECT `+intentColumns+` FROM pool_payments_v1.intents WHERE credit_id=$1 FOR UPDATE`, p.Credit.ID))
	if err != nil {
		return false, err
	}
	if stored.Wallet != f.wallet || stored.Hash != p.Hash || !sameCredit(stored.Credit, p.Credit) || !bytes.Equal(stored.Raw, p.Raw) {
		return false, errors.New("confirmation differs from durable signed intent")
	}
	var older bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_payments_v1.intents WHERE chain_id=$1::numeric AND sender=$2 AND state<>'confirmed' AND nonce<$3::numeric)`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:], strconv.FormatUint(p.Nonce, 10)).Scan(&older); err != nil || older {
		return false, errors.New("earlier payout intent remains unconfirmed")
	}
	var recipient, amount, blockHash string
	var paid bool
	err = tx.QueryRow(ctx, `SELECT miner_address,amount::text,block_hash,COALESCE(is_paid,FALSE) FROM public.credits WHERE id=$1 FOR UPDATE`, p.Credit.ID).Scan(&recipient, &amount, &blockHash, &paid)
	if err != nil {
		return false, err
	}
	if !common.IsHexAddress(recipient) || common.HexToAddress(recipient) != p.Credit.Recipient || amount != p.Credit.Amount.String() || blockHash != p.Credit.BlockHash {
		return false, errors.New("original credit changed during payment")
	}
	if state == "confirmed" {
		var oldHash []byte
		var oldNumber, payoutAmount string
		if err = tx.QueryRow(ctx, `SELECT confirmed_hash,confirmed_number::text FROM pool_payments_v1.intents WHERE credit_id=$1`, p.Credit.ID).Scan(&oldHash, &oldNumber); err != nil {
			return false, err
		}
		if !paid || !bytes.Equal(oldHash, r.BlockHash[:]) || oldNumber != r.BlockNumber.String() {
			return false, errors.New("confirmed legacy/payment evidence drift")
		}
		if err = tx.QueryRow(ctx, `SELECT amount::text FROM public.payouts WHERE tx_hash=$1`, p.Hash.Hex()).Scan(&payoutAmount); err != nil || payoutAmount != amount {
			return false, errors.New("confirmed legacy payout missing or changed")
		}
		return false, tx.Commit(ctx)
	}
	if paid {
		return false, errors.New("credit marked paid outside this durable intent; review required")
	}
	changed, err := tx.Exec(ctx, `UPDATE public.credits SET is_paid=TRUE WHERE id=$1 AND is_paid=FALSE`, p.Credit.ID)
	if err != nil {
		return false, err
	}
	if changed.RowsAffected() != 1 {
		return false, errors.New("credit paid flag changed outside confirmation")
	}
	if f.journal.beforePayoutInsert != nil {
		if err = f.journal.beforePayoutInsert(); err != nil {
			return false, err
		}
	}
	// Deliberately no ON CONFLICT adoption: a preexisting hash must never satisfy
	// a second credit. True idempotency is the confirmed-intent branch above.
	if _, err = tx.Exec(ctx, `INSERT INTO public.payouts(tx_hash,amount,created_at) VALUES($1,$2::numeric,clock_timestamp())`, p.Hash.Hex(), amount); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE pool_payments_v1.intents SET state='confirmed',hold_reason=NULL,confirmed_number=$2::numeric,confirmed_hash=$3,confirmed_at=clock_timestamp() WHERE credit_id=$1`, p.Credit.ID, r.BlockNumber.String(), r.BlockHash[:]); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES($1,'confirmed',$2)`, p.Credit.ID, r.BlockHash.Hex()); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE public.blocks SET status='PAID' WHERE hash=$1 AND status='MATURE' AND NOT EXISTS(SELECT 1 FROM public.credits WHERE block_hash=$1 AND is_paid IS DISTINCT FROM TRUE)`, p.Credit.BlockHash); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresJournal) String() string {
	return fmt.Sprintf("Postgres payment journal catalog=%s", strings.ToLower(s.fingerprint))
}
