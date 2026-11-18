package payments

import (
	"bytes"
	"context"
	"errors"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
)

func (f *postgresFence) History(ctx context.Context) (HistoryBatch, error) {
	var b HistoryBatch
	if err := f.live(); err != nil {
		return b, err
	}
	var number *string
	var hash []byte
	var cursor int64
	if err := f.conn.QueryRow(ctx, `SELECT checkpoint_number::text,checkpoint_hash,checkpoint_cursor FROM pool_payments_v1.wallet_state WHERE chain_id=$1::numeric AND sender=$2`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:]).Scan(&number, &hash, &cursor); err != nil {
		return b, err
	}
	if number != nil {
		n, err := strconv.ParseUint(*number, 10, 64)
		if err != nil || len(hash) != 32 || cursor <= 0 {
			return b, errors.New("stored history checkpoint malformed")
		}
		b.Checkpoint = &HistoryCheckpoint{Cursor: cursor, Number: n, Hash: common.BytesToHash(hash)}
	} else if len(hash) != 0 || cursor != 0 {
		return b, errors.New("partial history checkpoint")
	}
	// Deferred constraint triggers bind confirmations to immutable events, so
	// this need not rescan every historical intent on each bounded Step.
	rows, err := f.conn.Query(ctx, `SELECT e.event_id,p.credit_id,p.confirmed_number::text,p.confirmed_hash FROM pool_payments_v1.events e JOIN pool_payments_v1.intents p ON p.credit_id=e.credit_id WHERE e.kind='confirmed' AND p.state='confirmed' AND p.chain_id=$1::numeric AND p.sender=$2 AND e.event_id>$3 ORDER BY e.event_id LIMIT $4`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:], cursor, HistoryBatchLimit+1)
	if err != nil {
		return b, err
	}
	type record struct {
		event, id int64
		number    string
		hash      []byte
	}
	var records []record
	for rows.Next() {
		var r record
		if err := rows.Scan(&r.event, &r.id, &r.number, &r.hash); err != nil {
			rows.Close()
			return b, err
		}
		records = append(records, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return b, err
	}
	if len(records) > HistoryBatchLimit {
		b.More = true
		records = records[:HistoryBatchLimit]
	}
	for _, r := range records {
		p, state, err := readIntent(f.conn.QueryRow(ctx, `SELECT `+intentColumns+` FROM pool_payments_v1.intents WHERE credit_id=$1`, r.id))
		if err != nil {
			return b, err
		}
		if state != "confirmed" || p.Wallet != f.wallet || len(r.hash) != 32 {
			return b, errors.New("confirmation history identity drift")
		}
		n, err := strconv.ParseUint(r.number, 10, 64)
		if err != nil {
			return b, err
		}
		var valid bool
		if err := f.conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.credits c JOIN public.payouts paid ON paid.tx_hash=$2 WHERE c.id=$1 AND c.is_paid=TRUE AND c.block_hash=$3 AND lower(c.miner_address)=lower($4) AND c.amount=$5::numeric AND paid.amount=c.amount)`, p.Credit.ID, p.Hash.Hex(), p.Credit.BlockHash, p.Credit.Recipient.Hex(), p.Credit.Amount.String()).Scan(&valid); err != nil {
			return b, err
		}
		if !valid {
			return b, errors.New("confirmed payment source credit or legacy payout drift")
		}
		b.Payments = append(b.Payments, ConfirmedPayment{EventID: r.event, Intent: *p, Number: n, Hash: common.BytesToHash(r.hash)})
	}
	return b, nil
}

func (f *postgresFence) AdvanceHistory(ctx context.Context, prior *HistoryCheckpoint, next HistoryCheckpoint) error {
	if err := f.live(); err != nil {
		return err
	}
	if next.Cursor <= 0 || next.Hash == (common.Hash{}) || (prior != nil && (next.Cursor <= prior.Cursor || next.Number < prior.Number)) {
		return errors.New("history checkpoint cannot regress or be empty")
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var number *string
	var hash []byte
	var cursor int64
	var held *string
	if err = tx.QueryRow(ctx, `SELECT checkpoint_number::text,checkpoint_hash,checkpoint_cursor,hold_reason FROM pool_payments_v1.wallet_state WHERE chain_id=$1::numeric AND sender=$2 FOR UPDATE`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:]).Scan(&number, &hash, &cursor, &held); err != nil {
		return err
	}
	if held != nil {
		return errors.New("cannot advance held wallet history")
	}
	if prior == nil {
		if number != nil || hash != nil || cursor != 0 {
			return errors.New("history checkpoint changed before commit")
		}
	} else if number == nil || *number != strconv.FormatUint(prior.Number, 10) || !bytes.Equal(hash, prior.Hash[:]) || cursor != prior.Cursor {
		return errors.New("history checkpoint compare-and-set conflict")
	}
	var last *int64
	var highest *string
	var count int
	if err = tx.QueryRow(ctx, `SELECT max(e.event_id),max(p.confirmed_number)::text,count(*) FROM pool_payments_v1.events e JOIN pool_payments_v1.intents p ON p.credit_id=e.credit_id WHERE e.kind='confirmed' AND p.state='confirmed' AND p.chain_id=$1::numeric AND p.sender=$2 AND e.event_id>$3 AND e.event_id<=$4`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:], cursor, next.Cursor).Scan(&last, &highest, &count); err != nil {
		return err
	}
	if last == nil || *last != next.Cursor || highest == nil || count < 1 || count > HistoryBatchLimit {
		return errors.New("history checkpoint must bind a bounded confirmed event batch")
	}
	high, err := strconv.ParseUint(*highest, 10, 64)
	if err != nil {
		return err
	}
	if prior != nil && prior.Number > high {
		high = prior.Number
	}
	if high != next.Number {
		return errors.New("history checkpoint must anchor highest audited block")
	}
	var bound bool
	if prior != nil && prior.Number == next.Number && prior.Hash == next.Hash {
		bound = true
	}
	if !bound {
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_payments_v1.events e JOIN pool_payments_v1.intents p ON p.credit_id=e.credit_id WHERE e.kind='confirmed' AND p.state='confirmed' AND p.chain_id=$1::numeric AND p.sender=$2 AND e.event_id>$3 AND e.event_id<=$4 AND p.confirmed_number=$5::numeric AND p.confirmed_hash=$6)`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:], cursor, next.Cursor, strconv.FormatUint(next.Number, 10), next.Hash[:]).Scan(&bound); err != nil {
			return err
		}
	}
	if !bound {
		return errors.New("history checkpoint hash not bound to audited confirmation")
	}
	if _, err = tx.Exec(ctx, `UPDATE pool_payments_v1.wallet_state SET checkpoint_number=$3::numeric,checkpoint_hash=$4,checkpoint_cursor=$5 WHERE chain_id=$1::numeric AND sender=$2`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:], strconv.FormatUint(next.Number, 10), next.Hash[:], next.Cursor); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(kind,detail) VALUES('checkpoint',$1)`, f.wallet.Sender.Hex()+" cursor="+strconv.FormatInt(next.Cursor, 10)+" block="+next.Hash.Hex()); err != nil {
		return err
	}
	if f.journal.beforeHistoryCommit != nil {
		if err = f.journal.beforeHistoryCommit(); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (f *postgresFence) HoldWallet(ctx context.Context, reason string) error {
	if err := f.live(); err != nil {
		return err
	}
	if len(reason) == 0 || len(reason) > 1800 {
		return errors.New("invalid wallet hold reason")
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	r, err := tx.Exec(ctx, `UPDATE pool_payments_v1.wallet_state SET hold_reason=$3 WHERE chain_id=$1::numeric AND sender=$2 AND hold_reason IS NULL`, strconv.FormatUint(f.wallet.ChainID, 10), f.wallet.Sender[:], reason)
	if err != nil {
		return err
	}
	if r.RowsAffected() == 1 {
		if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(kind,detail) VALUES('wallet_held',$1)`, f.wallet.Sender.Hex()+" "+reason); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
