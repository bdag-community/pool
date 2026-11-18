package payments

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type LegacyNetwork interface {
	Network
	Transaction(context.Context, common.Hash) (*types.Transaction, error)
}

// OfflineProofDigest binds the separately reviewed, OS-no-network exact signed
// envelope reconstruction. It is not a "receipt missing => unpaid" shortcut.
// The coordinator must qualify that artifact before invoking this admin path.
type LegacyRepair struct {
	Wallet              WalletID
	Missing             Credit
	MissingHash         common.Hash
	MissingPayoutID     int64
	MissingXID          string
	Nonce               uint64
	Replacement         Credit
	ReplacementHash     common.Hash
	ReplacementPayoutID int64
	ReplacementXID      string
	OfflineProofDigest  string
}

type legacyCreditRow struct {
	ID        int64  `json:"id"`
	Block     string `json:"block"`
	Recipient string `json:"recipient"`
	Amount    string `json:"amount"`
	Paid      bool   `json:"paid"`
	XID       string `json:"xid"`
}
type legacyAudit struct {
	RepairID            string          `json:"repair_id"` // first field is a stable indexed prefix.
	Before              legacyCreditRow `json:"before"`
	MissingPayoutID     int64           `json:"missing_payout_id"`
	MissingHash         string          `json:"missing_hash"`
	ReplacementCreditID int64           `json:"replacement_credit_id"`
	ReplacementPayoutID int64           `json:"replacement_payout_id"`
	ReplacementHash     string          `json:"replacement_hash"`
	ReplacementXID      string          `json:"replacement_xid"`
	Nonce               uint64          `json:"consumed_nonce"`
	CanonicalBlock      string          `json:"canonical_block"`
	CanonicalHeight     uint64          `json:"canonical_height"`
	OfflineDigest       string          `json:"offline_proof_sha256"`
	BeforeBlockStatus   string          `json:"before_block_status"`
	AfterPaid           bool            `json:"after_paid"`
	AfterBlockStatus    string          `json:"after_block_status"`
}

func verifyLegacy(ctx context.Context, q LegacyRepair, n LegacyNetwork) (*types.Receipt, error) {
	chain, err := n.ChainID(ctx)
	if err != nil || chain == nil || chain.Cmp(new(big.Int).SetUint64(q.Wallet.ChainID)) != 0 {
		return nil, errors.New("legacy evidence chain identity unavailable")
	}
	tx, err := n.Transaction(ctx, q.MissingHash)
	if err != nil || tx != nil {
		return nil, errors.New("original transaction evidence changed or unavailable")
	}
	r, err := n.Receipt(ctx, q.MissingHash)
	if err != nil || r != nil {
		return nil, errors.New("original receipt evidence changed or unavailable")
	}
	tx, err = n.Transaction(ctx, q.ReplacementHash)
	if err != nil || tx == nil || tx.Hash() != q.ReplacementHash || tx.Type() != types.LegacyTxType || tx.ChainId().Cmp(chain) != 0 || tx.Nonce() != q.Nonce || tx.To() == nil || *tx.To() != q.Replacement.Recipient || tx.Value().Cmp(q.Replacement.Amount) != 0 || len(tx.Data()) != 0 || tx.Gas() != 21000 {
		return nil, errors.New("canonical nonce replacement body mismatch")
	}
	sender, err := types.Sender(types.LatestSignerForChainID(chain), tx)
	if err != nil || sender != q.Wallet.Sender {
		return nil, errors.New("canonical nonce replacement sender mismatch")
	}
	r, err = n.Receipt(ctx, q.ReplacementHash)
	if err != nil || r == nil || r.TxHash != q.ReplacementHash || r.Status != types.ReceiptStatusSuccessful || r.BlockNumber == nil || !r.BlockNumber.IsUint64() {
		return nil, errors.New("successful replacement receipt missing")
	}
	h, err := n.Header(ctx, r.BlockNumber)
	if err != nil || h == nil || h.Number == nil || h.Number.Cmp(r.BlockNumber) != 0 || h.Hash() != r.BlockHash {
		return nil, errors.New("replacement is not canonical")
	}
	head, err := n.Header(ctx, nil)
	if err != nil || head == nil || head.Number == nil || !head.Number.IsUint64() || head.Number.Cmp(r.BlockNumber) < 0 || head.Number.Uint64()-r.BlockNumber.Uint64() < MinimumConfirmations-1 {
		return nil, errors.New("replacement confirmation depth insufficient")
	}
	mined, err := n.MinedNonce(ctx, q.Wallet.Sender)
	if err != nil || mined <= q.Nonce {
		return nil, errors.New("nonce consumption not proven")
	}
	again, err := n.Header(ctx, r.BlockNumber)
	if err != nil || again == nil || again.Hash() != r.BlockHash {
		return nil, errors.New("replacement canonical block changed")
	}
	return r, nil
}

// RepairLegacyCredit is an explicit audited admin correction, never a worker
// fallback. It preserves payout history and the exact obligation. A later valid
// payment is protected from an old repair replay by its immutable audit event.
func (s *PostgresJournal) RepairLegacyCredit(ctx context.Context, q LegacyRepair, n LegacyNetwork, ready func(context.Context) (uint64, bool, error)) (bool, error) {
	return s.repairLegacyCredit(ctx, q, n, ready, true)
}

// PreflightLegacyCredit exercises the same locks and evidence validation, then
// rolls back before any credit, block-status or audit change. Acquire may create
// the empty wallet-fence metadata row; it cannot change a cash obligation.
func (s *PostgresJournal) PreflightLegacyCredit(ctx context.Context, q LegacyRepair, n LegacyNetwork, ready func(context.Context) (uint64, bool, error)) error {
	_, err := s.repairLegacyCredit(ctx, q, n, ready, false)
	return err
}

func (s *PostgresJournal) repairLegacyCredit(ctx context.Context, q LegacyRepair, n LegacyNetwork, ready func(context.Context) (uint64, bool, error), apply bool) (bool, error) {
	if !validWallet(q.Wallet) || n == nil || ready == nil {
		return false, errors.New("incomplete legacy repair authority")
	}
	if err := validateCredit(q.Missing); err != nil {
		return false, err
	}
	if err := validateCredit(q.Replacement); err != nil {
		return false, err
	}
	for _, x := range []string{q.MissingXID, q.ReplacementXID} {
		v, e := strconv.ParseUint(x, 10, 32)
		if e != nil || v == 0 {
			return false, errors.New("invalid original transaction identity")
		}
	}
	digest, err := hex.DecodeString(q.OfflineProofDigest)
	if err != nil || len(digest) != 32 || q.OfflineProofDigest == strings.Repeat("0", 64) || q.Missing.ID == q.Replacement.ID || q.MissingPayoutID <= 0 || q.ReplacementPayoutID <= 0 || q.MissingPayoutID == q.ReplacementPayoutID || q.MissingHash == (common.Hash{}) || q.ReplacementHash == (common.Hash{}) || q.MissingHash == q.ReplacementHash {
		return false, errors.New("invalid exact legacy repair binding")
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	max, eligible, err := ready(ctx)
	if err != nil || !eligible {
		return false, errors.New("native legacy repair readiness or maturity unavailable")
	}
	r, err := verifyLegacy(ctx, q, n)
	if err != nil {
		return false, err
	}
	lease, err := s.Acquire(ctx, q.Wallet)
	if err != nil {
		return false, err
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = lease.Close(c)
	}()
	f := lease.(*postgresFence)
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='15s'`); err != nil {
		return false, err
	}
	id := fmt.Sprintf("legacy-nonce-repair:v1:%d:%s:%d:%d:%s", q.Wallet.ChainID, strings.ToLower(q.Wallet.Sender.Hex()), q.Missing.ID, q.MissingPayoutID, q.MissingHash.Hex())
	prefix := fmt.Sprintf(`{"repair_id":%q,`, id)
	rows, err := tx.Query(ctx, `SELECT detail FROM pool_payments_v1.events WHERE credit_id=$1 AND kind='held' AND left(detail,$2)=$3 LIMIT 2`, q.Missing.ID, len(prefix), prefix)
	if err != nil {
		return false, err
	}
	var previous []string
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			rows.Close()
			return false, err
		}
		previous = append(previous, v)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return false, err
	}
	if len(previous) > 1 {
		return false, errors.New("duplicate legacy repair audit; review required")
	}
	if len(previous) == 1 {
		var a legacyAudit
		if err = json.Unmarshal([]byte(previous[0]), &a); err != nil {
			return false, err
		}
		if a.RepairID != id || a.Before.Amount != q.Missing.Amount.String() || a.Before.Block != q.Missing.BlockHash || !common.IsHexAddress(a.Before.Recipient) || common.HexToAddress(a.Before.Recipient) != q.Missing.Recipient || a.ReplacementHash != q.ReplacementHash.Hex() {
			return false, errors.New("legacy repair replay differs from immutable evidence")
		}
		return false, tx.Commit(ctx) // NEVER unpay a later legitimate payment.
	}
	readCredit := func(c Credit, xid string) (legacyCreditRow, error) {
		var row legacyCreditRow
		err := tx.QueryRow(ctx, `SELECT id,block_hash,miner_address,amount::text,is_paid,xmin::text FROM public.credits WHERE id=$1 FOR UPDATE`, c.ID).Scan(&row.ID, &row.Block, &row.Recipient, &row.Amount, &row.Paid, &row.XID)
		if err != nil {
			return row, err
		}
		if row.Block != c.BlockHash || !common.IsHexAddress(row.Recipient) || common.HexToAddress(row.Recipient) != c.Recipient || row.Amount != c.Amount.String() || !row.Paid || row.XID != xid {
			return row, errors.New("legacy credit identity changed before repair")
		}
		return row, nil
	}
	if _, err = readCredit(q.Replacement, q.ReplacementXID); err != nil {
		return false, err
	}
	before, err := readCredit(q.Missing, q.MissingXID)
	if err != nil {
		return false, err
	}
	for _, p := range []struct {
		id          int64
		hash        common.Hash
		amount, xid string
	}{{q.MissingPayoutID, q.MissingHash, q.Missing.Amount.String(), q.MissingXID}, {q.ReplacementPayoutID, q.ReplacementHash, q.Replacement.Amount.String(), q.ReplacementXID}} {
		var hash, amount, xid string
		if err = tx.QueryRow(ctx, `SELECT tx_hash,amount::text,xmin::text FROM public.payouts WHERE id=$1 FOR UPDATE`, p.id).Scan(&hash, &amount, &xid); err != nil {
			return false, err
		}
		if !strings.EqualFold(hash, p.hash.Hex()) || amount != p.amount || xid != p.xid {
			return false, errors.New("legacy payout row identity changed")
		}
	}
	var height uint64
	var status string
	if err = tx.QueryRow(ctx, `SELECT height,status FROM public.blocks WHERE hash=$1 FOR UPDATE`, q.Missing.BlockHash).Scan(&height, &status); err != nil {
		return false, err
	}
	var count int
	var intent bool
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM public.credits WHERE block_hash=$1),EXISTS(SELECT 1 FROM pool_payments_v1.intents WHERE credit_id=$2)`, q.Missing.BlockHash, q.Missing.ID).Scan(&count, &intent); err != nil {
		return false, err
	}
	if height > max || status != "PAID" || count != 1 || intent {
		return false, errors.New("legacy block maturity/cardinality or outbox binding changed")
	}
	next, eligible, err := ready(ctx)
	if err != nil || !eligible || next < height {
		return false, errors.New("native readiness changed before legacy correction")
	}
	check, err := verifyLegacy(ctx, q, n)
	if err != nil || check.BlockHash != r.BlockHash {
		return false, errors.New("canonical replacement changed before legacy correction")
	}
	if !apply {
		return false, tx.Rollback(ctx)
	}
	changed, err := tx.Exec(ctx, `UPDATE public.credits SET is_paid=FALSE WHERE id=$1 AND is_paid=TRUE AND xmin::text=$2`, q.Missing.ID, q.MissingXID)
	if err != nil || changed.RowsAffected() != 1 {
		return false, errors.New("legacy credit correction did not affect exactly one bound row")
	}
	if _, err = tx.Exec(ctx, `UPDATE public.blocks SET status='MATURE' WHERE hash=$1 AND status='PAID'`, q.Missing.BlockHash); err != nil {
		return false, err
	}
	if s.beforeLegacyAudit != nil {
		if err = s.beforeLegacyAudit(); err != nil {
			return false, err
		}
	}
	a := legacyAudit{RepairID: id, Before: before, MissingPayoutID: q.MissingPayoutID, MissingHash: q.MissingHash.Hex(), ReplacementCreditID: q.Replacement.ID, ReplacementPayoutID: q.ReplacementPayoutID, ReplacementHash: q.ReplacementHash.Hex(), ReplacementXID: q.ReplacementXID, Nonce: q.Nonce, CanonicalBlock: r.BlockHash.Hex(), CanonicalHeight: r.BlockNumber.Uint64(), OfflineDigest: q.OfflineProofDigest, BeforeBlockStatus: status, AfterPaid: false, AfterBlockStatus: "MATURE"}
	detail, err := json.Marshal(a)
	if err != nil || len(detail) > 2048 {
		return false, errors.New("legacy audit size invalid")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_payments_v1.events(credit_id,kind,detail) VALUES($1,'held',$2)`, q.Missing.ID, string(detail)); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
