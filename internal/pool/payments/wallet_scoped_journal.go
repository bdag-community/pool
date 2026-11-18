package payments

import (
	"context"
	"errors"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
)

// ScopedCreditGuard checks the reward block and its immutable evidence against
// the authenticated native and EVM chains before a payment can proceed.
type ScopedCreditGuard interface {
	VerifyScopedCredit(context.Context, WalletID, Credit) error
}

type walletScopedJournal struct {
	base  *PostgresJournal
	guard ScopedCreditGuard
}

func NewWalletScopedJournal(base *PostgresJournal, guard ScopedCreditGuard) (ReviewedJournal, error) {
	if base == nil || guard == nil {
		return nil, errors.New("wallet-scoped journal needs a canonical credit guard")
	}
	return &walletScopedJournal{base: base, guard: guard}, nil
}

func (s *walletScopedJournal) Verify(ctx context.Context) error {
	if s == nil || s.base == nil || s.guard == nil {
		return errors.New("wallet-scoped journal unavailable")
	}
	return s.base.Verify(ctx)
}

func (s *walletScopedJournal) Acquire(ctx context.Context, wallet WalletID) (Fence, error) {
	base, err := s.base.Acquire(ctx, wallet)
	if err != nil {
		return nil, err
	}
	raw, ok := base.(*postgresFence)
	if !ok {
		_ = base.Close(ctx)
		return nil, errors.New("payment fence type unavailable")
	}
	return &walletScopedFence{Fence: base, raw: raw, guard: s.guard}, nil
}

type walletScopedFence struct {
	Fence
	raw   *postgresFence
	guard ScopedCreditGuard
}

func (f *walletScopedFence) verify(ctx context.Context, credit Credit) error {
	if f == nil || f.raw == nil || f.guard == nil || f.raw.live() != nil {
		return errors.New("wallet-scoped payment fence unavailable")
	}
	if err := validateCredit(credit); err != nil {
		return err
	}
	var match bool
	err := f.raw.conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_reward_observations_v1.credit_bindings x JOIN pool_reward_observations_v1.allocations a USING(candidate_id) JOIN pool_reward_observations_v1.observations o USING(candidate_id) JOIN public.credits c ON c.id=x.credit_id WHERE x.credit_id=$1 AND x.wallet=$2 AND a.wallet=$2 AND o.wallet=$2 AND x.miner_address=$3 AND x.amount_wei=$4::numeric AND c.miner_address=x.miner_address AND c.amount=x.amount_wei AND c.block_hash=$5 AND c.block_hash=encode(o.native_block_hash,'hex') AND NOT EXISTS(SELECT 1 FROM pool_reward_observations_v1.conflicts))`, credit.ID, f.raw.wallet.Sender[:], credit.Recipient.Hex(), credit.Amount.String(), credit.BlockHash).Scan(&match)
	if err != nil || !match {
		return errors.New("credit does not belong to the reward wallet")
	}
	return f.guard.VerifyScopedCredit(ctx, f.raw.wallet, credit)
}

func (f *walletScopedFence) Active(ctx context.Context) (*Intent, error) {
	intent, err := f.Fence.Active(ctx)
	if err != nil || intent == nil {
		return intent, err
	}
	if err = f.verify(ctx, intent.Credit); err != nil {
		return nil, err
	}
	return intent, nil
}

func (f *walletScopedFence) PendingWindowV1(ctx context.Context) ([]Intent, error) {
	if f == nil || f.raw == nil {
		return nil, errors.New("wallet-scoped payment fence unavailable")
	}
	intents, err := f.raw.PendingWindowV1(ctx)
	if err != nil {
		return nil, err
	}
	for _, intent := range intents {
		if err = f.verify(ctx, intent.Credit); err != nil {
			return nil, err
		}
	}
	return intents, nil
}

func (f *walletScopedFence) NextCredit(ctx context.Context, max uint64) (*Credit, error) {
	if f == nil || f.raw == nil || f.raw.live() != nil {
		return nil, errors.New("wallet-scoped payment fence unavailable")
	}
	f.raw.maxEligible = &max
	var credit Credit
	var recipient, amount string
	err := f.raw.conn.QueryRow(ctx, `SELECT c.id,c.block_hash,c.miner_address,c.amount::text FROM pool_reward_observations_v1.credit_bindings x JOIN public.credits c ON c.id=x.credit_id JOIN public.blocks b ON b.hash=c.block_hash WHERE x.wallet=$1 AND c.is_paid=FALSE AND c.amount>0 AND b.status='MATURE' AND b.height<=$2::numeric AND NOT EXISTS(SELECT 1 FROM pool_payments_v1.intents p WHERE p.credit_id=c.id) ORDER BY b.height,c.id LIMIT 1`, f.raw.wallet.Sender[:], strconv.FormatUint(max, 10)).Scan(&credit.ID, &credit.BlockHash, &recipient, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !common.IsHexAddress(recipient) {
		return nil, errors.New("invalid scoped credit recipient")
	}
	credit.Recipient = common.HexToAddress(recipient)
	var ok bool
	credit.Amount, ok = new(big.Int).SetString(amount, 10)
	if !ok {
		return nil, errors.New("invalid scoped credit amount")
	}
	if err = validateCredit(credit); err != nil {
		return nil, err
	}
	if err = f.verify(ctx, credit); err != nil {
		return nil, err
	}
	return &credit, nil
}

// Scoped allocation omits zero-value lines and must not alter old credits.
func (f *walletScopedFence) SatisfyZeroCredits(context.Context, uint64) (int, error) { return 0, nil }

func (f *walletScopedFence) Save(ctx context.Context, intent Intent) (*Intent, error) {
	if err := f.verify(ctx, intent.Credit); err != nil {
		return nil, err
	}
	return f.Fence.Save(ctx, intent)
}

func (f *walletScopedFence) Confirm(ctx context.Context, intent Intent, receipt *types.Receipt, head *types.Header) (bool, error) {
	if err := f.verify(ctx, intent.Credit); err != nil {
		return false, err
	}
	return f.Fence.Confirm(ctx, intent, receipt, head)
}
