package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"sort"
	"strconv"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// MatureRewardVerifierV1 must independently check the exact native and EVM
// identities and maturity against the authenticated node, not the ledger.
type MatureRewardVerifierV1 interface {
	VerifyMatureRewardV1(context.Context, FrozenCandidateV1, RewardObservationV1) error
}

// A verifier that can recover reordered observations must return the exact
// current EVM block identity to bind it uniquely to the resulting allocation.
type matureRewardIdentityVerifierV1 interface {
	VerifyMatureRewardIdentityV1(context.Context, FrozenCandidateV1, RewardObservationV1) (common.Hash, error)
}

type reorderedAliasVerifierV1 interface {
	verifyNoReorderedAliasesV1(context.Context, pgx.Tx, common.Hash) error
}

type rewardCreditLineV1 struct {
	miner  string
	work   uint64
	amount string
}

// AllocateObservedRewardV1 creates wallet-bound miner credits for a confirmed
// observed reward and its immutable work cut. It performs no signing or
// broadcasting. The payer must recheck canonicality before each payment.
func (s *PostgresStorage) AllocateObservedRewardV1(ctx context.Context, id [32]byte, chainID *big.Int, backend string, wallet common.Address, verifier MatureRewardVerifierV1) (bool, error) {
	return s.verifyOrAllocateObservedRewardV1(ctx, id, chainID, backend, wallet, verifier, true)
}

func (s *PostgresStorage) verifyOrAllocateObservedRewardV1(ctx context.Context, id [32]byte, chainID *big.Int, backend string, wallet common.Address, verifier MatureRewardVerifierV1, allowCreate bool) (bool, error) {
	if s == nil || s.pool == nil || ctx == nil || ctx.Err() != nil || id == ([32]byte{}) || wallet == (common.Address{}) || verifier == nil {
		return false, errors.New("reward allocation inputs unavailable")
	}
	if _, ok := ctx.Deadline(); !ok {
		return false, errors.New("reward allocation requires a deadline")
	}
	if err := validateJournalIdentity(chainID, backend); err != nil {
		return false, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return false, err
	}
	defer rollbackUnpricedTx(tx)
	fallbackReason := ""
	if err = s.verifyUnpricedWorkCandidateTxV1(ctx, tx, chainID, backend, wallet, id[:]); err != nil {
		if !s.bestEffortAccounting.Load() || !allowCreate || !operatorAttributionUnavailableV1(err) || ctx.Err() != nil {
			return false, err
		}
		fallbackReason = "conflicting_or_missing_work"
	}
	view, err := scanCandidateView(ctx, tx, id)
	if err != nil || view.Candidate.Height == nil || !view.Candidate.Height.IsInt64() {
		return false, errors.New("accepted reward candidate unavailable")
	}
	if accepted, err := s.rewardCandidateAcceptedV1(ctx, tx, view); err != nil || !accepted {
		return false, errors.New("accepted reward candidate unavailable")
	}
	var obs RewardObservationV1
	var nativeHash, evmHash, stateRoot, sender, storedEvidence []byte
	var nativeOrder, nativeHeight, evmNumber string
	err = tx.QueryRow(ctx, `SELECT native_block_hash,native_order::text,native_height::text,evm_block_hash,evm_block_number::text,evm_state_root,wallet,subsidy_wei::text,other_sender_fees_wei::text,self_paid_fees_wei::text,evidence_digest FROM pool_reward_observations_v1.observations WHERE candidate_id=$1`, id[:]).Scan(&nativeHash, &nativeOrder, &nativeHeight, &evmHash, &evmNumber, &stateRoot, &sender, &obs.SubsidyWei, &obs.OtherFeesWei, &obs.SelfPaidFeesWei, &storedEvidence)
	if err != nil || len(nativeHash) != 32 || len(evmHash) != 32 || len(stateRoot) != 32 || len(sender) != 20 || len(storedEvidence) != 32 {
		return false, errors.New("reward observation unavailable or malformed")
	}
	obs.NativeBlockHash, obs.EVMBlockHash, obs.EVMStateRoot, obs.Wallet = common.BytesToHash(nativeHash), common.BytesToHash(evmHash), common.BytesToHash(stateRoot), common.BytesToAddress(sender)
	if obs.NativeOrder, err = strconv.ParseUint(nativeOrder, 10, 64); err != nil {
		return false, err
	}
	if obs.NativeHeight, err = strconv.ParseUint(nativeHeight, 10, 64); err != nil {
		return false, err
	}
	if obs.EVMBlockNumber, err = strconv.ParseUint(evmNumber, 10, 64); err != nil {
		return false, err
	}
	if err = validateRewardObservationV1(view.Candidate, wallet, obs); err != nil {
		return false, err
	}
	digest, _, err := rewardObservationDigestV1(id, obs)
	if err != nil || !bytes.Equal(digest, storedEvidence) {
		return false, errors.New("reward observation digest differs")
	}
	var conflict bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_reward_observations_v1.conflicts WHERE NOT $2::boolean OR candidate_id=$1)`, id[:], s.bestEffortAccounting.Load()).Scan(&conflict); err != nil || (conflict && !s.bestEffortAccounting.Load()) {
		return false, errors.New("reward observation conflict held")
	}
	if fallbackReason != "" {
		return retainOperatorRewardV1(ctx, tx, view, obs, wallet, digest, verifier, fallbackReason, allowCreate)
	}
	var shareID, storedWork []byte
	var feeRaw, totalRaw, targetRaw, policyRaw string
	var cut int64
	err = tx.QueryRow(ctx, `SELECT d.discovery_share_id,d.work_digest,d.fee_float_bits::text,d.total_work::text,p.target_work::text,p.boundary_policy,COALESCE(c.share_sequence,0) FROM pool_share_journal_v1.unpriced_discoveries_v1 d JOIN pool_share_journal_v1.allocation_policies p USING(candidate_id) JOIN pool_share_journal_v1.cuts c USING(candidate_id) WHERE d.candidate_id=$1`, id[:]).Scan(&shareID, &storedWork, &feeRaw, &totalRaw, &targetRaw, &policyRaw, &cut)
	if err != nil || len(storedWork) != 32 || cut < 0 {
		return false, errors.New("reward work unavailable")
	}
	if cut == 0 && len(shareID) == 0 && totalRaw == "0" && s.bestEffortAccounting.Load() {
		return retainOperatorRewardV1(ctx, tx, view, obs, wallet, digest, verifier, "no_attributable_work", allowCreate)
	}
	if cut == 0 {
		return false, errors.New("reward work unavailable")
	}
	feeBits, err := strconv.ParseUint(feeRaw, 10, 64)
	if err != nil {
		return false, err
	}
	totalWork, err := strconv.ParseUint(totalRaw, 10, 64)
	if err != nil || totalWork == 0 {
		return false, errors.New("reward work total invalid")
	}
	target, err := strconv.ParseUint(targetRaw, 10, 64)
	if err != nil || target == 0 {
		return false, errors.New("reward target invalid")
	}
	policy := sharejournal.BoundaryPolicy(policyRaw)
	window, err := sharejournal.ReadWindowTx(ctx, tx, cut, target)
	if err != nil {
		return false, err
	}
	work, actual, err := sharejournal.AllocateWindow(window, target, policy)
	if err != nil {
		return false, err
	}
	workDigest, err := unpricedWorkDigest(id, shareID, feeBits, target, policy, cut, work, actual)
	if err != nil || actual != totalWork || !bytes.Equal(workDigest[:], storedWork) {
		return false, errors.New("reward allocation work differs from discovery")
	}
	feePercent := math.Float64frombits(feeBits)
	if math.IsNaN(feePercent) || math.IsInf(feePercent, 0) || feePercent < 0 || feePercent > 100 {
		return false, errors.New("reward fee invalid")
	}
	subsidy, _ := new(big.Int).SetString(obs.SubsidyWei, 10)
	fees, _ := new(big.Int).SetString(obs.OtherFeesWei, 10)
	gross := new(big.Int).Add(subsidy, fees)
	if gross.Sign() <= 0 || len(gross.String()) > 30 {
		return false, errors.New("reward has no distributable income")
	}
	ratio := new(big.Rat).SetFloat64(feePercent)
	fee := new(big.Int).Quo(new(big.Int).Mul(gross, ratio.Num()), new(big.Int).Mul(ratio.Denom(), big.NewInt(100)))
	distributable := new(big.Int).Sub(gross, fee)
	miners := make([]string, 0, len(work))
	for miner := range work {
		miners = append(miners, miner)
	}
	sort.Strings(miners)
	lines := make([]rewardCreditLineV1, 0, len(miners))
	for _, miner := range miners {
		if !common.IsHexAddress(miner) || common.HexToAddress(miner) == (common.Address{}) {
			return false, errors.New("reward miner address invalid")
		}
		amount := new(big.Int).Quo(new(big.Int).Mul(distributable, new(big.Int).SetUint64(work[miner])), new(big.Int).SetUint64(totalWork))
		if amount.Sign() > 0 {
			lines = append(lines, rewardCreditLineV1{miner: common.HexToAddress(miner).Hex(), work: work[miner], amount: amount.String()})
		}
	}
	rewardIdentity := obs.EVMBlockHash
	if identityVerifier, ok := verifier.(matureRewardIdentityVerifierV1); ok {
		rewardIdentity, err = identityVerifier.VerifyMatureRewardIdentityV1(ctx, view.Candidate, obs)
	} else {
		err = verifier.VerifyMatureRewardV1(ctx, view.Candidate, obs)
	}
	if err != nil || rewardIdentity == (common.Hash{}) {
		if err != nil {
			return false, err
		}
		return false, errors.New("reward identity unavailable")
	}
	blockKey := hex.EncodeToString(view.Candidate.ExpectedBlockHash[:])
	var existing bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_reward_observations_v1.allocations WHERE candidate_id=$1)`, id[:]).Scan(&existing); err != nil {
		return false, err
	}
	if existing {
		var claimed []byte
		if err = tx.QueryRow(ctx, `SELECT evm_block_hash FROM pool_reward_observations_v1.reward_evm_claims WHERE candidate_id=$1`, id[:]).Scan(&claimed); err != nil || !bytes.Equal(claimed, rewardIdentity[:]) {
			return false, errors.New("existing reward identity differs")
		}
		if err = verifyRewardAllocationV1(ctx, tx, id, wallet, digest, storedWork, blockKey, view.Candidate.Height.Int64(), gross.String(), fee.String(), distributable.String(), totalWork, lines); err != nil {
			return false, err
		}
		return false, nil
	}
	if !allowCreate {
		return false, errors.New("reward allocation is not present")
	}
	if rewardIdentity != obs.EVMBlockHash {
		aliasVerifier, ok := verifier.(reorderedAliasVerifierV1)
		if !ok {
			return false, errors.New("reordered reward alias verification unavailable")
		}
		if err = aliasVerifier.verifyNoReorderedAliasesV1(ctx, tx, rewardIdentity); err != nil {
			return false, err
		}
		identityVerifier := verifier.(matureRewardIdentityVerifierV1)
		again, verifyErr := identityVerifier.VerifyMatureRewardIdentityV1(ctx, view.Candidate, obs)
		if verifyErr != nil || again != rewardIdentity {
			return false, ErrRewardEvidenceStale
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.reward_evm_claims(evm_block_hash,candidate_id) VALUES($1,$2)`, rewardIdentity[:], id[:]); err != nil {
		return false, errors.New("reward identity already claimed or unavailable")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO public.blocks(hash,height,reward,fees,status) VALUES($1,$2,$3::numeric,$4::numeric,'MATURE')`, blockKey, view.Candidate.Height.Int64(), gross.String(), fee.String()); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.allocations(candidate_id,wallet,evidence_digest,work_digest,gross_wei,fee_wei,distributable_wei,total_work,line_count) VALUES($1,$2,$3,$4,$5::numeric,$6::numeric,$7::numeric,$8::numeric,$9)`, id[:], wallet[:], digest, storedWork, gross.String(), fee.String(), distributable.String(), strconv.FormatUint(totalWork, 10), len(lines)); err != nil {
		return false, err
	}
	for _, line := range lines {
		var creditID int64
		if err = tx.QueryRow(ctx, `INSERT INTO public.credits(block_hash,miner_address,amount,is_paid) VALUES($1,$2,$3::numeric,FALSE) RETURNING id`, blockKey, line.miner, line.amount).Scan(&creditID); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.credit_bindings(credit_id,candidate_id,wallet,miner_address,work,amount_wei) VALUES($1,$2,$3,$4,$5::numeric,$6::numeric)`, creditID, id[:], wallet[:], line.miner, strconv.FormatUint(line.work, 10), line.amount); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, errors.New("reward allocation commit unproven")
	}
	return true, nil
}

func verifyRewardAllocationV1(ctx context.Context, tx pgx.Tx, id [32]byte, wallet common.Address, evidence, workDigest []byte, blockKey string, height int64, gross, fee, distributable string, total uint64, lines []rewardCreditLineV1) error {
	var matches bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pool_reward_observations_v1.allocations a JOIN public.blocks b ON b.hash=$7 WHERE a.candidate_id=$1 AND a.wallet=$2 AND a.evidence_digest=$3 AND a.work_digest=$4 AND a.gross_wei=$5::numeric AND a.fee_wei=$6::numeric AND a.distributable_wei=$8::numeric AND a.total_work=$9::numeric AND a.line_count=$10 AND b.reward=a.gross_wei AND b.fees=a.fee_wei AND b.height=$11 AND (SELECT count(*) FROM public.credits WHERE block_hash=$7)=$10)`, id[:], wallet[:], evidence, workDigest, gross, fee, blockKey, distributable, strconv.FormatUint(total, 10), len(lines), height).Scan(&matches)
	if err != nil || !matches {
		return errors.New("existing reward allocation differs")
	}
	rows, err := tx.Query(ctx, `SELECT x.miner_address,x.work::text,x.amount_wei::text,c.block_hash,c.miner_address,c.amount::text,x.wallet FROM pool_reward_observations_v1.credit_bindings x JOIN public.credits c ON c.id=x.credit_id WHERE x.candidate_id=$1 ORDER BY x.miner_address`, id[:])
	if err != nil {
		return err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var miner, work, amount, block, creditMiner, creditAmount string
		var owner []byte
		if err = rows.Scan(&miner, &work, &amount, &block, &creditMiner, &creditAmount, &owner); err != nil {
			return err
		}
		if i >= len(lines) || miner != lines[i].miner || work != strconv.FormatUint(lines[i].work, 10) || amount != lines[i].amount || block != blockKey || creditMiner != miner || creditAmount != amount || !bytes.Equal(owner, wallet[:]) {
			return errors.New("existing reward credit differs")
		}
		i++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if i != len(lines) {
		return errors.New("existing reward credit count differs")
	}
	return nil
}
