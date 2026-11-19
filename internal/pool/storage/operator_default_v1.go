package storage

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strings"

	"asicPool/internal/pool/sharejournal"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// Only durable attribution failures qualify. A timeout, database outage, or
// failed chain/wallet verification remains a retry, never an operator default.
func operatorAttributionUnavailableV1(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sharejournal.ErrShareCutMissing) {
		return true
	}
	message := err.Error()
	return (strings.HasPrefix(message, "unpriced verification failed ") && strings.HasSuffix(message, "unexpected_pricing=0")) ||
		message == "unpriced candidate work digest or total differs" ||
		message == "unpriced discovery share does not bind candidate" ||
		message == "unpriced discovery share does not bind cut" ||
		message == "unpriced verification candidate unavailable"
}

// A coinbase reward is already in the operator's wallet. Record retention
// without inventing a miner credit or sending a transaction to the same wallet.
func retainOperatorRewardV1(ctx context.Context, tx pgx.Tx, view CandidateViewV1, obs RewardObservationV1, wallet common.Address, evidence []byte, verifier MatureRewardVerifierV1, reason string, allowCreate bool) (bool, error) {
	if reason != "no_attributable_work" && reason != "conflicting_or_missing_work" {
		return false, errors.New("invalid operator default reason")
	}
	if err := validateRewardObservationV1(view.Candidate, wallet, obs); err != nil {
		return false, err
	}
	subsidy, ok := new(big.Int).SetString(obs.SubsidyWei, 10)
	if !ok {
		return false, errors.New("operator reward amount invalid")
	}
	fees, ok := new(big.Int).SetString(obs.OtherFeesWei, 10)
	if !ok {
		return false, errors.New("operator reward fees invalid")
	}
	gross := new(big.Int).Add(subsidy, fees)
	if gross.Sign() <= 0 || len(gross.String()) > 30 {
		return false, errors.New("operator reward amount invalid")
	}
	identity := obs.EVMBlockHash
	var err error
	if v, ok := verifier.(matureRewardIdentityVerifierV1); ok {
		identity, err = v.VerifyMatureRewardIdentityV1(ctx, view.Candidate, obs)
	} else {
		err = verifier.VerifyMatureRewardV1(ctx, view.Candidate, obs)
	}
	if err != nil {
		return false, err
	}
	if identity == (common.Hash{}) {
		return false, errors.New("operator reward identity unavailable")
	}
	if identity != obs.EVMBlockHash {
		aliases, ok := verifier.(reorderedAliasVerifierV1)
		if !ok {
			return false, errors.New("operator reward alias verification unavailable")
		}
		if err = aliases.verifyNoReorderedAliasesV1(ctx, tx, identity); err != nil {
			return false, err
		}
		again, e := verifier.(matureRewardIdentityVerifierV1).VerifyMatureRewardIdentityV1(ctx, view.Candidate, obs)
		if e != nil || again != identity {
			return false, ErrRewardEvidenceStale
		}
	}
	var storedWallet, storedHash, storedEvidence []byte
	var storedGross, storedReason string
	err = tx.QueryRow(ctx, `SELECT wallet,evm_block_hash,evidence_digest,gross_wei::text,reason FROM pool_reward_observations_v1.operator_defaults_v1 WHERE candidate_id=$1`, view.Candidate.CandidateID[:]).Scan(&storedWallet, &storedHash, &storedEvidence, &storedGross, &storedReason)
	if err == nil {
		if !bytes.Equal(storedWallet, wallet[:]) || !bytes.Equal(storedHash, identity[:]) || !bytes.Equal(storedEvidence, evidence) || storedGross != gross.String() || storedReason != reason {
			return false, errors.New("existing operator default differs")
		}
		return false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if !allowCreate {
		return false, errors.New("operator default not present")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.reward_evm_claims(evm_block_hash,candidate_id) VALUES($1,$2)`, identity[:], view.Candidate.CandidateID[:]); err != nil {
		return false, errors.New("operator reward identity already claimed or unavailable")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pool_reward_observations_v1.operator_defaults_v1(candidate_id,wallet,evm_block_hash,evidence_digest,gross_wei,reason) VALUES($1,$2,$3,$4,$5::numeric,$6)`, view.Candidate.CandidateID[:], wallet[:], identity[:], evidence, gross.String(), reason); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, errors.New("operator default commit unproven")
	}
	return true, nil
}
