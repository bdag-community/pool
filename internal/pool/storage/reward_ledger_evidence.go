package storage

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
)

// RewardLedgerEvidenceV1 is a consistency check for one candidate, accepted
// accounting effect, credit, and proposed reward wallet. It is not authority
// to sign or broadcast: callers must read the credit row under the appropriate
// database lock and independently prove the evidence came from the reviewed
// immutable schema, establish unique ownership, prove canonical reward
// eligibility on the chain, and fence the payer. The credit ID is not itself
// included in the accounting intent and is not authenticated by this check.
type RewardLedgerEvidenceV1 struct {
	Candidate       FrozenCandidateV1
	Accounting      AcceptedAccountingIntentV2
	ReceiptHash     [32]byte
	EffectDigest    [32]byte
	EffectBlockHash [32]byte
	EffectLineCount int64
	CreditID        int64
	CreditBlockHash string
	CreditAddress   string
	CreditAmountWei string
	RewardWallet    [20]byte
}

func addressBytesV1(raw string) ([20]byte, error) {
	var out [20]byte
	if len(raw) != 42 || !strings.EqualFold(raw[:2], "0x") {
		return out, errors.New("invalid credit address")
	}
	b, err := hex.DecodeString(raw[2:])
	if err != nil || len(b) != len(out) {
		return out, errors.New("invalid credit address")
	}
	copy(out[:], b)
	if out == ([20]byte{}) {
		return out, errors.New("zero credit address")
	}
	return out, nil
}

// VerifyRewardLedgerEvidenceV1 verifies only immutable ledger consistency and
// candidate header wallet binding. The receipt alone is not proof that the
// block remains canonical, that its reward was credited to the wallet, or
// that the accounting amount equals the liquid proceeds of that reward.
func VerifyRewardLedgerEvidenceV1(e RewardLedgerEvidenceV1) error {
	c, a := e.Candidate, e.Accounting
	if err := validateFrozenCandidate(c); err != nil {
		return errors.New("invalid frozen reward candidate")
	}
	if c.ChainID == nil || c.ChainID.Cmp(big.NewInt(1404)) != 0 || e.RewardWallet == ([20]byte{}) || !bytes.Equal(c.HeaderBytes[148:168], e.RewardWallet[:]) {
		return errors.New("reward wallet or chain does not match candidate header")
	}
	if err := validateAcceptedAccountingIntentV2(a); err != nil {
		return errors.New("invalid accepted accounting intent")
	}
	if a.CandidateID != c.CandidateID || a.LegacyBlockHash != c.SharePowHash || c.Height == nil || !c.Height.IsUint64() || a.Height != c.Height.Uint64() ||
		e.ReceiptHash != c.ExpectedBlockHash || e.EffectDigest != a.IntentDigest || e.EffectBlockHash != a.LegacyBlockHash || e.EffectLineCount != int64(len(a.CreditLines)) {
		return errors.New("candidate, receipt, and accounting identities do not match")
	}
	block, err := hex.DecodeString(e.CreditBlockHash)
	if err != nil || len(block) != 32 || !bytes.Equal(block, a.LegacyBlockHash[:]) || e.CreditID <= 0 {
		return errors.New("credit block identity is not bound to the accounting effect")
	}
	address, err := addressBytesV1(e.CreditAddress)
	if err != nil {
		return err
	}
	amount, err := canonicalWei(e.CreditAmountWei)
	if err != nil || amount.Sign() <= 0 {
		return errors.New("credit amount must be canonical positive wei")
	}
	matches := 0
	for _, line := range a.CreditLines {
		miner, err := addressBytesV1(line.MinerAddress)
		if err != nil {
			return errors.New("accounting intent contains invalid miner identity")
		}
		if miner == address && line.Amount == e.CreditAmountWei {
			matches++
		}
	}
	if matches != 1 {
		return errors.New("credit is missing or ambiguous in the accounting intent")
	}
	return nil
}
