// pool-proof-recover records proven transaction-free rewards whose local
// historical state was pruned. It never holds a wallet key or sends a payment.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"asicPool/internal/pool/storage"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

type item struct {
	CandidateID    string `json:"candidate_id"`
	Classification string `json:"classification"`
	Order          uint64 `json:"order"`
}
type accountProof struct {
	Address common.Address `json:"address"`
	Proofs  []struct {
		Balance      string   `json:"balance"`
		AccountProof []string `json:"accountProof"`
	} `json:"proofs"`
}
type nativeState struct {
	Hash      common.Hash `json:"Hash"`
	Order     uint64      `json:"Order"`
	Height    uint64      `json:"Height"`
	Valid     bool        `json:"Valid"`
	EVMRoot   common.Hash `json:"EVMStateRoot"`
	EVMHeight uint64      `json:"EVMHeight"`
	EVMHead   common.Hash `json:"EVMHead"`
}

func run() error {
	if len(os.Args) != 3 {
		return errors.New("usage: pool-proof-recover classification.json proof-directory")
	}
	var list struct {
		Records []item `json:"records"`
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &list); err != nil {
		return err
	}
	wallet := common.HexToAddress(os.Getenv("POOL_COINBASE_ADDRESS"))
	dir := os.Getenv("POOL_REWARD_RECOVERY_DIR")
	if wallet == (common.Address{}) || !filepath.IsAbs(dir) || os.Getenv("PG_URL") == "" || os.Getenv("NODE_RPC_URL") == "" || os.Getenv("EVM_RPC_URL") == "" {
		return errors.New("recovery configuration incomplete")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	db := storage.NewPostgresStorage(os.Getenv("PG_URL"))
	if err = db.Connect(ctx); err != nil {
		return err
	}
	defer db.Close()
	auth := base64.StdEncoding.EncodeToString([]byte(os.Getenv("NODE_RPC_USER") + ":" + os.Getenv("NODE_RPC_PASS")))
	native, err := rpc.DialOptions(ctx, os.Getenv("NODE_RPC_URL"), rpc.WithHeader("Authorization", "Basic "+auth))
	if err != nil {
		return errors.New("native RPC unavailable")
	}
	defer native.Close()
	evm, err := rpc.DialContext(ctx, os.Getenv("EVM_RPC_URL"))
	if err != nil {
		return errors.New("EVM RPC unavailable")
	}
	defer evm.Close()
	verifier := storage.RPCMatureRewardVerifierV1{Native: native, EVM: evm, Confirmations: 4096, RecoveryDirectory: dir}
	var created, skipped, failed int
	for _, record := range list.Records {
		if record.Classification != "pool_reward_no_transactions" && record.Classification != "pool_reward_with_transactions" {
			continue
		}
		if selected := os.Getenv("POOL_RECOVERY_ONLY"); selected != "" && selected != record.CandidateID {
			continue
		}
		result, err := recoverOne(ctx, db, native, evm, verifier, wallet, dir, os.Args[2], record)
		if err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "held %s: %v\n", record.CandidateID, err)
		} else if result {
			created++
			fmt.Printf("recorded %s\n", record.CandidateID)
		} else {
			skipped++
		}
	}
	fmt.Printf("proof recovery: recorded=%d already_present=%d held=%d\n", created, skipped, failed)
	if failed != 0 {
		return errors.New("one or more rewards remain held")
	}
	return nil
}

func recoverOne(parent context.Context, db *storage.PostgresStorage, native, evm *rpc.Client, verifier storage.RPCMatureRewardVerifierV1, wallet common.Address, dir, source string, record item) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	idBytes, err := hex.DecodeString(record.CandidateID)
	if err != nil || len(idBytes) != 32 || record.Order == 0 {
		return false, errors.New("candidate identity invalid")
	}
	var id [32]byte
	copy(id[:], idBytes)
	view, err := db.GetCandidate(ctx, id)
	if err != nil || view.Candidate.Height == nil || !view.Candidate.Height.IsUint64() {
		return false, errors.New("accepted candidate unavailable")
	}
	var proof accountProof
	raw, err := os.ReadFile(filepath.Join(source, record.CandidateID+".json"))
	if err != nil || json.Unmarshal(raw, &proof) != nil || len(proof.Proofs) != 2 || proof.Address != wallet {
		return false, errors.New("account proof unavailable")
	}
	before, err := hexutil.DecodeBig(proof.Proofs[0].Balance)
	if err != nil {
		return false, errors.New("parent balance invalid")
	}
	after, err := hexutil.DecodeBig(proof.Proofs[1].Balance)
	if err != nil {
		return false, errors.New("child balance invalid")
	}
	_ = new(big.Int).Sub(after, before) // The storage verifier checks this against the signed transaction.
	certificate := struct {
		Version     int        `json:"version"`
		CandidateID string     `json:"candidate_id"`
		NativeOrder uint64     `json:"native_order"`
		Proofs      [][]string `json:"account_proofs"`
	}{1, record.CandidateID, record.Order, [][]string{proof.Proofs[0].AccountProof, proof.Proofs[1].AccountProof}}
	encoded, err := json.Marshal(certificate)
	if err != nil {
		return false, err
	}
	path := filepath.Join(dir, record.CandidateID+".json")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		prior, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(prior, encoded) {
			return false, errors.New("existing certificate differs")
		}
	} else if err != nil {
		return false, err
	} else {
		if _, err = f.Write(encoded); err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			return false, errors.New("cannot persist certificate")
		}
	}
	var state nativeState
	if err = native.CallContext(ctx, &state, "getStateRoot", int64(record.Order), true); err != nil || !state.Valid || state.Order != record.Order || state.Hash != common.Hash(view.Candidate.ExpectedBlockHash) || state.Height != view.Candidate.Height.Uint64() {
		return false, errors.New("current native reward identity unavailable")
	}
	var block *struct {
		Hash         common.Hash   `json:"hash"`
		Extra        hexutil.Bytes `json:"extraData"`
		Transactions []common.Hash `json:"transactions"`
		BaseFee      *hexutil.Big  `json:"baseFeePerGas"`
	}
	if err = evm.CallContext(ctx, &block, "eth_getBlockByNumber", hexutil.EncodeUint64(state.EVMHeight), false); err != nil || block == nil || block.Hash != state.EVMHead || len(block.Extra) != 8 || block.Transactions == nil || len(block.Transactions) > 1 {
		return false, errors.New("current EVM reward block unavailable")
	}
	subsidy := new(big.Int).SetUint64(binary.BigEndian.Uint64(block.Extra))
	subsidy.Mul(subsidy, big.NewInt(10_000_000_000))
	subsidy.Mul(subsidy, big.NewInt(70))
	subsidy.Div(subsidy, big.NewInt(100))
	other, self := new(big.Int), new(big.Int)
	if len(block.Transactions) == 1 {
		var tx *types.Transaction
		if err = evm.CallContext(ctx, &tx, "eth_getTransactionByHash", block.Transactions[0]); err != nil || tx == nil || tx.Hash() != block.Transactions[0] {
			return false, errors.New("reward transaction unavailable")
		}
		sender, err := types.Sender(types.LatestSignerForChainID(big.NewInt(1404)), tx)
		if err != nil {
			return false, errors.New("reward transaction sender invalid")
		}
		var receipt *struct {
			GasUsed hexutil.Uint64 `json:"gasUsed"`
		}
		if err = evm.CallContext(ctx, &receipt, "eth_getTransactionReceipt", tx.Hash()); err != nil || receipt == nil || receipt.GasUsed == 0 {
			return false, errors.New("reward transaction receipt unavailable")
		}
		base := new(big.Int)
		if block.BaseFee != nil {
			base.Set((*big.Int)(block.BaseFee))
		}
		perGas, err := tx.EffectiveGasTip(base)
		if err != nil || perGas.Sign() < 0 {
			return false, errors.New("reward transaction tip invalid")
		}
		tip := new(big.Int).Mul(perGas, new(big.Int).SetUint64(uint64(receipt.GasUsed)))
		if sender == wallet {
			self.Set(tip)
		} else {
			other.Set(tip)
		}
	}
	obs := storage.RewardObservationV1{NativeBlockHash: state.Hash, NativeOrder: state.Order, NativeHeight: state.Height, EVMBlockHash: state.EVMHead, EVMBlockNumber: state.EVMHeight, EVMStateRoot: state.EVMRoot, Wallet: wallet, SubsidyWei: subsidy.String(), OtherFeesWei: other.String(), SelfPaidFeesWei: self.String()}
	if os.Getenv("POOL_RECOVERY_DRY_RUN") == "1" {
		return db.ValidateProvenUnobservedRewardV1(ctx, id, wallet, obs, verifier)
	}
	return db.RecordProvenUnobservedRewardV1(ctx, id, wallet, obs, verifier)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(err.Error()))
		os.Exit(1)
	}
}
