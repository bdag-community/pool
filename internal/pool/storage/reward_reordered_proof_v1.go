package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"golang.org/x/sys/unix"
)

// A recovery certificate contains only public Merkle proofs. Neither an
// external RPC response nor this file is trusted as a payout authorization.
type reorderedRewardProofV1 struct {
	Version     int        `json:"version"`
	CandidateID string     `json:"candidate_id"`
	NativeOrder uint64     `json:"native_order"`
	Proofs      [][]string `json:"account_proofs"`
}

type rewardNativeStateV1 struct {
	Hash      common.Hash `json:"Hash"`
	Order     uint64      `json:"Order"`
	Height    uint64      `json:"Height"`
	Valid     bool        `json:"Valid"`
	EVMRoot   common.Hash `json:"EVMStateRoot"`
	EVMHeight uint64      `json:"EVMHeight"`
	EVMHead   common.Hash `json:"EVMHead"`
}

type rewardProofBlockV1 struct {
	Hash         common.Hash    `json:"hash"`
	ParentHash   common.Hash    `json:"parentHash"`
	StateRoot    common.Hash    `json:"stateRoot"`
	Miner        common.Address `json:"miner"`
	Number       hexutil.Uint64 `json:"number"`
	Transactions []common.Hash  `json:"transactions"`
	Extra        hexutil.Bytes  `json:"extraData,omitempty"`
	Time         hexutil.Uint64 `json:"timestamp,omitempty"`
}

func readReorderedProofV1(directory string, id [32]byte) (reorderedRewardProofV1, error) {
	var result reorderedRewardProofV1
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return result, errors.New("reward recovery directory unavailable")
	}
	resolved, err := filepath.EvalSymlinks(directory)
	info, statErr := os.Stat(directory)
	owner, ok := func() (*syscall.Stat_t, bool) {
		if info == nil {
			return nil, false
		}
		x, yes := info.Sys().(*syscall.Stat_t)
		return x, yes
	}()
	if err != nil || statErr != nil || resolved != directory || !info.IsDir() || info.Mode().Perm()&0022 != 0 || !ok || owner.Uid != uint32(os.Geteuid()) {
		return result, errors.New("reward recovery directory unsafe")
	}
	name := hex.EncodeToString(id[:]) + ".json"
	fd, err := unix.Open(filepath.Join(directory, name), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return result, err
	}
	file := os.NewFile(uintptr(fd), "reward-recovery-proof")
	defer file.Close()
	var fileStat unix.Stat_t
	if unix.Fstat(fd, &fileStat) != nil || fileStat.Mode&unix.S_IFMT != unix.S_IFREG || fileStat.Nlink != 1 || fileStat.Uid != uint32(os.Geteuid()) || fileStat.Mode&0022 != 0 || fileStat.Size < 1 || fileStat.Size > 262144 {
		return result, errors.New("reward recovery proof file unsafe")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 262145))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&result); err != nil {
		return result, err
	}
	if decoder.Decode(new(any)) != io.EOF || result.Version != 1 || result.CandidateID != hex.EncodeToString(id[:]) || result.NativeOrder < 1 || result.NativeOrder > math.MaxInt64 || len(result.Proofs) != 2 {
		return reorderedRewardProofV1{}, errors.New("reward recovery proof invalid")
	}
	return result, nil
}

func provenAccountBalanceV1(root common.Hash, address common.Address, nodes []string) (*big.Int, error) {
	if root == (common.Hash{}) || address == (common.Address{}) || len(nodes) == 0 || len(nodes) > 256 {
		return nil, errors.New("reward account proof invalid")
	}
	db := memorydb.New()
	for _, encoded := range nodes {
		node, err := hexutil.Decode(encoded)
		if err != nil || len(node) == 0 || len(node) > 32768 {
			return nil, errors.New("reward proof node invalid")
		}
		if err = db.Put(crypto.Keccak256(node), node); err != nil {
			return nil, err
		}
	}
	value, err := trie.VerifyProof(root, crypto.Keccak256(address.Bytes()), db)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return new(big.Int), nil
	}
	var account types.StateAccount
	if err = rlp.DecodeBytes(value, &account); err != nil || account.Balance == nil || len(account.CodeHash) != common.HashLength || common.BytesToHash(account.CodeHash) != types.EmptyCodeHash {
		return nil, errors.New("reward proof is not a plain wallet account")
	}
	return account.Balance.ToBig(), nil
}

func (v RPCMatureRewardVerifierV1) verifyReorderedRewardV1(ctx context.Context, candidate FrozenCandidateV1, obs RewardObservationV1) (common.Hash, error) {
	proof, err := readReorderedProofV1(v.RecoveryDirectory, candidate.CandidateID)
	if err != nil {
		return common.Hash{}, err
	}
	if obs.OtherFeesWei != "0" || obs.SelfPaidFeesWei != "0" {
		return common.Hash{}, errors.New("transaction-fee reward requires replay")
	}
	var previous, current rewardNativeStateV1
	if err = v.Native.CallContext(ctx, &previous, "getStateRoot", int64(proof.NativeOrder-1), true); err != nil {
		return common.Hash{}, err
	}
	if err = v.Native.CallContext(ctx, &current, "getStateRoot", int64(proof.NativeOrder), true); err != nil {
		return common.Hash{}, err
	}
	if current.Hash != obs.NativeBlockHash || current.Order != proof.NativeOrder || !current.Valid || current.Height != obs.NativeHeight || previous.Order+1 != current.Order || previous.EVMHeight == math.MaxUint64 || previous.EVMHeight+1 != current.EVMHeight || previous.EVMHead == (common.Hash{}) || current.EVMHead == (common.Hash{}) || previous.EVMRoot == (common.Hash{}) || current.EVMRoot == (common.Hash{}) {
		return common.Hash{}, errors.New("native reward identity changed")
	}
	var parent, block *rewardProofBlockV1
	if err = v.EVM.CallContext(ctx, &parent, "eth_getBlockByNumber", hexutil.EncodeUint64(previous.EVMHeight), false); err != nil {
		return common.Hash{}, err
	}
	if err = v.EVM.CallContext(ctx, &block, "eth_getBlockByNumber", hexutil.EncodeUint64(current.EVMHeight), false); err != nil {
		return common.Hash{}, err
	}
	if parent == nil || block == nil || parent.Hash != previous.EVMHead || parent.StateRoot != previous.EVMRoot || uint64(parent.Number) != previous.EVMHeight || block.Hash != current.EVMHead || block.StateRoot != current.EVMRoot || uint64(block.Number) != current.EVMHeight || block.ParentHash != parent.Hash || block.Miner != obs.Wallet || block.Transactions == nil || len(block.Transactions) != 0 {
		return common.Hash{}, errors.New("canonical EVM reward identity differs")
	}
	before, err := provenAccountBalanceV1(parent.StateRoot, obs.Wallet, proof.Proofs[0])
	if err != nil {
		return common.Hash{}, err
	}
	after, err := provenAccountBalanceV1(block.StateRoot, obs.Wallet, proof.Proofs[1])
	if err != nil {
		return common.Hash{}, err
	}
	want, ok := new(big.Int).SetString(obs.SubsidyWei, 10)
	if !ok || want.Sign() <= 0 || new(big.Int).Sub(after, before).Cmp(want) != 0 {
		return common.Hash{}, errors.New("proven reward differs from recorded subsidy")
	}
	var final rewardNativeStateV1
	if err = v.Native.CallContext(ctx, &final, "getStateRoot", int64(proof.NativeOrder), true); err != nil || final != current {
		return common.Hash{}, errors.New("native reward changed during proof verification")
	}
	var finalBlock *rewardProofBlockV1
	if err = v.EVM.CallContext(ctx, &finalBlock, "eth_getBlockByNumber", hexutil.EncodeUint64(current.EVMHeight), false); err != nil || finalBlock == nil || finalBlock.Hash != block.Hash || finalBlock.StateRoot != block.StateRoot {
		return common.Hash{}, errors.New("EVM reward changed during proof verification")
	}
	return current.EVMHead, nil
}
