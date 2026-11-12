package server

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	lastVerifiedTemplateCoinbaseMu sync.RWMutex
	lastVerifiedTemplateCoinbase   string
)

func configurePoolCoinbaseAddress(cfg Config) error {
	explicit := strings.TrimSpace(cfg.PoolCoinbaseAddress)
	if explicit != "" {
		normalized, err := normalizePoolCoinbaseAddress(explicit)
		if err != nil {
			return fmt.Errorf("invalid pool coinbase address %q: %w", explicit, err)
		}
		poolAddress = normalized
		rememberVerifiedTemplateCoinbase(normalized)
		log.Printf("✅ Pool Coinbase Address Configured: %s", poolAddress)
		return nil
	}

	if strings.TrimSpace(cfg.PoolPrivateKey) != "" {
		normalized, err := derivePoolCoinbaseAddress(cfg.PoolPrivateKey)
		if err != nil {
			return fmt.Errorf("derive pool coinbase from private key: %w", err)
		}
		poolAddress = normalized
		rememberVerifiedTemplateCoinbase(normalized)
		log.Printf("✅ Pool Coinbase Address Derived: %s", poolAddress)
		return nil
	}

	return fmt.Errorf("pool coinbase address is required: set POOL_COINBASE_ADDRESS, MINING_POOL_ADDRESS, or MINING_ADDRESS")
}

func derivePoolCoinbaseAddress(privateKeyHex string) (string, error) {
	pkHex := strings.TrimSpace(privateKeyHex)
	pkHex = strings.TrimPrefix(pkHex, "0x")
	privateKey, err := crypto.HexToECDSA(pkHex)
	if err != nil {
		return "", err
	}
	addr := crypto.PubkeyToAddress(privateKey.PublicKey)
	return normalizePoolCoinbaseAddress(addr.Hex())
}

func normalizePoolCoinbaseAddress(raw string) (string, error) {
	addr, err := decodePoolCoinbaseAddress(raw)
	if err != nil {
		return "", err
	}
	return addr.Hex(), nil
}

func decodePoolCoinbaseAddress(raw string) (common.Address, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return common.Address{}, fmt.Errorf("empty address")
	}
	trimmed := strings.TrimPrefix(text, "0x")
	trimmed = strings.TrimPrefix(trimmed, "0X")
	if len(trimmed) != 40 {
		return common.Address{}, fmt.Errorf("address must be 20 bytes")
	}
	if _, err := hex.DecodeString(trimmed); err != nil {
		return common.Address{}, fmt.Errorf("decode address: %w", err)
	}
	addr := common.HexToAddress("0x" + trimmed)
	if addr == (common.Address{}) {
		return common.Address{}, fmt.Errorf("zero address is not a valid mining coinbase")
	}
	return addr, nil
}

func configuredPoolCoinbaseAddress() (common.Address, string, error) {
	normalized, err := normalizePoolCoinbaseAddress(poolAddress)
	if err != nil {
		return common.Address{}, "", fmt.Errorf("configured pool coinbase is invalid: %w", err)
	}
	addr := common.HexToAddress(normalized)
	return addr, normalized, nil
}

func enforceTemplateCoinbase(tpl *BlockTemplate, source string) error {
	if tpl == nil {
		return fmt.Errorf("nil block template")
	}
	configuredAddr, configuredHex, err := configuredPoolCoinbaseAddress()
	if err != nil {
		recordTemplateCoinbaseEvent("rejected", source)
		return err
	}

	raw := strings.TrimSpace(tpl.CoinbaseAddress)
	if raw == "" {
		tpl.CoinbaseAddress = configuredHex
		rememberVerifiedTemplateCoinbase(configuredHex)
		recordTemplateCoinbaseEvent("filled_missing", source)
		return nil
	}

	templateAddr, err := decodePoolCoinbaseAddress(raw)
	if err != nil {
		recordTemplateCoinbaseEvent("rejected", source)
		return fmt.Errorf("invalid template coinbase from %s: %w", source, err)
	}
	if templateAddr != configuredAddr {
		recordTemplateCoinbaseEvent("rejected_mismatch", source)
		return fmt.Errorf("template coinbase mismatch from %s: got %s want %s", source, templateAddr.Hex(), configuredHex)
	}

	tpl.CoinbaseAddress = configuredHex
	rememberVerifiedTemplateCoinbase(configuredHex)
	recordTemplateCoinbaseEvent("verified", source)
	return nil
}

func templateCoinbaseBytes(tpl *BlockTemplate, source string) ([]byte, error) {
	if err := enforceTemplateCoinbase(tpl, source); err != nil {
		return nil, err
	}
	addr, err := decodePoolCoinbaseAddress(tpl.CoinbaseAddress)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(addr.Bytes()))
	copy(out, addr.Bytes())
	return out, nil
}

func jobCoinbaseBytes(job *JobData, source string) ([]byte, error) {
	if job == nil {
		recordTemplateCoinbaseEvent("rejected", source)
		return nil, fmt.Errorf("nil job")
	}
	if len(job.CoinbaseAddress) != 20 {
		recordTemplateCoinbaseEvent("rejected", source)
		return nil, fmt.Errorf("job coinbase must be 20 bytes, got %d", len(job.CoinbaseAddress))
	}
	configuredAddr, configuredHex, err := configuredPoolCoinbaseAddress()
	if err != nil {
		recordTemplateCoinbaseEvent("rejected", source)
		return nil, err
	}
	var jobAddr common.Address
	copy(jobAddr[:], job.CoinbaseAddress)
	if jobAddr == (common.Address{}) {
		recordTemplateCoinbaseEvent("rejected_zero", source)
		return nil, fmt.Errorf("job coinbase is zero address")
	}
	if jobAddr != configuredAddr {
		recordTemplateCoinbaseEvent("rejected_mismatch", source)
		return nil, fmt.Errorf("job coinbase mismatch: got %s want %s", jobAddr.Hex(), configuredHex)
	}
	out := make([]byte, len(job.CoinbaseAddress))
	copy(out, job.CoinbaseAddress)
	recordTemplateCoinbaseEvent("verified", source)
	return out, nil
}

func validateSubmittedHeaderCoinbase(fullHeaderHex string) error {
	raw := strings.TrimPrefix(strings.TrimSpace(fullHeaderHex), "0x")
	header, err := hex.DecodeString(raw)
	if err != nil {
		recordTemplateCoinbaseEvent("rejected", "submit_header")
		return fmt.Errorf("decode submitted header: %w", err)
	}
	if len(header) < 168 {
		recordTemplateCoinbaseEvent("rejected", "submit_header")
		return fmt.Errorf("submitted header too short for coinbase: %d bytes", len(header))
	}
	job := &JobData{CoinbaseAddress: header[148:168]}
	_, err = jobCoinbaseBytes(job, "submit_header")
	return err
}

func rememberVerifiedTemplateCoinbase(address string) {
	lastVerifiedTemplateCoinbaseMu.Lock()
	lastVerifiedTemplateCoinbase = address
	lastVerifiedTemplateCoinbaseMu.Unlock()
}

func lastKnownTemplateCoinbaseAddress() string {
	lastVerifiedTemplateCoinbaseMu.RLock()
	defer lastVerifiedTemplateCoinbaseMu.RUnlock()
	if lastVerifiedTemplateCoinbase != "" {
		return lastVerifiedTemplateCoinbase
	}
	return poolAddress
}

func recordTemplateCoinbaseEvent(event, source string) {
	if templateCoinbaseEvents != nil {
		templateCoinbaseEvents.WithLabelValues(event, source).Inc()
	}
}
