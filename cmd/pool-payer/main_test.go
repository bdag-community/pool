package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestReadWalletRequiresMatchingPrivateKeyAndAddress(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	keyText := strings.Repeat("0", 63) + "1" // public test key
	key, err := crypto.HexToECDSA(keyText)
	if err != nil {
		t.Fatal(err)
	}
	address := crypto.PubkeyToAddress(key.PublicKey)
	path := filepath.Join(dir, ".env")
	content := "POOL_COINBASE_ADDRESS=" + address.Hex() + "\nMINING_POOL_ADDRESS=" + address.Hex() + "\nPOOL_PRIVATE_KEY=" + keyText + "\n"
	if err = os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	data, got, _, err := readWallet(path)
	clear(data)
	if err != nil || got != address {
		t.Fatalf("valid wallet rejected: %v", err)
	}
	if err = os.WriteFile(path, []byte(strings.Replace(content, address.Hex(), "0x1111111111111111111111111111111111111111", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = readWallet(path); err == nil {
		t.Fatal("mismatched wallet accepted")
	}
}
