package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestCreateWalletProducesMatchingPrivateConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	address, err := createWallet(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("wallet configuration permissions are not 0600")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			values[key] = value
		}
	}
	key, err := crypto.HexToECDSA(values["POOL_PRIVATE_KEY"])
	if err != nil || crypto.PubkeyToAddress(key.PublicKey).Hex() != address || values["POOL_COINBASE_ADDRESS"] != address || values["MINING_POOL_ADDRESS"] != address {
		t.Fatal("wallet public address and private key do not match")
	}
}

func TestCreateWalletNeverReplacesExistingConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	original := []byte("operator configuration\n")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := createWallet(path); err == nil {
		t.Fatal("existing configuration was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(original) {
		t.Fatal("existing configuration changed")
	}
}

func TestCreateWalletRejectsUnsafeParentAndSymlink(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "shared")
	if err := os.Mkdir(shared, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0777); err != nil {
		t.Fatal(err)
	}
	if _, err := createWallet(filepath.Join(shared, ".env")); err == nil {
		t.Fatal("accepted a directory writable by others")
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if _, err := createWallet(filepath.Join(link, ".env")); err == nil {
		t.Fatal("accepted a symlinked parent directory")
	}
}

func TestCreateWalletRejectsDirectoryOwnedByAnotherUser(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root owns the root directory")
	}
	if _, err := createWallet("/.pool-wallet-must-not-be-created"); err == nil {
		t.Fatal("accepted a directory owned by another user")
	}
}
