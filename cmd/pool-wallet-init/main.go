// pool-wallet-init creates an inactive local reward wallet without overwriting
// an existing configuration. It never sends a transaction or contacts an RPC.
package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/ethereum/go-ethereum/crypto"
)

func createWallet(path string) (string, error) {
	fullPath, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("invalid wallet configuration path")
	}
	parent := filepath.Dir(fullPath)
	resolved, err := filepath.EvalSymlinks(parent)
	info, statErr := os.Stat(parent)
	if err != nil || statErr != nil || resolved != parent || !info.IsDir() || info.Mode().Perm()&0022 != 0 {
		return "", errors.New("wallet configuration directory is missing, symlinked, or writable by others")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Geteuid()) {
		return "", errors.New("wallet configuration directory must be owned by the current user")
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		return "", errors.New("wallet key generation failed")
	}
	private := crypto.FromECDSA(key)
	defer clear(private)
	defer key.D.SetInt64(0)
	address := crypto.PubkeyToAddress(key.PublicKey).Hex()
	file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", errors.New("cannot create wallet configuration; existing files are never overwritten")
	}
	payload := make([]byte, 0, 256)
	payload = append(payload, "# Private pool wallet; protect this file and back it up securely.\nPOOL_COINBASE_ADDRESS="...)
	payload = append(payload, address...)
	payload = append(payload, "\nMINING_POOL_ADDRESS="...)
	payload = append(payload, address...)
	payload = append(payload, "\nPOOL_PRIVATE_KEY="...)
	payload = hex.AppendEncode(payload, private)
	payload = append(payload, '\n')
	defer clear(payload)
	if _, err = file.Write(payload); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", errors.New("wallet configuration write failed; inspect the file before retrying")
	}
	dir, err := os.Open(parent)
	if err != nil {
		return "", errors.New("wallet configuration directory sync failed")
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil || closeErr != nil {
		return "", errors.New("wallet configuration directory sync failed")
	}
	return address, nil
}

func main() {
	path := flag.String("env", ".env", "new private wallet configuration file")
	flag.Parse()
	if flag.NArg() != 0 {
		_, _ = fmt.Fprintln(os.Stderr, "unexpected argument")
		os.Exit(2)
	}
	address, err := createWallet(*path)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Pool wallet created: %s\n", address)
}
