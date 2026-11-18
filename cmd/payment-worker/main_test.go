package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"asicPool/internal/pool/payments"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type fakeHost struct {
	id         string
	old        predecessor
	pids       []byte
	err        error
	legacyErr  error
	processErr error
}

func (h fakeHost) machine() (string, error)                            { return h.id, h.err }
func (h fakeHost) legacy(context.Context, string) (predecessor, error) { return h.old, h.legacyErr }
func (h fakeHost) processes(string) ([]byte, error)                    { return h.pids, h.processErr }

func approvedFixture(t *testing.T) (approval, fakeHost) {
	t.Helper()
	key, _ := crypto.HexToECDSA(strings.Repeat("0", 63) + "1")
	w := payments.WalletID{ChainID: 1404, Sender: crypto.PubkeyToAddress(key.PublicKey)}
	a := approval{Version: 1, MachineID: strings.Repeat("a", 32), LegacyContainerID: strings.Repeat("b", 64),
		Wallet: w, StatusDir: t.TempDir(), KeyFile: filepath.Join(t.TempDir(), "not-created-key"),
		NativeURL: "http://127.0.0.1:48132", EVMURL: "http://127.0.0.1:18547",
		Baseline: payments.LegacyBaseline{Wallet: w, NonceFloor: 10}}
	if err := os.Chmod(a.StatusDir, 0700); err != nil {
		t.Fatal(err)
	}
	return a, fakeHost{id: a.MachineID, old: predecessor{ID: a.LegacyContainerID}}
}

func approvalFile(t *testing.T, a approval) (string, string) {
	t.Helper()
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "approval.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return path, hex.EncodeToString(sum[:])
}

func TestAuthorityRequiresActualStoppedPIDAndEmptyCgroupBeforeActivation(t *testing.T) {
	for _, mode := range []string{"active", "inactive", "running", "nonzero-pid", "child-in-cgroup", "wrong-host", "wrong-id", "inspect-error", "cgroup-error", "machine-error", "unbounded"} {
		t.Run(mode, func(t *testing.T) {
			a, h := approvedFixture(t)
			a.ActivationApproved = true
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch mode {
			case "inactive":
				a.ActivationApproved = false
			case "running":
				h.old.Running = true
			case "nonzero-pid":
				h.old.PID = 321
			case "child-in-cgroup":
				h.pids = []byte("321\n")
			case "wrong-host":
				h.id = strings.Repeat("c", 32)
			case "wrong-id":
				h.old.ID = strings.Repeat("c", 64)
			case "inspect-error":
				h.legacyErr = errors.New("unavailable")
			case "cgroup-error":
				h.processErr = errors.New("unavailable")
			case "machine-error":
				h.err = errors.New("unavailable")
			case "unbounded":
				ctx = context.Background()
			}
			err := authorityCheck(ctx, a, h)
			if (err == nil) != (mode == "active") {
				t.Fatal(mode, err)
			}
		})
	}
}

func TestCheckModeNeverReadsKeyOrConnectsDatabaseEvenWhenActivated(t *testing.T) {
	for _, active := range []bool{false, true} {
		a, h := approvedFixture(t)
		a.ActivationApproved = active
		a.KeyFileSHA256 = strings.Repeat("d", 64)
		a.KeyValueSHA256 = strings.Repeat("e", 64)
		path, sum := approvalFile(t, a)
		for _, oldAlive := range []bool{false, true} {
			h.old.Running = oldAlive
			var out bytes.Buffer
			if err := runWithHost(context.Background(), []string{"check", path, sum}, &out, h); err != nil {
				t.Fatal(err)
			}
			var result map[string]any
			if json.Unmarshal(out.Bytes(), &result) != nil || result["key_loaded"] != false || result["send_attempted"] != false || result["production_ready"] != false {
				t.Fatal(out.String())
			}
			if oldAlive && result["state"] != "awaiting_legacy_shutdown" {
				t.Fatal(out.String())
			}
			if _, err := os.Stat(a.KeyFile); !os.IsNotExist(err) {
				t.Fatal("test must use absent key file")
			}
		}
	}
}

type cancelWriter struct {
	cancel context.CancelFunc
	bytes.Buffer
}

func (w *cancelWriter) Write(data []byte) (int, error) {
	n, e := w.Buffer.Write(data)
	w.cancel()
	return n, e
}

func TestInactiveSupervisedLoopRemainsNonSigningAndCancelsCleanly(t *testing.T) {
	a, h := approvedFixture(t)
	path, sum := approvalFile(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &cancelWriter{cancel: cancel}
	if err := runWithHost(ctx, []string{"run", path, sum}, out, h); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(a.StatusDir, "status.json"))
	if err != nil || !bytes.Contains(data, []byte("awaiting_explicit_activation")) {
		t.Fatal(string(data), err)
	}
	// Cancellation released the same lifetime lock; no DB URL or key exists.
	lock, err := lockRuntime(a.StatusDir)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
}

func TestPrivateApprovalAndLifetimeLockRejectAliasesDriftAndCompetition(t *testing.T) {
	a, _ := approvedFixture(t)
	path, sum := approvalFile(t, a)
	if _, err := readApproval(path, sum); err != nil {
		t.Fatal(err)
	}
	if _, err := readApproval(path, strings.Repeat("0", 64)); err == nil {
		t.Fatal("approval drift accepted")
	}
	link := filepath.Join(t.TempDir(), "linked-approval")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readApproval(link, sum); err == nil {
		t.Fatal("approval symlink accepted")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readApproval(path, sum); err == nil {
		t.Fatal("public approval accepted")
	}
	first, err := lockRuntime(a.StatusDir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := lockRuntime(a.StatusDir); err == nil {
		second.Close()
		t.Fatal("second payer acquired lifetime lock")
	}
}

func TestApprovedKeyFactoryRejectsChangedBytesAndDiscardsLocalSigner(t *testing.T) {
	a, _ := approvedFixture(t)
	a.ActivationApproved = true
	secret := strings.Repeat("0", 63) + "1"
	data := []byte("POOL_PRIVATE_KEY=" + secret + "\n")
	fileSum := sha256.Sum256(data)
	valueSum := sha256.Sum256([]byte(secret))
	a.KeyFileSHA256 = hex.EncodeToString(fileSum[:])
	a.KeyValueSHA256 = hex.EncodeToString(valueSum[:])
	reader := func(string) ([]byte, error) { return append([]byte(nil), data...), nil }
	signer, err := signerFactory(a, reader)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	local := signer.(*payments.LocalSigner)
	local.Close()
	local.Close()
	if _, err := local.Sign(context.Background(), payments.Credit{}, 0, nil); err == nil {
		t.Fatal("closed signer usable")
	}
	a.KeyValueSHA256 = strings.Repeat("d", 64)
	if _, err := signerFactory(a, reader)(context.Background()); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("changed key accepted or leaked")
	}
	a.ActivationApproved = false
	calls := 0
	if _, err := signerFactory(a, func(string) ([]byte, error) { calls++; return nil, nil })(context.Background()); err == nil || calls != 0 {
		t.Fatal("inactive factory read key")
	}
}

func TestApprovedKeyFactoryAcceptsMatchingWalletEnvAndRejectsDrift(t *testing.T) {
	a, _ := approvedFixture(t)
	a.ActivationApproved = true
	secret := strings.Repeat("0", 63) + "1"
	valueSum := sha256.Sum256([]byte(secret))
	a.KeyValueSHA256 = hex.EncodeToString(valueSum[:])
	address := a.Wallet.Sender.Hex()
	for _, tc := range []struct {
		name, data string
		wantErr    bool
	}{
		{"matching", "# wallet\nPOOL_PRIVATE_KEY=" + secret + "\nPOOL_COINBASE_ADDRESS=" + address + "\nMINING_POOL_ADDRESS=" + address + "\n", false},
		{"wrong address", "POOL_PRIVATE_KEY=" + secret + "\nPOOL_COINBASE_ADDRESS=0x1111111111111111111111111111111111111111\n", true},
		{"duplicate address", "POOL_PRIVATE_KEY=" + secret + "\nMINING_POOL_ADDRESS=" + address + "\nMINING_POOL_ADDRESS=" + address + "\n", true},
		{"unrecognized field", "POOL_PRIVATE_KEY=" + secret + "\nUNRELATED=value\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(tc.data)
			fileSum := sha256.Sum256(data)
			a.KeyFileSHA256 = hex.EncodeToString(fileSum[:])
			signer, err := signerFactory(a, func(string) ([]byte, error) { return append([]byte(nil), data...), nil })(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("factory error = %v, want error %t", err, tc.wantErr)
			}
			if signer != nil {
				signer.(*payments.LocalSigner).Close()
			}
		})
	}
}

func TestApprovalRejectsUnsafeTargetAndLegacyFloor(t *testing.T) {
	for _, mode := range []string{"remote-rpc", "low-floor", "wrong-baseline-wallet", "active-without-key-hashes"} {
		a, _ := approvedFixture(t)
		switch mode {
		case "remote-rpc":
			a.EVMURL = "http://192.168.100.16:8545"
		case "low-floor":
			a.Baseline.NonceFloor = 1
		case "wrong-baseline-wallet":
			a.Baseline.Wallet.Sender = common.Address{}
		case "active-without-key-hashes":
			a.ActivationApproved = true
		}
		path, sum := approvalFile(t, a)
		if _, err := readApproval(path, sum); err == nil {
			t.Fatal("unsafe approval accepted", mode)
		}
	}
}
