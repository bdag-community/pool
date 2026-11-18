package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"asicPool/internal/pool/payments"
	"golang.org/x/sys/unix"
)

var errLegacyAlive = errors.New("legacy payer still running")
var errInactive = errors.New("payer activation not approved")

type approval struct {
	Version            int                     `json:"version"`
	ActivationApproved bool                    `json:"activation_approved"`
	MachineID          string                  `json:"machine_id"`
	LegacyContainerID  string                  `json:"legacy_container_id"`
	Wallet             payments.WalletID       `json:"wallet"`
	KeyFile            string                  `json:"key_file"`
	KeyFileSHA256      string                  `json:"key_file_sha256"`
	KeyValueSHA256     string                  `json:"key_value_sha256"`
	StatusDir          string                  `json:"status_dir"`
	NativeURL          string                  `json:"native_url"`
	EVMURL             string                  `json:"evm_url"`
	Baseline           payments.LegacyBaseline `json:"legacy_baseline"`
}

func hexSize(s string, size int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == size && strings.ToLower(s) == s
}

// Deployment still binds one exact approval digest, signer and wallet. Only
// these two owned native/EVM siblings are valid; mixed pairs and arbitrary RPC
// overrides are rejected before any key or network construction.
func approvedRPCPair(native, evm string) bool {
	return (native == "http://127.0.0.1:48132" && evm == "http://127.0.0.1:18547") ||
		(native == "http://127.0.0.1:48142" && evm == "http://127.0.0.1:18557")
}

// Open and inspect the same inode, never a symlink or a user-readable secret.
// This helper reads approval/key files only; callers must gate the KEY call.
func privateRead(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("private input path rejected")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil || parent != filepath.Dir(path) {
		return nil, errors.New("private input parent rejected")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("private input unavailable")
	}
	file := os.NewFile(uintptr(fd), "private-input")
	defer file.Close()
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || stat.Size < 1 || stat.Size > 16384 {
		return nil, errors.New("private input metadata rejected")
	}
	data, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil || len(data) > 16384 {
		return nil, errors.New("private input size rejected")
	}
	return data, nil
}

func readApproval(path, expected string) (approval, error) {
	var value approval
	if !hexSize(expected, 32) {
		return value, errors.New("exact approval digest required")
	}
	data, err := privateRead(path)
	if err != nil {
		return value, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != expected {
		return value, errors.New("approval bytes changed")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF {
		return value, errors.New("approval encoding rejected")
	}
	if value.Version != 1 || !hexSize(value.MachineID, 16) || !hexSize(value.LegacyContainerID, 32) || value.Wallet.ChainID != 1404 || value.Wallet.Sender == ([20]byte{}) || value.Baseline.Wallet != value.Wallet || value.Baseline.NonceFloor < 10 || !approvedRPCPair(value.NativeURL, value.EVMURL) || !filepath.IsAbs(value.KeyFile) || !filepath.IsAbs(value.StatusDir) {
		return value, errors.New("approved runtime identity rejected")
	}
	if value.ActivationApproved && (!hexSize(value.KeyFileSHA256, 32) || !hexSize(value.KeyValueSHA256, 32)) {
		return value, errors.New("active payer requires reviewed key digests")
	}
	return value, nil
}

type predecessor struct {
	ID      string
	PID     int
	Running bool
}

type hostInspector interface {
	machine() (string, error)
	legacy(context.Context, string) (predecessor, error)
	processes(string) ([]byte, error)
}

type localHost struct{}

func (localHost) machine() (string, error) {
	data, err := os.ReadFile("/etc/machine-id")
	return strings.TrimSpace(string(data)), err
}
func (localHost) legacy(ctx context.Context, id string) (predecessor, error) {
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// Never inspect Config.Env: the predecessor still contains a signing key.
	cmd := exec.CommandContext(bounded, "/usr/bin/docker", "inspect", "--format", "{{.Id}} {{.State.Pid}} {{.State.Running}}", id)
	data, err := cmd.Output()
	if err != nil || len(data) > 256 {
		return predecessor{}, errors.New("predecessor state unavailable")
	}
	parts := strings.Fields(string(data))
	if len(parts) != 3 || (parts[2] != "true" && parts[2] != "false") {
		return predecessor{}, errors.New("predecessor state rejected")
	}
	pid, err := strconv.Atoi(parts[1])
	if err != nil || pid < 0 {
		return predecessor{}, errors.New("predecessor PID rejected")
	}
	return predecessor{parts[0], pid, parts[2] == "true"}, nil
}
func (localHost) processes(id string) ([]byte, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/system.slice/docker-" + id + ".scope/cgroup.procs")
	if os.IsNotExist(err) {
		return nil, nil // Accepted only after exact Docker PID0/stopped evidence.
	}
	return data, err
}

func authorityCheck(ctx context.Context, a approval, host hostInspector) error {
	if _, bounded := ctx.Deadline(); !bounded || ctx.Err() != nil || !hexSize(a.LegacyContainerID, 32) {
		return errors.New("bounded host authority unavailable")
	}
	machine, err := host.machine()
	if err != nil || machine != a.MachineID {
		return errors.New("approved host identity unavailable")
	}
	old, err := host.legacy(ctx, a.LegacyContainerID)
	if err != nil || old.ID != a.LegacyContainerID {
		return errors.New("exact predecessor identity unavailable")
	}
	if old.Running || old.PID != 0 {
		return errLegacyAlive
	}
	processes, err := host.processes(a.LegacyContainerID)
	if err != nil || len(strings.TrimSpace(string(processes))) != 0 {
		return errors.New("predecessor cgroup not proved empty")
	}
	if !a.ActivationApproved {
		return errInactive
	}
	return ctx.Err()
}

func lockRuntime(directory string) (*os.File, error) {
	resolved, err := filepath.EvalSymlinks(directory)
	info, statErr := os.Stat(directory)
	if err != nil || statErr != nil || resolved != directory || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("private runtime directory required")
	}
	var directoryStat unix.Stat_t
	if unix.Stat(directory, &directoryStat) != nil || directoryStat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("runtime directory owner rejected")
	}
	fd, err := unix.Open(filepath.Join(directory, "payer.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, errors.New("payer lifetime lock unavailable")
	}
	file := os.NewFile(uintptr(fd), "payer-lifetime-lock")
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		file.Close()
		return nil, errors.New("another payer or invalid lifetime lock")
	}
	return file, nil
}
