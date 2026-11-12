package main

import (
	"os"
	"strings"
	"testing"
)

func TestGoldshellServerFirstProbeDefaultStaysDisabled(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}

	needle := `parseBoolEnv("POOL_STRATUM_SERVER_FIRST_DIFFICULTY_PROBE", false)`
	if !strings.Contains(string(raw), needle) {
		t.Fatalf("POOL_STRATUM_SERVER_FIRST_DIFFICULTY_PROBE must default false for Goldshell cloud-box/MCB production compatibility")
	}
}

func TestPoolMitigationDefaults(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	source := string(raw)

	for _, needle := range []string{
		`parseIntEnv("POOL_SUBMIT_RPC_TIMEOUT_MS", 3000)`,
		`parseIntEnv("POOL_PREEMPTIVE_BLOCK_CANDIDATE_REFRESH_INTERVAL_MS", 1100)`,
		`parseBoolEnv("POOL_SUBMIT_ORDERING_ENABLED", true)`,
	} {
		if !strings.Contains(source, needle) {
			t.Fatalf("main.go missing mitigation default %s", needle)
		}
	}
}

func TestNodeHealthProbeIntervalDefaultsAndAcceptsSafeValues(t *testing.T) {
	t.Setenv("POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS", "")
	value, err := parseMinIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS", 15, 10)
	if err != nil || value != 15 {
		t.Fatalf("unset probe interval = %d/%v, want 15/nil", value, err)
	}
	for _, configured := range []string{"10", "15"} {
		t.Setenv("POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS", configured)
		value, err := parseMinIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS", 15, 10)
		want := 10
		if configured == "15" {
			want = 15
		}
		if err != nil || value != want {
			t.Fatalf("probe interval %q = %d/%v, want %d/nil", configured, value, err, want)
		}
	}
}

func TestNodeHealthProbeIntervalRejectsUnsafeValues(t *testing.T) {
	for _, configured := range []string{"9", "0", "not-a-number"} {
		t.Run(configured, func(t *testing.T) {
			t.Setenv("POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS", configured)
			if _, err := parseMinIntEnv("POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS", 15, 10); err == nil {
				t.Fatalf("parseMinIntEnv() accepted %q", configured)
			}
		})
	}
}

func TestPoolProductVersion(t *testing.T) {
	if productVersion != "2.1.0-rc.1" {
		t.Fatalf("productVersion = %q, want 2.1.0-rc.1", productVersion)
	}
}
