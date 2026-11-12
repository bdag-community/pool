package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveASICMACForRemoteUsesARPBeforeStaleOverrideForASICLAN(t *testing.T) {
	arpPath := filepath.Join(t.TempDir(), "arp")
	if err := os.WriteFile(arpPath, []byte(`IP address       HW type     Flags       HW address            Mask     Device
192.168.1.105    0x1         0x2         00:11:22:33:44:55     *        eth0
`), 0o600); err != nil {
		t.Fatalf("write arp fixture: %v", err)
	}
	t.Setenv(asicARPTablePathEnv, arpPath)
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24")
	t.Setenv(asicMACOverridesEnv, "192.168.1.105=28-E2-97-4D-44-3A,192.168.1.106=2a:71:c7:f5:1f:1e")

	if got := resolveASICMACForRemote("192.168.1.105:40210"); got != "00:11:22:33:44:55" {
		t.Fatalf("resolved MAC = %q, want live ARP MAC over stale override", got)
	}
}

func TestResolveASICMACForRemoteUsesOverrideWhenARPUnavailable(t *testing.T) {
	arpPath := filepath.Join(t.TempDir(), "arp")
	if err := os.WriteFile(arpPath, []byte(`IP address       HW type     Flags       HW address            Mask     Device
`), 0o600); err != nil {
		t.Fatalf("write arp fixture: %v", err)
	}
	t.Setenv(asicARPTablePathEnv, arpPath)
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24")
	t.Setenv(asicMACOverridesEnv, "192.168.1.105=28-E2-97-4D-44-3A,192.168.1.106=2a:71:c7:f5:1f:1e")

	if got := resolveASICMACForRemote("192.168.1.105:40210"); got != "28:e2:97:4d:44:3a" {
		t.Fatalf("resolved MAC = %q, want fallback override MAC", got)
	}
}

func TestResolveASICMACForRemoteUsesARPTable(t *testing.T) {
	arpPath := filepath.Join(t.TempDir(), "arp")
	if err := os.WriteFile(arpPath, []byte(`IP address       HW type     Flags       HW address            Mask     Device
192.168.1.105    0x1         0x2         28:e2:97:4d:44:3a     *        eth0
192.168.1.106    0x1         0x2         00:00:00:00:00:00     *        eth0
`), 0o600); err != nil {
		t.Fatalf("write arp fixture: %v", err)
	}
	t.Setenv(asicARPTablePathEnv, arpPath)
	t.Setenv(asicMACOverridesEnv, "")

	if got := resolveASICMACForRemote("192.168.1.105:40210"); got != "28:e2:97:4d:44:3a" {
		t.Fatalf("resolved MAC = %q, want ARP MAC", got)
	}
	if got := resolveASICMACForRemote("192.168.1.106:40210"); got != "" {
		t.Fatalf("zero MAC should be ignored, got %q", got)
	}
}

func TestNormalizeASICMACHandlesCommonNotations(t *testing.T) {
	tests := map[string]string{
		"02:00:00:00:00:01": "02:00:00:00:00:01",
		"02-00-00-00-00-01": "02:00:00:00:00:01",
		"020000000001":      "02:00:00:00:00:01",
		"0200.0000.0001":    "02:00:00:00:00:01",
		" 0200.0000.0001 ":  "02:00:00:00:00:01",
		"not-a-mac":         "",
		"02:00:00:00:00":    "",
	}
	for input, want := range tests {
		if got := normalizeASICMAC(input); got != want {
			t.Fatalf("normalizeASICMAC(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestClientLaneIdentityPrefersMAC(t *testing.T) {
	if got := clientLaneIdentity("192.168.1.105:40210", "28-E2-97-4D-44-3A"); got != "mac:28:e2:97:4d:44:3a" {
		t.Fatalf("lane identity = %q, want MAC lane", got)
	}
	if got := clientLaneIdentity("192.168.1.105:40210", ""); got != "unknown" {
		t.Fatalf("lane identity = %q, want unknown local hardware lane without MAC", got)
	}
	if got := clientLaneIdentity("198.51.100.10:40210", ""); got != "ip:198.51.100.10" {
		t.Fatalf("lane identity = %q, want remote Stratum IP fallback", got)
	}
}

func TestClientLaneIdentityRequiresMACForConfiguredASICLAN(t *testing.T) {
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24")

	if got := clientLaneIdentity("192.168.1.105:40210", ""); got != "unknown" {
		t.Fatalf("lane identity = %q, want no IP fallback for configured ASIC LAN", got)
	}
	if got := clientLaneIdentity("198.51.100.10:40210", ""); got != "ip:198.51.100.10" {
		t.Fatalf("lane identity = %q, want remote Stratum IP fallback", got)
	}
}

func TestConfiguredASICLANHostParsesCIDRsAndSingleHosts(t *testing.T) {
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24,10.10.10.5")

	if !isConfiguredASICLANHost("192.168.1.105") {
		t.Fatal("expected 192.168.1.105 to be in configured ASIC LAN")
	}
	if !isConfiguredASICLANHost("10.10.10.5") {
		t.Fatal("expected single-host ASIC LAN entry to match")
	}
	if isConfiguredASICLANHost("10.10.10.6") {
		t.Fatal("unexpected match outside configured ASIC LAN")
	}
}

func TestRejectReasonForASICMACAllowlist(t *testing.T) {
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24")
	t.Setenv(asicMACAllowlistEnv, "28-E2-97-4D-44-3A,2a:71:c7:f5:1f:1e")

	if got := rejectReasonForASICMACAllowlist("192.168.1.105", "2A-71-C7-F5-1F-1E"); got != "" {
		t.Fatalf("allowed LAN MAC rejected with reason %q", got)
	}
	if got := rejectReasonForASICMACAllowlist("192.168.1.108", "40:ae:30:34:35:a1"); got != "local-asic-mac-not-allowed" {
		t.Fatalf("unexpected reject reason for disallowed LAN MAC: %q", got)
	}
	if got := rejectReasonForASICMACAllowlist("192.168.1.109", ""); got != "local-asic-mac-unresolved" {
		t.Fatalf("unexpected reject reason for unresolved LAN MAC: %q", got)
	}
	if got := rejectReasonForASICMACAllowlist("198.51.100.10", ""); got != "" {
		t.Fatalf("remote Stratum client should not be rejected by ASIC allowlist, got %q", got)
	}
}

func TestRejectReasonForASICMACAllowlistDisabledWhenEmpty(t *testing.T) {
	t.Setenv(asicLANCIDRsEnv, "192.168.1.0/24")
	t.Setenv(asicMACAllowlistEnv, "")

	if got := rejectReasonForASICMACAllowlist("192.168.1.108", "40:ae:30:34:35:a1"); got != "" {
		t.Fatalf("empty allowlist should not reject LAN clients, got %q", got)
	}
}

func TestRemoteHostFromAddr(t *testing.T) {
	tests := map[string]string{
		"192.168.1.105:40210":     "192.168.1.105",
		"[fe80::1%eth0]:40210":    "fe80::1",
		"192.168.1.105":           "192.168.1.105",
		"  192.168.1.105:40210  ": "192.168.1.105",
		"":                        "",
	}
	for input, want := range tests {
		if got := remoteHostFromAddr(input); got != want {
			t.Fatalf("remoteHostFromAddr(%q) = %q, want %q", input, got, want)
		}
	}
}
