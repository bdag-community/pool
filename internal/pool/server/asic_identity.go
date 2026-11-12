package server

import (
	"bufio"
	"net"
	"os"
	"regexp"
	"strings"
)

const (
	defaultASICARPTablePath = "/proc/net/arp"

	asicMACOverridesEnv = "POOL_ASIC_MAC_OVERRIDES"
	asicMACAllowlistEnv = "POOL_ASIC_MAC_ALLOWLIST"
	asicARPTablePathEnv = "POOL_ASIC_ARP_TABLE_PATH"
	asicLANCIDRsEnv     = "BDAG_ASIC_LAN_CIDRS"
)

var normalizedMACPattern = regexp.MustCompile(`^([0-9a-f]{2}:){5}[0-9a-f]{2}$`)

func resolveASICMACForRemote(addr string) string {
	host := remoteHostFromAddr(addr)
	if host == "" {
		return ""
	}
	arpMAC := lookupASICMACInARPTable(host, asicARPTablePath())
	overrideMAC := lookupASICMACOverride(host, os.Getenv(asicMACOverridesEnv))
	if arpMAC != "" && (overrideMAC == "" || isConfiguredASICLANHost(host)) {
		return arpMAC
	}
	if overrideMAC != "" {
		return overrideMAC
	}
	return arpMAC
}

func lookupASICMACOverride(host, raw string) string {
	host = strings.TrimSpace(host)
	if host == "" || strings.TrimSpace(raw) == "" {
		return ""
	}
	for _, entry := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	}) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			key, value, ok = strings.Cut(entry, "|")
		}
		if !ok {
			continue
		}
		if strings.TrimSpace(key) != host {
			continue
		}
		if mac := normalizeASICMAC(value); mac != "" {
			return mac
		}
	}
	return ""
}

func parseASICMACSet(raw string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, entry := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	}) {
		if mac := normalizeASICMAC(entry); mac != "" {
			result[mac] = struct{}{}
		}
	}
	return result
}

func rejectReasonForASICMACAllowlist(host, mac string) string {
	if !isConfiguredASICLANHost(host) {
		return ""
	}
	allowed := parseASICMACSet(os.Getenv(asicMACAllowlistEnv))
	if len(allowed) == 0 {
		return ""
	}
	normalized := normalizeASICMAC(mac)
	if normalized == "" {
		return "local-asic-mac-unresolved"
	}
	if _, ok := allowed[normalized]; !ok {
		return "local-asic-mac-not-allowed"
	}
	return ""
}

func lookupASICMACInARPTable(host, path string) string {
	host = strings.TrimSpace(host)
	if host == "" || strings.TrimSpace(path) == "" {
		return ""
	}
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 || fields[0] == "IP" || fields[0] != host {
			continue
		}
		if mac := normalizeASICMAC(fields[3]); mac != "" && mac != "00:00:00:00:00:00" {
			return mac
		}
	}
	return ""
}

func asicARPTablePath() string {
	if path := strings.TrimSpace(os.Getenv(asicARPTablePathEnv)); path != "" {
		return path
	}
	return defaultASICARPTablePath
}

func normalizeASICMAC(raw string) string {
	mac := strings.ToLower(strings.TrimSpace(raw))
	mac = strings.ReplaceAll(mac, "-", ":")
	mac = strings.ReplaceAll(mac, ".", "")
	if len(mac) == 12 {
		var b strings.Builder
		b.Grow(17)
		for i := 0; i < len(mac); i += 2 {
			if i > 0 {
				b.WriteByte(':')
			}
			b.WriteString(mac[i : i+2])
		}
		mac = b.String()
	}
	if !normalizedMACPattern.MatchString(mac) {
		return ""
	}
	return mac
}

func remoteHostFromAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if percent := strings.LastIndex(host, "%"); percent >= 0 {
		host = host[:percent]
	}
	return host
}

func configuredASICLANNetworks() []*net.IPNet {
	raw := os.Getenv(asicLANCIDRsEnv)
	var networks []*net.IPNet
	for _, entry := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	}) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if !strings.Contains(entry, "/") {
			entry += "/32"
		}
		_, network, err := net.ParseCIDR(entry)
		if err != nil || network == nil {
			continue
		}
		networks = append(networks, network)
	}
	return networks
}

func isDefaultLocalHardwareHost(host string) bool {
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil || ip.To4() == nil {
		return false
	}
	_, dockerBridge, _ := net.ParseCIDR("172.16.0.0/12")
	if dockerBridge != nil && dockerBridge.Contains(ip) {
		return false
	}
	return ip.IsPrivate() || ip.IsLinkLocalUnicast()
}

func isConfiguredASICLANHost(host string) bool {
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return false
	}
	networks := configuredASICLANNetworks()
	if len(networks) == 0 {
		return isDefaultLocalHardwareHost(host)
	}
	for _, network := range networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func clientLaneIdentity(addr, asicMAC string) string {
	if mac := normalizeASICMAC(asicMAC); mac != "" {
		return "mac:" + mac
	}
	if host := remoteHostFromAddr(addr); host != "" {
		if isConfiguredASICLANHost(host) {
			return "unknown"
		}
		return "ip:" + host
	}
	return "unknown"
}

func clientLaneIdentitySource(laneID string) string {
	switch {
	case strings.HasPrefix(laneID, "mac:"):
		return "mac"
	case strings.HasPrefix(laneID, "ip:"):
		return "ip"
	default:
		return "unknown"
	}
}

func (c *client) laneIdentity() string {
	if c == nil {
		return "unknown"
	}
	if lane := strings.TrimSpace(c.laneID); lane != "" {
		return lane
	}
	return clientLaneIdentity(c.addr, c.asicMAC)
}

func (c *client) identityLogLabel() string {
	if c == nil {
		return "unknown"
	}
	lane := c.laneIdentity()
	if c.addr == "" || lane == "" || lane == "unknown" {
		return lane
	}
	if strings.HasSuffix(lane, c.addr) {
		return lane
	}
	return lane + " addr:" + c.addr
}

func emptyAsUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}
