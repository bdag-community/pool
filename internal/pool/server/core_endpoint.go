package server

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

const defaultCoreRPCEndpoint = "http://127.0.0.1:38131/"

var coreRPCEndpointAliasNames = []string{
	"NODE_RPC_URL",
	"NODE_RPC_URLS",
	"POOL_SUBMIT_RPC_URLS",
	"BDAG_NODE_RPC_URL",
	"BDAG_NODE_RPC_URLS",
}

type namedCoreRPCEndpointInput struct {
	name  string
	value string
}

// ResolveCoreRPCEndpoint collapses all supported compatibility inputs to one
// normalized Core-native RPC endpoint. It never resolves DNS and never chooses
// one endpoint from a set of distinct candidates.
func ResolveCoreRPCEndpoint(inputs map[string]string, allowLocalDefault bool) (string, error) {
	entries := make([]namedCoreRPCEndpointInput, 0, len(inputs))
	names := make([]string, 0, len(inputs))
	for name := range inputs {
		if !isCoreRPCEndpointAlias(name) {
			return "", fmt.Errorf("unsupported Core RPC endpoint input %q", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entries = append(entries, namedCoreRPCEndpointInput{name: name, value: inputs[name]})
	}
	return resolveCoreRPCEndpointEntries(entries, allowLocalDefault)
}

func isCoreRPCEndpointAlias(name string) bool {
	for _, allowed := range coreRPCEndpointAliasNames {
		if name == allowed {
			return true
		}
	}
	return false
}

func resolveCoreRPCEndpointEntries(entries []namedCoreRPCEndpointInput, allowLocalDefault bool) (string, error) {
	resolved := make(map[string][]string)
	for _, entry := range entries {
		raw := trimASCIISpace(entry.value)
		if raw == "" {
			continue
		}
		parts := strings.Split(raw, ",")
		for index, part := range parts {
			part = trimASCIISpace(part)
			if part == "" {
				return "", fmt.Errorf("%s entry %d is empty", entry.name, index+1)
			}
			endpoint, err := stripCoreRPCEndpointLabel(part)
			if err != nil {
				return "", fmt.Errorf("%s entry %d: %w", entry.name, index+1, err)
			}
			normalized, err := normalizeCoreRPCEndpoint(endpoint)
			if err != nil {
				return "", fmt.Errorf("%s entry %d: %w", entry.name, index+1, err)
			}
			resolved[normalized] = append(resolved[normalized], entry.name)
		}
	}

	if len(resolved) == 0 {
		if allowLocalDefault {
			return defaultCoreRPCEndpoint, nil
		}
		return "", fmt.Errorf("exactly one explicit Core RPC endpoint is required")
	}
	if len(resolved) != 1 {
		return "", fmt.Errorf("Core RPC endpoint aliases resolve to %d distinct endpoints; exactly one is required", len(resolved))
	}
	for endpoint := range resolved {
		return endpoint, nil
	}
	panic("unreachable")
}

func stripCoreRPCEndpointLabel(value string) (string, error) {
	if separator := strings.IndexByte(value, '='); separator >= 0 && !strings.Contains(value[:separator], "://") {
		label := value[:separator]
		if label == "" {
			return "", fmt.Errorf("endpoint label is empty")
		}
		for _, char := range label {
			if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.') {
				return "", fmt.Errorf("endpoint label contains an invalid character")
			}
		}
		value = trimASCIISpace(value[separator+1:])
		if value == "" {
			return "", fmt.Errorf("labeled endpoint is empty")
		}
	}
	return value, nil
}

func normalizeCoreRPCEndpoint(raw string) (string, error) {
	raw = trimASCIISpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("malformed endpoint URL")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("endpoint URL must use http or https")
	}
	if !parsed.IsAbs() || parsed.Opaque != "" {
		return "", fmt.Errorf("endpoint URL must be absolute")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("endpoint URL must not contain credentials")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", fmt.Errorf("endpoint URL must not contain a query")
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("endpoint URL must not contain a fragment")
	}
	if (parsed.Path != "" && parsed.Path != "/") || (parsed.RawPath != "" && parsed.RawPath != "/") {
		return "", fmt.Errorf("endpoint URL path must be empty or /")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("endpoint URL host is required")
	}
	for _, char := range parsed.Host {
		if char > 0x7f {
			return "", fmt.Errorf("endpoint URL host must be ASCII")
		}
	}

	host := parsed.Hostname()
	if host == "" || strings.ContainsAny(host, "%[]") {
		return "", fmt.Errorf("endpoint URL host is malformed")
	}
	canonicalHost, isIP, err := canonicalCoreRPCHost(host)
	if err != nil {
		return "", err
	}

	port := parsed.Port()
	if strings.HasSuffix(parsed.Host, ":") {
		return "", fmt.Errorf("endpoint URL port is empty")
	}
	if port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return "", fmt.Errorf("endpoint URL port is invalid")
		}
		if (scheme == "http" && portNumber == 80) || (scheme == "https" && portNumber == 443) {
			port = ""
		} else {
			port = strconv.Itoa(portNumber)
		}
	}

	authority := canonicalHost
	if port != "" {
		authority = net.JoinHostPort(canonicalHost, port)
	} else if isIP && strings.Contains(canonicalHost, ":") {
		authority = "[" + canonicalHost + "]"
	}
	return scheme + "://" + authority + "/", nil
}

func canonicalCoreRPCHost(host string) (string, bool, error) {
	if parsedIP := net.ParseIP(host); parsedIP != nil {
		return parsedIP.String(), true, nil
	}
	if strings.Contains(host, ":") || onlyDigitsAndDots(host) {
		return "", false, fmt.Errorf("endpoint URL IP address is malformed")
	}
	host = strings.ToLower(host)
	if strings.HasSuffix(host, ".") {
		host = strings.TrimSuffix(host, ".")
	}
	if host == "" || len(host) > 253 || strings.HasSuffix(host, ".") {
		return "", false, fmt.Errorf("endpoint URL DNS host is malformed")
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", false, fmt.Errorf("endpoint URL DNS host is malformed")
		}
		for _, char := range label {
			if !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-') {
				return "", false, fmt.Errorf("endpoint URL DNS host is malformed")
			}
		}
	}
	return host, false, nil
}

func onlyDigitsAndDots(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && char != '.' {
			return false
		}
	}
	return true
}

func trimASCIISpace(value string) string {
	return strings.Trim(value, " \t\r\n\v\f")
}
