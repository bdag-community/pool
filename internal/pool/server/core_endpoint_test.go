package server

import (
	"strings"
	"testing"
)

func TestResolveCoreRPCEndpointDefaultsLocallyWhenAllowed(t *testing.T) {
	got, err := ResolveCoreRPCEndpoint(map[string]string{}, true)
	if err != nil {
		t.Fatalf("ResolveCoreRPCEndpoint() error = %v", err)
	}
	if got != defaultCoreRPCEndpoint {
		t.Fatalf("endpoint = %q, want %q", got, defaultCoreRPCEndpoint)
	}
	if _, err := ResolveCoreRPCEndpoint(map[string]string{}, false); err == nil {
		t.Fatal("explicit-only endpoint resolution accepted no endpoint")
	}
}

func TestResolveCoreRPCEndpointCollapsesEquivalentAliases(t *testing.T) {
	inputs := map[string]string{
		"NODE_RPC_URL":         " HTTP://Core.Example.:80 ",
		"NODE_RPC_URLS":        "primary=http://core.example/,http://core.example:80",
		"POOL_SUBMIT_RPC_URLS": "submit=http://CORE.EXAMPLE./",
		"BDAG_NODE_RPC_URL":    "http://core.example",
		"BDAG_NODE_RPC_URLS":   "",
	}
	got, err := ResolveCoreRPCEndpoint(inputs, false)
	if err != nil {
		t.Fatalf("ResolveCoreRPCEndpoint() error = %v", err)
	}
	if got != "http://core.example/" {
		t.Fatalf("endpoint = %q, want canonical endpoint", got)
	}
}

func TestResolveCoreRPCEndpointRejectsDistinctAliases(t *testing.T) {
	_, err := ResolveCoreRPCEndpoint(map[string]string{
		"NODE_RPC_URL":  "http://node-a:38131",
		"NODE_RPC_URLS": "http://node-b:38131",
	}, false)
	if err == nil || !strings.Contains(err.Error(), "2 distinct endpoints") {
		t.Fatalf("distinct aliases error = %v", err)
	}
}

func TestResolveCoreRPCEndpointRejectsUnsafeOrMalformedURLs(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "credentials", url: "http://user:pass@node:38131"},
		{name: "query", url: "http://node:38131/?role=primary"},
		{name: "fragment", url: "http://node:38131/#primary"},
		{name: "path", url: "http://node:38131/rpc"},
		{name: "malformed IPv4", url: "http://999.1.1.1:38131"},
		{name: "malformed IPv6", url: "http://[2001:db8:::1]:38131"},
		{name: "non HTTP", url: "ws://node:38131"},
		{name: "empty list element", url: "http://node:38131,,http://node:38131"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ResolveCoreRPCEndpoint(map[string]string{"NODE_RPC_URLS": test.url}, false); err == nil {
				t.Fatalf("ResolveCoreRPCEndpoint() accepted %q", test.url)
			}
		})
	}
}

func TestNormalizeCoreRPCEndpointCanonicalizesIPTextAndPorts(t *testing.T) {
	tests := map[string]string{
		"https://[2001:0db8:0:0:0:0:0:1]:443": "https://[2001:db8::1]/",
		"http://127.0.0.1:080":                "http://127.0.0.1/",
		"https://Example.COM:8443/":           "https://example.com:8443/",
	}
	for input, want := range tests {
		got, err := normalizeCoreRPCEndpoint(input)
		if err != nil {
			t.Fatalf("normalizeCoreRPCEndpoint(%q) error = %v", input, err)
		}
		if got != want {
			t.Fatalf("normalizeCoreRPCEndpoint(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestBuildRPCRouterConfigDefensivelyRejectsAliasMismatch(t *testing.T) {
	_, err := buildRPCRouterConfig(Config{
		RPCURL: "http://node-a:38131/",
		RPCEndpointAliases: map[string]string{
			"POOL_SUBMIT_RPC_URLS": "submit=http://node-b:38131/",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "distinct endpoints") {
		t.Fatalf("alias mismatch error = %v", err)
	}
}

func TestResolveCoreRPCEndpointDoesNotEquateDNSAndIP(t *testing.T) {
	_, err := ResolveCoreRPCEndpoint(map[string]string{
		"NODE_RPC_URL":  "http://localhost:38131",
		"NODE_RPC_URLS": "http://127.0.0.1:38131",
	}, false)
	if err == nil {
		t.Fatal("resolver equated localhost and 127.0.0.1")
	}
}
