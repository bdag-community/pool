package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// nodeHealth is the subset of the node's getTemplateHealth result the worker
// gates and probes on. Field names track core/json/blocktemplate.go in
// blockdag-corechain.
type nodeHealth struct {
	MineableNow        bool   `json:"mineable_now"`
	SubmitReady        bool   `json:"submit_ready"`
	ReasonCode         string `json:"reason_code"`
	Reason             string `json:"reason"`
	TemplateParentRoot string `json:"template_parent_root"`
	TemplateAgeMs      int64  `json:"template_age_ms"`
	LatestTemplateSeq  uint64 `json:"latest_template_seq"`

	// GetBlockTemplateReady is the node's low-cost sync preflight: true once the
	// node is synced enough to build templates, computed independently of
	// whether the miner has been enabled. It is the signal the start gate wants.
	//
	// The node's miner enables lazily, on the first getBlockTemplate request, so
	// a synced-but-unpoked node reports mineable_now=false with
	// reason_code=miner_disabled. Gating startup on mineable_now would deadlock:
	// the node cannot be mineable until the pool asks for a template, and the
	// pool would never start to ask. get_block_template_ready breaks that — it is
	// true for exactly the node the pool should start against, mineable or not.
	GetBlockTemplateReady      bool   `json:"get_block_template_ready"`
	GetBlockTemplateReasonCode string `json:"get_block_template_reason_code"`
}

// poolHealth is the subset of the pool's /health/job-state payload the worker
// probes on. Field names track internal/pool/server/health.go.
type poolHealth struct {
	Status             string `json:"status"`
	ReasonCode         string `json:"reason_code"`
	CurrentParent      string `json:"current_parent"`
	CurrentTemplateSeq uint64 `json:"current_template_seq"`
	ActiveConnections  int    `json:"active_connections"`
	ReadyConnections   int    `json:"ready_connections"`
}

type nodeClient struct {
	url        string
	user, pass string
	http       *http.Client
}

func newNodeClient(url, user, pass string, timeout time.Duration) *nodeClient {
	return &nodeClient{url: url, user: user, pass: pass, http: &http.Client{Timeout: timeout}}
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// templateHealth calls getTemplateHealth on the node's DAG RPC.
func (c *nodeClient) templateHealth(ctx context.Context) (*nodeHealth, error) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"getTemplateHealth","params":[]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.user != "" || c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Drain a bounded prefix so the message is useful without unbounded reads.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("getTemplateHealth: http %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}
	var rpc rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpc); err != nil {
		return nil, fmt.Errorf("getTemplateHealth: decode: %w", err)
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("getTemplateHealth: rpc error %d: %s", rpc.Error.Code, rpc.Error.Message)
	}
	var health nodeHealth
	if err := json.Unmarshal(rpc.Result, &health); err != nil {
		return nil, fmt.Errorf("getTemplateHealth: decode result: %w", err)
	}
	return &health, nil
}

type poolClient struct {
	url  string
	http *http.Client
}

func newPoolClient(metricsAddr string, timeout time.Duration) *poolClient {
	return &poolClient{
		url:  "http://" + normalizeHostPort(metricsAddr) + "/health/job-state",
		http: &http.Client{Timeout: timeout},
	}
}

// normalizeHostPort turns a bare listen address such as ":9090" into something
// dialable from this process.
func normalizeHostPort(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func (c *poolClient) jobState(ctx context.Context) (*poolHealth, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("job-state: http %d", resp.StatusCode)
	}
	var health poolHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return nil, fmt.Errorf("job-state: decode: %w", err)
	}
	return &health, nil
}

// dialStratum proves the pool is still accepting miner connections. A pool that
// answers /health/job-state but has stopped accepting on its stratum listener
// is wedged from every miner's point of view.
func dialStratum(ctx context.Context, addr string, timeout time.Duration) error {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", normalizeHostPort(addr))
	if err != nil {
		return err
	}
	return conn.Close()
}
