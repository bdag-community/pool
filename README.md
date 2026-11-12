# Blockdag ASIC Mining Pool

An open-source mining pool for BlockDAG supporting Stratum v1.

Candidate product version: `2.1.0-rc.1`.

## Features
- **Stratum Server**: High-performance TCP server for ASIC miners (X30, X100).
- **PPLNS Rewards**: Pay Per Last N Shares system with "Strict Limit" FIFO.
- **VarDiff**: Variable difficulty adjustment for optimal share submission rates.
- **Reorg Protection**: Two-stage maturity (Pending -> Mature -> Unlocked).
- **Dual-Ledger Rewards**: Automatically sums DAG Subsidy + EVM Fees.
- **Pool Fees**: Configurable percentage fee (default 1%).
- **Persistence**: PostgreSQL integration for reliable data storage.

## Prerequisites
- **Go**: 1.22+
- **BlockDAG Node**: Fully synced node with RPC enabled.
- **PostgreSQL**: Database for persisting shares, blocks, and credits.

## Quick Start

### 1. Database Setup
The easiest way to set up PostgreSQL and Adminer is using the provided helper script:

```bash
./setup_db
```

This will:
- Start a PostgreSQL container (`postgres`)
- Start an Adminer container (`db-gui`)
- Apply the database schema

**Adminer UI**
Access at [http://localhost:8080](http://localhost:8080).
- **System**: PostgreSQL
- **Server**: `postgres`
- **Username**: `postgres` (or `test` if configured in .env)
- **Password**: use the value configured outside this repository
- **Database**: `pool`

### 2. Configuration
Create a `.env` file in the root directory:

```ini
# Server Port
POOL_PORT=:3334

# BlockDAG Node RPC
NODE_RPC_URL=http://127.0.0.1:2011
NODE_RPC_USER=test
NODE_RPC_PASS=test
POOL_RPC_ROUTER_NODE_HEALTH_PROBE_SECONDS=15

# PPLNS Settings
PPLNS_N_WORK=1000          # Window size (Target Shares)

# Maturity Settings
POOL_BLOCK_MATURITY=10     # Blocks before balance shows as "Confirmed"
POOL_PAYOUT_MATURITY=4096  # Blocks before coins unlock for payout

# Economics
POOL_FEE_PERCENTAGE=1.0    # 1% Pool Fee

# Database Connection
PG_URL=postgres://<user>:<password>@<host>:5432/<database>
POOL_SUBMISSION_CHAIN_ID=1404
POOL_SUBMISSION_LEASE_MS=15000
POOL_SUBMISSION_RECONCILE_INTERVAL_MS=1000

# Optional ASIC hardware identity overrides for Docker deployments. The stack
# should also pass BDAG_ASIC_LAN_CIDRS so local hardware routes without a MAC
# remain unknown/unattributed instead of becoming IP lanes.
POOL_ASIC_MAC_OVERRIDES=192.168.1.105=28:e2:97:4d:44:3a,192.168.1.106=2a:71:c7:f5:1f:1e
BDAG_ASIC_LAN_CIDRS=192.168.1.0/24
```

The pool consumes exactly one Core-native RPC endpoint. `NODE_RPC_URL` is the
canonical setting. For compatibility it also accepts `NODE_RPC_URLS`,
`POOL_SUBMIT_RPC_URLS`, `BDAG_NODE_RPC_URL`, and `BDAG_NODE_RPC_URLS`; every
nonempty alias must normalize to the same endpoint or startup fails. List
aliases use comma-separated `[label=]URL` entries. URLs must be absolute HTTP(S)
addresses with no embedded credentials, query, fragment, or path other than
`/`. DNS names are normalized but never resolved, so `localhost` and
`127.0.0.1` are intentionally different identities.

At startup and before each template-subscription reconnect, the pool calls the
Core `getChainIdentity` method and pins all six fields of
`bdag.chain-identity.v1`. A missing or malformed method blocks startup. If the
identity later changes at the same endpoint, template and submit readiness stay
blocked until the pool is restarted and the endpoint is deliberately reviewed.
`POOL_SUBMISSION_CHAIN_ID` must also equal the verified Core EVM chain ID. This
requires Core native API `1.2.0` or newer. The node-health probe defaults to 15
seconds; explicitly configured values must be integers of at least 10.

Physical LAN ASICs are operationally tracked by MAC address only. The pool first
checks `POOL_ASIC_MAC_OVERRIDES`, then `/proc/net/arp` or the path set by
`POOL_ASIC_ARP_TABLE_PATH`. Local ASIC routes covered by `BDAG_ASIC_LAN_CIDRS`
that cannot be resolved to a MAC stay unknown/unattributed; they must not become
`ip:<host>` lanes. Remote Stratum clients outside the configured ASIC LAN may
use IP lane identity. Wallet usernames remain payout identities only and must
not be used as durable ASIC lane identifiers.

Goldshell cloud-box/MCB firmware can enter a mode where it opens TCP to the pool
and then closes before sending `mining.subscribe` or `mining.authorize`. That is
not a bad-wallet condition and it is not paid mining. The pool records this as
`pool_stratum_no_request_disconnects_total` and logs `reason=no-request-eof` so
watchdogs can distinguish an ASIC protocol/control-plane issue from normal
Stratum rejects. Keep `POOL_STRATUM_SERVER_FIRST_DIFFICULTY_PROBE=false` for
Goldshell cloud-box/MCB production deployments; the pool binary, stack compose,
and stack installer all default this setting to false. On 2026-06-22, enabling
the server-first difficulty probe made live miners send `mining.subscribe` and
then disconnect before `mining.authorize`. Set it true only for explicit lab
diagnostics against firmware that has proven it needs server-first recovery, and
soak-test before any mining deployment.

### 3. Run the Pool

```bash
# Build
go build -o bin/pool ./cmd/pool

# Run
./bin/pool
```

## Kubernetes Deployment

When deploying to Kubernetes (e.g., K3s), the pool service should be configured as a `LoadBalancer` to expose the Stratum port (`3334`) directly to external miners.

### Service Configuration
Ensure your `pool` service manifest specifies `type: LoadBalancer`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: pool
spec:
  type: LoadBalancer
  ports:
    - name: stratum
      port: 3334
      targetPort: 3334
  selector:
    app: pool
```

### External Access
Once deployed, find the external IP:
```bash
kubectl get svc pool -n bdag-local
```
Point miners to the `EXTERNAL-IP` on port `3334`.

## Connecting Miners
Point your ASICs to:
- **URL**: `stratum+tcp://<YOUR_POOL_IP_OR_DDNS>:3334`
- **User**: `<EVM_WALLET_ADDRESS>` (Must be a valid 0x address)
- **Pass**: `x` (ignored)

## Development
- **Run Tests**: `go test ./...`
- **Lint**: `golangci-lint run`
