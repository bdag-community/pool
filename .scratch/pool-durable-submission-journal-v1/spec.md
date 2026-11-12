# POOL-DURABLE-SUBMISSION-JOURNAL/v1

Status: ready-for-agent

## Scope

Implement the protected Pool-only durable submission journal against Core
`submitBlockHeaderV2` at the supplied immutable subject. Freeze each 185-byte
candidate and exact four-byte `extraNonce2`, commit candidate and prepared RPC
intent before HTTP, reconcile through one leased worker, classify only strict
structured outcomes, and persist one immutable accepted-submission receipt.

## Hard invariants

- PostgreSQL is required; the binary never auto-runs migrations.
- No RPC call occurs inside a PostgreSQL transaction.
- Retries use journaled request bytes and never consult mutable `JobData`.
- Core identity is double BLAKE2b-256 over the exact 185 submitted bytes.
- Only matching structured `accepted` or `already_accepted` finalizes.
- Unknown, malformed, nil, null, generic duplicate, queued, timeout, and
  transport outcomes remain pending; identity or terminal contradictions enter
  the global conflict latch.
- The packet writes no legacy `block_submissions`, `blocks`, `credits`,
  `payouts`, maturity, payout, wallet, or accounting state.

## Authorized surface and commit topology

Only the packet-approved files may change. Produce exactly these source
commits, in order:

1. `pool: add durable submission journal v1 schema and storage`
2. `pool: add frozen candidate identity and strict outcome classification`
3. `pool: add leased exact-byte submission reconciliation`
4. `pool: route block candidates through durable finalization`

## Verification

Run focused, race, full, vet, migration, SQL identity, concurrency, restart,
fault, legacy/V2, and handler tests. The disposable PostgreSQL suite is a hard
gate. The maximum claim is the tested Pool implementation envelope only; this
does not establish accounting, canonical inclusion, payout correctness,
live-mining conversion, release readiness, or publication.
