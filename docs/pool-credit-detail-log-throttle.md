# Credit Detail Log Throttle

## Problem

With seven ASICs and high block production, every accepted block logs a full
PPLNS credit breakdown. In a live two-minute sample this produced:

- 200 `PPLNS SNAPSHOT` lines,
- 100 `BLOCK CREDITS` headers,
- 800 per-miner credit lines,
- 100 revenue lines,
- 100 block reward lines.

Those details are useful when debugging accounting, but they are excessive for
normal production mining. The database remains the source of truth for credits,
and the dashboard already reads credits from PostgreSQL rather than parsing
these verbose lines.

## Branch

`feature/pool-credit-detail-log-throttle`

This branch is stacked on `feature/pool-valid-share-log-throttle`.

## Scope

- Keep one compact credit summary line per accepted block.
- Throttle full per-miner credit detail dumps to a configurable interval.
- Include the number of suppressed detailed credit dumps on the next detailed
  dump.
- Remove redundant pre-credit PPLNS/reward lines from the submit path because
  the compact credit summary contains the same operational values.
- Preserve DB block/credit writes, PPLNS accounting, block submission behavior,
  and dashboard-visible database state.

## Configuration

- `POOL_CREDIT_DETAIL_LOG_INTERVAL_SECONDS`
  - default: `300`
  - `0`: disable full per-miner detail dumps
  - positive value: emit full detail at most once per interval

## Non-Goals

- No database schema changes.
- No accounting algorithm changes.
- No payout behavior changes.

## Validation

- Unit-test detail throttling and suppressed-block summaries.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.
- Deploy as a local candidate with rollback and compare direct log pattern
  counts before and after.
