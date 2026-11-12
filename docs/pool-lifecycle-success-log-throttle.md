# Pool Lifecycle Success Log Throttle

## Problem

After throttling routine submit, share, credit-detail, template-error, and
refresh logs, the live pool still writes a high volume of routine success logs:

- one `Block & Credits saved to Database` line per accepted block;
- one `Block ... MATURE` line per block that crosses the maturity threshold.

These messages are useful evidence, but writing one line per successful block or
per matured block adds Docker log IO without improving mining behavior. Failures
are the lines that must stay immediate.

## Design

- Keep all database and payout failure logs immediate.
- Keep block/credit database writes unchanged.
- Replace per-block DB success lines with a rate-limited summary.
- Replace per-block maturity success lines with one maturity summary per
  lifecycle tick.
- Keep Prometheus counters unchanged.

## Configuration

- `POOL_DB_SUCCESS_LOG_INTERVAL_SECONDS`
  - default: `60`
  - `0` or lower disables routine DB success summaries

## Expected Effect

Routine success log volume should fall materially while preserving operator
visibility that blocks and credits are still being persisted and matured.

## Validation

- Unit-test the DB success throttle helper.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.
- Deploy as a pool-only candidate.
- Compare two-minute pre/post log counts and verify:
  - dashboard status remains `ok`;
  - ASICs remain connected;
  - block submissions continue;
  - database and maturity errors remain immediate;
  - no backend node restarts occur from the deploy.
