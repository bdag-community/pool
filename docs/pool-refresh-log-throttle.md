# Pool Refresh Log Throttle

## Problem

The live pool emits a routine `[REFRESH]` log line for every normal template
broadcast. At current chain speed this is one of the largest remaining sources
of pool log volume after accepted-share, submit, low-difficulty, and credit
detail logs were throttled.

The logs are useful when diagnosing stale template or WebSocket behavior, but
the normal case is repetitive. Keeping every normal refresh line increases
Docker log IO and makes real submit/template failures harder to see.

## Design

- Add an opt-out throttle for routine refresh logs.
- Keep refresh error logs immediate.
- Group routine refresh logs by refresh class, not by full reason text, because
  tip-change reasons include a unique hash.
- Emit the current refresh line periodically with `suppressed=N` so operators
  can still see that template updates are flowing.
- Keep template broadcasting, job sequencing, miner notification, metrics, and
  submit behavior unchanged.

## Configuration

- `POOL_REFRESH_LOG_INTERVAL_SECONDS`
  - default: `5`
  - `0` or lower disables routine refresh logging
  - invalid values fall back to the default

## Expected Effect

Routine `[REFRESH]` lines should fall from per-template frequency to at most one
line per refresh class per interval, while stale/error refresh events remain
visible immediately.

## Validation

- Unit-test refresh log keying and interval behavior.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.
- Build and deploy as a pool-only candidate.
- Compare two-minute pre/post log counts and verify:
  - dashboard status remains `ok`;
  - ASICs stay connected;
  - block submissions continue;
  - refresh errors remain visible;
  - no backend node restarts occur.
