# Accepted Share Log Throttle

## Problem

After throttling template errors, VarDiff pushes, low-difficulty rejects, and
routine pre-classification submit lines, accepted-share logs are the largest
remaining routine log source during normal ASIC mining.

The line is operationally useful because the local dashboard currently uses it
for miner freshness, per-miner accepted share count, and approximate accepted
work. Removing it outright would reduce IO but would make the dashboard less
accurate.

## Branch

`feature/pool-valid-share-log-throttle`

This branch is stacked on `feature/pool-routine-submit-log-throttle`.

## Scope

- Add a per-client accepted-share log throttle.
- Preserve share validation, VarDiff, PPLNS accounting, and block submission
  behavior.
- Keep a visible summary with:
  - suppressed accepted-share count,
  - suppressed share difficulty sum,
  - suppressed work sum,
  - latest worker/job details.
- Keep block-found and block-submit outcome logs immediate.
- Update tests for the throttle state machine.

## Non-Goals

- No share difficulty algorithm changes.
- No database schema changes.
- No replacement of dashboard log parsing in this branch.

## Validation

- Unit-test that accepted-share logs are throttled per client.
- Unit-test that suppressed counts and work sums are emitted on the next visible
  line.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.

## Live Compatibility

The production dashboard parser must count `suppressed=N`, `suppressedDiff=...`,
and `suppressedWork=...` on accepted-share log lines before this image is kept in
production. Without that parser update, raw log volume would fall but dashboard
miner work history would undercount accepted work.
