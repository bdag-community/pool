# Pool Routine Submit Log Throttle

## Problem

The pool logs `submit from worker=...` for every Stratum share submission before
the share is classified. In live operation this remained the largest log source
after throttling repeated template errors, VarDiff pushes, and low-difficulty
rejects. During reconnect or retarget windows one miner can emit hundreds or
thousands of routine submit lines in a short window.

That log volume costs disk I/O, increases Docker log churn, and makes the
important operator signal harder to see. It does not add value for every normal
share because the pool already logs explicit error paths, block candidates,
accepted blocks, duplicate blocks, stale candidates, and low-difficulty reject
summaries.

## Constraints

- Do not change share validation, VarDiff, PPLNS, block submission, reject
  accounting, or miner protocol responses.
- Keep the first routine submit line visible for each client.
- Keep periodic routine submit visibility for diagnostics.
- Do not suppress block-found, submit-error, stale, duplicate, low-difficulty,
  or accepted-share outcome logs in this change.
- Keep state local to each Stratum client so one noisy miner cannot hide another
  miner's routine submit signal.

## Approach

Add a per-client throttle for the pre-classification routine submit log. The
first routine submit for a client logs immediately. Repeated routine submit
lines are suppressed until the throttle interval expires, then the pool logs the
latest submit details with a suppressed count.

This is intentionally a log-I/O optimization only. It should not affect mining
semantics or live earnings behavior.

## Validation

- Unit-test that routine submit logs are throttled per client.
- Unit-test that separate clients maintain separate throttle state.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.
- If deployed, compare live Docker log pattern counts before and after while
  confirming dashboard status remains `ok`, connected ASICs remain stable, and
  block submissions continue.
