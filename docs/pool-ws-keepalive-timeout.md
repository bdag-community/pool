# Pool WebSocket Keepalive Timeout

## Problem

Live sequence metrics showed repeated WebSocket `i/o timeout` events on both RPC
backends. The source currently sets `templateStreamIdleTimeout` to 15 seconds but
sends WebSocket pings every 30 seconds. That means a quiet but healthy stream can
hit the read deadline before the keepalive has a chance to refresh it.

Each false timeout forces polling/reconnect behavior, can trigger template-freeze
fallback, and increases stale/overdue block submissions.

## Design

- Make the WebSocket read deadline longer than the keepalive interval.
- Add an explicit `templateStreamPingInterval` constant.
- Use a 45-second read deadline and 15-second ping interval.
- Keep the freeze recovery path and selected-backend validation unchanged.
- Do not change block submission, VarDiff, PPLNS, ASIC handling, database logic,
  or watchdog behavior.

## Expected Effect

False WebSocket idle timeouts should drop. The pool should spend more time on a
continuous template stream and less time in polling/reconnect/freeze recovery.
That should reduce stale/overdue submissions caused by template stream churn.

## Validation

- Unit-test the keepalive constants so the ping interval remains below the read
  deadline.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.
- Deploy as a pool-only candidate with rollback.
- Compare live WebSocket timeout count, template-stream reconnects, template-freeze events,
  submit-error ratio, and pool status before/after.
