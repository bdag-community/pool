# Pool WebSocket Sequence Stale Guard

## Context

The BlockDAG node includes a `seq` value in WebSocket block-template
notifications. The pool now decodes that value and exports sequence/gap/reset
metrics, but it still accepts every decoded notification for broadcast.

If an old or out-of-order notification arrives after a newer template from the
same backend, the pool can refresh miners with stale work. This is exactly the
kind of failure that shows up operationally as stale-job rejects, overdue submit
errors, and unnecessary backend churn.

## Goal

Use the source sequence as a guard in the WebSocket path:

- accept the first positive sequence from a backend;
- accept strictly increasing sequence values;
- count gaps without rejecting the newer template;
- reject old or duplicate sequence values from the same backend;
- keep HTTP/poll templates working because they do not carry a source sequence.

## Non-Goals

- Do not change node behavior.
- Do not change Stratum protocol behavior.
- Do not reject templates that have no `seq`; HTTP fallback must remain usable.
- Do not restart ASICs, pool databases, or blockchain nodes as part of this
  source change.

## Validation Plan

- Unit-test sequence acceptance for first, increasing, gap, duplicate, and old
  sequence values.
- Verify `go test ./internal/pool/server -count=1` and `go test ./...`.
- Build a candidate pool image with a rollback save point.
- Deploy only `asic-pool`.
- Validate at least five minutes of live metrics:
  - dashboard status remains `ok`;
  - miners remain connected;
  - WebSocket endings and template-stream reconnects do not increase;
  - sequence reset/reject behavior is visible in metrics or logs;
  - stale/overdue submit rate does not worsen.
