# Pool Template Sequence Metrics

## Problem

The BlockDAG node's WebSocket `blockTemplate` payload includes a monotonic
`seq` field. The pool currently ignores it. A previous behavior-changing
sequence guard was rolled back because it reduced live submit quality, but the
sequence is still valuable as passive telemetry.

Without passive sequence metrics, the operator cannot tell whether WebSocket
template pushes were missed, reordered, or reset by a node restart without
reading detailed logs.

## Design

- Decode the node-provided `seq` from WebSocket template notifications.
- Store it on the pool's internal template snapshot for telemetry only.
- Expose bounded Prometheus metrics by backend:
  - latest observed node template sequence,
  - cumulative missed sequence gaps,
  - sequence reset count.
- Do not reject templates based on sequence.
- Do not change template selection, routing, block submission, VarDiff, PPLNS,
  ASIC connections, or watchdog behavior.

## Expected Effect

Grafana and alerts can show whether a backend's WebSocket stream is delivering a
continuous template sequence. This improves diagnosis of template freshness and
WebSocket health without affecting mining decisions.

## Validation

- Unit-test that WebSocket payload `seq` is preserved in the internal template.
- Unit-test backend state sequence tracking for normal sequence updates and HTTP
  templates with no sequence.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.
- Deploy as a pool-only candidate with rollback.
- Verify live metrics expose template sequence for the selected WebSocket
  backend and pool status remains `ok`.
