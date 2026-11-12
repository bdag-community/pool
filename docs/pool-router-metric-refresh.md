# Pool Router Metric Refresh Design

## Problem

The pool already exposes Prometheus gauges for backend selection, backend health,
backend score, WebSocket connection state, and template age. Those gauges are
published when router events occur. During quiet or partially degraded periods,
`pool_rpc_backend_template_age_seconds` can become stale because the metric value
itself is not refreshed as wall-clock time advances.

This weakens Grafana and alerting because template-age based warnings can lag
behind actual runtime state unless another router event happens to publish the
metrics.

## Design

- Add a lightweight router metrics publisher ticker in the pool process.
- Refresh all backend router gauges periodically from in-memory state.
- Keep the interval small enough for useful freshness detection and large enough
  to avoid measurable overhead. Use two seconds.
- Do not make network calls, database calls, miner calls, node calls, or routing
  decisions in this ticker.
- Do not change block submission, template selection, WebSocket, VarDiff, or PPLNS
  behavior.

## Expected Effect

Grafana and Prometheus can detect backend template age, backend health, selected
backend, and WebSocket state from source-native metrics without relying on Docker
log timing. This improves early degraded-state visibility with negligible CPU and
IO cost.

## Validation

- Add a unit test for the ticker loop stopping cleanly and calling the publish
  hook.
- Run `go test ./internal/pool/server -count=1`.
- Run `go test ./...`.
- Deploy as a pool-only candidate with rollback.
- Verify metrics endpoint exposes updating router gauges and pool status remains
  `ok`.
