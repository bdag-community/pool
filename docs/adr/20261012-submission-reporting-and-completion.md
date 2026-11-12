# ADR: separate reporting and timed-out RPC result recording

## Status

Accepted for owner-authorized private Node 2 repair only. Live sustained and
restart qualification remain mandatory. Not a shared release or mainnet change.

## Context

The previous metrics rate limit still lets a full-ledger scan delay first-
accepted parent refresh. One context also covers RPC and result recording:
when RPC times out, completion cannot write even a pending observation.
Both failures reproduce independently in coordinator tests and the latter in
the real isolated PostgreSQL journal.

## Decision

Keep accounting/receipt/effect checks synchronous and fenced. Remove only gauge
publication from the accepted hot path; existing background reconciliation
loops share the one-second publisher. Direct drain/recovery reads stay fresh.

Preserve the configured RPC deadline. Complete the observed result under the
existing two-second persistence budget, not the expired network context.
Timeouts remain pending; no acceptance is fabricated, no lease gate is removed.
Accounting remains idempotent and bounded; interrupted materialization is
handled by the existing consumer/startup reconciliation.

## Consequences

- Parent refresh no longer waits for reporting aggregates.
- Timed-out observations can be recorded and retried with retained evidence.
- Completion can outlive the RPC context by at most two seconds. This is an
  explicit lifecycle separation, not an increased RPC timeout.
- Gauges are eventually refreshed by existing loops, not immediate acceptance.
- No new workers, queues, shutdown ownership, policy or database migration.

Startup follow-up: exact failed probe shows an 83-second-old first job after
the final post-reconciliation audit. Health priming can fetch templates, so it
now runs after **both** financial audits, still before the listener. Audit and
TTL checks are unchanged. An AST ordering regression is RED before this move;
live cold startup remains the behavioral acceptance requirement.

## Alternatives

- Increase all deadlines or relax freshness: rejected; masks the coupling.
- Spawn a new metrics worker per acceptance: rejected; avoid lifecycle/load risk.
- Refresh before committing a durable receipt: rejected; unproven acceptance.

## Verification

RED/GREEN blocked-reader and real HTTP timeout tests; SQL pending-attempt test;
exact-once fault/restart suites, race tests, actual-DB conservation/runtime gate,
matched builds, unchanged strict live monitor and restart acceptance.
