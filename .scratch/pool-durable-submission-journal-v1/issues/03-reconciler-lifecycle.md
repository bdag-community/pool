# Reconciler lifecycle

Status: ready-for-agent
Type: task

Implement one-worker leases, restart reclamation, exact prepared-attempt
transport outside SQL transactions, bounded response evidence, completion
fencing, method-not-found fallback, SQL-derived journal metrics, and startup
schema/identity/conflict gates.

## Acceptance

Expired leases recover, unexpired leases remain owned, stale completions cannot
finalize, and every retry sends the exact stored request bytes.

## Comments
