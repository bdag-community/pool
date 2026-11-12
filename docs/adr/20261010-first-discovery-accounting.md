# ADR: discovery is distinct from an economic write

## Status

Accepted for the owner-authorized private Node 2 recovery only. Not a shared
node release or general multi-miner qualification.

## Context

The previous job-sequence correction preserves exact work identity but still
compares a freshly recomputed PPLNS snapshot with its immutable first cut.
Identical work arriving later cannot own a new allocation. Disabling that
comparison for all economic writes would conceal genuine tampering.

## Decision

Separate trusted discovery from strict allocation writes. Discovery accepts a
lazy allocation factory invoked only by the transaction that inserts the
candidate. Duplicate discovery validates exact work and the stored allocation,
then reuses it. Existing strict PutCandidateV2 retains all comparisons.
Unique-key arbitration, not a process-local mutex, chooses the first writer.

## Consequences

- Exact-once ownership survives competing goroutines/processes and restarts.
- Strict changed-allocation callers still latch; no general digest exception.
- Recovery must explicitly prove unchanged money/recipients and the exact
  changed work digest, preserve the old conflict, and pin its resolution.
- Actual preserved digest reconstruction proved a 50,000,000 work increment,
  from 5,000,000,000 to 5,050,000,000, with the same 140-BDAG sole-recipient cut.
- A separate immutable discovery recovery links the strict accepted attempt,
  original allocation and two independent native-header/checkpoint witnesses.
  Its receipt/effect/credit commit atomically. Startup validates both original
  recoveries; runtime SQL pins their exact data, not a category of conflicts.
- Extra internal interface/test coverage is needed. Recovery remains offline
  and cannot be requested by a miner or public RPC.

## Alternatives

- Ignore changed digests everywhere: rejected, hides economic tampering.
- Read-before-write or coordinator mutex only: rejected, concurrent/process race.
- Reset accounting database: rejected, loses accepted work and audit evidence.

## References

- 20261009-refreshed-job-recovery.md: historical recovery remains unchanged.
- MINING-005 and the retained actual pool database dump.
