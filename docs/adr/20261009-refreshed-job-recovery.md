# ADR: additive recovery of a proven refreshed-job duplicate

Status: accepted for private Testnet Node 2; preparation and then gated live
recovery explicitly approved by Francois on 9 October 2026.

## Context

The original immutable journal correctly prevents reopening conflicts. A local
job counter is not a block identity. Native accepted one block while its two
local job numbers raced. Resetting the journal would lose audit/payment state.

## Decision

Keep every old row and trigger. Add a narrowly typed immutable resolution and
missing accepted receipt, using the original completed attempt and economic
intent. Require exact candidate/payload validation, only a sequence difference,
matching independent native headers and checkpoint evidence, no contradictory
attempt or economic conflict, and an explicit offline operator invocation.
The first preparation did not change runtime latch handling. The subsequently
approved integration verifies the sole exact recovery, canonical proof, source
attempt, receipt and original accounting effect at startup, then caches its
identity. Runtime SQL admits only that unchanged proof/receipt/effect and blocks
every other conflict. New audit records are never automatically trusted.
Historical raw-state/conflict metrics remain visible beside recovered and
unresolved counts. The historical state stays 'conflict' in SQL; API views may
report accepted with explicit historical/recovered metadata.

## Alternatives

- Delete conflict/reset ledger: rejected, destroys evidence and payment state.
- Force conflicted state back to pending: rejected, violates immutable trigger.
- Ignore all sequence conflicts at startup: rejected, insufficient proof.
- Add resolution/receipt without changing original state: selected, auditable
  and idempotent, but needs carefully reviewed runtime integration afterwards.

## Failure handling and limits

Transactions make resolution plus receipt all-or-nothing. Existing accounting
effects enforce exactly-once payment attribution. Interrupted accounting is
replayed, not duplicated. Malformed evidence or any additional contradiction
leaves the latch set. Keep mining stopped during preparation/recovery. Backups
and isolated restore are required before any separately approved live apply.
