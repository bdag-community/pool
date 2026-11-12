# Historical proof revalidation rejects unrelated prepared work

Status: ready-for-agent

The fresh actual DB copy retained 479 prepared attempts and no in-flight leases.
The original recovery verifier fails before restart with `recovery requires
drained offline submission writers`. This is not an accounting conflict: the
offline first-recovery guard also runs before an already-applied historical
proof's read-only verification.

## Repair

Move the unchanged drained-writer check after the already-applied proof branch.
Every immutable catalog, original candidate/attempt, exact proof, receipt,
intent/effect and economic-conflict check still runs. Only a fully verified,
already-applied proof returns early without mutation. Preparing or applying a
new recovery remains blocked unless all writers/attempts are drained.

## Acceptance

- New restart regression reproduces the original failure with unrelated
  in-flight/prepared work and requires byte-identical historical/work rows.
- New recovery dry-run and application still reject undrained writers.
- Full storage/server tests run serially on disposable DBs (both packages
  reset their fixtures, so simultaneous use of one DB is unsafe).
- Fresh actual-DB verification must pass without deleting/reclassifying work.

## Comments

- RED reproduced the intended old error. Original 479 attempts remain untouched.
- This is a pool-only candidate; no node/core/contract release changes.
