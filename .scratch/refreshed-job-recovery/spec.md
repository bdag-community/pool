# Refreshed-job duplicate and append-only recovery

Owner: Francois. Scope: private Node 2 pool only, not core/contracts.

- Preserve the first frozen block, original accounting intent and all conflicts.
- Compare refreshed jobs after normalizing only the local template sequence.
- Different work or economic intent still sets the immutable global latch.
- Offline recovery accepts only one candidate-identity conflict whose fully
  validated payload differs solely in sequence, with a completed accepted V2
  attempt and independently collected matching native-header evidence.
- Append an immutable recovery record and the missing receipt transactionally.
  Never update the conflicted state, original candidate, attempt or conflict.
- Replay original accepted accounting through existing unique-effect machinery.
- Francois subsequently approved gated live recovery. Runtime startup must
  verify the exact immutable proof, receipt and original accounting effect.
  Pin only this conflict's identity in memory; SQL must reject new conflicts
  or any mismatch in the cached proof/receipt/effect. Keep raw history visible.
- Exercise malformed payloads, wrong proof/attempt, unrelated conflicts, repeated
  recovery, failures before/after commit, restart and exactly-once credits.

Acceptance on an isolated copy of the actual dump is mandatory before the
separately approved live application and strict resumed-mining qualification.
