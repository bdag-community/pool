# Credit recovery lookup

Restore sustained mining on the existing Node-2 X30 pool without changing the
binary, consensus, submission deadlines, proof guards or any accounting record.

- The recovery latch performs unconditional block-hash credit lookups; the
  existing partial unpaid index cannot serve them.
- Introduce one non-unique, unfiltered btree index. Preserve the unpaid index,
  every constraint and all conflict/recovery evidence.
- Qualify on the isolated actual database backup before live maintenance:
  RED/GREEN query-plan regression, row/constraint conservation, idempotence,
  incompatible-object rejection and the original runtime verifier.
- Back up cold, apply only while pool writers are drained, then verify common
  native/EVM checkpoints, accepted-work accounting and sustained X30 mining.

Evidence: `devnet-ops/evidence/20261012-frontend-e2e/MINING-REPAIR.md` in the BDAG workspace.
