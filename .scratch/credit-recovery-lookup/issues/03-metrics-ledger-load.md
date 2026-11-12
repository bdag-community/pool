# Bound repeated full-ledger metrics scans

Status: ready-for-agent

- Remaining live failure: the indexed repair starts safely and produces blocks,
  but finalization still times out under solved-block load. Pool stopped again.
- Evidence: concurrent full-ledger reporting queries consume ~0.9 s each on the
  two-core pool host. Reporting runs per acceptance, reconciler and consumer.
  The indexed immutable accounting guard itself takes 0.579 ms in isolation.
- Acceptance: demonstrate sequential/concurrent reporting amplification RED;
  share a bounded metrics publisher across the three runtime paths; keep every
  durable accounting, drain and conflict check fresh and unchanged. Full suites,
  pinned builds and sustained live qualification required before resolution.
- Non-goals: no deadline increases, no cached authorization/accounting checks,
  no journal deletion, consensus or contract changes.

## Comments

- Sequential/concurrent regressions RED: 20 and 100 scans per immediate burst.
- GREEN: one shared reporting scan; next-interval refresh, error/retry and direct
  authoritative reads covered. Accounting decisions remain immediate/fresh.
- Storage, server and recovery-tool suites PASS on the protected disposable DB.
  Live load qualification still pending; do not mark resolved yet.
