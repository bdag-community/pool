# Pool submission critical path

Objective: restore reliable dedicated X30 mining without weakening native,
peer, identity, proof, lease or accounting validation.

- Reassess after the previous live runs: synchronous reporting delays parent
  refresh; RPC cancellation is also incorrectly reused for durable completion.
- Keep the RPC timeout unchanged. Preserve exactly-once receipts/effects and
  lease fencing. Use the existing two-second persistence budget for recording
  an observed result; a timeout remains pending, never invented as accepted.
- Move reporting to the existing reconciliation loops, not new workers. No
  reporting queries on the accepted-work/parent-refresh path; direct drain and
  recovery reads remain fresh. Shared bounded gauge publishing stays.
- Prove both failures RED, then GREEN, full SQL/fault/race suites. Qualify a
  fresh actual-DB restore, independent builds, startup, ten-minute sustained
  production and graceful restart. Persist complete failed-probe inputs.
- No core/contract/frontend deployment, consensus/schedule/clock change,
  relaxed monitor thresholds, discarded journal/history or other miner.

Owner selected continued X30 repair on 12 October after the escalation.
