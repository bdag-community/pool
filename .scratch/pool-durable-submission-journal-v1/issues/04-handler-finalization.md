# Handler finalization

Status: ready-for-agent
Type: task

Route block candidates through durable finalization. Remove direct unjournaled
submission, unconditional Stratum success, capability probes, and legacy
`logCredits` authority from the accepted path. Keep existing legacy tables and
accounting code unchanged and make accepted effects non-economic and gated by
`FirstAccepted`.

## Acceptance

Candidate/attempt durability precedes HTTP; accepted receipt/state is unique and
fenced; pending/rejected/conflict responses are deterministic; reconciler work
never writes to an old Stratum connection; accounting, maturity, payout, and
wallet calls remain unused.

## Comments
