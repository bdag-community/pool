# First-discovery accounting

Objective: re-delivery of identical solved work reuses the first committed
PPLNS discovery cut, with exactly-once native receipt and allocation effects.

Gap: coordinator recomputes accounting on every re-delivery. First cut is
immutable, so a new work count is correctly rejected as economic tampering.

Scope: a distinct discovery entrypoint freezes accounting only for an inserted
candidate. PostgreSQL's unique-key arbitration decides the first discovery.
Strict PutCandidateV2 remains strict. Existing candidate work/header/chain,
stored allocation integrity and conflict latches must still verify.

Done when: actual digest reproduced; real SQL concurrent/duplicate/hostile
tests pass; witnessed, append-only recovery on actual dump is idempotent and
restart-safe; live Node 2 X30 ten-minute soak/restart passes with CPU stopped.

Non-goals: consensus/contract/schedule changes, shared release replacement,
public source, mainnet or deletion of conflict/payment evidence.
