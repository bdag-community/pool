# Duplicate discovery must retain its original economic cut

Status: implemented, private QA passed; live qualification pending

MINING-005 captures candidate 682c5550…f7c7d7c8, same solved share under jobs
224 and 225. Stored total work is 5,000,000,000. Incoming digest differs.
Reconstruct its exact changed work before modifying production logic.

Reconstruction passed: incoming work is exactly 5,050,000,000. It adds one
0.05-difficulty share (50,000,000 units), without changing money or recipient.
SQL concurrent discovery, original-intent reuse and rollback checks pass.
Actual-dump append-only recovery and negative-evidence tests pass. Historical
table digests remain unchanged. Serial full-suite and race checks pass.

Acceptance: strict economic writes reject changed recipients/digests. Trusted
submit-pipeline discovery freezes only once, under SQL insertion arbitration,
including concurrent deliveries. Failed factory rolls back candidate insertion.
