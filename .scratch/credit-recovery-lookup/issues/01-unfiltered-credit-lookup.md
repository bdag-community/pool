# Unfiltered credit lookup scans the growing ledger

Status: ready-for-agent

At 16:24 UTC on 12 October 2026, all three coordinator nodes agreed but the X30
pool repeatedly timed out inserting accepted credit lines. The credit ledger
had 24,211 rows and over two million sequential scans. The immutable recovery
latch searches `credits.block_hash` without a paid filter; only a primary-key
index and an unpaid partial index existed.

## Acceptance

- Test fails on the untouched restored database for the missing index and
  sequential scan; passes after the additive migration.
- No rows, historical proofs or constraints change; idempotence and rejection
  of an incompatible index are demonstrated.
- Original runtime verification passes before live resume.
- Live mining and the pending frontend claim are independently verified.

## Comments

- Pool stopped gracefully; node remains active. Root-only full dump SHA-256:
  `817e4a5950d76b5dd87bc63afafc5422aa71312d3262bbb266dca683a6ae92ed`.
- Fresh isolated PostgreSQL 15 restore completed. Both regression tests failed
  for the intended reason; lookup scanned 24,210 unrelated credit rows.
