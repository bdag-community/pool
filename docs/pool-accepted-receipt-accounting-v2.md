# Accepted receipt accounting V2

Pool publication sequences and Core response template counters are independent.
Receipts retain Core's unsigned counter as metadata; acceptance is bound to the
exact candidate hash, height and final acceptance flags, not equality between
those counters. Historical candidate bytes remain unchanged.

`PPLNS-DISCOVERY-CUT/v2` preserves the V1 candidate, receipt, effect, and
legacy-accounting tables.  It changes only the frozen monetary representation:
gross reward, fee, distributable reward, and credit lines are canonical
unsigned decimal Wei strings, limited to 30 digits.  V1 rows retain their
original fixed-width encoding and float rounding and are never resealed.

For V2, `gross = reward_atoms * 10_000_000_000 + block_fees_wei`; the frozen
IEEE-754 fee percentage is converted to its exact rational value, then fee and
each PPLNS line use integer floor division.  The resulting remainder remains
undistributed.  Negative template fees and rewards outside the signed ingress
boundary are rejected before a miner job is issued.

Apply `migrations/20260906_wide_reward_accounting_v2.sql` after the V1 journal
migration.  It widens only the V2 monetary columns to `NUMERIC(30,0)`, permits
both policy versions, and fails if the installed precondition or final catalog
does not match.  Downgrade does not narrow columns or delete V2 rows; an older
binary must fail closed rather than process a V2 intent.

Payout reads `credits.amount::text`, validates canonical Wei, and passes a
fresh `big.Int` to the wallet.  No payout path may scan the database numeric
through `uint64`.
