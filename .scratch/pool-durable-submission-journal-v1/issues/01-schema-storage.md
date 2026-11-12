# Schema and storage

Status: ready-for-agent
Type: task

Add the five versioned journal tables, exact constraints, immutable triggers,
catalog verification, migration, fresh-schema definitions, and the narrow
`SubmissionJournal` storage interface. Preserve all legacy rows and tables
byte-for-byte. Prove equality replays, conflicts, leases, prepared-attempt
immutability, one-time completion, and atomic accepted receipt/state commit in
isolated PostgreSQL.

## Acceptance

The supplied schema contract is enforced without touching legacy accounting
tables, and schema drift fails closed.

## Comments
