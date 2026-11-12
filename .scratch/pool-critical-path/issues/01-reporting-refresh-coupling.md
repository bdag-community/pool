# Reporting blocks accepted-parent refresh

Status: ready-for-agent

`applyAcceptedAccounting` synchronously scans all journal metrics before the
first-accepted effects can refresh the miner's parent. Reporting is not a
durable acceptance gate. Existing reconciliation loops already publish it.

Test: block only the metrics reader; durable acceptance/accounting and parent
refresh must finish without waiting. Keep direct integrity/drain reads intact.

## Comments

RED reproduced blocked parent refresh. GREEN removes reporting from the
accepted path; existing background publisher tests still pass. Full/fault/SQL
and focused race suites pass. Live qualification remains pending.
