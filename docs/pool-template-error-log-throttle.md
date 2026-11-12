# Pool Template Error Log Throttle Design

## Purpose

The live pool regularly sees transient template-builder errors such as
`Blockdag is downloading tx`, `Blockdag is downloading blocks`, `Client in
initial download`, and `nonce too low` while mining remains productive. Logging
every occurrence creates unnecessary disk churn and obscures the errors that
actually need operator attention.

This document is the non-behavioral discovery/design commit for
`feature/pool-template-error-log-throttle-v3`.

## Design Goals

- Throttle only known transient template-fetch errors.
- Preserve full logging for hard, unknown, malformed, or transport errors.
- Include a suppressed-count summary when a transient message is emitted again.
- Avoid changing template routing, submit behavior, share validation, payouts,
  or database writes.
- Keep the throttle in-process and lightweight.

## Transient Classes

Known transient template-fetch classes:

- `nonce-too-low`
- `initial-download`
- `block-download`
- `tx-download`

## Acceptance Tests

- Classification treats known transient messages as benign.
- Unknown errors are not classified as benign.
- Throttling emits the first transient log and suppresses repeated matching
  logs until the interval expires.

## Live Validation

Compare short windows before and after deploy:

- template-error log count;
- accepted block count;
- block-error count;
- too-late count;
- dashboard status and connected ASIC count.

Success means log volume drops without a decline in accepted block production or
an increase in stale/degraded status.
