# Pool Low-Difficulty Reject Log Throttle Design

## Purpose

Live validation after pool restarts shows a short low-difficulty reject storm
while ASICs reconnect and VarDiff retargets upward. The rejects are handled
correctly and the flood disconnect remains available, but logging every reject
creates avoidable disk I/O and makes useful submit/template diagnostics harder
to see.

This document is the non-behavioral discovery/design commit for
`feature/pool-low-diff-log-throttle`.

## Design Goals

- Throttle only the noisy low-difficulty reject log line.
- Keep share rejection behavior unchanged.
- Keep low-difficulty flood disconnect behavior unchanged.
- Keep metrics increments unchanged.
- Include a suppressed count when the next low-difficulty line is emitted.
- Scope throttling per Stratum connection so one bad device does not hide
  another device's first warning.

## Non-Goals

- No ASIC restart.
- No ASIC difficulty or pool address reconfiguration.
- No VarDiff algorithm change.
- No lower/raise of the flood disconnect threshold.

## Acceptance Tests

- First low-difficulty reject logs immediately.
- Repeated low-difficulty rejects within the throttle interval are suppressed.
- A later low-difficulty reject logs again with the suppressed count.
- Existing flood disconnect tests continue to pass.

## Live Validation

Compare restart/retarget windows before and after deploy:

- low-difficulty reject log lines;
- accepted block submits;
- submit errors;
- dashboard status and connected ASIC count.

Success means log volume drops without reducing accepted block production.
