# Pool Difficulty Push Log Interval

## Context

The live pool already throttles routine submit, accepted-share, refresh,
credit-detail, and database-success logs. The remaining routine Stratum
`mining.set_difficulty` log line (`PUSHDIF`) still appears about once per miner
per minute during normal operation. This is lower than the historical log storm,
but it is still routine IO that does not usually carry new information.

## Goal

Make the difficulty-push log interval configurable and raise the default
interval so normal mining produces fewer routine `PUSHDIF` lines while still
logging immediately when the assigned difficulty changes.

## Non-Goals

- Do not change VarDiff math.
- Do not change when `mining.set_difficulty` messages are sent.
- Do not change share validation, PPLNS accounting, or block submission.

## Validation Plan

- Unit-test default interval behavior.
- Unit-test env override behavior.
- Verify `go test ./internal/pool/server -count=1` and `go test ./...`.
- Candidate deploy only the pool service with rollback.
- Compare visible `PUSHDIF`/`mining.set_difficulty` lines before and after over
  equal live windows while checking dashboard status stays `ok`.
