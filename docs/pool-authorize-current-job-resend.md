# Pool Authorize Current Job Resend

## Context

The pool can broadcast the first WebSocket template before ASICs finish their
Stratum authorize flow after a pool restart. `handleAuthorize` currently marks
the client authorized but does not resend the current job. If no new WebSocket
template arrives quickly, ASICs can remain connected and authorized with no work.

## Goal

Resend the current job as soon as a client is both subscribed and authorized:

- after `mining.authorize`, resend if the client already subscribed;
- after `mining.subscribe`, resend if the client was already authorized;
- keep `ResendJobToClient` as a no-op when no current job exists;
- avoid changing Stratum difficulty, share validation, or block submission.

## Validation Plan

- Unit-test the readiness predicate.
- Verify `go test ./internal/pool/server -count=1` and `go test ./...`.
- Candidate deploy only `asic-pool` with rollback.
- Validate after restart that authorized miners begin submitting without waiting
  for a timeout fallback or a new external template event.
