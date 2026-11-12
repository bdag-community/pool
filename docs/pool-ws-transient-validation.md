# Pool WebSocket Transient Validation Design

## Purpose

The pool now recovers from frozen template streams, but the live stack exposed a
separate churn mode immediately after deployment: a WebSocket subscription can
connect and subscribe successfully, while the initial HTTP `getBlockTemplate`
validation probe returns a transient mining-template error such as
`Blockdag is downloading tx... : Client in initial download`.

The current pool treats that validation error as fatal and immediately tears
down the WebSocket subscription. On the live host this created repeated
subscribe/poll/retry loops even while blocks were being submitted successfully.

This document is the non-behavioral discovery/design commit for
`feature/pool-ws-transient-validation`.

## Design Goals

- Preserve hard validation failures for transport errors, malformed responses,
  and unknown node RPC errors.
- Treat known transient template-construction errors as soft subscription
  validation failures.
- Keep the WebSocket subscription alive after a soft validation failure so the
  next pushed template can prove the backend healthy.
- Continue recording the backend as temporarily degraded until a real template
  arrives.
- Honor the existing WebSocket idle-timeout constant so a quiet stream falls
  back to polling quickly instead of waiting for a long read deadline.

## Soft Validation Errors

The first implementation should treat only known mining-template lifecycle
errors as soft:

- `Client in initial download`
- `Blockdag is downloading tx`
- `Blockdag is downloading blocks`
- `nonce too low`
- `too busy`

These errors mean the HTTP template builder is temporarily unavailable or
throttled. They should not prove that an already connected WebSocket template
stream is unusable.

## Non-Goals

- No ASIC configuration change.
- No node restart.
- No change to submit semantics.
- No default router disablement.
- No broad swallowing of unknown RPC errors.

## Acceptance Tests

- Initial validation returns nil for a known transient template error.
- The backend records the transient reason until a pushed template succeeds.
- Initial validation still fails for unknown RPC errors.
- The WebSocket read loop uses `templateStreamIdleTimeout`, not a 5-minute
  deadline that can hide a stalled stream.
