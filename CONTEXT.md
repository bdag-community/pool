# Pool Context

## Domain Terms

### Mining Readiness

The module that turns node health, router state, cached-template freshness, submit-readiness holds, and EVM head guard results into clear mining decisions. Callers use it to decide whether to fetch a template, accept an active-template fallback, run submit recovery, or submit a block candidate.

### Submit Pipeline

The module that owns Stratum `mining.submit` processing from request parameter mapping through lifecycle reporting. Its implementation includes job admission, duplicate suppression, stale policy, hash validation, block-candidate handling, share accounting, and submit instrumentation.
