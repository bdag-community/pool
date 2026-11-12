# Startup prime precedes a long final audit

Status: ready-for-agent

Exact failed probe: correct ASIC identity, same parents/two fresh peers, native
ready, but the pool's first job was 83,627 ms old/expired and cached health stale.
Pool stopped before accepting work. Both financial audits had already passed.

Source order: side-effecting health prime → readiness reconciliation → full
post-reconciliation accounting audit → listener. Move the prime after both
audits, before listener. Keep every audit and original TTL/health limit intact.
Structural RED/GREEN ordering test plus full suites and live cold start/soak.
