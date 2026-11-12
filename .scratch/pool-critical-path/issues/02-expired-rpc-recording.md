# Expired RPC context prevents journal completion

Status: ready-for-agent

One context currently covers preparation, RPC and completion. At the RPC
deadline the observation cannot be durably recorded, leaving a prepared orphan.

Test: real HTTP timeout and a context-respecting journal must record a pending
transport observation within the existing bounded persistence budget, never
emit acceptance/effects, and leave the configured RPC timeout unchanged.

## Comments

RED in deterministic context-respecting and real SQL tests: expired context
abandoned pending completion. GREEN uses the existing two-second persistence
budget after the unchanged RPC deadline. No fictitious receipt/effect; SQL/fault
and race suites pass. No storage, lease or classification rule changed.
