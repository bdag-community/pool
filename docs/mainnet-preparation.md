# Mainnet pool preparation

Qualified Testnet source: `2d22e013cf4b952823e11f79d58073843fe0701d`.
Retain `testnet-reliability` as that exact source reference. This preparation
branch changes packaging hygiene only: no Go implementation, financial policy,
accounting journal, recovery proof, mining guard, nonce or transaction is changed.

The inherited tracked `.env` is removed from the new source tree/archive and
replaced with a deliberately non-operational input template. Provision the
reviewed runtime profile privately, with fresh Mainnet credentials, its own
loopback database and a dedicated production payout wallet. Never use inherited
repository credentials, a shared Testnet wallet, or an unverified public RPC.
Git history is not a secrets store; deleting a file from the tip does not erase
history or rotate a credential. Old RPC/DB credential fields were already present
in the existing private repository and were not changed/copied from live hosts
by this preparation. Review historical key custody before production.

Do not enable automatic payout signing or change existing financial safeguards
just to start a canary. Qualify the exact independently built pool/nodeworker/node
combination against the final Mainnet manifest before a separately approved
48-hour X30 trial. Preserve the existing ledger and append-only conflict proofs;
Testnet acceptance is not a production multi-user payout audit. No live service
installation/restart, miner reassignment or Mainnet action is performed here.
