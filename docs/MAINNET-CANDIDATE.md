# Pool Mainnet review candidate — 2 November 2026

This branch preserves the qualified Testnet reliability/accounting implementation and the preparation tree's exclusion of the inherited `.env`. No pool runtime or financial rule changed in this review round. Fresh production credentials, payout address and configuration must be supplied privately; never reuse the shared X30 Testnet key.

Run the complete Go suite against a disposable PostgreSQL database whose URL has a loopback host, explicit port and database name beginning `pool_submission_journal_test_`. Tests deliberately refuse other targets and reset that disposable schema. Serialize packages sharing that database: `go test -p 1 -parallel 1 ./...`. Concurrent-submit/accounting tests still exercise their own goroutines. A parallel-package run against one shared test DB caused destructive test-fixture interference; no runtime safeguard was changed to make it pass.

The local full SQL suite includes exactly-once accounting and process-restart/recovery tests. It is not a new live multi-pool/Mainnet payout acceptance or the48-hour canary. The production canary must retain all peer/chain/identity/accounting checks, actual transaction inclusion, canonical confirmations and a dedicated payout wallet.
