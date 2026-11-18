// Package migrations provides the exact SQL used by the fresh pool installer.
package migrations

import "embed"

//go:embed 20260901_block_submission_journal_v1.sql 20260902_accepted_receipt_accounting_v1.sql 20260906_wide_reward_accounting_v2.sql 20261117_durable_share_journal_v1.sql 20261117_unpriced_discovery_v1.sql 20261117_reward_observations_v1.sql 20261117_reward_credits_v1.sql 20261118_reward_evm_claims_v1.sql 20261118_payment_pipeline_v1.sql
var Files embed.FS
