-- A reward with no recoverable miner attribution stays in the coinbase wallet.
-- Record it once without inventing a miner credit or a transfer to self.
BEGIN;
CREATE TABLE pool_reward_observations_v1.operator_defaults_v1 (
    candidate_id BYTEA PRIMARY KEY REFERENCES pool_reward_observations_v1.observations(candidate_id),
    wallet BYTEA NOT NULL CHECK (octet_length(wallet)=20),
    evm_block_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(evm_block_hash)=32),
    evidence_digest BYTEA NOT NULL CHECK (octet_length(evidence_digest)=32),
    gross_wei NUMERIC(30,0) NOT NULL CHECK (gross_wei > 0),
    reason TEXT NOT NULL CHECK (reason IN ('no_attributable_work','conflicting_or_missing_work')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER operator_defaults_immutable BEFORE UPDATE OR DELETE ON pool_reward_observations_v1.operator_defaults_v1
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER operator_defaults_no_truncate BEFORE TRUNCATE ON pool_reward_observations_v1.operator_defaults_v1
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
COMMIT;
