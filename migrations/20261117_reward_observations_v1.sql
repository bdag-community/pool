-- Experimental local payout integration. A recorded observation is not
-- finality, maturity, a monetary credit, or authorization to sign.
BEGIN;
CREATE SCHEMA pool_reward_observations_v1;
CREATE TABLE pool_reward_observations_v1.observations (
    candidate_id BYTEA PRIMARY KEY REFERENCES pool_share_journal_v1.unpriced_discoveries_v1(candidate_id),
    native_block_hash BYTEA NOT NULL CHECK (octet_length(native_block_hash)=32),
    native_order NUMERIC(20,0) NOT NULL CHECK (native_order > 0),
    native_height NUMERIC(20,0) NOT NULL CHECK (native_height >= 0),
    evm_block_hash BYTEA NOT NULL CHECK (octet_length(evm_block_hash)=32),
    evm_block_number NUMERIC(20,0) NOT NULL CHECK (evm_block_number > 0),
    evm_state_root BYTEA NOT NULL CHECK (octet_length(evm_state_root)=32),
    wallet BYTEA NOT NULL CHECK (octet_length(wallet)=20),
    subsidy_wei NUMERIC(30,0) NOT NULL CHECK (subsidy_wei >= 0),
    other_sender_fees_wei NUMERIC(30,0) NOT NULL CHECK (other_sender_fees_wei >= 0),
    self_paid_fees_wei NUMERIC(30,0) NOT NULL CHECK (self_paid_fees_wei >= 0),
    evidence_digest BYTEA NOT NULL CHECK (octet_length(evidence_digest)=32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE pool_reward_observations_v1.conflicts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    candidate_id BYTEA NOT NULL REFERENCES pool_reward_observations_v1.observations(candidate_id),
    original_digest BYTEA NOT NULL CHECK (octet_length(original_digest)=32),
    incoming_digest BYTEA NOT NULL CHECK (octet_length(incoming_digest)=32),
    incoming_evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER observations_immutable BEFORE UPDATE OR DELETE ON pool_reward_observations_v1.observations
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER observations_no_truncate BEFORE TRUNCATE ON pool_reward_observations_v1.observations
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER conflicts_immutable BEFORE UPDATE OR DELETE ON pool_reward_observations_v1.conflicts
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER conflicts_no_truncate BEFORE TRUNCATE ON pool_reward_observations_v1.conflicts
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
COMMIT;
