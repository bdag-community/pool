-- Experimental wallet-scoped allocation. Install only with the matching
-- payout implementation; a row alone is not permission to sign or send.
BEGIN;
CREATE TABLE pool_reward_observations_v1.allocations (
    candidate_id BYTEA PRIMARY KEY REFERENCES pool_reward_observations_v1.observations(candidate_id),
    wallet BYTEA NOT NULL CHECK (octet_length(wallet)=20),
    evidence_digest BYTEA NOT NULL CHECK (octet_length(evidence_digest)=32),
    work_digest BYTEA NOT NULL CHECK (octet_length(work_digest)=32),
    gross_wei NUMERIC(30,0) NOT NULL CHECK (gross_wei > 0),
    fee_wei NUMERIC(30,0) NOT NULL CHECK (fee_wei >= 0),
    distributable_wei NUMERIC(30,0) NOT NULL CHECK (distributable_wei >= 0),
    total_work NUMERIC(20,0) NOT NULL CHECK (total_work > 0),
    line_count INTEGER NOT NULL CHECK (line_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (fee_wei + distributable_wei = gross_wei)
);
CREATE TABLE pool_reward_observations_v1.credit_bindings (
    credit_id BIGINT PRIMARY KEY REFERENCES public.credits(id),
    candidate_id BYTEA NOT NULL REFERENCES pool_reward_observations_v1.allocations(candidate_id),
    wallet BYTEA NOT NULL CHECK (octet_length(wallet)=20),
    miner_address TEXT NOT NULL CHECK (length(miner_address)=42),
    work NUMERIC(20,0) NOT NULL CHECK (work > 0),
    amount_wei NUMERIC(30,0) NOT NULL CHECK (amount_wei > 0),
    UNIQUE(candidate_id,miner_address)
);
CREATE TRIGGER allocations_immutable BEFORE UPDATE OR DELETE ON pool_reward_observations_v1.allocations
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER allocations_no_truncate BEFORE TRUNCATE ON pool_reward_observations_v1.allocations
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER credit_bindings_immutable BEFORE UPDATE OR DELETE ON pool_reward_observations_v1.credit_bindings
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER credit_bindings_no_truncate BEFORE TRUNCATE ON pool_reward_observations_v1.credit_bindings
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
COMMIT;
