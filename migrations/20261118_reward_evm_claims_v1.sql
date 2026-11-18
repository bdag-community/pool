-- Bind each EVM reward to at most one candidate. Historical allocations are
-- backfilled while allocation inserts are locked, then the trigger covers
-- allocations made by both old and new pool binaries.
BEGIN;
LOCK TABLE pool_reward_observations_v1.allocations IN SHARE MODE;
CREATE TABLE pool_reward_observations_v1.reward_evm_claims (
    evm_block_hash BYTEA PRIMARY KEY CHECK (octet_length(evm_block_hash)=32),
    candidate_id BYTEA NOT NULL UNIQUE REFERENCES pool_reward_observations_v1.observations(candidate_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO pool_reward_observations_v1.reward_evm_claims(evm_block_hash,candidate_id)
SELECT o.evm_block_hash,a.candidate_id
FROM pool_reward_observations_v1.allocations a
JOIN pool_reward_observations_v1.observations o USING(candidate_id);
CREATE FUNCTION pool_reward_observations_v1.claim_allocated_evm_reward() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pool_reward_observations_v1.reward_evm_claims WHERE candidate_id=NEW.candidate_id) THEN
        INSERT INTO pool_reward_observations_v1.reward_evm_claims(evm_block_hash,candidate_id)
        SELECT evm_block_hash,candidate_id FROM pool_reward_observations_v1.observations WHERE candidate_id=NEW.candidate_id;
    END IF;
    RETURN NEW;
END
$fn$;
CREATE TRIGGER allocations_claim_evm_reward AFTER INSERT ON pool_reward_observations_v1.allocations
FOR EACH ROW EXECUTE FUNCTION pool_reward_observations_v1.claim_allocated_evm_reward();
CREATE TRIGGER reward_evm_claims_immutable BEFORE UPDATE OR DELETE ON pool_reward_observations_v1.reward_evm_claims
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
CREATE TRIGGER reward_evm_claims_no_truncate BEFORE TRUNCATE ON pool_reward_observations_v1.reward_evm_claims
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
COMMIT;
