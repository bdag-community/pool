-- Experimental additive schema: install only in an isolated qualification
-- database until catalog, startup/recovery and payout checks are complete.
-- A discovery is not a monetary credit and carries no authorization to pay.
BEGIN;

CREATE TABLE pool_share_journal_v1.unpriced_discoveries_v1 (
    candidate_id BYTEA PRIMARY KEY REFERENCES public.block_submission_candidates_v1(candidate_id),
    discovery_share_id BYTEA REFERENCES pool_share_journal_v1.shares(share_id),
    fee_float_bits NUMERIC(20,0) NOT NULL CHECK (fee_float_bits BETWEEN 0 AND 18446744073709551615),
    total_work NUMERIC(20,0) NOT NULL CHECK (total_work BETWEEN 0 AND 18446744073709551615),
    work_digest BYTEA NOT NULL CHECK (octet_length(work_digest)=32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (octet_length(candidate_id)=32),
    CHECK (discovery_share_id IS NULL OR octet_length(discovery_share_id)=32),
    FOREIGN KEY (candidate_id) REFERENCES pool_share_journal_v1.allocation_policies(candidate_id)
);

CREATE TRIGGER immutable_discovery BEFORE UPDATE OR DELETE ON pool_share_journal_v1.unpriced_discoveries_v1
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

CREATE TRIGGER immutable_discovery_truncate BEFORE TRUNCATE ON pool_share_journal_v1.unpriced_discoveries_v1
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

-- Preserve the exact incoming economic inputs alongside the original durable
-- discovery when a later strict write conflicts. The common submission
-- conflict row retains the corresponding frozen block payload.
CREATE TABLE pool_share_journal_v1.unpriced_conflicts_v1 (
    conflict_id BYTEA PRIMARY KEY REFERENCES public.block_submission_conflicts_v1(conflict_id),
    candidate_id BYTEA NOT NULL REFERENCES pool_share_journal_v1.unpriced_discoveries_v1(candidate_id),
    reason TEXT NOT NULL CHECK (reason IN ('candidate_identity','discovery_policy','discovery_share','discovery_allocation')),
    original_work_digest BYTEA NOT NULL CHECK (octet_length(original_work_digest)=32),
    incoming_fee_bits NUMERIC(20,0) NOT NULL CHECK (incoming_fee_bits BETWEEN 0 AND 18446744073709551615),
    incoming_target_work NUMERIC(20,0) NOT NULL CHECK (incoming_target_work BETWEEN 1 AND 18446744073709551615),
    incoming_policy TEXT NOT NULL CHECK (incoming_policy IN ('whole-share','partial-share')),
    incoming_share_id BYTEA,
    incoming_miner_address TEXT,
    incoming_share_work NUMERIC(20,0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (octet_length(conflict_id)=16),
    CHECK ((incoming_share_id IS NULL AND incoming_miner_address IS NULL AND incoming_share_work IS NULL) OR
           (incoming_share_id IS NOT NULL AND incoming_miner_address IS NOT NULL AND incoming_share_work IS NOT NULL AND
            octet_length(incoming_share_id)=32 AND length(incoming_miner_address)=42 AND incoming_share_work BETWEEN 1 AND 18446744073709551615))
);

CREATE TRIGGER immutable_unpriced_conflict BEFORE UPDATE OR DELETE ON pool_share_journal_v1.unpriced_conflicts_v1
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

CREATE TRIGGER immutable_unpriced_conflict_truncate BEFORE TRUNCATE ON pool_share_journal_v1.unpriced_conflicts_v1
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

COMMIT;
