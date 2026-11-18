-- Explicit migration for an independent accepted-share journal. Installation
-- and startup verification are separate qualification steps; do not run this
-- automatically against an existing production database.
CREATE SCHEMA pool_share_journal_v1;

CREATE TABLE pool_share_journal_v1.shares (
    sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    share_id BYTEA NOT NULL UNIQUE CHECK (octet_length(share_id) = 32),
    miner_address TEXT NOT NULL CHECK (length(miner_address) = 42),
    work NUMERIC(20,0) NOT NULL CHECK (work BETWEEN 1 AND 18446744073709551615),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE pool_share_journal_v1.cuts (
    candidate_id BYTEA PRIMARY KEY CHECK (octet_length(candidate_id) = 32),
    share_sequence BIGINT REFERENCES pool_share_journal_v1.shares(sequence),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE pool_share_journal_v1.allocation_policies (
    candidate_id BYTEA PRIMARY KEY REFERENCES pool_share_journal_v1.cuts(candidate_id),
    boundary_policy TEXT NOT NULL CHECK (boundary_policy IN ('whole-share', 'partial-share')),
    target_work NUMERIC(20,0) NOT NULL CHECK (target_work BETWEEN 1 AND 18446744073709551615)
);

CREATE FUNCTION pool_share_journal_v1.immutable_share() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'accepted share journal is append only';
END;
$$;

CREATE TRIGGER immutable_share BEFORE UPDATE OR DELETE ON pool_share_journal_v1.shares
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

CREATE TRIGGER immutable_truncate BEFORE TRUNCATE ON pool_share_journal_v1.shares
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

CREATE TRIGGER immutable_cut BEFORE UPDATE OR DELETE ON pool_share_journal_v1.cuts
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

CREATE TRIGGER immutable_cut_truncate BEFORE TRUNCATE ON pool_share_journal_v1.cuts
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

CREATE TRIGGER immutable_policy BEFORE UPDATE OR DELETE ON pool_share_journal_v1.allocation_policies
FOR EACH ROW EXECUTE FUNCTION pool_share_journal_v1.immutable_share();

CREATE TRIGGER immutable_policy_truncate BEFORE TRUNCATE ON pool_share_journal_v1.allocation_policies
FOR EACH STATEMENT EXECUTE FUNCTION pool_share_journal_v1.immutable_share();
