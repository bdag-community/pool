-- Miners table (optional, for stats/settings)
CREATE TABLE IF NOT EXISTS miners (
    address TEXT PRIMARY KEY,
    joined_at TIMESTAMP DEFAULT NOW(),
    last_active TIMESTAMP
);

-- Blocks table (Tracks maturity)
CREATE TABLE IF NOT EXISTS blocks (
    hash TEXT PRIMARY KEY,
    height BIGINT NOT NULL,
    reward NUMERIC(30,0) NOT NULL, -- Total Reward
    fees NUMERIC(30,0) NOT NULL,   -- Pool Fee Amount
    status TEXT DEFAULT 'PENDING', -- PENDING, ORPHANED, MATURE, PAID
    created_at TIMESTAMP DEFAULT NOW()
);

-- Durable history of solved block candidates submitted to backend nodes.
-- This is separate from payout accounting so rejected candidates are observable
-- without changing the credits flow.
CREATE TABLE IF NOT EXISTS block_submissions (
    id SERIAL PRIMARY KEY,
    candidate_hash TEXT NOT NULL,
    node_block_hash TEXT,
    height BIGINT,
    backend TEXT,
    template_seq BIGINT,
    accepted BOOLEAN NOT NULL DEFAULT FALSE,
    outcome TEXT NOT NULL,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS block_submissions_created_at_idx
    ON block_submissions (created_at);

CREATE INDEX IF NOT EXISTS block_submissions_outcome_created_idx
    ON block_submissions (outcome, created_at);

-- Credits table (Who gets what for which block)
CREATE TABLE IF NOT EXISTS credits (
    id SERIAL PRIMARY KEY,
    block_hash TEXT REFERENCES blocks(hash),
    miner_address TEXT NOT NULL,
    amount NUMERIC(30,0) NOT NULL,
    is_paid BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Lifecycle scans run frequently and only need blocks that are eligible by
-- status and height. Keep these indexes idempotent so setup can be rerun.
CREATE INDEX IF NOT EXISTS blocks_status_height_idx
    ON blocks(status, height);

CREATE INDEX IF NOT EXISTS credits_unpaid_block_idx
    ON credits(block_hash)
    WHERE is_paid = FALSE;

-- Recovery guards must also verify paid credits. The partial unpaid index
-- cannot serve these exact block-hash lookups as the ledger grows.
CREATE INDEX IF NOT EXISTS credits_block_hash_idx
    ON credits(block_hash);

-- Payouts (History)
CREATE TABLE IF NOT EXISTS payouts (
    id SERIAL PRIMARY KEY,
    tx_hash TEXT UNIQUE NOT NULL,
    amount NUMERIC(30,0) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);


-- Durable non-economic submission journal v1. These tables are additive;
-- no legacy accounting table is referenced by the journal.
CREATE TABLE block_submission_candidates_v1 (
  candidate_id BYTEA NOT NULL CONSTRAINT block_submission_candidates_v1_pkey PRIMARY KEY,
  frozen_digest BYTEA NOT NULL,
  frozen_payload BYTEA NOT NULL,
  chain_id NUMERIC(78,0) NOT NULL,
  expected_block_hash BYTEA NOT NULL,
  header_bytes BYTEA NOT NULL,
  extra_nonce2 BYTEA NOT NULL,
  nonce_bytes BYTEA NOT NULL,
  share_pow_hash BYTEA NOT NULL,
  parent_hash BYTEA NOT NULL,
  height NUMERIC(20,0) NOT NULL,
  template_seq NUMERIC(20,0) NOT NULL,
  backend_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT block_submission_candidates_v1_expected_block_hash_key UNIQUE (expected_block_hash),
  CONSTRAINT block_submission_candidates_v1_candidate_id_len_check CHECK (octet_length(candidate_id)=32),
  CONSTRAINT block_submission_candidates_v1_frozen_digest_len_check CHECK (octet_length(frozen_digest)=32),
  CONSTRAINT block_submission_candidates_v1_frozen_payload_nonempty_check CHECK (octet_length(frozen_payload)>0),
  CONSTRAINT block_submission_candidates_v1_chain_id_nonnegative_check CHECK (chain_id>=0),
  CONSTRAINT block_submission_candidates_v1_expected_block_hash_len_check CHECK (octet_length(expected_block_hash)=32),
  CONSTRAINT block_submission_candidates_v1_header_bytes_len_check CHECK (octet_length(header_bytes)=185),
  CONSTRAINT block_submission_candidates_v1_extra_nonce2_len_check CHECK (octet_length(extra_nonce2)=4),
  CONSTRAINT block_submission_candidates_v1_nonce_bytes_len_check CHECK (octet_length(nonce_bytes)=4),
  CONSTRAINT block_submission_candidates_v1_share_pow_hash_len_check CHECK (octet_length(share_pow_hash)=32),
  CONSTRAINT block_submission_candidates_v1_parent_hash_len_check CHECK (octet_length(parent_hash)=32),
  CONSTRAINT block_submission_candidates_v1_height_range_check CHECK (height BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT block_submission_candidates_v1_template_seq_range_check CHECK (template_seq BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT block_submission_candidates_v1_backend_id_format_check CHECK (backend_id ~ '^[a-z0-9._-]{1,64}$')
);

CREATE TABLE block_submission_state_v1 (
  candidate_id BYTEA NOT NULL CONSTRAINT block_submission_state_v1_pkey PRIMARY KEY,
  state TEXT NOT NULL,
  state_version BIGINT NOT NULL,
  attempt_count BIGINT NOT NULL,
  next_attempt_at TIMESTAMPTZ,
  lease_owner BYTEA,
  lease_token BYTEA,
  lease_expires_at TIMESTAMPTZ,
  last_attempt_id BYTEA,
  last_code TEXT,
  last_message TEXT,
  final_code TEXT,
  authoritative_block_hash BYTEA,
  authoritative_height NUMERIC(20,0),
  authoritative_template_seq NUMERIC(20,0),
  finalized_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT block_submission_state_v1_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES block_submission_candidates_v1(candidate_id),
  CONSTRAINT block_submission_state_v1_state_enum_check CHECK (state IN ('pending','in_flight','accepted_final','rejected_final','conflict')),
  CONSTRAINT block_submission_state_v1_state_version_positive_check CHECK (state_version>0),
  CONSTRAINT block_submission_state_v1_attempt_count_nonnegative_check CHECK (attempt_count>=0),
  CONSTRAINT block_submission_state_v1_lease_owner_len_check CHECK (lease_owner IS NULL OR octet_length(lease_owner)=16),
  CONSTRAINT block_submission_state_v1_lease_token_len_check CHECK (lease_token IS NULL OR octet_length(lease_token)=32),
  CONSTRAINT block_submission_state_v1_lease_shape_check CHECK ((state='in_flight') = (lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
  CONSTRAINT block_submission_state_v1_nonflight_lease_shape_check CHECK (state='in_flight' OR (lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)),
  CONSTRAINT block_submission_state_v1_last_attempt_id_len_check CHECK (last_attempt_id IS NULL OR octet_length(last_attempt_id)=16),
  CONSTRAINT block_submission_state_v1_terminal_next_attempt_check CHECK (state IN ('pending','in_flight') OR next_attempt_at IS NULL),
  CONSTRAINT block_submission_state_v1_authoritative_hash_len_check CHECK (authoritative_block_hash IS NULL OR octet_length(authoritative_block_hash)=32),
  CONSTRAINT block_submission_state_v1_authoritative_height_range_check CHECK (authoritative_height IS NULL OR authoritative_height BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT block_submission_state_v1_authoritative_seq_range_check CHECK (authoritative_template_seq IS NULL OR authoritative_template_seq BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT block_submission_state_v1_accepted_shape_check CHECK (state <> 'accepted_final' OR (final_code IN ('accepted','already_accepted') AND octet_length(authoritative_block_hash)=32)),
  CONSTRAINT block_submission_state_v1_terminal_finalized_check CHECK (state NOT IN ('accepted_final','rejected_final','conflict') OR finalized_at IS NOT NULL)
);

CREATE TABLE block_submission_attempts_v1 (
  attempt_id BYTEA NOT NULL CONSTRAINT block_submission_attempts_v1_pkey PRIMARY KEY,
  candidate_id BYTEA NOT NULL,
  attempt_no BIGINT NOT NULL,
  lease_token BYTEA NOT NULL,
  rpc_method TEXT NOT NULL,
  request_body BYTEA NOT NULL,
  request_sha256 BYTEA NOT NULL,
  phase TEXT NOT NULL,
  prepared_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  transport_class TEXT,
  http_status INTEGER,
  response_prefix BYTEA,
  response_truncated BOOLEAN NOT NULL DEFAULT FALSE,
  response_prefix_sha256 BYTEA NOT NULL DEFAULT decode(repeat('00',32),'hex'),
  accepted BOOLEAN,
  final BOOLEAN,
  retryable BOOLEAN,
  code TEXT,
  message TEXT,
  block_hash BYTEA,
  height NUMERIC(20,0),
  template_seq NUMERIC(20,0),
  decision TEXT NOT NULL,
  CONSTRAINT block_submission_attempts_v1_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES block_submission_candidates_v1(candidate_id),
  CONSTRAINT block_submission_attempts_v1_attempt_id_len_check CHECK (octet_length(attempt_id)=16),
  CONSTRAINT block_submission_attempts_v1_attempt_no_positive_check CHECK (attempt_no>0),
  CONSTRAINT block_submission_attempts_v1_lease_token_len_check CHECK (octet_length(lease_token)=32),
  CONSTRAINT block_submission_attempts_v1_method_check CHECK (rpc_method IN ('submitBlockHeaderV2','submitBlockHeader')),
  CONSTRAINT block_submission_attempts_v1_request_nonempty_check CHECK (octet_length(request_body)>0),
  CONSTRAINT block_submission_attempts_v1_request_sha256_len_check CHECK (octet_length(request_sha256)=32),
  CONSTRAINT block_submission_attempts_v1_phase_check CHECK (phase IN ('prepared','completed')),
  CONSTRAINT block_submission_attempts_v1_response_prefix_len_check CHECK (response_prefix IS NULL OR octet_length(response_prefix)<=65536),
  CONSTRAINT block_submission_attempts_v1_response_sha256_len_check CHECK (octet_length(response_prefix_sha256)=32),
  CONSTRAINT block_submission_attempts_v1_block_hash_len_check CHECK (block_hash IS NULL OR octet_length(block_hash)=32),
  CONSTRAINT block_submission_attempts_v1_height_range_check CHECK (height IS NULL OR height BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT block_submission_attempts_v1_template_seq_range_check CHECK (template_seq IS NULL OR template_seq BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT block_submission_attempts_v1_decision_check CHECK (decision IN ('pending','accepted_final','rejected_final','conflict','method_not_found')),
  CONSTRAINT block_submission_attempts_v1_phase_timestamp_check CHECK ((phase='prepared' AND completed_at IS NULL) OR (phase='completed' AND completed_at IS NOT NULL)),
  CONSTRAINT block_submission_attempts_v1_prepared_shape_check CHECK (phase='completed' OR (transport_class IS NULL AND http_status IS NULL AND response_prefix IS NULL AND accepted IS NULL AND final IS NULL AND retryable IS NULL AND code IS NULL AND message IS NULL AND block_hash IS NULL AND height IS NULL AND template_seq IS NULL AND decision='pending'))
);

CREATE TABLE block_submission_conflicts_v1 (
  conflict_id BYTEA NOT NULL CONSTRAINT block_submission_conflicts_v1_pkey PRIMARY KEY,
  candidate_id BYTEA NOT NULL,
  conflict_kind TEXT NOT NULL,
  existing_frozen_digest BYTEA,
  incoming_frozen_digest BYTEA,
  incoming_frozen_payload BYTEA,
  related_attempt_id BYTEA,
  difference_mask TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT block_submission_conflicts_v1_conflict_id_len_check CHECK (octet_length(conflict_id)=16),
  CONSTRAINT block_submission_conflicts_v1_candidate_id_len_check CHECK (octet_length(candidate_id)=32),
  CONSTRAINT block_submission_conflicts_v1_existing_digest_len_check CHECK (existing_frozen_digest IS NULL OR octet_length(existing_frozen_digest)=32),
  CONSTRAINT block_submission_conflicts_v1_incoming_digest_len_check CHECK (incoming_frozen_digest IS NULL OR octet_length(incoming_frozen_digest)=32),
  CONSTRAINT block_submission_conflicts_v1_related_attempt_len_check CHECK (related_attempt_id IS NULL OR octet_length(related_attempt_id)=16)
);

CREATE TABLE accepted_block_submission_receipts_v1 (
  candidate_id BYTEA NOT NULL CONSTRAINT accepted_block_submission_receipts_v1_pkey PRIMARY KEY,
  source_attempt_id BYTEA NOT NULL,
  chain_id NUMERIC(78,0) NOT NULL,
  block_hash BYTEA NOT NULL,
  candidate_height NUMERIC(20,0) NOT NULL,
  core_code TEXT NOT NULL,
  core_height NUMERIC(20,0),
  core_template_seq NUMERIC(20,0) NOT NULL,
  accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT accepted_block_submission_receipts_v1_source_attempt_id_key UNIQUE (source_attempt_id),
  CONSTRAINT accepted_block_submission_receipts_v1_block_hash_key UNIQUE (block_hash),
  CONSTRAINT accepted_block_submission_receipts_v1_candidate_id_len_check CHECK (octet_length(candidate_id)=32),
  CONSTRAINT accepted_receipts_v1_source_attempt_len_ck CHECK (octet_length(source_attempt_id)=16),
  CONSTRAINT accepted_receipts_v1_chain_nonnegative_ck CHECK (chain_id>=0),
  CONSTRAINT accepted_block_submission_receipts_v1_block_hash_len_check CHECK (octet_length(block_hash)=32),
  CONSTRAINT accepted_receipts_v1_candidate_height_range_ck CHECK (candidate_height BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT accepted_block_submission_receipts_v1_core_code_check CHECK (core_code IN ('accepted','already_accepted')),
  CONSTRAINT accepted_block_submission_receipts_v1_core_height_range_check CHECK (core_height IS NULL OR core_height BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT accepted_receipts_v1_core_seq_range_ck CHECK (core_template_seq BETWEEN 0 AND 18446744073709551615)
);

CREATE UNIQUE INDEX block_submission_attempts_candidate_no_v1_idx ON block_submission_attempts_v1(candidate_id,attempt_no);
CREATE INDEX block_submission_state_due_v1_idx ON block_submission_state_v1(next_attempt_at,candidate_id) WHERE state='pending';
CREATE INDEX block_submission_conflicts_v1_candidate_v1_idx ON block_submission_conflicts_v1(candidate_id,observed_at);
CREATE INDEX accepted_block_submission_receipts_time_v1_idx ON accepted_block_submission_receipts_v1(accepted_at);

CREATE FUNCTION reject_immutable_submission_row_v1() RETURNS trigger LANGUAGE plpgsql AS $fn$
    BEGIN
      RAISE EXCEPTION 'immutable durable submission row: %', TG_TABLE_NAME;
    END;
    $fn$;

CREATE FUNCTION reject_completed_submission_attempt_update_v1() RETURNS trigger LANGUAGE plpgsql AS $fn$
    BEGIN
      IF OLD.phase='completed' OR OLD.attempt_id IS DISTINCT FROM NEW.attempt_id OR OLD.candidate_id IS DISTINCT FROM NEW.candidate_id OR OLD.attempt_no IS DISTINCT FROM NEW.attempt_no OR OLD.lease_token IS DISTINCT FROM NEW.lease_token OR OLD.rpc_method IS DISTINCT FROM NEW.rpc_method OR OLD.request_body IS DISTINCT FROM NEW.request_body OR OLD.request_sha256 IS DISTINCT FROM NEW.request_sha256 OR OLD.prepared_at IS DISTINCT FROM NEW.prepared_at THEN
        RAISE EXCEPTION 'immutable durable submission attempt: %', OLD.attempt_id;
      END IF;
      RETURN NEW;
    END;
    $fn$;

CREATE FUNCTION enforce_submission_state_transition_v1() RETURNS trigger LANGUAGE plpgsql AS $fn$
    BEGIN
      IF OLD.state='conflict' AND NEW.state <> 'conflict' THEN
        RAISE EXCEPTION 'conflict durable submission state cannot reopen: %', OLD.candidate_id;
      END IF;
      IF OLD.state IN ('accepted_final','rejected_final') AND NEW.state NOT IN (OLD.state,'conflict') THEN
        RAISE EXCEPTION 'terminal durable submission state cannot reopen: %', OLD.candidate_id;
      END IF;
      RETURN NEW;
    END;
    $fn$;

CREATE FUNCTION make_submission_retry_delay(attempts BIGINT) RETURNS interval LANGUAGE sql IMMUTABLE AS $fn$
      SELECT make_interval(secs => CASE LEAST(GREATEST(attempts-1,0),6)
        WHEN 0 THEN 1 WHEN 1 THEN 2 WHEN 2 THEN 4 WHEN 3 THEN 8
        WHEN 4 THEN 16 WHEN 5 THEN 32 ELSE 60 END);
    $fn$;

CREATE TRIGGER block_submission_candidates_v1_immutable BEFORE UPDATE OR DELETE ON block_submission_candidates_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
CREATE TRIGGER block_submission_state_v1_transition BEFORE UPDATE ON block_submission_state_v1 FOR EACH ROW EXECUTE FUNCTION enforce_submission_state_transition_v1();
CREATE TRIGGER block_submission_attempts_v1_immutable BEFORE UPDATE ON block_submission_attempts_v1 FOR EACH ROW EXECUTE FUNCTION reject_completed_submission_attempt_update_v1();
CREATE TRIGGER block_submission_attempts_v1_no_delete BEFORE DELETE ON block_submission_attempts_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
CREATE TRIGGER block_submission_conflicts_v1_immutable BEFORE UPDATE OR DELETE ON block_submission_conflicts_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
CREATE TRIGGER accepted_block_submission_receipts_v1_immutable BEFORE UPDATE OR DELETE ON accepted_block_submission_receipts_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();

-- Discovery-time PPLNS accounting intent and exactly-once accepted-receipt
-- materialization. These tables are additive and never invoke wallet/payout
-- operations or rewrite historical accounting rows.
CREATE TABLE accepted_block_accounting_intents_v1 (
  candidate_id BYTEA NOT NULL CONSTRAINT accepted_block_accounting_intents_v1_pkey PRIMARY KEY,
  intent_digest BYTEA NOT NULL,
  policy_version TEXT NOT NULL,
  legacy_block_hash BYTEA NOT NULL,
  height NUMERIC(20,0) NOT NULL,
  gross_reward NUMERIC(30,0) NOT NULL,
  pool_fee_float_bits NUMERIC(20,0) NOT NULL,
  fee_amount NUMERIC(30,0) NOT NULL,
  distributable NUMERIC(30,0) NOT NULL,
  total_work NUMERIC(20,0) NOT NULL,
  credit_line_count INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT accepted_accounting_intents_v1_candidate_fk FOREIGN KEY(candidate_id) REFERENCES block_submission_candidates_v1(candidate_id),
  CONSTRAINT accepted_accounting_intents_v1_candidate_len_ck CHECK (octet_length(candidate_id)=32),
  CONSTRAINT accepted_accounting_intents_v1_digest_len_ck CHECK (octet_length(intent_digest)=32),
  CONSTRAINT accepted_accounting_intents_v1_policy_ck CHECK (policy_version IN ('PPLNS-DISCOVERY-CUT/v1','PPLNS-DISCOVERY-CUT/v2')),
  CONSTRAINT accepted_accounting_intents_v1_legacy_hash_len_ck CHECK (octet_length(legacy_block_hash)=32),
  CONSTRAINT accepted_accounting_intents_v1_height_ck CHECK (height BETWEEN 0 AND 9223372036854775807),
  CONSTRAINT accepted_accounting_intents_v1_gross_ck CHECK (gross_reward>=0 AND gross_reward<1000000000000000000000000000000 AND (policy_version<>'PPLNS-DISCOVERY-CUT/v1' OR gross_reward<=18446744073709551615)),
  CONSTRAINT accepted_accounting_intents_v1_fee_bits_ck CHECK (pool_fee_float_bits BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT accepted_accounting_intents_v1_fee_ck CHECK (fee_amount>=0 AND fee_amount<=gross_reward AND (policy_version<>'PPLNS-DISCOVERY-CUT/v1' OR fee_amount<=18446744073709551615)),
  CONSTRAINT accepted_accounting_intents_v1_distributable_ck CHECK (distributable=gross_reward-fee_amount AND distributable>=0 AND distributable<1000000000000000000000000000000 AND (policy_version<>'PPLNS-DISCOVERY-CUT/v1' OR distributable<=18446744073709551615)),
  CONSTRAINT accepted_accounting_intents_v1_work_ck CHECK (total_work BETWEEN 0 AND 18446744073709551615),
  CONSTRAINT accepted_accounting_intents_v1_line_count_ck CHECK (credit_line_count BETWEEN 0 AND 100000)
);

CREATE TABLE accepted_block_accounting_credit_lines_v1 (
  candidate_id BYTEA NOT NULL,
  line_index INTEGER NOT NULL,
  miner_address TEXT NOT NULL,
  work NUMERIC(20,0) NOT NULL,
  amount NUMERIC(30,0) NOT NULL,
  CONSTRAINT accepted_accounting_credit_lines_v1_pkey PRIMARY KEY(candidate_id,line_index),
  CONSTRAINT accepted_accounting_credit_lines_v1_miner_key UNIQUE(candidate_id,miner_address),
  CONSTRAINT accepted_accounting_credit_lines_v1_intent_fk FOREIGN KEY(candidate_id) REFERENCES accepted_block_accounting_intents_v1(candidate_id),
  CONSTRAINT accepted_accounting_credit_lines_v1_candidate_len_ck CHECK (octet_length(candidate_id)=32),
  CONSTRAINT accepted_accounting_credit_lines_v1_index_ck CHECK (line_index BETWEEN 0 AND 99999),
  CONSTRAINT accepted_accounting_credit_lines_v1_miner_ck CHECK (octet_length(miner_address) BETWEEN 1 AND 512),
  CONSTRAINT accepted_accounting_credit_lines_v1_work_ck CHECK (work BETWEEN 1 AND 18446744073709551615),
  CONSTRAINT accepted_accounting_credit_lines_v1_amount_ck CHECK (amount>=0 AND amount<1000000000000000000000000000000)
);

CREATE FUNCTION enforce_accepted_accounting_line_policy_v2() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE policy text;
BEGIN
  SELECT policy_version INTO policy FROM accepted_block_accounting_intents_v1 WHERE candidate_id=NEW.candidate_id;
  IF policy IS NULL OR policy NOT IN ('PPLNS-DISCOVERY-CUT/v1','PPLNS-DISCOVERY-CUT/v2') THEN RAISE EXCEPTION 'missing/invalid accounting intent policy'; END IF;
  IF NEW.amount<0 OR NEW.amount>=1000000000000000000000000000000 OR (policy='PPLNS-DISCOVERY-CUT/v1' AND NEW.amount>18446744073709551615) THEN RAISE EXCEPTION 'accounting line amount violates policy'; END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER accepted_accounting_credit_lines_v1_policy_bound BEFORE INSERT ON accepted_block_accounting_credit_lines_v1 FOR EACH ROW EXECUTE FUNCTION enforce_accepted_accounting_line_policy_v2();

CREATE TABLE accepted_block_accounting_effects_v1 (
  candidate_id BYTEA NOT NULL CONSTRAINT accepted_block_accounting_effects_v1_pkey PRIMARY KEY,
  intent_digest BYTEA NOT NULL,
  legacy_block_hash BYTEA NOT NULL,
  credit_line_count INTEGER NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT accepted_accounting_effects_v1_receipt_fk FOREIGN KEY(candidate_id) REFERENCES accepted_block_submission_receipts_v1(candidate_id),
  CONSTRAINT accepted_accounting_effects_v1_intent_fk FOREIGN KEY(candidate_id) REFERENCES accepted_block_accounting_intents_v1(candidate_id),
  CONSTRAINT accepted_accounting_effects_v1_candidate_len_ck CHECK (octet_length(candidate_id)=32),
  CONSTRAINT accepted_accounting_effects_v1_digest_len_ck CHECK (octet_length(intent_digest)=32),
  CONSTRAINT accepted_accounting_effects_v1_legacy_hash_len_ck CHECK (octet_length(legacy_block_hash)=32),
  CONSTRAINT accepted_accounting_effects_v1_line_count_ck CHECK (credit_line_count BETWEEN 0 AND 100000)
);

CREATE TABLE accepted_block_accounting_conflicts_v1 (
  conflict_id BYTEA NOT NULL CONSTRAINT accepted_block_accounting_conflicts_v1_pkey PRIMARY KEY,
  candidate_id BYTEA NOT NULL,
  conflict_kind TEXT NOT NULL,
  existing_intent_digest BYTEA,
  incoming_intent_digest BYTEA,
  detail TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT accepted_accounting_conflicts_v1_candidate_fk FOREIGN KEY(candidate_id) REFERENCES block_submission_candidates_v1(candidate_id),
  CONSTRAINT accepted_accounting_conflicts_v1_id_len_ck CHECK (octet_length(conflict_id)=16),
  CONSTRAINT accepted_accounting_conflicts_v1_candidate_len_ck CHECK (octet_length(candidate_id)=32),
  CONSTRAINT accepted_accounting_conflicts_v1_existing_digest_len_ck CHECK (existing_intent_digest IS NULL OR octet_length(existing_intent_digest)=32),
  CONSTRAINT accepted_accounting_conflicts_v1_incoming_digest_len_ck CHECK (incoming_intent_digest IS NULL OR octet_length(incoming_intent_digest)=32),
  CONSTRAINT accepted_accounting_conflicts_v1_kind_ck CHECK (octet_length(conflict_kind) BETWEEN 1 AND 96),
  CONSTRAINT accepted_accounting_conflicts_v1_detail_ck CHECK (octet_length(detail) BETWEEN 1 AND 2048)
);

CREATE INDEX accepted_accounting_conflicts_v1_candidate_idx ON accepted_block_accounting_conflicts_v1(candidate_id,observed_at);
CREATE TRIGGER accepted_block_accounting_intents_v1_immutable BEFORE UPDATE OR DELETE ON accepted_block_accounting_intents_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
CREATE TRIGGER accepted_accounting_credit_lines_v1_immutable BEFORE UPDATE OR DELETE ON accepted_block_accounting_credit_lines_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
CREATE TRIGGER accepted_block_accounting_effects_v1_immutable BEFORE UPDATE OR DELETE ON accepted_block_accounting_effects_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
CREATE TRIGGER accepted_block_accounting_conflicts_v1_immutable BEFORE UPDATE OR DELETE ON accepted_block_accounting_conflicts_v1 FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
