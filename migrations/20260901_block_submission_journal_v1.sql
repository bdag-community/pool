BEGIN;

-- This is a version-specific lock. No legacy table is altered by this file.
SELECT pg_advisory_xact_lock(hashtextextended('POOL-DURABLE-SUBMISSION-JOURNAL/v1', 1));

-- A migration may create the journal only from an empty namespace. Existing
-- same-name objects are verified below; partial or non-table objects fail
-- before any DDL can silently repair them.
DO $$
DECLARE
  present integer;
  unexpected integer;
BEGIN
  SELECT count(*) INTO present
    FROM pg_class
   WHERE relnamespace = current_schema()::regnamespace
     AND relname IN ('block_submission_candidates_v1', 'block_submission_state_v1',
                     'block_submission_attempts_v1', 'block_submission_conflicts_v1',
                     'accepted_block_submission_receipts_v1');
  IF EXISTS (
    SELECT 1 FROM pg_class
     WHERE relnamespace = current_schema()::regnamespace
       AND relname IN ('block_submission_candidates_v1', 'block_submission_state_v1',
                       'block_submission_attempts_v1', 'block_submission_conflicts_v1',
                       'accepted_block_submission_receipts_v1')
       AND relkind <> 'r'
  ) THEN
    RAISE EXCEPTION 'durable submission journal name is not a table';
  END IF;
  IF present <> 0 AND present <> 5 THEN
    RAISE EXCEPTION 'partial durable submission journal schema: % of 5 objects exist', present;
  END IF;
  SELECT count(*) INTO unexpected
    FROM pg_class
   WHERE relnamespace = current_schema()::regnamespace
     AND relname IN ('block_submission_candidates_v1_pkey',
                     'block_submission_candidates_v1_expected_block_hash_key',
                     'block_submission_state_v1_pkey',
                     'block_submission_attempts_v1_pkey',
                     'block_submission_attempts_candidate_no_v1_idx',
                     'block_submission_state_due_v1_idx',
                     'block_submission_conflicts_v1_candidate_v1_idx',
                     'accepted_block_submission_receipts_v1_pkey',
                     'accepted_block_submission_receipts_v1_source_attempt_id_key',
                     'accepted_block_submission_receipts_v1_block_hash_key',
                     'accepted_block_submission_receipts_time_v1_idx');
  IF present = 0 AND unexpected <> 0 THEN
    RAISE EXCEPTION 'durable submission journal has indexes without its tables';
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
             WHERE n.nspname=current_schema() AND p.proname IN
             ('reject_immutable_submission_row_v1',
              'reject_completed_submission_attempt_update_v1',
              'enforce_submission_state_transition_v1',
              'make_submission_retry_delay'))
     AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace
                     AND relname='block_submission_candidates_v1' AND relkind='r') THEN
    RAISE EXCEPTION 'durable submission journal has functions without its tables';
  END IF;
END $$;

CREATE TEMP TABLE _pool_submission_journal_v1_migration (fresh boolean) ON COMMIT DROP;
INSERT INTO _pool_submission_journal_v1_migration(fresh)
SELECT NOT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace
                   AND relname='block_submission_candidates_v1' AND relkind='r');

DO $$
DECLARE
  present integer;
BEGIN
  SELECT count(*) INTO present
    FROM pg_class
   WHERE relnamespace=current_schema()::regnamespace
     AND relname IN ('block_submission_candidates_v1', 'block_submission_state_v1',
                     'block_submission_attempts_v1', 'block_submission_conflicts_v1',
                     'accepted_block_submission_receipts_v1')
     AND relkind='r';
  IF present = 0 THEN
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
  END IF;
END $$;

-- Functions and triggers are created only on the fresh path. Reapplication
-- never repairs a missing or rebound object; the final catalog gate rejects it.
DO $$
DECLARE
  fresh boolean;
BEGIN
  SELECT m.fresh INTO fresh FROM _pool_submission_journal_v1_migration m;
  IF fresh THEN
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
  END IF;
END $$;

-- Final catalog gate. It intentionally checks more than names: a same-name
-- table with a changed type/default/constraint/index/trigger is drift, not a
-- migration target. The runtime verifier repeats these safety checks.
DO $$
DECLARE
  actual_type text;
  actual_nullable text;
  actual_default text;
  expected_name text;
  n integer;
  r record;
BEGIN
  FOR r IN SELECT * FROM (VALUES
    ('block_submission_candidates_v1','candidate_id','bytea','NO','none'),('block_submission_candidates_v1','frozen_digest','bytea','NO','none'),('block_submission_candidates_v1','frozen_payload','bytea','NO','none'),('block_submission_candidates_v1','chain_id','numeric(78,0)','NO','none'),('block_submission_candidates_v1','expected_block_hash','bytea','NO','none'),('block_submission_candidates_v1','header_bytes','bytea','NO','none'),('block_submission_candidates_v1','extra_nonce2','bytea','NO','none'),('block_submission_candidates_v1','nonce_bytes','bytea','NO','none'),('block_submission_candidates_v1','share_pow_hash','bytea','NO','none'),('block_submission_candidates_v1','parent_hash','bytea','NO','none'),('block_submission_candidates_v1','height','numeric(20,0)','NO','none'),('block_submission_candidates_v1','template_seq','numeric(20,0)','NO','none'),('block_submission_candidates_v1','backend_id','text','NO','none'),('block_submission_candidates_v1','created_at','timestamp with time zone','NO','clock'),
    ('block_submission_state_v1','candidate_id','bytea','NO','none'),('block_submission_state_v1','state','text','NO','none'),('block_submission_state_v1','state_version','bigint','NO','none'),('block_submission_state_v1','attempt_count','bigint','NO','none'),('block_submission_state_v1','next_attempt_at','timestamp with time zone','YES','none'),('block_submission_state_v1','lease_owner','bytea','YES','none'),('block_submission_state_v1','lease_token','bytea','YES','none'),('block_submission_state_v1','lease_expires_at','timestamp with time zone','YES','none'),('block_submission_state_v1','last_attempt_id','bytea','YES','none'),('block_submission_state_v1','last_code','text','YES','none'),('block_submission_state_v1','last_message','text','YES','none'),('block_submission_state_v1','final_code','text','YES','none'),('block_submission_state_v1','authoritative_block_hash','bytea','YES','none'),('block_submission_state_v1','authoritative_height','numeric(20,0)','YES','none'),('block_submission_state_v1','authoritative_template_seq','numeric(20,0)','YES','none'),('block_submission_state_v1','finalized_at','timestamp with time zone','YES','none'),('block_submission_state_v1','updated_at','timestamp with time zone','NO','clock'),
    ('block_submission_attempts_v1','attempt_id','bytea','NO','none'),('block_submission_attempts_v1','candidate_id','bytea','NO','none'),('block_submission_attempts_v1','attempt_no','bigint','NO','none'),('block_submission_attempts_v1','lease_token','bytea','NO','none'),('block_submission_attempts_v1','rpc_method','text','NO','none'),('block_submission_attempts_v1','request_body','bytea','NO','none'),('block_submission_attempts_v1','request_sha256','bytea','NO','none'),('block_submission_attempts_v1','phase','text','NO','none'),('block_submission_attempts_v1','prepared_at','timestamp with time zone','NO','none'),('block_submission_attempts_v1','completed_at','timestamp with time zone','YES','none'),('block_submission_attempts_v1','transport_class','text','YES','none'),('block_submission_attempts_v1','http_status','integer','YES','none'),('block_submission_attempts_v1','response_prefix','bytea','YES','none'),('block_submission_attempts_v1','response_truncated','boolean','NO','false'),('block_submission_attempts_v1','response_prefix_sha256','bytea','NO','decode'),('block_submission_attempts_v1','accepted','boolean','YES','none'),('block_submission_attempts_v1','final','boolean','YES','none'),('block_submission_attempts_v1','retryable','boolean','YES','none'),('block_submission_attempts_v1','code','text','YES','none'),('block_submission_attempts_v1','message','text','YES','none'),('block_submission_attempts_v1','block_hash','bytea','YES','none'),('block_submission_attempts_v1','height','numeric(20,0)','YES','none'),('block_submission_attempts_v1','template_seq','numeric(20,0)','YES','none'),('block_submission_attempts_v1','decision','text','NO','none'),
    ('block_submission_conflicts_v1','conflict_id','bytea','NO','none'),('block_submission_conflicts_v1','candidate_id','bytea','NO','none'),('block_submission_conflicts_v1','conflict_kind','text','NO','none'),('block_submission_conflicts_v1','existing_frozen_digest','bytea','YES','none'),('block_submission_conflicts_v1','incoming_frozen_digest','bytea','YES','none'),('block_submission_conflicts_v1','incoming_frozen_payload','bytea','YES','none'),('block_submission_conflicts_v1','related_attempt_id','bytea','YES','none'),('block_submission_conflicts_v1','difference_mask','text','NO','none'),('block_submission_conflicts_v1','observed_at','timestamp with time zone','NO','clock'),
    ('accepted_block_submission_receipts_v1','candidate_id','bytea','NO','none'),('accepted_block_submission_receipts_v1','source_attempt_id','bytea','NO','none'),('accepted_block_submission_receipts_v1','chain_id','numeric(78,0)','NO','none'),('accepted_block_submission_receipts_v1','block_hash','bytea','NO','none'),('accepted_block_submission_receipts_v1','candidate_height','numeric(20,0)','NO','none'),('accepted_block_submission_receipts_v1','core_code','text','NO','none'),('accepted_block_submission_receipts_v1','core_height','numeric(20,0)','YES','none'),('accepted_block_submission_receipts_v1','core_template_seq','numeric(20,0)','NO','none'),('accepted_block_submission_receipts_v1','accepted_at','timestamp with time zone','NO','clock')
  ) AS x(table_name,column_name,want_type,want_nullable,want_default) LOOP
    SELECT format_type(a.atttypid,a.atttypmod),CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END,COALESCE(pg_get_expr(d.adbin,d.adrelid),'')
      INTO actual_type,actual_nullable,actual_default
      FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
     WHERE a.attrelid=to_regclass(r.table_name) AND a.attname=r.column_name AND a.attnum>0 AND NOT a.attisdropped;
    IF actual_type IS NULL THEN RAISE EXCEPTION 'missing journal column %.%',r.table_name,r.column_name; END IF;
    IF actual_type<>r.want_type THEN RAISE EXCEPTION 'journal type drift in %.%: % != %',r.table_name,r.column_name,actual_type,r.want_type; END IF;
    IF actual_nullable<>r.want_nullable THEN RAISE EXCEPTION 'journal nullability drift in %.%',r.table_name,r.column_name; END IF;
    IF r.want_default='clock' AND replace(actual_default,' ','')<>'clock_timestamp()' THEN RAISE EXCEPTION 'journal default drift in %.%',r.table_name,r.column_name; END IF;
    IF r.want_default='decode' AND position('decode' in replace(actual_default,' ',''))=0 THEN RAISE EXCEPTION 'journal default drift in %.%',r.table_name,r.column_name; END IF;
    IF r.want_default='false' AND replace(actual_default,' ','')<>'false' THEN RAISE EXCEPTION 'journal default drift in %.%',r.table_name,r.column_name; END IF;
    IF r.want_default='none' AND actual_default<>'' THEN RAISE EXCEPTION 'unexpected journal default in %.%',r.table_name,r.column_name; END IF;
  END LOOP;

  FOREACH expected_name IN ARRAY ARRAY['block_submission_candidates_v1_candidate_id_len_check','block_submission_candidates_v1_frozen_digest_len_check','block_submission_candidates_v1_frozen_payload_nonempty_check','block_submission_candidates_v1_chain_id_nonnegative_check','block_submission_candidates_v1_expected_block_hash_len_check','block_submission_candidates_v1_header_bytes_len_check','block_submission_candidates_v1_extra_nonce2_len_check','block_submission_candidates_v1_nonce_bytes_len_check','block_submission_candidates_v1_share_pow_hash_len_check','block_submission_candidates_v1_parent_hash_len_check','block_submission_candidates_v1_height_range_check','block_submission_candidates_v1_template_seq_range_check','block_submission_candidates_v1_backend_id_format_check','block_submission_candidates_v1_pkey','block_submission_candidates_v1_expected_block_hash_key','block_submission_state_v1_candidate_id_fkey','block_submission_state_v1_state_enum_check','block_submission_state_v1_state_version_positive_check','block_submission_state_v1_attempt_count_nonnegative_check','block_submission_state_v1_lease_owner_len_check','block_submission_state_v1_lease_token_len_check','block_submission_state_v1_lease_shape_check','block_submission_state_v1_nonflight_lease_shape_check','block_submission_state_v1_last_attempt_id_len_check','block_submission_state_v1_terminal_next_attempt_check','block_submission_state_v1_authoritative_hash_len_check','block_submission_state_v1_authoritative_height_range_check','block_submission_state_v1_authoritative_seq_range_check','block_submission_state_v1_accepted_shape_check','block_submission_state_v1_terminal_finalized_check','block_submission_attempts_v1_candidate_id_fkey','block_submission_attempts_v1_pkey','block_submission_attempts_v1_attempt_id_len_check','block_submission_attempts_v1_attempt_no_positive_check','block_submission_attempts_v1_lease_token_len_check','block_submission_attempts_v1_method_check','block_submission_attempts_v1_request_nonempty_check','block_submission_attempts_v1_request_sha256_len_check','block_submission_attempts_v1_phase_check','block_submission_attempts_v1_response_prefix_len_check','block_submission_attempts_v1_response_sha256_len_check','block_submission_attempts_v1_block_hash_len_check','block_submission_attempts_v1_height_range_check','block_submission_attempts_v1_template_seq_range_check','block_submission_attempts_v1_decision_check','block_submission_attempts_v1_phase_timestamp_check','block_submission_attempts_v1_prepared_shape_check','block_submission_conflicts_v1_pkey','block_submission_conflicts_v1_conflict_id_len_check','block_submission_conflicts_v1_candidate_id_len_check','block_submission_conflicts_v1_existing_digest_len_check','block_submission_conflicts_v1_incoming_digest_len_check','block_submission_conflicts_v1_related_attempt_len_check','accepted_block_submission_receipts_v1_pkey','accepted_block_submission_receipts_v1_source_attempt_id_key','accepted_block_submission_receipts_v1_block_hash_key','accepted_block_submission_receipts_v1_candidate_id_len_check','accepted_receipts_v1_source_attempt_len_ck','accepted_receipts_v1_chain_nonnegative_ck','accepted_block_submission_receipts_v1_block_hash_len_check','accepted_receipts_v1_candidate_height_range_ck','accepted_block_submission_receipts_v1_core_code_check','accepted_block_submission_receipts_v1_core_height_range_check','accepted_receipts_v1_core_seq_range_ck'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conname=expected_name) THEN RAISE EXCEPTION 'missing journal constraint %',expected_name; END IF;
  END LOOP;
  SELECT count(*) INTO n FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid::regclass::text IN ('block_submission_candidates_v1','block_submission_state_v1','block_submission_attempts_v1','block_submission_conflicts_v1','accepted_block_submission_receipts_v1');
  IF n<>65 THEN RAISE EXCEPTION 'journal constraint count drift: %',n; END IF;

  FOREACH expected_name IN ARRAY ARRAY['block_submission_candidates_v1_pkey','block_submission_candidates_v1_expected_block_hash_key','block_submission_state_v1_pkey','block_submission_attempts_v1_pkey','block_submission_attempts_candidate_no_v1_idx','block_submission_state_due_v1_idx','block_submission_conflicts_v1_pkey','block_submission_conflicts_v1_candidate_v1_idx','accepted_block_submission_receipts_v1_pkey','accepted_block_submission_receipts_v1_source_attempt_id_key','accepted_block_submission_receipts_v1_block_hash_key','accepted_block_submission_receipts_time_v1_idx'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=expected_name AND relkind='i') THEN RAISE EXCEPTION 'missing journal index %',expected_name; END IF;
  END LOOP;
  SELECT md5(string_agg(indexname||':'||regexp_replace(indexdef,current_schema()||'\\.','','g'),'|' ORDER BY indexname)) INTO actual_default FROM pg_indexes WHERE schemaname=current_schema() AND indexname IN ('block_submission_candidates_v1_pkey','block_submission_candidates_v1_expected_block_hash_key','block_submission_state_v1_pkey','block_submission_attempts_v1_pkey','block_submission_attempts_candidate_no_v1_idx','block_submission_state_due_v1_idx','block_submission_conflicts_v1_pkey','block_submission_conflicts_v1_candidate_v1_idx','accepted_block_submission_receipts_v1_pkey','accepted_block_submission_receipts_v1_source_attempt_id_key','accepted_block_submission_receipts_v1_block_hash_key','accepted_block_submission_receipts_time_v1_idx');
  IF actual_default<>'456eddae33659d738af6010b00e2ee5d' THEN RAISE EXCEPTION 'journal index definition drift'; END IF;

  SELECT count(*) INTO n FROM pg_trigger t WHERE t.tgrelid=ANY(ARRAY['block_submission_candidates_v1','block_submission_state_v1','block_submission_attempts_v1','block_submission_conflicts_v1','accepted_block_submission_receipts_v1']::regclass[]) AND NOT t.tgisinternal;
  IF n<>6 THEN RAISE EXCEPTION 'journal trigger count drift: %',n; END IF;
  IF EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname='block_submission_candidates_v1_immutable' AND (t.tgrelid<>'block_submission_candidates_v1'::regclass OR p.proname<>'reject_immutable_submission_row_v1')) THEN RAISE EXCEPTION 'journal trigger binding drift'; END IF;
  IF EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname='block_submission_state_v1_transition' AND (t.tgrelid<>'block_submission_state_v1'::regclass OR p.proname<>'enforce_submission_state_transition_v1')) THEN RAISE EXCEPTION 'journal trigger binding drift'; END IF;
  IF EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname='block_submission_attempts_v1_immutable' AND (t.tgrelid<>'block_submission_attempts_v1'::regclass OR p.proname<>'reject_completed_submission_attempt_update_v1')) THEN RAISE EXCEPTION 'journal trigger binding drift'; END IF;
  IF EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname='block_submission_attempts_v1_no_delete' AND (t.tgrelid<>'block_submission_attempts_v1'::regclass OR p.proname<>'reject_immutable_submission_row_v1')) THEN RAISE EXCEPTION 'journal trigger binding drift'; END IF;
  IF EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname='block_submission_conflicts_v1_immutable' AND (t.tgrelid<>'block_submission_conflicts_v1'::regclass OR p.proname<>'reject_immutable_submission_row_v1')) THEN RAISE EXCEPTION 'journal trigger binding drift'; END IF;
  IF EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgname='accepted_block_submission_receipts_v1_immutable' AND (t.tgrelid<>'accepted_block_submission_receipts_v1'::regclass OR p.proname<>'reject_immutable_submission_row_v1')) THEN RAISE EXCEPTION 'journal trigger binding drift'; END IF;

  SELECT md5(prosrc) INTO actual_default FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace WHERE ns.nspname=current_schema() AND p.proname='reject_immutable_submission_row_v1' AND p.pronargs=0;
  IF actual_default<>'5e1b176dcfcc518ea0855f912089d4b3' THEN RAISE EXCEPTION 'journal function body drift'; END IF;
  SELECT md5(prosrc) INTO actual_default FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace WHERE ns.nspname=current_schema() AND p.proname='reject_completed_submission_attempt_update_v1' AND p.pronargs=0;
  IF actual_default<>'34f9e3688206d1263773dc207619ba58' THEN RAISE EXCEPTION 'journal function body drift'; END IF;
  SELECT md5(prosrc) INTO actual_default FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace WHERE ns.nspname=current_schema() AND p.proname='enforce_submission_state_transition_v1' AND p.pronargs=0;
  IF actual_default<>'c51eba4451f2e49b5a097d4262733022' THEN RAISE EXCEPTION 'journal function body drift'; END IF;
  SELECT md5(prosrc) INTO actual_default FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace WHERE ns.nspname=current_schema() AND p.proname='make_submission_retry_delay' AND p.pronargs=1;
  IF actual_default<>'4adf74bb20c113a4b8a35f9b6bb8c04c' THEN RAISE EXCEPTION 'journal function body drift'; END IF;
END $$;

COMMIT;
