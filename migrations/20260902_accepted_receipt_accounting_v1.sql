BEGIN;

DO $accepted_accounting_v1$
DECLARE
  installed_tables INTEGER;
  actual_columns INTEGER;
  expected_name TEXT;
  actual_function TEXT;
  actual_hash TEXT;
  actual_type TEXT;
  actual_not_null BOOLEAN;
  actual_default TEXT;
  expected_type TEXT;
  expected_not_null BOOLEAN;
  expected_default TEXT;
BEGIN
  IF to_regprocedure('reject_immutable_submission_row_v1()') IS NULL THEN
    RAISE EXCEPTION 'durable submission journal v1 must be installed first';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid=to_regclass('blocks') AND relkind='r')
     OR NOT EXISTS (SELECT 1 FROM pg_class WHERE oid=to_regclass('credits') AND relkind='r') THEN
    RAISE EXCEPTION 'legacy blocks and credits tables must be installed first';
  END IF;

  FOR expected_name,expected_type,expected_not_null,expected_default IN
    SELECT * FROM (VALUES
      ('blocks.hash','text',true,''),
      ('blocks.height','bigint',true,''),
      ('blocks.reward','numeric(30,0)',true,''),
      ('blocks.fees','numeric(30,0)',true,''),
      ('blocks.status','text',NULL::boolean,'pending'),
      ('blocks.created_at','timestamp without time zone',false,'now'),
      ('credits.id','integer',true,'nextval'),
      ('credits.block_hash','text',NULL::boolean,''),
      ('credits.miner_address','text',true,''),
      ('credits.amount','numeric(30,0)',true,''),
      ('credits.is_paid','boolean',NULL::boolean,'false'),
      ('credits.created_at','timestamp without time zone',false,'now')
    ) AS expected(column_key,column_type,column_not_null,default_tag)
  LOOP
    SELECT format_type(a.atttypid,a.atttypmod),a.attnotnull,
      CASE
        WHEN COALESCE(pg_get_expr(ad.adbin,ad.adrelid),'')='' THEN ''
        WHEN expected_name='blocks.status' AND position('PENDING' in pg_get_expr(ad.adbin,ad.adrelid))>0 THEN 'pending'
        WHEN expected_name IN ('blocks.created_at','credits.created_at') AND position('now()' in replace(pg_get_expr(ad.adbin,ad.adrelid),' ',''))>0 THEN 'now'
        WHEN expected_name='credits.id' AND position('nextval' in pg_get_expr(ad.adbin,ad.adrelid))>0 THEN 'nextval'
        WHEN expected_name='credits.is_paid' AND replace(pg_get_expr(ad.adbin,ad.adrelid),' ','')='false' THEN 'false'
        ELSE 'unexpected'
      END
      INTO actual_type,actual_not_null,actual_default
      FROM pg_attribute a
      LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
     WHERE a.attrelid=to_regclass(split_part(expected_name,'.',1))
       AND a.attname=split_part(expected_name,'.',2)
       AND a.attnum>0 AND NOT a.attisdropped;
    IF actual_type IS DISTINCT FROM expected_type
       OR (expected_not_null IS NOT NULL AND actual_not_null IS DISTINCT FROM expected_not_null)
       OR actual_default IS DISTINCT FROM expected_default THEN
      RAISE EXCEPTION 'legacy accounting target column drift in %',expected_name;
    END IF;
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='blocks'::regclass AND contype='p' AND pg_get_constraintdef(oid,true)='PRIMARY KEY (hash)')
     OR NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='credits'::regclass AND contype='p' AND pg_get_constraintdef(oid,true)='PRIMARY KEY (id)')
     OR NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='credits'::regclass AND contype='f' AND pg_get_constraintdef(oid,true)='FOREIGN KEY (block_hash) REFERENCES blocks(hash)') THEN
    RAISE EXCEPTION 'legacy accounting target key drift';
  END IF;

  SELECT count(*) INTO installed_tables
    FROM pg_class
   WHERE relnamespace=current_schema()::regnamespace
     AND relkind='r'
     AND relname IN (
       'accepted_block_accounting_intents_v1',
       'accepted_block_accounting_credit_lines_v1',
       'accepted_block_accounting_effects_v1',
       'accepted_block_accounting_conflicts_v1'
     );
  IF installed_tables NOT IN (0,4) THEN
    RAISE EXCEPTION 'accepted accounting v1 is partially installed: % tables',installed_tables;
  END IF;

  IF installed_tables=0 THEN
    CREATE TABLE accepted_block_accounting_intents_v1 (
      candidate_id BYTEA NOT NULL CONSTRAINT accepted_block_accounting_intents_v1_pkey PRIMARY KEY,
      intent_digest BYTEA NOT NULL,
      policy_version TEXT NOT NULL,
      legacy_block_hash BYTEA NOT NULL,
      height NUMERIC(20,0) NOT NULL,
      gross_reward NUMERIC(20,0) NOT NULL,
      pool_fee_float_bits NUMERIC(20,0) NOT NULL,
      fee_amount NUMERIC(20,0) NOT NULL,
      distributable NUMERIC(20,0) NOT NULL,
      total_work NUMERIC(20,0) NOT NULL,
      credit_line_count INTEGER NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      CONSTRAINT accepted_accounting_intents_v1_candidate_fk FOREIGN KEY(candidate_id) REFERENCES block_submission_candidates_v1(candidate_id),
      CONSTRAINT accepted_accounting_intents_v1_candidate_len_ck CHECK (octet_length(candidate_id)=32),
      CONSTRAINT accepted_accounting_intents_v1_digest_len_ck CHECK (octet_length(intent_digest)=32),
      CONSTRAINT accepted_accounting_intents_v1_policy_ck CHECK (policy_version='PPLNS-DISCOVERY-CUT/v1'),
      CONSTRAINT accepted_accounting_intents_v1_legacy_hash_len_ck CHECK (octet_length(legacy_block_hash)=32),
      CONSTRAINT accepted_accounting_intents_v1_height_ck CHECK (height BETWEEN 0 AND 9223372036854775807),
      CONSTRAINT accepted_accounting_intents_v1_gross_ck CHECK (gross_reward BETWEEN 0 AND 18446744073709551615),
      CONSTRAINT accepted_accounting_intents_v1_fee_bits_ck CHECK (pool_fee_float_bits BETWEEN 0 AND 18446744073709551615),
      CONSTRAINT accepted_accounting_intents_v1_fee_ck CHECK (fee_amount BETWEEN 0 AND gross_reward),
      CONSTRAINT accepted_accounting_intents_v1_distributable_ck CHECK (distributable=gross_reward-fee_amount),
      CONSTRAINT accepted_accounting_intents_v1_work_ck CHECK (total_work BETWEEN 0 AND 18446744073709551615),
      CONSTRAINT accepted_accounting_intents_v1_line_count_ck CHECK (credit_line_count BETWEEN 0 AND 100000)
    );

    CREATE TABLE accepted_block_accounting_credit_lines_v1 (
      candidate_id BYTEA NOT NULL,
      line_index INTEGER NOT NULL,
      miner_address TEXT NOT NULL,
      work NUMERIC(20,0) NOT NULL,
      amount NUMERIC(20,0) NOT NULL,
      CONSTRAINT accepted_accounting_credit_lines_v1_pkey PRIMARY KEY(candidate_id,line_index),
      CONSTRAINT accepted_accounting_credit_lines_v1_miner_key UNIQUE(candidate_id,miner_address),
      CONSTRAINT accepted_accounting_credit_lines_v1_intent_fk FOREIGN KEY(candidate_id) REFERENCES accepted_block_accounting_intents_v1(candidate_id),
      CONSTRAINT accepted_accounting_credit_lines_v1_candidate_len_ck CHECK (octet_length(candidate_id)=32),
      CONSTRAINT accepted_accounting_credit_lines_v1_index_ck CHECK (line_index BETWEEN 0 AND 99999),
      CONSTRAINT accepted_accounting_credit_lines_v1_miner_ck CHECK (octet_length(miner_address) BETWEEN 1 AND 512),
      CONSTRAINT accepted_accounting_credit_lines_v1_work_ck CHECK (work BETWEEN 1 AND 18446744073709551615),
      CONSTRAINT accepted_accounting_credit_lines_v1_amount_ck CHECK (amount BETWEEN 0 AND 18446744073709551615)
    );

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

    CREATE TRIGGER accepted_block_accounting_intents_v1_immutable
      BEFORE UPDATE OR DELETE ON accepted_block_accounting_intents_v1
      FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
    CREATE TRIGGER accepted_accounting_credit_lines_v1_immutable
      BEFORE UPDATE OR DELETE ON accepted_block_accounting_credit_lines_v1
      FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
    CREATE TRIGGER accepted_block_accounting_effects_v1_immutable
      BEFORE UPDATE OR DELETE ON accepted_block_accounting_effects_v1
      FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
    CREATE TRIGGER accepted_block_accounting_conflicts_v1_immutable
      BEFORE UPDATE OR DELETE ON accepted_block_accounting_conflicts_v1
      FOR EACH ROW EXECUTE FUNCTION reject_immutable_submission_row_v1();
  END IF;

  FOR expected_name,actual_columns IN
    SELECT * FROM (VALUES
      ('accepted_block_accounting_intents_v1',12),
      ('accepted_block_accounting_credit_lines_v1',5),
      ('accepted_block_accounting_effects_v1',5),
      ('accepted_block_accounting_conflicts_v1',7)
    ) AS expected(table_name,column_count)
  LOOP
    SELECT count(*) INTO installed_tables FROM information_schema.columns
     WHERE table_schema=current_schema() AND table_name=expected_name;
    IF installed_tables<>actual_columns THEN
      RAISE EXCEPTION 'accepted accounting column-count drift in %',expected_name;
    END IF;
  END LOOP;

  IF EXISTS (
    SELECT 1 FROM (VALUES
      ('accepted_block_accounting_intents_v1','candidate_id','bytea'),
      ('accepted_block_accounting_intents_v1','intent_digest','bytea'),
      ('accepted_block_accounting_intents_v1','height','numeric(20,0)'),
      ('accepted_block_accounting_intents_v1','pool_fee_float_bits','numeric(20,0)'),
      ('accepted_block_accounting_credit_lines_v1','line_index','integer'),
      ('accepted_block_accounting_credit_lines_v1','work','numeric(20,0)'),
      ('accepted_block_accounting_effects_v1','intent_digest','bytea'),
      ('accepted_block_accounting_conflicts_v1','conflict_id','bytea')
    ) AS expected(table_name,column_name,want_type)
    LEFT JOIN pg_attribute a ON a.attrelid=to_regclass(expected.table_name) AND a.attname=expected.column_name AND a.attnum>0 AND NOT a.attisdropped
    WHERE a.attname IS NULL OR format_type(a.atttypid,a.atttypmod)<>expected.want_type
  ) THEN
    RAISE EXCEPTION 'accepted accounting column type drift';
  END IF;

  FOREACH expected_name IN ARRAY ARRAY[
    'accepted_block_accounting_intents_v1_pkey',
    'accepted_accounting_credit_lines_v1_pkey',
    'accepted_accounting_credit_lines_v1_miner_key',
    'accepted_block_accounting_effects_v1_pkey',
    'accepted_block_accounting_conflicts_v1_pkey',
    'accepted_accounting_intents_v1_candidate_fk',
    'accepted_accounting_credit_lines_v1_intent_fk',
    'accepted_accounting_effects_v1_receipt_fk',
    'accepted_accounting_effects_v1_intent_fk'
  ] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conname=expected_name) THEN
      RAISE EXCEPTION 'missing accepted accounting constraint %',expected_name;
    END IF;
  END LOOP;

  FOR expected_name,actual_function IN
    SELECT * FROM (VALUES
      ('accepted_block_accounting_intents_v1_immutable','reject_immutable_submission_row_v1'),
      ('accepted_accounting_credit_lines_v1_immutable','reject_immutable_submission_row_v1'),
      ('accepted_block_accounting_effects_v1_immutable','reject_immutable_submission_row_v1'),
      ('accepted_block_accounting_conflicts_v1_immutable','reject_immutable_submission_row_v1')
    ) AS expected(trigger_name,function_name)
  LOOP
    SELECT p.proname INTO actual_function FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
     WHERE t.tgname=expected_name AND NOT t.tgisinternal;
    IF actual_function IS DISTINCT FROM 'reject_immutable_submission_row_v1' THEN
      RAISE EXCEPTION 'accepted accounting trigger binding drift in %',expected_name;
    END IF;
  END LOOP;

  -- Exact catalog hashes make reapplication a drift detector rather than a
  -- same-name existence check. They bind every user column (including
  -- nullability/default), constraint definition, index definition, and
  -- trigger relation/function/event mask for the four accounting tables.
  SELECT md5(string_agg(
      c.relname||'.'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||
      a.attnotnull::text||':'||COALESCE(pg_get_expr(ad.adbin,ad.adrelid),''),
      '|' ORDER BY c.relname,a.attnum))
    INTO actual_hash
    FROM pg_class c
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum
   WHERE c.relnamespace=current_schema()::regnamespace
     AND c.relname IN (
       'accepted_block_accounting_intents_v1',
       'accepted_block_accounting_credit_lines_v1',
       'accepted_block_accounting_effects_v1',
       'accepted_block_accounting_conflicts_v1');
  IF actual_hash IS DISTINCT FROM '2e91c7ca5ed29b8782156f7d1cd12238' THEN
    RAISE EXCEPTION 'accepted accounting column catalog drift';
  END IF;

  SELECT md5(string_agg(
      conname||':'||conrelid::regclass::text||':'||pg_get_constraintdef(oid,true),
      '|' ORDER BY conname))
    INTO actual_hash
    FROM pg_constraint
   WHERE connamespace=current_schema()::regnamespace
     AND conrelid=ANY(ARRAY[
       'accepted_block_accounting_intents_v1',
       'accepted_block_accounting_credit_lines_v1',
       'accepted_block_accounting_effects_v1',
       'accepted_block_accounting_conflicts_v1']::regclass[]);
  IF actual_hash IS DISTINCT FROM 'ceeb12a3dbe1b793858f15fbe46e01d5' THEN
    RAISE EXCEPTION 'accepted accounting constraint definition drift';
  END IF;

  SELECT md5(string_agg(
      indexname||':'||regexp_replace(indexdef,current_schema()||'\.','','g'),
      '|' ORDER BY indexname))
    INTO actual_hash
    FROM pg_indexes
   WHERE schemaname=current_schema()
     AND tablename IN (
       'accepted_block_accounting_intents_v1',
       'accepted_block_accounting_credit_lines_v1',
       'accepted_block_accounting_effects_v1',
       'accepted_block_accounting_conflicts_v1');
  IF actual_hash IS DISTINCT FROM 'e79c2e18807f569f5fc7a3dc0c9f8d5d' THEN
    RAISE EXCEPTION 'accepted accounting index definition drift';
  END IF;

  SELECT md5(string_agg(
      t.tgname||':'||t.tgrelid::regclass::text||':'||p.proname||':'||t.tgtype::text||':'||t.tgenabled::text,
      '|' ORDER BY t.tgname))
    INTO actual_hash
    FROM pg_trigger t
    JOIN pg_proc p ON p.oid=t.tgfoid
   WHERE t.tgrelid=ANY(ARRAY[
       'accepted_block_accounting_intents_v1',
       'accepted_block_accounting_credit_lines_v1',
       'accepted_block_accounting_effects_v1',
       'accepted_block_accounting_conflicts_v1']::regclass[])
     AND NOT t.tgisinternal;
  IF actual_hash IS DISTINCT FROM '62c2fe9e11f71f55df658a8a6afbe70d' THEN
    RAISE EXCEPTION 'accepted accounting trigger topology drift';
  END IF;

  SELECT md5(prosrc) INTO actual_hash
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
   WHERE n.nspname=current_schema()
     AND p.proname='reject_immutable_submission_row_v1'
     AND p.pronargs=0;
  IF actual_hash IS DISTINCT FROM '5e1b176dcfcc518ea0855f912089d4b3' THEN
    RAISE EXCEPTION 'accepted accounting immutable function drift';
  END IF;
END
$accepted_accounting_v1$;

COMMIT;
