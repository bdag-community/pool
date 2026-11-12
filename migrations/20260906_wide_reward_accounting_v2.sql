BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '5min';

-- Exact V1 -> V2 widening only. A V2 reapply performs catalog verification,
-- not DDL repair. Hashes were derived from the canonical V1 DDL and the exact
-- V2 DDL below on PostgreSQL in a disposable, rolled-back transaction.
DO $wide_accounting_v2$
DECLARE
  target_schema text := current_schema();
  target_namespace oid := current_schema()::regnamespace;
  table_ids regclass[];
  lock_names text;
  column_hash text;
  constraint_hash text;
  index_hash text;
  trigger_hash text;
  trigger_definition_hash text;
  is_wide boolean;
  pass integer;
BEGIN
  SELECT array_agg(c.oid::regclass ORDER BY c.relname),
         string_agg(format('%I.%I',target_schema,c.relname),',' ORDER BY c.relname)
    INTO table_ids,lock_names
    FROM pg_class c
   WHERE c.relnamespace=target_namespace
     AND c.relname IN ('accepted_block_accounting_intents_v1',
       'accepted_block_accounting_credit_lines_v1',
       'accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1');
  IF coalesce(cardinality(table_ids),0)<>4 THEN
    RAISE EXCEPTION 'accepted accounting migration requires all four installed tables';
  END IF;
  EXECUTE 'LOCK TABLE '||lock_names||' IN ACCESS EXCLUSIVE MODE';

  FOR pass IN 1..2 LOOP
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid=ANY(table_ids)
        AND (relkind<>'r' OR relpersistence<>'p' OR relispartition
             OR relrowsecurity OR relforcerowsecurity))
       OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=ANY(table_ids) OR inhparent=ANY(table_ids))
       OR EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class=ANY(table_ids))
       OR EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=ANY(table_ids) AND attnum>0
          AND (attisdropped OR attidentity<>'' OR attgenerated<>''))
       OR EXISTS (SELECT 1 FROM pg_index WHERE indrelid=ANY(table_ids)
          AND (NOT indisvalid OR NOT indisready OR NOT indislive)) THEN
      RAISE EXCEPTION 'accepted accounting relation/index metadata drift';
    END IF;

    SELECT md5(string_agg(c.relname||'.'||a.attname||':'||
        format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull::text||':'||
        coalesce(pg_get_expr(ad.adbin,ad.adrelid),''),'|' ORDER BY c.relname,a.attnum))
      INTO column_hash
      FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum
     WHERE c.oid=ANY(table_ids);
    IF column_hash='2e91c7ca5ed29b8782156f7d1cd12238' THEN
      is_wide := false;
    ELSIF column_hash='fe1c4d76a309237388c360d349ed8856' THEN
      is_wide := true;
    ELSE
      RAISE EXCEPTION 'accepted accounting column catalog is neither exact V1 nor exact V2';
    END IF;
    IF pass=2 AND NOT is_wide THEN
      RAISE EXCEPTION 'accepted accounting widening did not reach V2';
    END IF;

    SELECT md5(string_agg(conname||':'||conrelid::regclass::text||':'||
        pg_get_constraintdef(oid,true),'|' ORDER BY conname))
      INTO constraint_hash FROM pg_constraint
     WHERE connamespace=target_namespace AND conrelid=ANY(table_ids);
    IF constraint_hash IS DISTINCT FROM (CASE WHEN is_wide
        THEN '9fd85774c4654d4e7bf0978bb09a155f' ELSE 'ceeb12a3dbe1b793858f15fbe46e01d5' END) THEN
      RAISE EXCEPTION 'accepted accounting constraint definition drift';
    END IF;
    SELECT md5(string_agg(indexname||':'||replace(indexdef,quote_ident(target_schema)||'.',''),
        '|' ORDER BY indexname))
      INTO index_hash FROM pg_indexes
     WHERE schemaname=target_schema AND tablename IN ('accepted_block_accounting_intents_v1',
       'accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1',
       'accepted_block_accounting_conflicts_v1');
    IF index_hash IS DISTINCT FROM 'e79c2e18807f569f5fc7a3dc0c9f8d5d' THEN
      RAISE EXCEPTION 'accepted accounting index definition drift';
    END IF;
    SELECT md5(string_agg(t.tgname||':'||t.tgrelid::regclass::text||':'||p.proname||':'||
        t.tgtype::text||':'||t.tgenabled::text,'|' ORDER BY t.tgname)),
        md5(string_agg(t.tgname||':'||pg_get_triggerdef(t.oid,true),'|' ORDER BY t.tgname))
      INTO trigger_hash,trigger_definition_hash
      FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
     WHERE t.tgrelid=ANY(table_ids) AND NOT t.tgisinternal;
    IF trigger_hash IS DISTINCT FROM (CASE WHEN is_wide
        THEN '6595cc44fdcefb521a98f428e09efe1b' ELSE '62c2fe9e11f71f55df658a8a6afbe70d' END)
       OR trigger_definition_hash IS DISTINCT FROM (CASE WHEN is_wide
        THEN '37617230fb58fa7822c9e0101dfca804' ELSE '71d019e794e2ca6b7e3ba4651137228f' END) THEN
      RAISE EXCEPTION 'accepted accounting trigger topology/definition drift';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        WHERE t.tgrelid=ANY(table_ids)
          AND (t.tgenabled<>'O' OR (NOT t.tgisinternal AND p.pronamespace<>target_namespace))) THEN
      RAISE EXCEPTION 'accepted accounting trigger function namespace/enabled drift';
    END IF;

    IF (SELECT count(*) FROM pg_proc WHERE pronamespace=target_namespace
          AND proname='reject_immutable_submission_row_v1')<>1
       OR NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
         WHERE p.pronamespace=target_namespace AND p.proname='reject_immutable_submission_row_v1'
           AND p.pronargs=0 AND p.pronargdefaults=0 AND p.prorettype='trigger'::regtype
           AND p.prokind='f' AND l.lanname='plpgsql' AND NOT p.prosecdef
           AND NOT p.proleakproof AND NOT p.proisstrict AND p.provolatile='v'
           AND p.proparallel='u' AND p.proconfig IS NULL
           AND md5(p.prosrc)='5e1b176dcfcc518ea0855f912089d4b3') THEN
      RAISE EXCEPTION 'accepted accounting immutable function drift';
    END IF;
    IF is_wide THEN
      IF (SELECT count(*) FROM pg_proc WHERE pronamespace=target_namespace
            AND proname='enforce_accepted_accounting_line_policy_v2')<>1
         OR NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
           WHERE p.pronamespace=target_namespace AND p.proname='enforce_accepted_accounting_line_policy_v2'
             AND p.pronargs=0 AND p.pronargdefaults=0 AND p.prorettype='trigger'::regtype
             AND p.prokind='f' AND l.lanname='plpgsql' AND NOT p.prosecdef
             AND NOT p.proleakproof AND NOT p.proisstrict AND p.provolatile='v'
             AND p.proparallel='u' AND p.proconfig IS NULL
             AND md5(p.prosrc)='235473cd68ae168af6c8bce1abdf47e8') THEN
        RAISE EXCEPTION 'accepted accounting V2 policy function drift';
      END IF;
      -- This return also makes exact V2 reapplication a pure verifier.
      RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace=target_namespace
        AND proname='enforce_accepted_accounting_line_policy_v2') THEN
      RAISE EXCEPTION 'partial V2 policy function already exists on V1 catalog';
    END IF;

    -- Every protected catalog check above has passed before the first ALTER.
    EXECUTE format('ALTER TABLE %I.accepted_block_accounting_intents_v1
      ALTER COLUMN gross_reward TYPE NUMERIC(30,0),
      ALTER COLUMN fee_amount TYPE NUMERIC(30,0),
      ALTER COLUMN distributable TYPE NUMERIC(30,0)',target_schema);
    EXECUTE format('ALTER TABLE %I.accepted_block_accounting_credit_lines_v1
      ALTER COLUMN amount TYPE NUMERIC(30,0)',target_schema);
    EXECUTE format('ALTER TABLE %I.accepted_block_accounting_intents_v1
      DROP CONSTRAINT accepted_accounting_intents_v1_policy_ck,
      DROP CONSTRAINT accepted_accounting_intents_v1_gross_ck,
      DROP CONSTRAINT accepted_accounting_intents_v1_fee_ck,
      DROP CONSTRAINT accepted_accounting_intents_v1_distributable_ck',target_schema);
    EXECUTE format($ddl$ALTER TABLE %I.accepted_block_accounting_intents_v1
      ADD CONSTRAINT accepted_accounting_intents_v1_policy_ck CHECK (policy_version IN ('PPLNS-DISCOVERY-CUT/v1','PPLNS-DISCOVERY-CUT/v2')),
      ADD CONSTRAINT accepted_accounting_intents_v1_gross_ck CHECK (gross_reward>=0 AND gross_reward<1000000000000000000000000000000 AND (policy_version<>'PPLNS-DISCOVERY-CUT/v1' OR gross_reward<=18446744073709551615)),
      ADD CONSTRAINT accepted_accounting_intents_v1_fee_ck CHECK (fee_amount>=0 AND fee_amount<=gross_reward AND (policy_version<>'PPLNS-DISCOVERY-CUT/v1' OR fee_amount<=18446744073709551615)),
      ADD CONSTRAINT accepted_accounting_intents_v1_distributable_ck CHECK (distributable=gross_reward-fee_amount AND distributable>=0 AND distributable<1000000000000000000000000000000 AND (policy_version<>'PPLNS-DISCOVERY-CUT/v1' OR distributable<=18446744073709551615))$ddl$,target_schema);
    EXECUTE format('ALTER TABLE %I.accepted_block_accounting_credit_lines_v1
      DROP CONSTRAINT accepted_accounting_credit_lines_v1_amount_ck,
      ADD CONSTRAINT accepted_accounting_credit_lines_v1_amount_ck CHECK (amount>=0 AND amount<1000000000000000000000000000000)',target_schema);
    EXECUTE format($ddl$CREATE FUNCTION %I.enforce_accepted_accounting_line_policy_v2() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE policy text;
BEGIN
  SELECT policy_version INTO policy FROM accepted_block_accounting_intents_v1 WHERE candidate_id=NEW.candidate_id;
  IF policy IS NULL OR policy NOT IN ('PPLNS-DISCOVERY-CUT/v1','PPLNS-DISCOVERY-CUT/v2') THEN RAISE EXCEPTION 'missing/invalid accounting intent policy'; END IF;
  IF NEW.amount<0 OR NEW.amount>=1000000000000000000000000000000 OR (policy='PPLNS-DISCOVERY-CUT/v1' AND NEW.amount>18446744073709551615) THEN RAISE EXCEPTION 'accounting line amount violates policy'; END IF;
  RETURN NEW;
END $fn$$ddl$,target_schema);
    EXECUTE format('CREATE TRIGGER accepted_accounting_credit_lines_v1_policy_bound
      BEFORE INSERT ON %I.accepted_block_accounting_credit_lines_v1
      FOR EACH ROW EXECUTE FUNCTION %I.enforce_accepted_accounting_line_policy_v2()',target_schema,target_schema);
    -- Next loop verifies the entire exact postcondition before COMMIT.
  END LOOP;
END $wide_accounting_v2$;

COMMIT;
