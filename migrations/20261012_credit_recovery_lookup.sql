-- Additive maintenance only. Stop/drain submission and payout writers first.
-- No accounting row, constraint, proof, deadline or runtime guard is changed.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE INDEX IF NOT EXISTS credits_block_hash_idx
    ON public.credits(block_hash);

-- Fail closed if an incompatible pre-existing object reused this name.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index i
        JOIN pg_class ix ON ix.oid = i.indexrelid
        JOIN pg_am a ON a.oid = ix.relam
        JOIN pg_attribute c ON c.attrelid = i.indrelid AND c.attname = 'block_hash'
        WHERE i.indexrelid = 'public.credits_block_hash_idx'::regclass
          AND i.indrelid = 'public.credits'::regclass AND a.amname = 'btree'
          AND i.indisvalid AND i.indisready AND i.indislive
          AND NOT i.indisunique AND i.indnatts = 1 AND i.indnkeyatts = 1
          AND i.indkey[0] = c.attnum AND i.indpred IS NULL AND i.indexprs IS NULL
    ) THEN
        RAISE EXCEPTION 'credits_block_hash_idx does not match the reviewed recovery lookup index';
    END IF;
END $$;
COMMIT;
