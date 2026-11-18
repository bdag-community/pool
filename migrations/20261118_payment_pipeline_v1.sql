-- Apply only while the matching pipeline payer is stopped and after verifying
-- the existing reviewed catalog. Signed intents and their credit rows remain
-- immutable; unique wallet/nonce and unique credit constraints are retained.
BEGIN;
LOCK TABLE pool_payments_v1.intents IN ACCESS EXCLUSIVE MODE;
DROP INDEX pool_payments_v1.intents_one_active_wallet;
CREATE INDEX intents_active_wallet_nonce ON pool_payments_v1.intents(chain_id,sender,nonce) WHERE state<>'confirmed';
COMMIT;
