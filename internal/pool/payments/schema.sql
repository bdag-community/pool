-- Explicit additive migration. Never run implicitly during pool startup.
CREATE SCHEMA pool_payments_v1;

CREATE TABLE pool_payments_v1.wallet_state (
    chain_id NUMERIC(20,0) NOT NULL CHECK (chain_id=1404),
    sender BYTEA NOT NULL CHECK (octet_length(sender)=20),
    hold_reason TEXT,
    checkpoint_number NUMERIC(20,0),
    checkpoint_hash BYTEA,
    checkpoint_cursor BIGINT NOT NULL DEFAULT 0 CHECK (checkpoint_cursor>=0),
    PRIMARY KEY(chain_id,sender),
    CHECK ((checkpoint_number IS NULL AND checkpoint_hash IS NULL) OR
           (checkpoint_number IS NOT NULL AND checkpoint_hash IS NOT NULL AND checkpoint_number BETWEEN 0 AND 18446744073709551615 AND octet_length(checkpoint_hash)=32)),
    CHECK ((checkpoint_cursor=0 AND checkpoint_number IS NULL) OR (checkpoint_cursor>0 AND checkpoint_number IS NOT NULL))
);

CREATE TABLE pool_payments_v1.intents (
    credit_id INTEGER PRIMARY KEY REFERENCES public.credits(id),
    chain_id NUMERIC(20,0) NOT NULL,
    sender BYTEA NOT NULL,
    credit_block_hash TEXT NOT NULL CHECK (credit_block_hash ~ '^[0-9a-fA-F]{64}$'),
    recipient BYTEA NOT NULL CHECK (octet_length(recipient)=20),
    amount NUMERIC(30,0) NOT NULL CHECK (amount>0),
    nonce NUMERIC(20,0) NOT NULL CHECK (nonce BETWEEN 0 AND 18446744073709551615),
    gas_price NUMERIC(78,0) NOT NULL CHECK (gas_price>0 AND gas_price<power(2::numeric,256)),
    raw_tx BYTEA NOT NULL CHECK (octet_length(raw_tx) BETWEEN 1 AND 4096),
    tx_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(tx_hash)=32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','held','confirmed')),
    hold_reason TEXT,
    confirmed_number NUMERIC(20,0),
    confirmed_hash BYTEA,
    confirmed_at TIMESTAMPTZ,
    FOREIGN KEY(chain_id,sender) REFERENCES pool_payments_v1.wallet_state(chain_id,sender),
    UNIQUE(chain_id,sender,nonce),
    CHECK ((state='pending' AND hold_reason IS NULL AND confirmed_number IS NULL AND confirmed_hash IS NULL AND confirmed_at IS NULL) OR
           (state='held' AND hold_reason IS NOT NULL AND confirmed_number IS NULL AND confirmed_hash IS NULL AND confirmed_at IS NULL) OR
           (state='confirmed' AND hold_reason IS NULL AND confirmed_number IS NOT NULL AND confirmed_hash IS NOT NULL AND confirmed_number BETWEEN 0 AND 18446744073709551615 AND octet_length(confirmed_hash)=32 AND confirmed_at IS NOT NULL))
);
CREATE INDEX intents_active_wallet_nonce ON pool_payments_v1.intents(chain_id,sender,nonce) WHERE state<>'confirmed';

CREATE TABLE pool_payments_v1.events (
    event_id BIGSERIAL PRIMARY KEY,
    credit_id INTEGER REFERENCES public.credits(id),
    kind TEXT NOT NULL CHECK (kind IN ('prepared','held','confirmed','wallet_held','checkpoint','zero_satisfied')),
    detail TEXT NOT NULL CHECK (octet_length(detail)<=2048),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (credit_id IS NOT NULL OR kind IN ('wallet_held','checkpoint'))
);
CREATE UNIQUE INDEX events_one_confirmation_per_credit ON pool_payments_v1.events(credit_id) WHERE kind='confirmed';
CREATE INDEX events_confirmed_cursor ON pool_payments_v1.events(event_id,credit_id) WHERE kind='confirmed';

CREATE FUNCTION pool_payments_v1.protect_intent() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'payment intents cannot be deleted'; END IF;
    IF ROW(OLD.credit_id,OLD.chain_id,OLD.sender,OLD.credit_block_hash,OLD.recipient,OLD.amount,OLD.nonce,OLD.gas_price,OLD.raw_tx,OLD.tx_hash,OLD.created_at)
       IS DISTINCT FROM ROW(NEW.credit_id,NEW.chain_id,NEW.sender,NEW.credit_block_hash,NEW.recipient,NEW.amount,NEW.nonce,NEW.gas_price,NEW.raw_tx,NEW.tx_hash,NEW.created_at) THEN
       RAISE EXCEPTION 'signed payment identity is immutable';
    END IF;
    IF OLD.state='confirmed' AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'confirmed payment evidence is immutable'; END IF;
    IF OLD.state='held' AND NEW.state='pending' THEN RAISE EXCEPTION 'held payment needs canonical confirmation or review'; END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER protect_intent BEFORE UPDATE OR DELETE ON pool_payments_v1.intents FOR EACH ROW EXECUTE FUNCTION pool_payments_v1.protect_intent();

CREATE FUNCTION pool_payments_v1.protect_event() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
BEGIN RAISE EXCEPTION 'payment events are append only'; END;
$body$;
CREATE TRIGGER protect_event BEFORE UPDATE OR DELETE ON pool_payments_v1.events FOR EACH ROW EXECUTE FUNCTION pool_payments_v1.protect_event();

-- Confirmation state and its cursor event must commit together. Otherwise a
-- bounded history scan could silently skip a confirmed intent without an event.
CREATE FUNCTION pool_payments_v1.check_confirmation_link() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
BEGIN
    IF TG_TABLE_NAME='intents' THEN
        IF NEW.state<>'confirmed' THEN RETURN NULL; END IF;
    ELSE
        IF NEW.kind<>'confirmed' THEN RETURN NULL; END IF;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM pool_payments_v1.intents p
        JOIN pool_payments_v1.events e ON e.credit_id=p.credit_id AND e.kind='confirmed'
        WHERE p.credit_id=NEW.credit_id AND p.state='confirmed') THEN
        RAISE EXCEPTION 'confirmed payment and immutable event must commit together';
    END IF;
    RETURN NULL;
END;
$body$;
CREATE CONSTRAINT TRIGGER confirmation_has_event AFTER INSERT OR UPDATE ON pool_payments_v1.intents
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pool_payments_v1.check_confirmation_link();
CREATE CONSTRAINT TRIGGER event_has_confirmation AFTER INSERT ON pool_payments_v1.events
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pool_payments_v1.check_confirmation_link();
