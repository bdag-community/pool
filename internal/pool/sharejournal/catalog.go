package sharejournal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/jackc/pgx/v5"
)

type catalogBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

// Qualified only against the pinned isolated PostgreSQL 15 migration subject.
// Other database versions and authority layouts require separate qualification.
const ReviewedCatalogFingerprint = "f1a1c6d26123a39d280de4f9baf7e487604b01bb061041fc9e8bffde8961c487"

// VerifyCatalog fails closed on missing or structurally changed schema. It
// does not verify data, role privileges outside this namespace, node identity,
// reward eligibility, or signer authority.
func VerifyCatalog(ctx context.Context, db catalogBeginner) error {
	actual, err := CatalogFingerprint(ctx, db)
	if err != nil {
		return err
	}
	if actual != ReviewedCatalogFingerprint {
		return errors.New("share journal catalog drift; unpriced discovery held")
	}
	return nil
}

// CatalogFingerprint hashes the structure of the journal namespace, not its
// contents. A caller must compare it with a separately reviewed, compiled
// fingerprint and verify database identity and authority. This function by
// itself does not authorize startup, candidate submission, or payment.
func CatalogFingerprint(ctx context.Context, db catalogBeginner) (string, error) {
	if db == nil || !bounded(ctx) {
		return "", errors.New("bounded catalog query and database are required")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SET LOCAL search_path=pg_catalog`); err != nil {
		return "", err
	}
	var descriptor []byte
	err = tx.QueryRow(ctx, `SELECT json_build_object(
 'schema',(SELECT json_build_object('owner_ok',n.nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user),'acl',COALESCE(n.nspacl::text,'')) FROM pg_namespace n WHERE n.nspname='pool_share_journal_v1'),
 'relations',(SELECT json_agg(x ORDER BY x.name) FROM (SELECT c.relname name,c.relkind::text kind,c.relpersistence::text persistence,c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) owner_ok,c.relrowsecurity rls,c.relforcerowsecurity force_rls,COALESCE(c.relacl::text,'') acl FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_share_journal_v1') x),
 'columns',(SELECT json_agg(x ORDER BY x.tbl,x.pos) FROM (SELECT c.relname tbl,a.attnum pos,a.attname name,format_type(a.atttypid,a.atttypmod) typ,a.attnotnull nn,a.attidentity::text identity,COALESCE(pg_get_expr(d.adbin,d.adrelid),'') def FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE n.nspname='pool_share_journal_v1' AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped) x),
 'constraints',(SELECT json_agg(x ORDER BY x.tbl,x.name) FROM (SELECT c.relname tbl,k.conname name,pg_get_constraintdef(k.oid,true) def,k.convalidated valid,k.condeferrable is_deferrable,k.condeferred is_deferred FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_share_journal_v1') x),
 'indexes',(SELECT json_agg(x ORDER BY x.name) FROM (SELECT i.relname name,pg_get_indexdef(i.oid) def,idx.indisvalid valid,idx.indisready ready,idx.indisunique uniq,idx.indisprimary primary_key FROM pg_index idx JOIN pg_class i ON i.oid=idx.indexrelid JOIN pg_class c ON c.oid=idx.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_share_journal_v1') x),
 'triggers',(SELECT json_agg(x ORDER BY x.tbl,x.name) FROM (SELECT c.relname tbl,t.tgname name,t.tgenabled::text enabled,pg_get_triggerdef(t.oid,true) def FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_share_journal_v1' AND NOT t.tgisinternal) x),
 'functions',(SELECT json_agg(x ORDER BY x.name,x.args) FROM (SELECT p.proname name,p.prosrc body,p.provolatile::text volatility,p.prosecdef security_definer,p.proconfig config,pg_get_function_identity_arguments(p.oid) args,pg_get_function_result(p.oid) result,p.proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) owner_ok,COALESCE(p.proacl::text,'') acl FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pool_share_journal_v1') x),
 'sequences',(SELECT json_agg(x ORDER BY x.name) FROM (SELECT c.relname name,format_type(s.seqtypid,NULL) typ,s.seqstart,s.seqincrement,s.seqmax,s.seqmin,s.seqcache,s.seqcycle FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pool_share_journal_v1') x)
)::text`).Scan(&descriptor)
	if err != nil {
		return "", err
	}
	// Rolling back this read-only transaction also restores SET LOCAL when
	// Begin created a savepoint inside a caller-owned transaction. Releasing
	// that savepoint would leave the caller's search_path changed.
	if err = tx.Rollback(ctx); err != nil {
		return "", err
	}
	sum := sha256.Sum256(descriptor)
	return hex.EncodeToString(sum[:]), nil
}
