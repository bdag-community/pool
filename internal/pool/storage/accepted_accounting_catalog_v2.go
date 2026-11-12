package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// V2 shares the V1 tables and legacy target contract. Its catalog is a closed
// schema, not a collection of same-name or substring checks.
func verifyAcceptedAccountingCatalogV2(ctx context.Context, q accountingCatalogQueryer) error {
	legacyColumns := []struct {
		table, column, typ string
		notNull            bool
		nullabilityExempt  bool
		defaultTag         string
	}{
		{"blocks", "hash", "text", true, false, ""},
		{"blocks", "height", "bigint", true, false, ""},
		{"blocks", "reward", "numeric(30,0)", true, false, ""},
		{"blocks", "fees", "numeric(30,0)", true, false, ""},
		{"blocks", "status", "text", false, true, "PENDING"},
		{"blocks", "created_at", "timestamp without time zone", false, false, "now()"},
		{"credits", "id", "integer", true, false, "nextval"},
		{"credits", "block_hash", "text", false, true, ""},
		{"credits", "miner_address", "text", true, false, ""},
		{"credits", "amount", "numeric(30,0)", true, false, ""},
		{"credits", "is_paid", "boolean", false, true, "false"},
		{"credits", "created_at", "timestamp without time zone", false, false, "now()"},
	}
	for _, check := range legacyColumns {
		var typ, defaultExpression string
		var notNull bool
		if err := q.QueryRow(ctx, `SELECT format_type(a.atttypid,a.atttypmod),a.attnotnull,COALESCE(pg_get_expr(ad.adbin,ad.adrelid),'') FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum WHERE a.attrelid=to_regclass($1) AND a.attname=$2 AND a.attnum>0 AND NOT a.attisdropped`, check.table, check.column).Scan(&typ, &notNull, &defaultExpression); err != nil {
			return fmt.Errorf("verify legacy accounting target %s.%s: %w", check.table, check.column, err)
		}
		if typ != check.typ || (!check.nullabilityExempt && notNull != check.notNull) {
			return fmt.Errorf("legacy accounting target column drift in %s.%s", check.table, check.column)
		}
		compactDefault := strings.ReplaceAll(defaultExpression, " ", "")
		if check.defaultTag == "" && defaultExpression != "" {
			return fmt.Errorf("unexpected legacy accounting target default in %s.%s", check.table, check.column)
		}
		if check.defaultTag != "" && !strings.Contains(compactDefault, check.defaultTag) {
			return fmt.Errorf("legacy accounting target default drift in %s.%s", check.table, check.column)
		}
	}
	for _, check := range []struct {
		table, kind, definition string
	}{
		{"blocks", "p", "PRIMARY KEY (hash)"},
		{"credits", "p", "PRIMARY KEY (id)"},
		{"credits", "f", "FOREIGN KEY (block_hash) REFERENCES blocks(hash)"},
	} {
		var exists bool
		if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype=$2::"char" AND pg_get_constraintdef(oid,true)=$3)`, check.table, check.kind, check.definition).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("legacy accounting target key drift for %s", check.table)
		}
	}

	const relations = "ARRAY['accepted_block_accounting_intents_v1','accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1']::regclass[]"
	var invalid bool
	if err := q.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM pg_class WHERE oid=ANY(`+relations+`) AND relnamespace=current_schema()::regnamespace)<>4
		OR EXISTS (SELECT 1 FROM pg_class WHERE oid=ANY(`+relations+`) AND
			(relkind<>'r' OR relpersistence<>'p' OR relispartition OR relrowsecurity OR relforcerowsecurity))
		OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=ANY(`+relations+`) OR inhparent=ANY(`+relations+`))
		OR EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class=ANY(`+relations+`))
		OR EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=ANY(`+relations+`) AND attnum>0
			AND (attisdropped OR attidentity<>'' OR attgenerated<>''))
		OR EXISTS (SELECT 1 FROM pg_index WHERE indrelid=ANY(`+relations+`)
			AND (NOT indisvalid OR NOT indisready OR NOT indislive))
		OR EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
			WHERE t.tgrelid=ANY(`+relations+`)
				AND (t.tgenabled<>'O' OR (NOT t.tgisinternal AND p.pronamespace<>current_schema()::regnamespace)))`).Scan(&invalid); err != nil {
		return fmt.Errorf("verify accepted accounting V2 relation metadata: %w", err)
	}
	if invalid {
		return errors.New("accepted accounting V2 relation/index/trigger metadata drift")
	}
	checks := []struct{ name, query, want string }{
		{"column catalog", `SELECT md5(string_agg(c.relname||'.'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull::text||':'||COALESCE(pg_get_expr(ad.adbin,ad.adrelid),''),'|' ORDER BY c.relname,a.attnum))
			FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
			LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum
			WHERE c.oid=ANY(` + relations + `)`, "fe1c4d76a309237388c360d349ed8856"},
		{"constraint definitions", `SELECT md5(string_agg(conname||':'||conrelid::regclass::text||':'||pg_get_constraintdef(oid,true),'|' ORDER BY conname))
			FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid=ANY(` + relations + `)`, "9fd85774c4654d4e7bf0978bb09a155f"},
		{"index definitions", `SELECT md5(string_agg(indexname||':'||replace(indexdef,quote_ident(current_schema())||'.',''),'|' ORDER BY indexname))
			FROM pg_indexes WHERE schemaname=current_schema() AND tablename IN ('accepted_block_accounting_intents_v1','accepted_block_accounting_credit_lines_v1','accepted_block_accounting_effects_v1','accepted_block_accounting_conflicts_v1')`, "e79c2e18807f569f5fc7a3dc0c9f8d5d"},
		{"trigger topology", `SELECT md5(string_agg(t.tgname||':'||t.tgrelid::regclass::text||':'||p.proname||':'||t.tgtype::text||':'||t.tgenabled::text,'|' ORDER BY t.tgname))
			FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=ANY(` + relations + `) AND NOT t.tgisinternal`, "6595cc44fdcefb521a98f428e09efe1b"},
		{"trigger definitions", `SELECT md5(string_agg(tgname||':'||pg_get_triggerdef(oid,true),'|' ORDER BY tgname))
			FROM pg_trigger WHERE tgrelid=ANY(` + relations + `) AND NOT tgisinternal`, "37617230fb58fa7822c9e0101dfca804"},
	}
	for _, check := range checks {
		var got string
		if err := q.QueryRow(ctx, check.query).Scan(&got); err != nil {
			return fmt.Errorf("verify accepted accounting V2 %s: %w", check.name, err)
		}
		if got != check.want {
			return fmt.Errorf("accepted accounting V2 %s drift", check.name)
		}
	}
	for _, function := range []struct{ name, bodyHash string }{
		{"reject_immutable_submission_row_v1", "5e1b176dcfcc518ea0855f912089d4b3"},
		{"enforce_accepted_accounting_line_policy_v2", "235473cd68ae168af6c8bce1abdf47e8"},
	} {
		var exact bool
		if err := q.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM pg_proc WHERE pronamespace=current_schema()::regnamespace AND proname=$1)=1
			AND EXISTS (SELECT 1 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
				WHERE p.pronamespace=current_schema()::regnamespace AND p.proname=$1
				AND p.pronargs=0 AND p.pronargdefaults=0 AND p.prorettype='trigger'::regtype
				AND p.prokind='f' AND l.lanname='plpgsql' AND NOT p.prosecdef
				AND NOT p.proleakproof AND NOT p.proisstrict AND p.provolatile='v'
				AND p.proparallel='u' AND p.proconfig IS NULL AND md5(p.prosrc)=$2)`,
			function.name, function.bodyHash).Scan(&exact); err != nil {
			return fmt.Errorf("verify accepted accounting V2 function %s: %w", function.name, err)
		}
		if !exact {
			return fmt.Errorf("accepted accounting V2 function %s drift", function.name)
		}
	}
	return nil
}
