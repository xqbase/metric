package com.xqbase.h2pg;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.engine.SessionLocal;
import org.h2.result.ResultInterface;
import org.h2.server.pg.PgServer;
import org.h2.server.pg.PgServerThread;
import org.h2.util.Bits;
import org.h2.util.ScriptReader;
import org.h2.util.Utils;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.parser.TokenMgrException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.UnionOp;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;

public class PgServerThreadCompat implements Runnable {
	private static Field initDone, out, dataInRaw, stop, portalsField, sessionField;
	private static Field formatField, portalPrep, preparedPrep;
	private static Method process, getEncoding, close, setProcessId, setThread;
	private static Constructor<PgServerThread> newThread;

	private static Field getField(Class<?> clazz,
			String name) throws ReflectiveOperationException {
		Field field = clazz.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}

	private static Field getField(String name) throws ReflectiveOperationException {
		return getField(PgServerThread.class, name);
	}

	private static Method getMethod(String name, Class<?>... paramTypes)
			throws ReflectiveOperationException {
		Method method = PgServerThread.class.getDeclaredMethod(name, paramTypes);
		method.setAccessible(true);
		return method;
	}

	private static SelectBody select(String sql) {
		CCJSqlParser parser = new CCJSqlParser(new StringProvider(sql));
		try {
			return ((Select) parser.Statement()).getSelectBody();
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	private static Table table(String table, String alias) {
		Table t = new Table(table);
		t.setAlias(new Alias(alias));
		return t;
	}

	private static Join join(String table, String alias) {
		Join join = new Join();
		join.setRightItem(table(table, alias));
		join.setSimple(true);
		return join;
	}

	private static final Column FALSE = new Column("FALSE");
	private static final String IN_PARENTHESES_PLACEHOLDER = "SELECT '{IN_PARENTHESES}'";
	private static final String NOOP = "SET NETWORK_TIMEOUT 0";
	private static final String JOIN_INDEX =
			"JOIN pg_namespace ni ON ni.nspname = i.index_schema " +
			"JOIN pg_class ci ON ci.relnamespace = ni.id " +
			"AND ci.relname = i.index_name AND ci.relkind = 'i' ";
	private static final String JOIN_TABLE =
			"JOIN pg_namespace nt ON nt.nspname = t.table_schema " +
			"JOIN pg_class ct ON ct.relnamespace = nt.id " +
			"AND ct.relname = t.table_name AND ct.relkind = 'r' ";
	private static final String[] REPLACE_FROM = {
		"(indpred IS NOT NULL)",
		// "check", "deferrable" and "tablespace" are keywords in JSqlParser
		" as check,",
		", condeferrable::int AS deferrable, ",
		"tablespace) AS tablespace",
		"')) as tablespace, ",
		", t.spcname AS tablespace, ",
		" CAST('*' AS pg_catalog.text) ",
		// JSqlParser cannot parse SELECT a = b, ... Just replace:
		// nsp.nspname = ANY('{information_schema}') -> nsp.nspname = 'information_schema'
		" WHEN nsp.nspname = ANY('{information_schema}')",
		// https://github.com/JSQLParser/JSqlParser/issues/720
		") IS NOT NULL AS attisserial,",
		// for phpPgAdmin 7.12.1
		"max(SUBSTRING(array_dims(c.conkey) FROM  $pattern$^\\[.*:(.*)\\]$$pattern$)) as nb",
		// for phpPgAdmin 7.0-dev (docker.io/dockage/phppgadmin)
		"max(SUBSTRING(array_dims(c.conkey) FROM  $patern$^\\[.*:(.*)\\]$$patern$)) as nb",
		// JSqlParser cannot parse `value::"type"` and `value::information_schema.type`
		"'::\"char\"",
		")::information_schema.",
		// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.9
		" (i.keys).n ",
		" (i.keys).x ",
		// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.10
		" (information_schema._pg_expandarray(i.indkey)).n ",
		" result.A_ATTNUM = (result.KEYS).x ",
		// for DatabaseMetaData.getVersionColumns() in PgJDBC
		" typinput='array_in'::regproc",
		// for Postico
		", ARRAY(SELECT pg_opclass.opcname FROM generate_series(0, indnatts-1) AS t(i) " +
		"LEFT JOIN pg_opclass ON pg_opclass.oid = indclass[i]) AS opclasses, ",
		// for Navicat
		", (c.reltriggers > 0) AS hastriggers, ",
		", ((SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid) > 0) AS inhtable, ",
		// for pgcli
		" = ANY(ARRAY[E'r',E'p',E'f'])",
		" = ANY(ARRAY[E'v',E'm'])",
		"(SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)",
		"d.deptype = 'e' is_extension,",
		"((t.tgtype)::integer & ",
		", pg_get_expr(indpred, indrelid, true) AS constraint, ",
		// for dbForge
		" dv.adsrc IS NOT NULL AND dv.adsrc LIKE 'nextval(%)' AS autoinc,",
	};
	private static final String[] REPLACE_TO = {
		"(NVL2(indpred, TRUE, FALSE))",
		" as \"check\",",
		", 0 \"deferrable\", ",
		"tablespace) \"tablespace\"",
		"')) as \"tablespace\", ",
		", t.spcname AS \"tablespace\", ",
		" '*' ",
		" WHEN nsp.nspname = 'information_schema'",
		")::CAST_TO_FALSE attisserial,",
		"MAX(array_length(c.conkey)) nb",
		"MAX(array_length(c.conkey)) nb",
		"'",
		")::",
		" i.keys_n ",
		" i.keys_x ",
		" i.keys_n ",
		" result.A_ATTNUM = result.keys_x ",
		" FALSE",
		", NULL opclasses, ",
		", (CASE WHEN c.reltriggers > 0 THEN TRUE ELSE FALSE END) AS hastriggers, ",
		", (CASE WHEN (SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid) > 0 " +
		"THEN TRUE ELSE FALSE END) AS inhtable, ",
		" IN ('r', 'p', 'f')",
		" IN ('v', 'm')",
		"(SELECT (CASE c.relkind WHEN 'c' THEN TRUE ELSE FALSE END) " +
		"FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)",
		"(CASE d.deptype WHEN 'e' THEN TRUE ELSE FALSE END) is_extension,",
		"(",
		", pg_get_expr(indpred, indrelid, true) AS \"constraint\", ",
		" (CASE WHEN dv.adsrc IS NOT NULL AND dv.adsrc LIKE 'nextval(%)' " +
		"THEN TRUE ELSE FALSE END) AS autoinc,",
	};

	private static ValuesList pgGetKeywords = new ValuesList();
	private static Map<String, SelectBody> tableMap = new HashMap<>();
	private static Map<String, Expression> functionMap = new HashMap<>();

	private static void addTable(String name, String sql) {
		SelectBody sb = select(sql.replace("${owner}",
				"(SELECT oid FROM pg_user WHERE usename = current_user())"));
		tableMap.put(name, sb);
		if (!name.startsWith("information_schema.")) {
			tableMap.put("pg_catalog." + name, sb);
		}
	}

	private static void addColumns(String name, String columns) {
		addTable(name, "SELECT *, " + columns + " FROM " + name);
	}

	private static void addEmptyTable(String name, String columns) {
		addTable(name, "SELECT " + columns + " WHERE FALSE");
	}

	private static void addFunction(String name, Expression exp) {
		functionMap.put(name, exp);
		if (!name.startsWith("information_schema.")) {
			functionMap.put("pg_catalog." + name, exp);
		}
	}

	static {
		String[] tokens;
		try {
			initDone = getField("initDone");
			out = getField("out");
			dataInRaw = getField("dataInRaw");
			stop = getField("stop");
			portalsField = getField("portals");
			sessionField = getField("session");
			Class<?> portalClass = Class.forName("org.h2.server.pg.PgServerThread$Portal");
			formatField = getField(portalClass, "resultColumnFormat");
			portalPrep = getField(portalClass, "prep");
			preparedPrep = getField(Class.
					forName("org.h2.server.pg.PgServerThread$Prepared"), "prep");
			process = getMethod("process");
			getEncoding = getMethod("getEncoding");
			close = getMethod("close");
			setProcessId = getMethod("setProcessId", int.class);
			setThread = getMethod("setThread", Thread.class);
			newThread = PgServerThread.class.getDeclaredConstructor(Socket.class, PgServer.class);
			newThread.setAccessible(true);
			Field tokensField = Parser.class.getDeclaredField("TOKENS");
			tokensField.setAccessible(true);
			tokens = (String[]) tokensField.get(null);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
		MultiExpressionList words = new MultiExpressionList();
		for (String token : tokens) {
			if (token != null && !token.isEmpty()) {
				char c = token.charAt(0);
				if (c >= 'A' && c <= 'Z') {
					words.addExpressionList(new StringValue(token.toLowerCase()));
				}
			}
		}
		pgGetKeywords.setMultiExpressionList(words);
		pgGetKeywords.setColumnNames(Arrays.asList("word"));
		pgGetKeywords.setAlias(new Alias("pg_get_keywords"));

		addColumns("pg_attribute", "0 attndims, 0 attstattarget, NULL attstorage");
		addColumns("pg_class", "FALSE relisshared, 0 relnatts, 0 reloftype, 0 reltoastrelid, " +
				"NULL relacl, ${owner} relowner, TRUE relhaspkey, FALSE relhastriggers, " +
				"FALSE relhassubclass, NULL tableoid, NULL reloptions, NULL relpersistence");
		addColumns("pg_constraint", "FALSE condeferrable, FALSE condeferred, " +
				"NULL confkey, NULL confdeltype, NULL confmatchtype, NULL confupdtype, " +
				"NULL connamespace, NULL tableoid, NULL conbin, NULL consrc");
		addColumns("pg_database", "-1 datconnlimit, FALSE datistemplate");
		addColumns("pg_namespace", "id oid, ${owner} nspowner, NULL nspacl");
		addColumns("pg_proc", "NULL proallargtypes, NULL proargmodes, NULL proargnames, " +
				"NULL prolang, NULL proretset, 0 pronargs, FALSE proisagg");
		addColumns("pg_roles", "TRUE rolcanlogin, -1 rolconnlimit, NULL rolvaliduntil");
		addColumns("pg_user", "oid usesysid");
		addColumns("information_schema.columns", "NULL udt_schema, data_type udt_name");
		addColumns("information_schema.routines", "NULL type_udt_name");
		addEmptyTable("pg_collation", "0 oid, '' collnamespace, '' collname");
		addEmptyTable("pg_depend", "'' deptype, 0 classid, 0 refclassid, " +
				"0 objid, 0 objsubid, 0 refobjid, 0 refobjsubid");
		addEmptyTable("pg_event_trigger", "0");
		addEmptyTable("pg_language", "0 oid, '' lanname");
		addEmptyTable("pg_rewrite", "0 oid, '' rulename, " +
				"0 ev_class, '' ev_type, FALSE is_instead");
		addEmptyTable("pg_shdepend", "'' deptype, 0 classid, 0 refclassid, " +
				"0 objid, 0 objsubid, 0 refobjid, 0 refobjsubid");
		addEmptyTable("pg_stat_activity", "'' state, '' datname");
		addEmptyTable("pg_stat_database", "0 xact_commit, 0 xact_rollback, " +
				"0 tup_inserted, 0 tup_updated, 0 tup_deleted, 0 tup_fetched, 0 tup_returned, " +
				"0 blks_read, 0 blks_hit, '' datname");
		addEmptyTable("pg_trigger", "0 oid, '' tgname, 0 tableoid, 0 tgfoid, 0 tgrelid, " +
				"0 tgconstrrelid, '' tgenabled, FALSE tgisconstraint, FALSE tgisinternal, " +
				"FALSE tgdeferrable, FALSE tginitdeferred, '' tgconstrname, 0 tgnargs, NULL tgargs");
		addEmptyTable("information_schema.role_table_grants", "'' grantor, '' grantee, " +
				"'' table_schema, '' table_name, '' privilege_type, " +
				"FALSE is_grantable, FALSE with_hierarchy");
		addTable("pg_index", "SELECT ci.oid indexrelid, ct.oid indrelid, COUNT(*) indnatts, " +
				"(CASE index_type_name WHEN 'PRIMARY KEY' THEN TRUE ELSE FALSE END) indisclustered, " +
				"(CASE index_type_name WHEN 'PRIMARY KEY' THEN TRUE " +
				"WHEN 'UNIQUE INDEX' THEN TRUE ELSE FALSE END) indisunique, " +
				"(CASE index_type_name WHEN 'PRIMARY KEY' THEN TRUE ELSE FALSE END) indisprimary, " +
				// should be ARRAY_AGG(c.ordinal_position ORDER BY i.ordinal_position,
				// but not supported by JSqlParser
				"NULL indexprs, ARRAY_AGG(c.ordinal_position) indkey, NULL indclass, " +
				"NULL indpred, NULL indoption FROM information_schema.index_columns ic " +
				"JOIN information_schema.indexes i USING (index_schema, index_name) " + JOIN_INDEX +
				"JOIN information_schema.tables t USING (table_schema, table_name) " + JOIN_TABLE +
				"JOIN information_schema.columns c USING (table_schema, table_name, column_name) " +
				"GROUP BY ci.oid");
		addTable("pg_indexes", "SELECT nt.nspname AS schemaname, ct.relname AS tablename, " +
				"ci.relname AS indexname, ts.spcname AS \"tablespace\", " +
				"pg_get_indexdef(ci.oid) AS indexdef FROM information_schema.indexes i " + JOIN_INDEX +
				"JOIN information_schema.tables t USING (table_schema, table_name) " + JOIN_TABLE +
				"LEFT JOIN pg_tablespace ts ON ts.oid = ci.reltablespace");
		addTable("pg_settings", "SELECT name, setting, '' source FROM pg_settings " +
				"UNION ALL SELECT 'max_index_keys', '32', ''");
		addTable("pg_tables", "SELECT n.nspname schemaname, c.relname tablename FROM pg_class c " +
				"LEFT JOIN pg_namespace n ON n.id = c.relnamespace WHERE c.relkind IN ('r', 'p')");
		addTable("pg_type", "SELECT oid, (CASE oid " +
				"WHEN 16 THEN 'bool' WHEN 17 THEN 'bytea' WHEN 20 THEN 'int8' " +
				"WHEN 21 THEN 'int2' WHEN 23 THEN 'int4' WHEN 25 THEN 'text' " +
				"WHEN 700 THEN 'float4' WHEN 701 THEN 'float8' WHEN 1042 THEN 'bpchar' " +
				"WHEN 1043 THEN 'varchar' WHEN 1184 THEN 'timestampz' WHEN 1266 THEN 'timetz' " +
				"ELSE LOWER(typname) END) \"typname\", " +
				"typnamespace, typlen, typtype, typdelim, typrelid, " +
				"typelem, typbasetype, typtypmod, typnotnull, typinput, " +
				"FALSE typbyval, NULL typcategory, NULL typcollation, NULL typdefault, " +
				"0 typndims, 0 typarray, ${owner} typowner, NULL typalign, NULL typstorage " +
				"FROM pg_type");
		addTable("pg_views", "SELECT n.nspname schemaname, c.relname viewname FROM pg_class c " +
				"LEFT JOIN pg_namespace n ON n.id = c.relnamespace WHERE c.relkind = 'v'");
		addTable("INFORMATION_SCHEMA.character_sets", "SELECT 'UTF8' character_set_name");

		NullValue nul = new NullValue();
		addFunction("information_schema._pg_char_max_length", nul);
		addFunction("information_schema._pg_numeric_precision", nul);
		addFunction("information_schema._pg_numeric_precision_radix", nul);
		addFunction("information_schema._pg_numeric_scale", nul);
		addFunction("information_schema._pg_datetime_precision", nul);
		addFunction("col_description", nul);
		addFunction("generate_subscripts", nul);
		addFunction("oidvectortypes", nul);
		addFunction("pg_get_constraintdef", nul);
		addFunction("pg_get_triggerdef", nul);
		addFunction("pg_get_functiondef", nul);
		addFunction("pg_get_ruledef", nul);
		addFunction("pg_get_viewdef", nul);
		addFunction("pg_relation_filepath", nul);
		addFunction("shobj_description", nul);
		Column tru = new Column("TRUE");
		addFunction("pg_cancel_backend", tru);
		addFunction("pg_function_is_visible", tru);
		addFunction("pg_type_is_visible", tru);
		addFunction("pg_is_other_temp_schema", FALSE);
		LongValue zero = new LongValue(0);
		addFunction("pg_backend_pid", zero);
		addFunction("pg_database_size", zero);
		// https://github.com/h2database/h2database/issues/2509
		addFunction("pg_relation_size", zero);
		addFunction("pg_total_relation_size", zero);
		addFunction("pg_size_pretty", new StringValue("0"));
		addFunction("array_lower", new LongValue(1));
		addFunction("current_schemas", new Column("ARRAY['public']"));
		addFunction("row_to_json", new StringValue("{}"));
	}

	private static ExpressionList getExpressionList(String value) {
		String ss = value;
		int len = ss.length();
		if (len >= 2 && ss.charAt(0) == '{' && ss.charAt(len - 1) == '}') {
			ss = ss.substring(1, len - 1);
		}
		List<Expression> inExps = new ArrayList<>();
		for (String s : ss.split(",")) {
			inExps.add(new StringValue(s));
		}
		return new ExpressionList(inExps);
	}

	private boolean replaced = false;
	private boolean anyArray = false;

	private Function arrayContains(Expression array, Expression value) {
		ExpressionList el = new ExpressionList(array, value);
		replace(array, e -> el.getExpressions().set(0, e));
		replace(value, e -> el.getExpressions().set(1, e));
		Function func = new Function();
		func.setName("ARRAY_CONTAINS");
		func.setParameters(el);
		return func;
	}

	private void replace(Expression exp, Consumer<Expression> parentSet) {
		if (exp instanceof Column) {
			return;
		}
		if (exp instanceof Function) {
			Function func = (Function) exp;
			ExpressionList el = func.getParameters();
			Expression result = functionMap.get(func.getName());
			if (result != null) {
				parentSet.accept(result);
				replaced = true;
				return;
			}
			switch (func.getName()) {
			case "array_length":
			case "pg_catalog.array_length":
				if (el != null) {
					List<Expression> exps = el.getExpressions();
					if (exps.size() > 1) {
						el.setExpressions(Arrays.asList(exps.get(0)));
						replaced = true;
					}
				}
				break;
			case "array_upper":
			case "pg_catalog.array_upper":
				if (el != null && el.getExpressions().size() > 0) {
					Expression exp0 = el.getExpressions().get(0);
					// array_upper(p.pro[all]argtypes, 1) -> -1
					// p.pro[all]argtypes in sub-query does not work
					switch (exp0.toString()) {
					case "p.proargtypes":
					case "p.proallargtypes":
						parentSet.accept(new LongValue(-1));
						replaced = true;
						return;
					default:
					}
					el.setExpressions(Arrays.asList(exp0));
				}
				func.setName("array_length");
				replaced = true;
				break;
			default:
			}
			if (el != null) {
				List<Expression> exps = el.getExpressions();
				for (int i = 0; i < exps.size(); i ++) {
					int i_ = i;
					replace(exps.get(i), e -> exps.set(i_, e));
				}
			}
			return;
		}
		if (exp instanceof NotExpression) {
			NotExpression ne = (NotExpression) exp;
			replace(ne.getExpression(), ne::setExpression);
			return;
		}
		if (exp instanceof IsNullExpression) {
			IsNullExpression ine = (IsNullExpression) exp;
			replace(ine.getLeftExpression(), ine::setLeftExpression);
			return;
		}
		if (exp instanceof CaseExpression) {
			CaseExpression ce = (CaseExpression) exp;
			replace(ce.getSwitchExpression(), ce::setSwitchExpression);
			for (WhenClause wc : ce.getWhenClauses()) {
				replace(wc.getWhenExpression(), wc::setWhenExpression);
				replace(wc.getThenExpression(), wc::setThenExpression);
			}
			replace(ce.getElseExpression(), ce::setElseExpression);
			return;
		}
		if (exp instanceof Parenthesis) {
			Parenthesis parenth = (Parenthesis) exp;
			replace(parenth.getExpression(), parenth::setExpression);
			return;
		}
		if (exp instanceof CastExpression) {
			CastExpression ce = (CastExpression) exp;
			String type = ce.getType().getDataType();
			if (type.equals("CAST_TO_FALSE")) {
				parentSet.accept(FALSE);
				replaced = true;
				return;
			}
			Expression oldLeft = ce.getLeftExpression();
			Expression left = oldLeft;
			while (left instanceof CastExpression) {
				left = ((CastExpression) left).getLeftExpression();
			}
			switch (type) {
			case "cardinal_number":
			case "character_data":
			case "name":
			case "sql_identifier":
				parentSet.accept(left);
				replace(left, parentSet);
				replaced = true;
				return;
			case "oid":
			case "regclass":
				if (left instanceof LongValue && type.equals("regclass")) {
					// number::regclass ->
					// IFNULL(SELECT relname FROM pg_class WHERE oid = number, number::text)
					SubSelect ss = new SubSelect();
					ss.setSelectBody(select("SELECT relname FROM pg_class WHERE oid = " +
							(int) ((LongValue) left).getValue()));
					Function func = new Function();
					func.setName("IFNULL");
					func.setParameters(new ExpressionList(ss, new StringValue(left.toString())));
					parentSet.accept(func);
					replaced = true;
					return;
				}
				if (left instanceof StringValue) {
					// '"schema"."table"'::regclass -> 'table'::regclass
					String s = ((StringValue) left).getValue();
					int dot = s.lastIndexOf('.');
					if (dot >= 0) {
						s = s.substring(dot + 1);
					}
					int len = s.length();
					if (len >= 2 && s.charAt(0) == '"' && s.charAt(len - 1) == '"') {
						s = s.substring(1, len - 1);
					}
					ce.setLeftExpression(new StringValue(s));
					ce.getType().setDataType("regclass");
					replaced = true;
					return;
				}
				break;
			case "yes_or_no":
				CaseExpression ce2 = new CaseExpression();
				ce2.setSwitchExpression(left);
				replace(left, ce2::setSwitchExpression);
				WhenClause wc = new WhenClause();
				wc.setWhenExpression(new StringValue("YES"));
				wc.setThenExpression(new Column("TRUE"));
				ce2.setWhenClauses(Arrays.asList(wc));
				ce2.setElseExpression(FALSE);
				parentSet.accept(ce2);
				replaced = true;
				return;
			default:
			}
			if (left != oldLeft) {
				ce.setLeftExpression(left);
				replaced = true;
			}
			replace(left, ce::setLeftExpression);
			return;
		}
		if (exp instanceof ArrayExpression) {
			ArrayExpression ae = (ArrayExpression) exp;
			// indkey in PostgreSQL is int2vector (0-base), but here 1-base
			if (ae.getObjExpression().toString().equals("i.indkey")) {
				Expression ie = ae.getIndexExpression();
				if (ie.toString().equals("ia.attnum - 1")) {
					ae.setIndexExpression(new Column("ia.attnum"));
					replaced = true;
				} else if (ie instanceof LongValue) {
					// just set i.indkey[0] to -1, otherwise ODBC will use ctid
					// ae.setIndexExpression(new LongValue(((LongValue) ie).getValue() + 1));
					parentSet.accept(new LongValue(-1));
					replaced = true;
				}
			}
			replace(ae.getObjExpression(), ae::setObjExpression);
			replace(ae.getIndexExpression(), ae::setIndexExpression);
			return;
		}
		if (exp instanceof SubSelect) {
			SubSelect ss = (SubSelect) exp;
			replace(ss.getSelectBody(), ss::setSelectBody, null);
			return;
		}
		if (exp instanceof ExistsExpression) {
			ExistsExpression ee = ((ExistsExpression) exp);
			replace(ee.getRightExpression(), ee::setRightExpression);
			return;
		}
		if (exp instanceof InExpression) {
			InExpression ie = (InExpression) exp;
			replace(ie.getLeftExpression(), ie::setLeftExpression);
			ItemsList il = ie.getRightItemsList();
			if (il instanceof ExpressionList) {
				List<Expression> exps = ((ExpressionList) il).getExpressions();
				// attnum IN ('') -> FALSE
				// attnum IN ('{1,2,3}') -> attnum IN (1, 2, 3)
				if (exps.size() == 1 && exps.get(0) instanceof StringValue &&
						ie.getLeftExpression().toString().equals("attnum")) {
					String value = ((StringValue) exps.get(0)).getValue();
					if (value.isEmpty()) {
						parentSet.accept(FALSE);
					} else {
						ie.setRightItemsList(getExpressionList(value));
					}
					replaced = true;
					return;
				}
				for (int i = 0; i < exps.size(); i ++) {
					int i_ = i;
					replace(exps.get(i), e -> exps.set(i_, e));
				}
			} else if (il instanceof SubSelect) {
				replace((Expression) il, null);
			}
			return;
		}
		if (exp instanceof AnyComparisonExpression) {
			AnyComparisonExpression ace = (AnyComparisonExpression) exp;
			replace(ace.getSubSelect(), null);
			return;
		}
		if (!(exp instanceof BinaryExpression)) {
			return;
		}
		if (exp.toString().equals("p.prorettype::regtype != 'trigger'::regtype")) {
			// for TestPgClients.testPgCli(), test case #4
			parentSet.accept(new Column("TRUE"));
			replaced = true;
			return;
		}
		BinaryExpression be = (BinaryExpression) exp;
		Expression left = be.getLeftExpression();
		Expression right = be.getRightExpression();
		// array @> ARRAY[value] -> ARRAY_CONTAINS(array, value)
		if (be instanceof JsonOperator && right instanceof ArrayExpression &&
				((JsonOperator) be).getStringExpression().equals("@>")) {
			ArrayExpression array = (ArrayExpression) right;
			if (array.getObjExpression().toString().equals("ARRAY")) {
				parentSet.accept(arrayContains(left, array.getIndexExpression()));
				replaced = true;
				return;
			}
		}
		if (!(be instanceof EqualsTo)) {
			replace(left, be::setLeftExpression);
			replace(right, be::setRightExpression);
			return;
		}
		// attnum = ANY ((SELECT con.conkey ...)::oid[]) ->
		// ARRAY_CONTAINS(SELECT con.conkey ..., attnum)
		if (anyArray && right instanceof AnyComparisonExpression) {
			SubSelect ss = ((AnyComparisonExpression) right).getSubSelect();
			parentSet.accept(arrayContains(ss, left));
			replaced = true;
			return;
		}
		if (right instanceof Function &&
				((Function) right).getName().toUpperCase().equals("ANY")) {
			Function func = (Function) right;
			List<Expression> exps = func.getParameters().getExpressions();
			if (exps.size() == 1) {
				Expression exp0 = exps.get(0);
				String value = null;
				if (exp0 instanceof StringValue) {
					// value = ANY('{...}') -> value IN (...)
					value = ((StringValue) exp0).getValue();
				} else if (exp0 instanceof CastExpression) {
					// value = ANY('{...}'::char[]) -> value IN (...)
					CastExpression ce = (CastExpression) exp0;
					Expression ceLeft = ce.getLeftExpression();
					if (ce.getType().toString().equals("char[]") &&
							ceLeft instanceof StringValue) {
						value = ((StringValue) ceLeft).getValue();
					}
				}
				if (value == null) {
					// value = ANY(array) -> ARRAY_CONTAINS(array, value)
					// ANY(SubSelect) is parsed as AnyComparisonExpression
					parentSet.accept(arrayContains(exp0, left));
				} else {
					InExpression in = new InExpression(left, getExpressionList(value));
					replace(left, in::setLeftExpression);
					parentSet.accept(in);
				}
				replaced = true;
				return;
			}
		}
		if (left instanceof Column && right instanceof StringValue &&
				((StringValue) right).getValue().equals("t")) {
			switch (((Column) left).getColumnName()) {
			case "indisclustered":
			case "indisunique":
			case "indisprimary":
				parentSet.accept(left);
				replaced = true;
				return;
			default:
			}
		}
		replace(left, be::setLeftExpression);
		replace(right, be::setRightExpression);
	}

	private void replace(FromItem fi, Consumer<FromItem> parentSet, PlainSelect parent) {
		if (fi instanceof Table) {
			Table table = (Table) fi;
			String name = table.getFullyQualifiedName();
			SelectBody sb = tableMap.get(name);
			if (sb == null) {
				switch (name) {
				case "pg_shdescription":
				case "pg_catalog.pg_shdescription":
					table.setName("pg_description");
					replaced = true;
					break;
				default:
				}
				return;
			}
			SubSelect ss = new SubSelect();
			ss.setSelectBody(sb);
			Alias alias = fi.getAlias();
			if (alias == null) {
				alias = new Alias(table.getName());
			}
			ss.setAlias(alias);
			parentSet.accept(ss);
			replaced = true;
			return;
		}
		if (fi instanceof SubSelect) {
			SubSelect ss = (SubSelect) fi;
			replace(ss.getSelectBody(), ss::setSelectBody, null);
			return;
		}
		if (fi instanceof TableFunction) {
			TableFunction tf = (TableFunction) fi;
			Function func = tf.getFunction();
			if (func.getName().equals("pg_get_keywords")) {
				parentSet.accept(pgGetKeywords);
				replaced = true;
				return;
			}
			replace(func, exp -> {
				if (exp instanceof Function) {
					tf.setFunction((Function) exp);
				} else {
					PlainSelect ps1 = new PlainSelect();
					ps1.setSelectItems(Arrays.asList(new SelectExpressionItem(exp)));
					SubSelect ss = new SubSelect();
					ss.setSelectBody(ps1);
					parentSet.accept(ss);
				}
			});
			return;
		}
		if (!(fi instanceof SubJoin)) {
			return;
		}
		SubJoin sj = (SubJoin) fi;
		FromItem left = sj.getLeft();
		List<Join> joins = sj.getJoinList();
		if (joins != null) {
			// for TestPgClients.testTableau(), test case #4
			if (parent != null && joins.size() == 1 && joins.get(0).toString().
					equals("LEFT OUTER JOIN pg_catalog.pg_constraint cn " +
					"ON cn.conrelid = ref.confrelid AND cn.contype = 'p'")) {
				SubSelect ss = new SubSelect();
				ss.setSelectBody(select("SELECT *, NULL confupdtype, NULL confdeltype, " +
						"FALSE condeferrable, FALSE condeferred, 0 i FROM pg_constraint"));
				ss.setAlias(new Alias("ref"));
				// parentSet.accept(ss);
				parent.setFromItem(ss);
				joins = new ArrayList<>();
				joins.add(join("pg_namespace", "n1"));
				joins.add(join("pg_namespace", "n2"));
				joins.add(join("pg_class", "c1"));
				joins.add(join("pg_class", "c2"));
				joins.add(join("pg_attribute", "a1"));
				joins.add(join("pg_attribute", "a2"));
				joins.add(join("pg_constraint", "cn"));
				parent.setJoins(joins);
				parent.setWhere(new Column("FALSE"));
				replaced = true;
				return;
			}
			for (Join join : joins) {
				replace(join.getRightItem(), join::setRightItem, null);
				replace(join.getOnExpression(), join::setOnExpression);
			}
		}
		switch (left.toString()) {
		case "pg_inherits i":
			Alias alias = sj.getAlias();
			if (alias == null || !alias.getName().equals("i2")) {
				break;
			}
			// for TestPgClients.testNavicat(), test case #2
			PlainSelect ps = new PlainSelect();
			ps.setFromItem(left);
			replace(left, ps::setFromItem, null);
			ps.setSelectItems(Arrays.asList(
					new SelectExpressionItem(new Column("nspname")),
					new SelectExpressionItem(new Column("relname")),
					new SelectExpressionItem(new Column("inhrelid"))));
			ps.setJoins(joins);
			SubSelect ss = new SubSelect();
			ss.setAlias(alias);
			ss.setSelectBody(ps);
			parentSet.accept(ss);
			replaced = true;
			break;
		case "pg_depend":
			// JOIN (pg_depend JOIN pg_class cs ON ...) ->
			// JOIN (SELECT * FROM (SELECT ... WHERE FALSE) pg_depend JOIN ...) cs
			ps = new PlainSelect();
			ps.setFromItem(left);
			replace(left, ps::setFromItem, null);
			ps.setSelectItems(Arrays.asList(new AllColumns()));
			ps.setJoins(joins);
			ss = new SubSelect();
			ss.setAlias(new Alias("cs"));
			ss.setSelectBody(ps);
			parentSet.accept(ss);
			replaced = true;
			break;
		default:
			// H2 cannot use alias in sub-select in join, so keep sub-join and
			// avoid extracting table to sub-select in this case
			// replace(left, si::setLeft);
		}
	}

	private void replace(List<SelectItem> sis, Consumer<SelectBody> parentSet, PlainSelect ps) {
		for (SelectItem si : sis) {
			if (!(si instanceof SelectExpressionItem)) {
				continue;
			}
			SelectExpressionItem sei = (SelectExpressionItem) si;
			switch (sei.toString()) {
			case "information_schema._pg_expandarray(i.indkey) AS keys":
				// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.9
				parentSet.accept(select("SELECT ci.oid indexrelid, ct.oid indrelid, " +
						"(CASE index_type_name WHEN 'PRIMARY KEY' " +
						"THEN TRUE ELSE FALSE END) indisprimary, " +
						"c.ordinal_position keys_x, ic.ordinal_position keys_n " +
						"FROM information_schema.index_columns ic " +
						"JOIN information_schema.indexes i USING (index_schema, index_name) " +
						JOIN_INDEX +
						"JOIN information_schema.tables t USING (table_schema, table_name) " +
						JOIN_TABLE +
						"JOIN information_schema.columns c " +
						"USING (table_schema, table_name, column_name)"));
				replaced = true;
				return;
			case "information_schema._pg_expandarray(i.indkey) AS KEYS":
				// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.10
				if (ps == null) {
					break;
				}
				List<Join> joins = ps.getJoins();
				if (joins == null) {
					break;
				}
				for (Join join : ps.getJoins()) {
					if (!join.getRightItem().toString().equals("pg_catalog.pg_index i")) {
						continue;
					}
					sei.setExpression(new Column("i.keys_x"));
					sei.setAlias(new Alias("keys_x"));
					SubSelect ss = new SubSelect();
					ss.setAlias(join.getRightItem().getAlias());
					ss.setSelectBody(select("SELECT ci.oid indexrelid, ct.oid indrelid, " +
							"(CASE index_type_name WHEN 'PRIMARY KEY' " +
							"THEN TRUE ELSE FALSE END) indisprimary, " +
							"c.ordinal_position keys_x, ic.ordinal_position keys_n " +
							"FROM information_schema.index_columns ic " +
							"JOIN information_schema.indexes i USING (index_schema, index_name) " +
							JOIN_INDEX +
							"JOIN information_schema.tables t USING (table_schema, table_name) " +
							JOIN_TABLE +
							"JOIN information_schema.columns c " +
							"USING (table_schema, table_name, column_name)"));
					join.setRightItem(ss);
					replaced = true;
					return;
				}
				break;
			default:
			}
			Expression exp = sei.getExpression();
			if (exp instanceof Function && ps != null) {
				String name = ((Function) exp).getName();
				switch (name) {
				case "generate_series":
				case "unnest":
					switch (exp.toString()) {
					case "unnest(conkey)":
					case "unnest(confkey)":
						// for TestPgClients.testLibreOffice(), test case #1
						sei.setExpression(new NullValue());
						break;
					default:
						sei.setExpression(new Column(name.equals("unnest") ? "\"C1\"" : "\"X\""));
						TableFunction tf = new TableFunction();
						tf.setFunction((Function) exp);
						replace(exp, null);
						if (ps.getFromItem() == null) {
							ps.setFromItem(tf);
						} else {
							Join join = new Join();
							join.setRightItem(tf);
							join.setSimple(true);
							List<Join> joins = ps.getJoins();
							if (joins == null) {
								ps.setJoins(Arrays.asList(join));
							} else {
								joins.add(join);
							}
						}
					}
					replaced = true;
					break;
				case "pg_listening_channels":
					sei.setExpression(new NullValue());
					sei.setAlias(new Alias("pg_listening_channels"));
					ps.setWhere(FALSE);
					replaced = true;
					break;
				default:
				}
			}
			replace(exp, e -> {
				sei.setExpression(e);
				if (sei.getAlias() == null && exp instanceof Column) {
					sei.setAlias(new Alias(((Column) exp).getColumnName()));
				}
			});
			Alias alias = sei.getAlias();
			if (alias == null) {
				continue;
			}
			String name = alias.getName();
			switch (name) {
			case "default":
			case "table":
				alias.setName('"' + name + '"');
				replaced = true;
				break;
			default:
			}
		}
	}

	private void replace(SelectBody sb, Consumer<SelectBody> parentSet,
			Consumer<Statement> parentParentSet) {
		if (sb instanceof SetOperationList) {
			SetOperationList sol = (SetOperationList) sb;
			boolean countOnly = true;
			List<SelectBody> sbs = ((SetOperationList) sb).getSelects();
			for (int i = 0; i < sbs.size(); i ++) {
				SelectBody sbi = sbs.get(i);
				if (!sbi.toString().startsWith("SELECT COUNT(*) FROM ")) {
					countOnly = false;
				}
				int i_ = i;
				replace(sbi, e -> sbs.set(i_, e), null);
			}
			if (countOnly) {
				// SELECT COUNT(*) FROM ... UNION SELECT COUNT(*) FROM ... ->
				// SELECT COUNT(*) FROM ... UNION ALL SELECT COUNT(*) FROM ...
				for (SetOperation so : sol.getOperations()) {
					if (so instanceof UnionOp && !((UnionOp) so).isAll()) {
						((UnionOp) so).setAll(true);
						replaced = true;
					}
				}
			}
			return;
		}
		if (sb instanceof WithItem) {
			WithItem wi = (WithItem) sb;
			replace(wi.getSelectBody(), wi::setSelectBody, null);
			List<SelectItem> sis = wi.getWithItemList();
			if (sis != null) {
				replace(wi.getWithItemList(), parentSet, null);
			}
			return;
		}
		if (!(sb instanceof PlainSelect)) {
			return;
		}
		PlainSelect ps = (PlainSelect) sb;
		FromItem fi = ps.getFromItem();
		// for TestPgClients.testNavicat(), test case #3
		if (fi instanceof SubJoin && fi.toString().equals("((pg_rewrite r " +
					"JOIN pg_class c ON ((c.oid = r.ev_class))) " +
					"LEFT JOIN pg_namespace n ON ((n.oid = c.relnamespace)))")) {
			fi = table("pg_rewrite", "r");
			ps.setFromItem(fi);
			List<Join> joins = ps.getJoins();
			if (joins == null) {
				joins = new ArrayList<>();
				ps.setJoins(joins);
			}
			joins.add(join("pg_class", "c"));
			joins.add(join("pg_namespace", "n"));
			replaced = true;
		}
		replace(fi, ps::setFromItem, ps);
		boolean[] ret = {false};
		replace(ps.getSelectItems(), e -> {
			parentSet.accept(e);
			ret[0] = true;
		}, ps);
		if (ret[0]) {
			return;
		}
		List<Join> joins = ps.getJoins();
		if (joins != null) {
			for (Join join : ps.getJoins()) {
				if (join.toString().equals("JOIN pg_proc ON pg_proc.oid = a.typreceive")) {
					// for TestPgClients.testNpgsql(), test case #1
					join.setLeft(true);
					join.setOnExpression(FALSE);
					replaced = true;
				} else {
					replace(join.getRightItem(), join::setRightItem, null);
					replace(join.getOnExpression(), join::setOnExpression);
				}
			}
		}
		// GROUP BY <number> -> GROUP BY <select-items>
		GroupByElement gbe = ps.getGroupBy();
		if (gbe != null) {
			List<SelectItem> sis = ps.getSelectItems();
			List<Expression> gbes = gbe.getGroupByExpressions();
			for (int i = 0; i < gbes.size(); i ++) {
				Expression e = gbes.get(i);
				if (!(e instanceof LongValue)) {
					continue;
				}
				int j = (int) ((LongValue) e).getValue() - 1;
				if (j < 0 || j >= sis.size()) {
					continue;
				}
				SelectItem si = sis.get(j);
				if (!(si instanceof SelectExpressionItem)) {
					continue;
				}
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Expression gb = sei.getAlias() == null ? sei.getExpression() :
						new Column(sei.getAlias().getName());
				gbes.set(j, gb);
				replaced = true;
			}
		}
		// ORDER BY (SELECT ...) -> ORDER BY 1
		List<OrderByElement> obes = ps.getOrderByElements();
		if (obes != null && obes.size() == 1 &&
				obes.get(0).getExpression() instanceof SubSelect &&
				ps.getSelectItems().size() == 1) {
			obes.get(0).setExpression(new LongValue(1));
			replaced = true;
		}
		// OFFSET m LIMIT n -> LIMIT n OFFSET m
		if (ps.getOffset() != null && ps.getLimit() != null) {
			replaced = true;
		}
		replace(ps.getWhere(), ps::setWhere);
		// INTO
		List<Table> its = ps.getIntoTables();
		if (its != null && its.size() == 1) {
			ps.setIntoTables(null);
			Select select = new Select();
			select.setSelectBody(sb);
			Insert insert = new Insert();
			insert.setTable(its.get(0));
			insert.setUseValues(false);
			insert.setSelect(select);
			parentParentSet.accept(insert);
			replaced = true;
		}
	}

	private String getSQL(String s) {
		anyArray = false;
		String sqlInParentheses = null;
		String sql = s;
		if (sql.equals("SET TRANSACTION READ ONLY")) {
			replaced = true;
			return "SET AUTOCOMMIT FALSE";
		}
		if (sql.equals("RESET statement_timeout")) {
			replaced = true;
			return NOOP;
		}
		// for TestPgClients.testPostico(), test case #1
		if (sql.equals("SELECT oid, nspname, nspname = ANY (current_schemas(true)) " +
				"AS is_on_search_path, oid = pg_my_temp_schema() AS is_my_temp_schema, " +
				"pg_is_other_temp_schema(oid) AS is_other_temp_schema FROM pg_namespace")) {
			replaced = true;
			return "SELECT oid, nspname, nspname = 'public' is_on_search_path, " +
					"FALSE is_my_temp_schema, FALSE is_other_temp_schema FROM pg_namespace";
		}
		if (sql.startsWith("EXPLAIN VERBOSE ")) {
			replaced = true;
			return "EXPLAIN " + sql.substring(16);
		}
		if (sql.startsWith("SET LOCAL join_collapse_limit=")) {
			replaced = true;
			return "SET join_collapse_limit=" + sql.substring(30);
		}
		if (sql.startsWith("SET TIMEZONE TO ")) {
			replaced = true;
			return NOOP;
		}
		if (sql.startsWith("SELECT n.nspname = ANY(current_schemas(true)), ")) {
			// JSqlParser cannot parse SELECT a = b, ... Just replace:
			// SELECT n.nspname = ANY(current_schemas(true)) -> SELECT n.nspname = 'public'
			// and ignore JSqlParser's failure
			sql = "SELECT n.nspname = 'public', " + sql.substring(47);
			replaced = true;
		} else if (sql.startsWith("SELECT (nc.nspname)::information_schema.sql_identifier")) {
			// for TestPgClients.testToadEdge(), test case #3
			sql = sql.replace("'::\"char\"", "'").replaceAll("\\s+", " ").
					replace(", a.attfdwoptions AS options ", ", NULL AS options ").
					replace("(c.relkind = ANY (ARRAY ['r', 'f', 'v', 'm']))",
					"c.relkind IN ('r', 'f', 'v', 'm')");
			int leftParentheses = sql.indexOf(" ( ( ( ( ( ( ");
			int rightParentheses = sql.lastIndexOf(")))))");
			if (leftParentheses >= 0 && rightParentheses >= 0) {
				sqlInParentheses = sql.substring(leftParentheses + 7, rightParentheses + 3);
				sql = sql.substring(0, leftParentheses + 7) +
						IN_PARENTHESES_PLACEHOLDER + sql.substring(rightParentheses + 3);
			}
			replaced = true;
		} else if (sql.startsWith("SELECT DISTINCT")) {
			// for TestPgClients.testToadEdge(), test case #5
			if (sql.replaceAll("\\s+", " ").startsWith("SELECT DISTINCT trg.oid, " +
					"trg.tgname AS trigger_name, tbl.relname AS parent_name, " +
					"p.proname AS function_name, ")) {
				replaced = true;
				return "SELECT 0 oid, '' trigger_name, '' parent_name, '' function_name, " +
						"'' trigger_type, '' trigger_event, '' action_orientation, " +
						"'' enabled, '' action_condition, '' description, '' action_statement, " +
						"FALSE deferrable, FALSE deferred, 0 tgconstraint WHERE FALSE";
			}
		} else if (sql.startsWith("select quote_ident(c.relname) as table_name,")) {
			// for TestPgClients.testOmniDB(), test case #1
			int dataLengthFrom = sql.indexOf("(select case when x.truetypmod = -1");
			int dataLengthTo = sql.lastIndexOf(") as data_length,");
			if (dataLengthFrom >= 0 && dataLengthTo >= 0) {
				sql = sql.substring(0, dataLengthFrom) + " NULL" + sql.substring(dataLengthTo + 4);
				replaced = true;
			}
		} else if (sql.endsWith("::oid)::oid[])")) {
			// attnum = ANY ((SELECT con.conkey ...)::oid[]) ->
			// ARRAY_CONTAINS(SELECT con.conkey ..., attnum)
			sql = sql.substring(0, sql.length() - 14) + "))";
			replaced = true;
			anyArray = true;
		}
		for (int i = 0; i < REPLACE_FROM.length; i ++) {
			int index;
			while ((index = sql.indexOf(REPLACE_FROM[i])) >= 0) {
				sql = sql.substring(0, index) + REPLACE_TO[i] +
						sql.substring(index + REPLACE_FROM[i].length());
				replaced = true;
			}
		}

		try {
			CCJSqlParser parser = new CCJSqlParser(new StringProvider(sql));
			Statement st = parser.Statement();
			if (st instanceof Select) {
				Select select = (Select) st;
				Statement[] retSt = {null};
				replace(select.getSelectBody(),
						select::setSelectBody, ret -> retSt[0] = ret);
				if (retSt[0] != null) {
					replaced = true;
					return retSt[0].toString();
				}
				List<WithItem> wis = select.getWithItemsList();
				if (wis != null) {
					for (int i = 0; i < wis.size(); i ++) {
						int i_ = i;
						replace(wis.get(i), e -> wis.set(i_, (WithItem) e), null);
					}
				}
				if (replaced) {
					sql = st.toString();
					if (sqlInParentheses != null) {
						sql = sql.replace(IN_PARENTHESES_PLACEHOLDER, sqlInParentheses);
					}
				}
			} else if (st instanceof Insert) {
				Insert insert = ((Insert) st);
				if (insert.isReturningAllColumns() ||
						insert.getReturningExpressionList() != null) {
					insert.setReturningAllColumns(false);
					insert.setReturningExpressionList(null);
					replaced = true;
					return insert.toString();
				}
			} else if (st instanceof Update) {
				Update update = ((Update) st);
				if (update.isReturningAllColumns() ||
						update.getReturningExpressionList() != null) {
					update.setReturningAllColumns(false);
					update.setReturningExpressionList(null);
					replaced = true;
					return update.toString();
				}
			} else if (st instanceof ShowStatement) {
				switch (((ShowStatement) st).getName()) {
				case "LC_COLLATE":
					replaced = true;
					return "SELECT 'C' lc_collate";
				case "server_version_num":
					replaced = true;
					return "SELECT '80223' server_version_num";
				case "integer_datetimes":
					replaced = true;
					return "SELECT TRUE integer_datetimes";
				case "transaction_isolation":
					replaced = true;
					return "SHOW TRANSACTION ISOLATION LEVEL";
				default:
				}
			} else if (st instanceof SetStatement) {
				switch (((SetStatement) st).getName()) {
				case "extra_float_digits":
					replaced = true;
					return NOOP;
				default:
				}
			} else if (st instanceof CreateTable) {
				CreateTable ct = (CreateTable) st;
				List<?> tableOptions = ct.getTableOptionsStrings();
				if (tableOptions != null && (tableOptions.remove("PRESERVE") |
						tableOptions.remove("ROWS"))) {
					replaced = true;
					return ct.toString();
				}
			}
		} catch (ParseException | TokenMgrException e) {
			// Ignored
			// System.err.println(sql + ": " + e);
		}
		return sql;
	}

	private static int findZero(byte[] b, int left, int right) throws EOFException {
		for (int i = left; i < right; i ++) {
			if (b[i] == 0) {
				return i;
			}
		}
		throw new EOFException();
	}

	PgServerThread thread;

	private Socket socket;
	private PgServerCompat server;
	private InputStream ins;
	private OutputStream outs;
	private Map<?, ?> portals;
	private SessionLocal session = null;

	public PgServerThreadCompat(Socket socket, PgServerCompat server) {
		try {
			this.thread = newThread.newInstance(socket, server.server);
			this.socket = socket;
			this.server = server;
			portals = (Map<?, ?>) portalsField.get(thread);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	private void read(byte[] b, int off, int len) throws IOException {
		int off_ = off;
		int len_ = len;
		while (len_ > 0) {
			int l = ins.read(b, off_, len_);
			if (l < 0) {
				throw new EOFException();
			}
			off_ += l;
			len_ -= l;
		}
	}

	// See PgServerThread.writeDataColumn(), binary part
	private static final Set<Integer> BINARY_TYPES = IntStream.of(new int[] {
		PgServer.PG_TYPE_INT2, PgServer.PG_TYPE_INT4, PgServer.PG_TYPE_INT8,
		PgServer.PG_TYPE_FLOAT4, PgServer.PG_TYPE_FLOAT8, PgServer.PG_TYPE_BYTEA,
		PgServer.PG_TYPE_DATE, PgServer.PG_TYPE_TIME, PgServer.PG_TYPE_TIMETZ,
		PgServer.PG_TYPE_TIMESTAMP, PgServer.PG_TYPE_TIMESTAMPTZ,
	}).boxed().collect(Collectors.toSet());

	private void read() throws IOException, ReflectiveOperationException {
		int x = 0;
		int initLen = 0;
		if (initDone.getBoolean(thread)) {
			x = ins.read();
			if (x < 0) {
				throw new EOFException();
			}
			initLen = 1;
		}
		byte[] head = new byte[4];
		read(head, 0, 4);
		int dataLen = Bits.readInt(head, 0);
		if (dataLen < 4) {
			throw new EOFException();
		}
		byte[] data = Utils.newBytes(initLen + dataLen);
		if (initLen == 1) {
			data[0] = (byte) x;
		}
		System.arraycopy(head, 0, data, initLen, 4);
		read(data, initLen + 4, dataLen - 4);
		Charset charset = (Charset) getEncoding.invoke(thread);
		switch (x) {
		case 'E':
			int z1 = findZero(data, 5, data.length);
			if (Bits.readInt(data, z1 + 1) > 0) {
				session = (SessionLocal) sessionField.get(thread);
				if (session.isLazyQueryExecution()) {
					session = null;
				} else {
					session.setLazyQueryExecution(true);
				}
			}
			String name = new String(data, 5, z1 - 5, charset);
			Object portal = portals.get(name);
			if (portal == null) {
				break;
			}
			int[] resultColumnFormat = (int[]) formatField.get(portal);
			if (resultColumnFormat == null) {
				break;
			}
			boolean binary = false;
			for (int i = 0; i < resultColumnFormat.length; i ++) {
				if (resultColumnFormat[i] > 0) {
					binary = true;
					break;
				}
			}
			if (!binary) {
				break;
			}
			ResultInterface result = ((CommandInterface) preparedPrep.
					get(portalPrep.get(portal))).getMetaData();
			if (result == null) {
				break;
			}
			int columnCount = result.getVisibleColumnCount();
			if (resultColumnFormat.length != columnCount) {
				resultColumnFormat = new int[columnCount];
				Arrays.fill(resultColumnFormat, 1);
				formatField.set(portal, resultColumnFormat);
			}
			for (int i = 0; i < columnCount; i ++) {
				if (resultColumnFormat[i] != 0 &&
						!BINARY_TYPES.contains(Integer.valueOf(PgServer.
						convertType(result.getColumnType(i))))) {
					resultColumnFormat[i] = 0;
				}
			}
			replaced = true;
			break;
		case 'P':
			z1 = findZero(data, 5, data.length) + 1;
			int z2 = findZero(data, z1, data.length);
			if (z1 == z2) {
				break;
			}
			replaced = false;
			String sql = getSQL(new String(data, z1, z2 - z1, charset).trim());
			if (!replaced) {
				break;
			}
			byte[] sqlb = sql.getBytes(charset);
			byte[] data_ = new byte[data.length - z2 + z1 + sqlb.length];
			data_[0] = 'P';
			Bits.writeInt(data_, 1, data_.length - 1);
			System.arraycopy(data, 5, data_, 5, z1 - 5);
			System.arraycopy(sqlb, 0, data_, z1, sqlb.length);
			System.arraycopy(data, z2, data_, z1 + sqlb.length, data.length - z2);
			data = data_;
			break;
		case 'Q':
			z1 = findZero(data, 5, data.length);
			replaced = false;
			StringBuilder sb = new StringBuilder();
			try (ScriptReader reader = new ScriptReader(new
					InputStreamReader(new ByteArrayInputStream(data, 5, z1 - 5), charset))) {
				String line;
				while ((line = reader.readStatement()) != null) {
					line = line.trim();
					if (line.isEmpty()) {
						replaced = true;
					} else {
						sb.append(getSQL(line)).append(';');
					}
				}
			}
			if (!replaced || sb.length() == 0) {
				break;
			}
			sqlb = sb.substring(0, sb.length() - 1).getBytes(charset);
			data_ = new byte[data.length - z1 + 5 + sqlb.length];
			data_[0] = 'Q';
			Bits.writeInt(data_, 1, data_.length - 1);
			System.arraycopy(sqlb, 0, data_, 5, sqlb.length);
			System.arraycopy(data, z1, data_, 5 + sqlb.length, data.length - z1);
			data = data_;
			break;
		default:
		}
		dataInRaw.set(thread, new DataInputStream(new ByteArrayInputStream(data)));
	}

	@Override
	public void run() {
		try {
			server.trace("Connect");
			ins = socket.getInputStream();
			outs = socket.getOutputStream();
			out.set(thread, outs);
			// dataInRaw.set(thread, new DataInputStream(ins));
			while (!stop.getBoolean(thread)) {
				read();
				process.invoke(thread);
				if (session != null) {
					session.setLazyQueryExecution(false);
					session = null;
				}
				// not necessary to flush SocketOutputStream
				// outs.flush();
			}
		} catch (EOFException e) {
			// more or less normal disconnect
		} catch (IOException | ReflectiveOperationException e) {
			server.traceError(e);
		} finally {
			server.trace("Disconnect");
			try {
				close.invoke(thread);
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException(e);
			}
		}
	}

	void setProcessId(int id) {
		try {
			setProcessId.invoke(thread, Integer.valueOf(id));
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	void setThread(Thread thread) {
		try {
			setThread.invoke(this.thread, thread);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}
}