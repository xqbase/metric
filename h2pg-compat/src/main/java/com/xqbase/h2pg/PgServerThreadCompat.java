package com.xqbase.h2pg;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.h2.command.Parser;
import org.h2.server.pg.PgServerThread;
import org.h2.server.pg.PgServerThreadEx;
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
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.parser.TokenMgrException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;

public class PgServerThreadCompat extends PgServerThreadEx {
	private static Field initDone, out, dataInRaw, stop;
	private static Method process, getEncoding;

	private static Field getField(String name) throws ReflectiveOperationException {
		Field field = PgServerThread.class.getDeclaredField(name);
		field.setAccessible(true);
		return field;
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

	private static final LongValue ZERO = new LongValue(0);
	private static final LongValue ONE = new LongValue(1);
	private static final LongValue MINUS_ONE = new LongValue(-1);
	private static final NullValue NULL = new NullValue();
	private static final Column TRUE = new Column("TRUE");
	private static final Column FALSE = new Column("FALSE");
	private static final Column CURRENT_SCHEMAS = new Column("ARRAY['public']");
	private static final StringValue ROW_TO_JSON = new StringValue("{}");
	private static final ValuesList PG_GET_KEYWORDS = new ValuesList();
	private static final NullValue PG_LISTENING_CHANNELS = new NullValue();
	private static final Alias PG_LISTENING_CHANNELS_ALIAS =
			new Alias("pg_listening_channels");
	private static final Alias PGJDBC_KEYS_X_ALIAS = new Alias("keys_x");
	private static final Column GENERATE_SERIES_COLUMN = new Column("\"X\"");
	private static final Column PGJDBC_KEYS_X_COLUMN = new Column("i.keys_x");
	private static final List<SelectItem> ALL_COLUMNS = Arrays.asList(new AllColumns());
	private static final SelectExpressionItem RELNAME_COLUMN =
			new SelectExpressionItem();
	private static final ColDataType TEXT_TYPE = new ColDataType();
	private static final SelectBody PGJDBC_KEYS =
			select("SELECT i.id indexrelid, t.id indrelid, " +
			"(CASE index_type_name WHEN 'PRIMARY KEY' THEN TRUE ELSE FALSE END) indisprimary, " +
			"c.ordinal_position keys_x, i.ordinal_position keys_n " +
			"FROM information_schema.indexes i " +
			"JOIN information_schema.tables t USING (table_schema, table_name) " +
			"JOIN information_schema.columns c USING (table_schema, table_name, column_name)");
	private static final Map<String, SelectBody> TABLE_MAP = new HashMap<>();
	private static final Map<String, Expression> FUNCTION_MAP = new HashMap<>();

	private static final String[] REPLACE_FROM = {
		"(indpred IS NOT NULL)",
		// "deferrable" and "tablespace" are keywords in JSqlParser
		", condeferrable::int AS deferrable, ",
		"tablespace) AS tablespace",
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
		"(c.relkind = 'v'::\"char\")",
		// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.9
		" (i.keys).n ",
		" (i.keys).x ",
		// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.10
		" (information_schema._pg_expandarray(i.indkey)).n ",
		" result.A_ATTNUM = (result.KEYS).x ",
	};
	private static final String[] REPLACE_TO = {
		"(NVL2(indpred, TRUE, FALSE))",
		", 0 \"deferrable\", ",
		"tablespace) \"tablespace\"",
		" '*' ",
		" WHEN nsp.nspname = 'information_schema'",
		")::CAST_TO_FALSE attisserial,",
		"MAX(array_length(c.conkey)) nb",
		"MAX(array_length(c.conkey)) nb",
		"(c.relkind = 'v')",
		" i.keys_n ",
		" i.keys_x ",
		" i.keys_n ",
		" result.A_ATTNUM = result.keys_x ",
	};
	private static final int[] REPLACE_FROM_LEN = new int[REPLACE_FROM.length];

	private static void addTable(String name, String sql) {
		SelectBody sb = select(sql);
		TABLE_MAP.put(name, sb);
		if (!name.startsWith("information_schema.")) {
			TABLE_MAP.put("pg_catalog." + name, sb);
		}
	}

	private static void addColumns(String name, String columns) {
		addTable(name, "SELECT *, " + columns.replace("${owner}",
				"(SELECT oid FROM pg_user WHERE usename = current_user())") +
				" FROM " + name);
	}

	private static void addEmptyTable(String name, String columns) {
		addTable(name, "SELECT " + columns + " WHERE FALSE");
	}

	private static void addFunction(String name, Expression exp) {
		FUNCTION_MAP.put(name, exp);
		if (!name.startsWith("information_schema.")) {
			FUNCTION_MAP.put("pg_catalog." + name, exp);
		}
	}

	static {
		String[] tokens;
		try {
			initDone = getField("initDone");
			out = getField("out");
			dataInRaw = getField("dataInRaw");
			stop = getField("stop");
			process = getMethod("process");
			getEncoding = getMethod("getEncoding");
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
		PG_GET_KEYWORDS.setMultiExpressionList(words);
		PG_GET_KEYWORDS.setColumnNames(Arrays.asList("word"));
		PG_GET_KEYWORDS.setAlias(new Alias("pg_get_keywords"));
		TEXT_TYPE.setDataType("text");
		RELNAME_COLUMN.setExpression(new Column("relname"));

		addColumns("pg_attribute", "0 attndims, 0 attstattarget, NULL attstorage");
		addColumns("pg_class", "NULL relacl, ${owner} relowner, NULL tableoid");
		addColumns("pg_constraint", "NULL confkey, NULL confdeltype, NULL confmatchtype, " +
				"NULL confupdtype, NULL connamespace, NULL tableoid");
		addColumns("pg_database", "-1 datconnlimit, FALSE datistemplate");
		addColumns("pg_namespace", "id oid, ${owner} nspowner");
		addColumns("pg_proc", "NULL proallargtypes, NULL proargmodes, " +
				"NULL prolang, 0 pronargs, FALSE proisagg");
		addColumns("pg_type", "NULL typcategory, NULL typcollation, " +
				"NULL typdefault, 0 typndims, ${owner} typowner, NULL typstorage");
		addColumns("pg_user", "oid usesysid");
		addColumns("information_schema.routines", "NULL type_udt_name");
		addColumns("information_schema.triggers",
				"NULL event_object_table, NULL event_object_schema");
		addEmptyTable("pg_collation", "0 oid, '' collnamespace, '' collname");
		addEmptyTable("pg_depend", "'' deptype, 0 classid, 0 refclassid, " +
				"0 objid, 0 objsubid, 0 refobjid, 0 refobjsubid");
		addEmptyTable("pg_event_trigger", "0");
		addEmptyTable("pg_language", "0 oid, '' lanname");
		addEmptyTable("pg_rewrite", "0 oid, '' rulename, 0 ev_class");
		addEmptyTable("pg_shdepend", "'' deptype, 0 classid, 0 refclassid, " +
				"0 objid, 0 objsubid, 0 refobjid, 0 refobjsubid");
		addEmptyTable("pg_stat_activity", "'' state, '' datname");
		addEmptyTable("pg_stat_database", "0 xact_commit, 0 xact_rollback, " +
				"0 tup_inserted, 0 tup_updated, 0 tup_deleted, 0 tup_fetched, 0 tup_returned, " +
				"0 blks_read, 0 blks_hit, '' datname");
		addEmptyTable("pg_trigger", "0 oid, '' tgname, 0 tableoid, " +
				"0 tgfoid, 0 tgrelid, '' tgenabled, FALSE tgisconstraint");
		addTable("pg_index", "SELECT i.id indexrelid, t.id indrelid, " +
				"(CASE index_type_name WHEN 'PRIMARY KEY' THEN TRUE ELSE FALSE END) indisclustered, " +
				"(CASE index_type_name WHEN 'PRIMARY KEY' THEN TRUE " +
				"WHEN 'UNIQUE INDEX' THEN TRUE ELSE FALSE END) indisunique, " +
				"(CASE index_type_name WHEN 'PRIMARY KEY' THEN TRUE ELSE FALSE END) indisprimary, " +
				"NULL indexprs, ARRAY_AGG(c.ordinal_position" /* ORDER BY i.ordinal_position */ + ") indkey, " +
				"NULL indpred, NULL indoption FROM information_schema.indexes i " +
				"JOIN information_schema.tables t USING (table_schema, table_name) " +
				"JOIN information_schema.columns c USING (table_schema, table_name, column_name) " +
				"GROUP BY i.id");
		addTable("pg_indexes", "SELECT DISTINCT n.nspname AS schemaname, c.relname AS tablename, " +
				"i.relname AS indexname, t.spcname AS \"tablespace\", " +
				"pg_get_indexdef(i.oid) AS indexdef FROM information_schema.indexes x " +
				"JOIN information_schema.tables it USING (table_schema, table_name) " +
				"JOIN pg_class c ON c.oid = it.id " +
				"JOIN pg_class i ON i.oid = x.id " +
				"LEFT JOIN pg_namespace n ON n.oid = c.relnamespace " +
				"LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace " +
				"WHERE c.relkind IN ('r', 'm', 'p') AND i.relkind IN ('i', 'I')");
		addTable("pg_tables", "SELECT n.nspname schemaname, c.relname tablename FROM pg_class c " +
				"LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p')");
		addTable("pg_views", "SELECT n.nspname schemaname, c.relname viewname FROM pg_class c " +
				"LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'v'");

		addFunction("information_schema._pg_char_max_length", NULL);
		addFunction("information_schema._pg_numeric_precision", NULL);
		addFunction("information_schema._pg_numeric_scale", NULL);
		addFunction("information_schema._pg_datetime_precision", NULL);
		addFunction("col_description", NULL);
		addFunction("oidvectortypes", NULL);
		addFunction("pg_get_constraintdef", NULL);
		addFunction("pg_get_triggerdef", NULL);
		addFunction("pg_get_functiondef", NULL);
		addFunction("pg_get_viewdef", NULL);
		addFunction("shobj_description", NULL);
		addFunction("pg_backend_pid", ZERO);
		addFunction("pg_cancel_backend", TRUE);
		addFunction("pg_function_is_visible", TRUE);
		addFunction("pg_database_size", ZERO);
		addFunction("pg_total_relation_size", ZERO);
		addFunction("array_lower", ONE);
		addFunction("current_schemas", CURRENT_SCHEMAS);
		addFunction("pg_listening_channels", PG_LISTENING_CHANNELS);
		addFunction("row_to_json", ROW_TO_JSON);

		for (int i = 0; i < REPLACE_FROM.length; i ++) {
			REPLACE_FROM_LEN[i] = REPLACE_FROM[i].length();
		}
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

	private void replace(Expression exp, Consumer<Expression> parentSet) {
		if (exp instanceof Column) {
			return;
		}
		if (exp instanceof Function) {
			Function func = (Function) exp;
			ExpressionList el = func.getParameters();
			Expression result = FUNCTION_MAP.get(func.getName());
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
						parentSet.accept(MINUS_ONE);
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
			if (type.equals("regclass")) {
				if (left instanceof LongValue) {
					// number::regclass -> IFNULL(SELECT relname FROM pg_class WHERE oid = number, number::text)
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
					replaced = true;
					return;
				}
			}
			ce.setLeftExpression(left);
			replaced |= left != oldLeft;
			replace(left, ce::setLeftExpression);
			return;
		}
		if (exp instanceof ArrayExpression) {
			ArrayExpression ae = (ArrayExpression) exp;
			replace(ae.getObjExpression(), ae::setObjExpression);
			replace(ae.getIndexExpression(), ae::setIndexExpression);
			return;
		}
		if (exp instanceof SubSelect) {
			SubSelect ss = (SubSelect) exp;
			replace(ss.getSelectBody(), ss::setSelectBody);
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
			replace((Expression) ace.getSubSelect(), null);
			return;
		}
		if (!(exp instanceof BinaryExpression)) {
			return;
		}
		BinaryExpression be = (BinaryExpression) exp;
		Expression left = be.getLeftExpression();
		Expression right = be.getRightExpression();
		// attnum = ANY ((SELECT con.conkey ...)::oid[]) ->
		// ARRAY_CONTAINS(SELECT con.conkey ..., attnum)
		if (anyArray && be instanceof EqualsTo &&
				right instanceof AnyComparisonExpression) {
			SubSelect ss = ((AnyComparisonExpression) right).getSubSelect();
			Function func = new Function();
			ExpressionList el = new ExpressionList(ss, left);
			replace(ss, (Consumer<Expression>) e -> el.getExpressions().set(0, e));
			replace(left, e -> el.getExpressions().set(1, e));
			func.setName("ARRAY_CONTAINS");
			func.setParameters(el);
			parentSet.accept(func);
			replaced = true;
			return;
		}
		if (be instanceof EqualsTo && right instanceof Function &&
				((Function) right).getName().toUpperCase().equals("ANY")) {
			Function func = (Function) right;
			List<Expression> exps = func.getParameters().getExpressions();
			if (exps.size() == 1) {
				Expression exp0 = exps.get(0);
				if (exp0 instanceof StringValue) {
					// value = ANY('{...}') -> value IN (...)
					InExpression in = new InExpression(left,
							getExpressionList(((StringValue) exp0).getValue()));
					replace(left, in::setLeftExpression);
					parentSet.accept(in);
				} else {
					// value = ANY(array) -> ARRAY_CONTAINS(array, value)
					// ANY(SubSelect) is parsed as AnyComparisonExpression
					// if (!(exp0 instanceof SubSelect)) {
					ExpressionList el = new ExpressionList(exp0, left);
					replace(exp0, e -> el.getExpressions().set(0, e));
					replace(left, e -> el.getExpressions().set(1, e));
					func.setName("ARRAY_CONTAINS");
					func.setParameters(el);
					parentSet.accept(func);
				}
				replaced = true;
				return;
			}
		}
		replace(left, be::setLeftExpression);
		replace(right, be::setRightExpression);
	}

	private void replace(FromItem fi, Consumer<FromItem> parentSet) {
		if (fi instanceof Table) {
			Table table = (Table) fi;
			String name = table.getFullyQualifiedName();
			SelectBody sb = TABLE_MAP.get(name);
			if (sb == null) {
				if (name.equals("pg_catalog.pg_shdescription")) {
					table.setName("pg_description");
					replaced = true;
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
			replace(ss.getSelectBody(), ss::setSelectBody);
			return;
		}
		if (fi instanceof SubJoin) {
			SubJoin si = (SubJoin) fi;
			FromItem left = si.getLeft();
			// H2 cannot use alias in sub-query in join, so keep sub-join and
			// avoid extracting table to sub-query in this case
			if (left.toString().equals("pg_catalog.pg_class AS r2")) {
				List<Join> joins = si.getJoinList();
				if (joins != null) {
					for (Join join : joins) {
						replace(join.getRightItem(), join::setRightItem);
						replace(join.getOnExpression(), join::setOnExpression);
					}
				}
				return;
			}
			PlainSelect ps = new PlainSelect();
			ps.setFromItem(left);
			ps.setSelectItems(ALL_COLUMNS);
			replace(left, ps::setFromItem);
			Alias alias = si.getAlias();
			List<Join> joins = si.getJoinList();
			if (joins != null) {
				for (Join join : joins) {
					replace(join.getRightItem(), join::setRightItem);
					replace(join.getOnExpression(), join::setOnExpression);
					if (alias == null) {
						alias = join.getRightItem().getAlias();
					}
				}
				ps.setJoins(joins);
			}
			SubSelect ss = new SubSelect();
			ss.setAlias(alias);
			ss.setSelectBody(ps);
			parentSet.accept(ss);
			replaced = true;
			return;
		}
		if (!(fi instanceof TableFunction)) {
			return;
		}
		TableFunction ti = (TableFunction) fi;
		Function func = ti.getFunction();
		if (func.getName().equals("pg_get_keywords")) {
			parentSet.accept(PG_GET_KEYWORDS);
			replaced = true;
			return;
		}
		replace(func, exp -> {
			if (exp instanceof Function) {
				ti.setFunction((Function) exp);
			} else {
				PlainSelect ps1 = new PlainSelect();
				ps1.setSelectItems(Arrays.asList(new SelectExpressionItem(exp)));
				SubSelect ss = new SubSelect();
				ss.setSelectBody(ps1);
				parentSet.accept(ss);
			}
		});
	}

	private void replace(SelectBody sb, Consumer<SelectBody> parentSet) {
		if (sb instanceof SetOperationList) {
			List<SelectBody> sbs = ((SetOperationList) sb).getSelects();
			for (int i = 0; i < sbs.size(); i ++) {
				int i_ = i;
				replace(sbs.get(i), e -> sbs.set(i_, e));
			}
			return;
		}
		if (!(sb instanceof PlainSelect)) {
			return;
		}
		PlainSelect ps = (PlainSelect) sb;
		replace(ps.getFromItem(), ps::setFromItem);
		for (SelectItem si : ps.getSelectItems()) {
			if (!(si instanceof SelectExpressionItem)) {
				continue;
			}
			SelectExpressionItem sei = (SelectExpressionItem) si;
			switch (sei.toString()) {
			case "information_schema._pg_expandarray(i.indkey) AS keys":
				// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.9
				parentSet.accept(PGJDBC_KEYS);
				replaced = true;
				return;
			case "information_schema._pg_expandarray(i.indkey) AS KEYS":
				// for DatabaseMetaData.getPrimaryKeys() in PgJDBC 42.2.10
				List<Join> joins = ps.getJoins();
				if (joins == null) {
					break;
				}
				for (Join join : ps.getJoins()) {
					if (!join.getRightItem().toString().equals("pg_catalog.pg_index i")) {
						continue;
					}
					sei.setExpression(PGJDBC_KEYS_X_COLUMN);
					sei.setAlias(PGJDBC_KEYS_X_ALIAS);
					SubSelect ss = new SubSelect();
					ss.setAlias(join.getRightItem().getAlias());
					ss.setSelectBody(PGJDBC_KEYS);
					join.setRightItem(ss);
					replaced = true;
					return;
				}
				break;
			default:
			}
			Expression exp = sei.getExpression();
			if (exp instanceof Function &&
					((Function) exp).getName().equals("generate_series") &&
					ps.getSelectItems().size() == 1 && ps.getFromItem() == null) {
				sei.setExpression(GENERATE_SERIES_COLUMN);
				TableFunction tf = new TableFunction();
				tf.setFunction((Function) exp);
				replace(exp, null);
				ps.setFromItem(tf);
				replaced = true;
				break;
			}
			replace(exp, e -> {
				sei.setExpression(e);
				if (e == PG_LISTENING_CHANNELS) {
					sei.setAlias(PG_LISTENING_CHANNELS_ALIAS);
					ps.setWhere(FALSE);
				} else if (sei.getAlias() == null && exp instanceof Column) {
					sei.setAlias(new Alias(((Column) exp).getColumnName()));
				}
			});
			Alias alias = sei.getAlias();
			if (alias != null && alias.getName().equals("table")) {
				alias.setName("\"table\"");
			}
		}
		List<Join> joins = ps.getJoins();
		if (joins != null) {
			for (Join join : ps.getJoins()) {
				replace(join.getRightItem(), join::setRightItem);
				replace(join.getOnExpression(), join::setOnExpression);
			}
		}
		// ORDER BY (SELECT ...) -> ORDER BY 1
		List<OrderByElement> obes = ps.getOrderByElements();
		if (obes != null && obes.size() == 1 &&
				obes.get(0).getExpression() instanceof SubSelect &&
				ps.getSelectItems().size() == 1) {
			obes.get(0).setExpression(ONE);
		}
		// OFFSET m LIMIT n -> LIMIT n OFFSET m
		if (ps.getOffset() != null && ps.getLimit() != null) {
			replaced = true;
		}
		replace(ps.getWhere(), ps::setWhere);
	}

	private String getSQL(String s) {
		anyArray = false;
		String sql = s;
		if (sql.equals("SET TRANSACTION READ ONLY")) {
			replaced = true;
			return "SET AUTOCOMMIT FALSE";
		}
		if (sql.equals("RESET statement_timeout")) {
			replaced = true;
			return "SET NETWORK_TIMEOUT 0";
		}
		// The result for `select oid,typname from pg_type` is different from PostgreSQL,
		// which may lead `pg_fieldtype` crash. Just make it fail.
		if (sql.equals("select oid,typname from pg_type")) {
			replaced = true;
			return "SET NETWORK_TIMEOUT 0";
		}
		if (sql.startsWith("EXPLAIN VERBOSE ")) {
			sql = "EXPLAIN " + sql.substring(16);
			replaced = true;
		} else if (sql.startsWith("SET LOCAL join_collapse_limit=")) {
			sql = "SET join_collapse_limit=" + sql.substring(30);
			replaced = true;
		} else if (sql.startsWith("SELECT n.nspname = ANY(current_schemas(true)), ")) {
			// JSqlParser cannot parse SELECT a = b, ... Just replace:
			// SELECT n.nspname = ANY(current_schemas(true)) -> SELECT n.nspname = 'public'
			// and ignore JSqlParser's failure
			sql = "SELECT n.nspname = 'public', " + sql.substring(47);
			replaced = true;
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
						sql.substring(index + REPLACE_FROM_LEN[i]);
				replaced = true;
			}
		}

		try {
			CCJSqlParser parser = new CCJSqlParser(new StringProvider(sql));
			Statement st = parser.Statement();
			if (st instanceof Select) {
				replace(((Select) st).getSelectBody(), null);
				if (replaced) {
					return st.toString();
				}
			} else if (st instanceof Insert) {
				Insert insert = ((Insert) st);
				if (insert.getReturningExpressionList() != null) {
					insert.setReturningExpressionList(null);
					replaced = true;
					return insert.toString();
				}
			} else if (st instanceof ShowStatement) {
				if (((ShowStatement) st).getName().equals("LC_COLLATE")) {
					replaced = true;
					return "SELECT 'C' lc_collate";
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

	private Socket socket;
	private PgServerCompat server;
	private InputStream ins;
	private OutputStream outs;

	public PgServerThreadCompat(Socket socket, PgServerCompat server) {
		super(socket, server);
		this.socket = socket;
		this.server = server;
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

	private void read() throws IOException, ReflectiveOperationException {
		int x = 0;
		int initLen = 0;
		if (initDone.getBoolean(this)) {
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
		switch (x) {
		case 'P':
			int z1 = findZero(data, 5, data.length) + 1;
			int z2 = findZero(data, z1, data.length);
			Charset charset = (Charset) getEncoding.invoke(this);
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
			charset = (Charset) getEncoding.invoke(this);
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
		dataInRaw.set(this, new DataInputStream(new ByteArrayInputStream(data)));
	}

	@Override
	public void run() {
		try {
			server.trace("Connect");
			ins = socket.getInputStream();
			outs = socket.getOutputStream();
			out.set(this, outs);
			// dataInRaw.set(this, new DataInputStream(ins));
			while (!stop.getBoolean(this)) {
				read();
				process.invoke(this);
				// not necessary to flush SocketOutputStream
				// outs.flush();
			}
		} catch (EOFException e) {
			// more or less normal disconnect
		} catch (IOException | ReflectiveOperationException e) {
			server.traceError(e);
		} finally {
			server.trace("Disconnect");
			close();
		}
	}
}