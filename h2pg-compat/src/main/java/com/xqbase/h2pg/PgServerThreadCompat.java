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
import java.util.Arrays;
import java.util.Collections;
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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
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
	private static final Function CURRENT_USER = new Function();
	private static final ValuesList PG_GET_KEYWORDS = new ValuesList();
	private static final NullValue PG_LISTENING_CHANNELS = new NullValue();
	private static final Alias PG_LISTENING_CHANNELS_ALIAS =
			new Alias("pg_listening_channels");
	private static final Column GENERATE_SERIES_COLUMN = new Column("\"X\"");
	private static final Column OID_COLUMN = new Column("oid");
	private static final Table PG_CLASS = new Table("pg_catalog.pg_class");
	private static final SelectExpressionItem RELNAME_COLUMN =
			new SelectExpressionItem();
	private static final ColDataType TEXT_TYPE = new ColDataType();
	private static final Map<String, SelectBody> TABLE_MAP = new HashMap<>();

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
		CURRENT_USER.setName("current_user");
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
		TABLE_MAP.put("pg_event_trigger", select("SELECT 0 WHERE FALSE"));
		TABLE_MAP.put("pg_depend",
				select("SELECT '' AS deptype, 0 AS classid, 0 AS refclassid, 0 AS objid, " +
				"0 AS objsubid, 0 AS refobjid, 0 AS refobjsubid WHERE FALSE"));
		TABLE_MAP.put("pg_collation",
				select("SELECT 0 AS oid, '' AS collnamespace, '' AS collname WHERE FALSE"));
		TABLE_MAP.put("pg_language",
				select("SELECT 0 AS oid, '' AS lanname WHERE FALSE"));
		TABLE_MAP.put("pg_trigger",
				select("SELECT 0 AS oid, '' AS tgname, 0 AS tgrelid WHERE FALSE"));
		TABLE_MAP.put("pg_tables",
				select("SELECT n.nspname AS schemaname, c.relname AS tablename " +
				"FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace " +
				"WHERE c.relkind IN ('r', 'p')"));
		TABLE_MAP.put("pg_views",
				select("SELECT n.nspname AS schemaname, c.relname AS viewname FROM pg_class c " +
				"LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'v'"));
	}

	private static boolean replace(Expression exp, Consumer<Expression> parentSet) {
		return replace(exp, null, parentSet);
	}

	private static boolean replace(Expression exp,
			String table, Consumer<Expression> parentSet) {
		if (exp instanceof Column) {
			Column col = (Column) exp;
			switch (col.getFullyQualifiedName()) {
			case "att.attndims":
				if ("pg_attribute".equals(table)) {
					parentSet.accept(ZERO);
					return true;
				}
				break;
			case "confdeltype":
			case "confmatchtype":
			case "confupdtype":
				if ("pg_constraint".equals(table)) {
					parentSet.accept(NULL);
					return true;
				}
				break;
			case "datconnlimit":
				if ("pg_database".equals(table)) {
					parentSet.accept(MINUS_ONE);
					return true;
				}
				break;
			case "indoption":
				if ("pg_index".equals(table)) {
					parentSet.accept(NULL);
					return true;
				}
				break;
			case "t.typdefault":
				if ("pg_type".equals(table)) {
					parentSet.accept(NULL);
					return true;
				}
				break;
			case "t.typndims":
				if ("pg_type".equals(table)) {
					parentSet.accept(ZERO);
					return true;
				}
				break;
			case "type_udt_name":
				if ("information_schema.routines".equals(table)) {
					parentSet.accept(NULL);
					return true;
				}
				break;
			default:
			}
			return false;
		}
		if (exp instanceof Function) {
			Function func = (Function) exp;
			ExpressionList el = func.getParameters();
			boolean replaced = false;
			switch (func.getName()) {
			case "array_length":
				if (el != null) {
					List<Expression> exps = el.getExpressions();
					if (exps.size() > 1) {
						el.setExpressions(Arrays.asList(exps.get(0)));
						replaced = true;
					}
				}
				break;
			case "array_lower":
				parentSet.accept(ONE);
				return true;
			case "array_upper":
			case "pg_catalog.array_upper":
				if (el != null && el.getExpressions().size() > 0) {
					Expression exp0 = el.getExpressions().get(0);
					if (exp0 instanceof Column) {
						// array_upper(p.proargtypes, 1) -> -1
						switch (((Column) exp0).getFullyQualifiedName()) {
						case "p.proargtypes":
						case "p.proallargtypes":
							parentSet.accept(MINUS_ONE);
							return true;
						default:
						}
					}
					el.setExpressions(Arrays.asList(exp0));
				}
				func.setName("array_length");
				replaced = true;
				break;
			case "array_to_string":
				if (el != null && el.getExpressions().size() > 0) {
					Expression exp0 = el.getExpressions().get(0);
					if (exp0 instanceof Column && ((Column) exp0).
							getFullyQualifiedName().equals("p.proargmodes")) {
						parentSet.accept(NULL);
						return true;
					}
				}
				break;
			case "col_description":
			case "information_schema._pg_char_max_length":
			case "information_schema._pg_numeric_precision":
			case "information_schema._pg_numeric_scale":
			case "information_schema._pg_datetime_precision":
			case "pg_get_constraintdef":
			case "pg_get_functiondef":
			case "pg_get_viewdef":
			case "shobj_description":
				parentSet.accept(NULL);
				return true;
			case "pg_catalog.pg_function_is_visible":
				parentSet.accept(TRUE);
				return true;
			case "pg_get_userbyid":
				List<Expression> exps = func.getParameters().getExpressions();
				if (exps != null && exps.size() == 1 &&
						exps.get(0) instanceof Column) {
					// pg_get_userbyid(*owner) -> current_user()
					switch (((Column) exps.get(0)).getFullyQualifiedName()) {
					case "nspowner":
					case "relowner":
					case "t.typowner":
						parentSet.accept(CURRENT_USER);
						return true;
					default:
					}
				}
				break;
			case "pg_listening_channels":
				parentSet.accept(PG_LISTENING_CHANNELS);
				return true;
			case "pg_total_relation_size":
				parentSet.accept(ZERO);
				return true;
			default:
			}
			if (el != null) {
				List<Expression> exps = el.getExpressions();
				for (int i = 0; i < exps.size(); i ++) {
					Expression ei = exps.get(i);
					int i_ = i;
					replaced |= replace(ei, e -> exps.set(i_, e));
				}
			}
			return replaced;
		}
		if (exp instanceof NotExpression) {
			NotExpression ne = (NotExpression) exp;
			Expression exp1 = ne.getExpression();
			if (exp1 instanceof Column && ((Column) exp1).
					getFullyQualifiedName().equals("datistemplate")) {
				parentSet.accept(TRUE);
				return true;
			}
			return replace(exp1, ne::setExpression);
		}
		if (exp instanceof CaseExpression) {
			boolean replaced = false;
			CaseExpression ce = (CaseExpression) exp;
			replaced |= replace(ce.getSwitchExpression(), ce::setSwitchExpression);
			for (WhenClause wc : ce.getWhenClauses()) {
				Expression when = wc.getWhenExpression();
				if (when instanceof IsNullExpression) {
					IsNullExpression isNull = (IsNullExpression) when;
					Expression left = isNull.getLeftExpression();
					// WHEN p.proallargtypes IS NULL -> WHEN TRUE
					if (!isNull.isNot() && left instanceof Column && ((Column) left).
							getFullyQualifiedName().equals("p.proallargtypes")) {
						wc.setWhenExpression(TRUE);
						replaced = true;
					}
				}
				replaced |= replace(wc.getWhenExpression(), wc::setWhenExpression) |
						replace(wc.getThenExpression(), wc::setThenExpression);
			}
			return replaced | replace(ce.getElseExpression(), ce::setElseExpression);
		}
		if (exp instanceof Parenthesis) {
			Parenthesis parenth = (Parenthesis) exp;
			return replace(parenth.getExpression(), parenth::setExpression);
		}
		if (exp instanceof CastExpression) {
			CastExpression ce = (CastExpression) exp;
			Expression oldLeft = ce.getLeftExpression();
			Expression left = oldLeft;
			while (left instanceof CastExpression) {
				left = ((CastExpression) left).getLeftExpression();
			}
			// x::regclass -> IFNULL(SELECT relname FROM pg_class WHERE oid = x, oid::text)
			if (ce.getType().getDataType().equals("regclass")) {
				Expression[] ceLeft = {ce.getLeftExpression()};
				replace(ceLeft[0], e -> ceLeft[0] = e);
				PlainSelect ps = new PlainSelect();
				ps.addSelectItems(RELNAME_COLUMN);
				ps.setFromItem(PG_CLASS);
				EqualsTo et = new EqualsTo();
				et.setLeftExpression(OID_COLUMN);
				et.setRightExpression(ceLeft[0]);
				ps.setWhere(et);
				SubSelect ss = new SubSelect();
				ss.setSelectBody(ps);
				Function func = new Function();
				func.setName("IFNULL");
				CastExpression ceText = new CastExpression();
				ceText.setType(TEXT_TYPE);
				ceText.setLeftExpression(ceLeft[0]);
				func.setParameters(new ExpressionList(ss, ceText));
				parentSet.accept(func);
				return true;
			}
			ce.setLeftExpression(left);
			return left != oldLeft | replace(left, ce::setLeftExpression);
		}
		if (exp instanceof SubSelect) {
			SelectBody sb = ((SubSelect) exp).getSelectBody();
			return sb instanceof PlainSelect && replace((PlainSelect) sb);
		}
		if (!(exp instanceof BinaryExpression)) {
			return false;
		}
		BinaryExpression be = (BinaryExpression) exp;
		Expression left = be.getLeftExpression();
		Expression right = be.getRightExpression();
		if (be instanceof EqualsTo) {
			switch (be.toString()) {
			case "c.oid = t.typcollation":
			case "con.tableoid = dep.refclassid":
			case "dep.classid = ci.tableoid":
			case "ns.oid = c.connamespace":
			case "p.prolang = l.oid":
				parentSet.accept(FALSE);
				return true;
			case "p.pronargs = 0":
				parentSet.accept(TRUE);
				return true;
			default:
			}
			if (left instanceof Column) {
				switch (((Column) left).getFullyQualifiedName()) {
				case "event_object_table":
					if (right instanceof StringValue) {
						parentSet.accept(FALSE);
						return true;
					}
					break;
				case "proisagg":
				case "p.proisagg":
					if (right instanceof Column && ((Column) right).
							getFullyQualifiedName().toUpperCase().equals("FALSE")) {
						parentSet.accept(TRUE);
						return true;
					}
					break;
				default:
				}
			}
			// value = ANY(array) -> ARRAY_CONTAINS(array, value)
			if (right instanceof Function &&
					((Function) right).getName().toUpperCase().equals("ANY")) {
				Function func = (Function) right;
				List<Expression> exps = func.getParameters().getExpressions();
				if (exps.size() == 1 && !(exps.get(0) instanceof SubSelect)) {
					ExpressionList el = new ExpressionList(exps.get(0), left);
					replace(exps.get(0), e -> el.getExpressions().set(0, e));
					replace(left, e -> el.getExpressions().set(1, e));
					func.setName("ARRAY_CONTAINS");
					func.setParameters(el);
					parentSet.accept(func);
					return true;
				}
			}
		} else if (be instanceof NotEqualsTo) {
			switch (be.toString()) {
			case "T.typcategory != 'A'":
				parentSet.accept(TRUE);
				return true;
			default:
			}
		}
		// Use `|` instead of `||` to avoid short-circuit
		return replace(left, be::setLeftExpression) |
				replace(right, be::setRightExpression);
	}

	private static boolean replace(FromItem fi,
			Consumer<FromItem> parentSet, String[] table) {
		if (fi instanceof Table) {
			String name = ((Table) fi).getFullyQualifiedName();
			SelectBody sb = TABLE_MAP.get(name);
			if (sb != null) {
				SubSelect ss = new SubSelect();
				ss.setSelectBody(sb);
				ss.setAlias(fi.getAlias());
				parentSet.accept(ss);
				return true;
			}
			table[0] = name;
			return false;
		}
		if (fi instanceof SubSelect) {
			SelectBody sb = ((SubSelect) fi).getSelectBody();
			return sb instanceof PlainSelect && replace((PlainSelect) sb);
		}
		if (!(fi instanceof TableFunction)) {
			return false;
		}
		TableFunction ti = (TableFunction) fi;
		Function func = ti.getFunction();
		if (func.getName().equals("pg_get_keywords")) {
			parentSet.accept(PG_GET_KEYWORDS);
			return true;
		}
		return replace(func, exp -> {
			if (exp instanceof Function) {
				ti.setFunction((Function) exp);
			} else {
				PlainSelect ps1 = new PlainSelect();
				ps1.setSelectItems(Collections.
						singletonList(new SelectExpressionItem(exp)));
				SubSelect ss = new SubSelect();
				ss.setSelectBody(ps1);
				parentSet.accept(ss);
			}
		});
	}

	private static boolean replace(PlainSelect ps) {
		String[] table = {null};
		boolean replaced = replace(ps.getFromItem(), ps::setFromItem, table);
		for (SelectItem si : ps.getSelectItems()) {
			if (si instanceof SelectExpressionItem) {
				SelectExpressionItem sei = (SelectExpressionItem) si;
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
				replaced |= replace(exp, table[0], e -> {
					sei.setExpression(e);
					if (e == PG_LISTENING_CHANNELS) {
						sei.setAlias(PG_LISTENING_CHANNELS_ALIAS);
						ps.setWhere(FALSE);
					} else if (sei.getAlias() == null && exp instanceof Column) {
						sei.setAlias(new Alias(((Column) exp).getColumnName()));
					}
				});
			}
		}
		List<Join> joins = ps.getJoins();
		if (joins != null) {
			for (Join join : ps.getJoins()) {
				replaced |= replace(join.getRightItem(), join::setRightItem, table) |
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
		replaced |= replace(ps.getWhere(), ps::setWhere);
		return replaced;
	}

	private static boolean replace(SelectBody sb) {
		if (sb instanceof PlainSelect) {
			return replace((PlainSelect) sb);
		}
		if (!(sb instanceof SetOperationList)) {
			return false;
		}
		boolean replaced = false;
		for (SelectBody sbi : ((SetOperationList) sb).getSelects()) {
			replaced |= replace(sbi);
		}
		return replaced;
	}

	private static final String[] REPLACE_FROM = {
		"(indpred IS NOT NULL)",
		// "deferrable" is a keyword in JSqlParser
		", condeferrable::int AS deferrable, ",
		" CAST('*' AS pg_catalog.text) ",
	};
	private static final String[] REPLACE_TO = {
		"(NVL2(indpred, TRUE, FALSE))",
		", 0 AS \"deferrable\", ",
		" '*' ",
	};
	private static final int[] REPLACE_FROM_LEN = {
		21,
		36,
		30,
	};

	private static String getSQL(String s) {
		boolean replaced = false;
		String sql = s;
		if (sql.startsWith("EXPLAIN VERBOSE ")) {
			sql = "EXPLAIN " + sql.substring(16);
			replaced = true;
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
				if (replace(((Select) st).getSelectBody())) {
					return st.toString();
				}
			} else if (st instanceof Insert) {
				Insert ins = ((Insert) st);
				if (ins.getReturningExpressionList() != null) {
					ins.setReturningExpressionList(null);
					return ins.toString();
				}
			} else if (st instanceof ShowStatement) {
				ShowStatement ss = (ShowStatement) st;
				switch (ss.getName()) {
				case "LC_COLLATE":
					ss.setName("client_encoding");
					return ss.toString();
				default:
				}
			}
		} catch (ParseException e) {
			// Ignored
			// System.err.println(s + ": " + e);
		}
		return replaced ? sql : null;
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
			String sql = getSQL(new String(data, z1, z2 - z1, charset));
			if (sql != null) {
				byte[] sqlb = sql.getBytes(charset);
				byte[] data_ = new byte[data.length - z2 + z1 + sqlb.length];
				data_[0] = 'P';
				Bits.writeInt(data_, 1, data_.length - 1);
				System.arraycopy(data, 5, data_, 5, z1 - 5);
				System.arraycopy(sqlb, 0, data_, z1, sqlb.length);
				System.arraycopy(data, z2, data_, z1 + sqlb.length, data.length - z2);
				data = data_;
			}
			break;
		case 'Q':
			z1 = findZero(data, 5, data.length);
			charset = (Charset) getEncoding.invoke(this);
			StringBuilder sb = new StringBuilder();
			boolean replaced = false;
			try (ScriptReader reader = new ScriptReader(new
					InputStreamReader(new ByteArrayInputStream(data, 5, z1 - 5), charset))) {
				String line;
				while ((line = reader.readStatement()) != null) {
					sql = getSQL(line);
					if (sql == null) {
						sb.append(line).append(';');
					} else {
						sb.append(sql).append(';');
						replaced = true;
					}
				}
			}
			if (replaced) {
				byte[] sqlb = sb.substring(0, sb.length() - 1).getBytes(charset);
				byte[] data_ = new byte[data.length - z1 + 5 + sqlb.length];
				data_[0] = 'Q';
				Bits.writeInt(data_, 1, data_.length - 1);
				System.arraycopy(sqlb, 0, data_, 5, sqlb.length);
				System.arraycopy(data, z1, data_, 5 + sqlb.length, data.length - z1);
				data = data_;
			}
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
			out.set(this, socket.getOutputStream());
			// dataInRaw.set(this, new DataInputStream(ins));
			while (!stop.getBoolean(this)) {
				read();
				process.invoke(this);
				((OutputStream) out.get(this)).flush();
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