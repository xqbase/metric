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
import java.util.List;
import java.util.function.Consumer;

import org.h2.server.pg.PgServerThread;
import org.h2.server.pg.PgServerThreadEx;
import org.h2.util.Bits;
import org.h2.util.ScriptReader;
import org.h2.util.Utils;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

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

	static {
		try {
			initDone = getField("initDone");
			out = getField("out");
			dataInRaw = getField("dataInRaw");
			stop = getField("stop");
			process = getMethod("process");
			getEncoding = getMethod("getEncoding");
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	private static final LongValue ZERO = new LongValue(0);
	private static final StringValue EMPTY = new StringValue("");
	private static final NullValue NULL = new NullValue();

	private static boolean replace(Expression exp, Consumer<Expression> parentSet) {
		return replace(exp, null, null, parentSet);
	}

	private static boolean replace(Expression exp, String alias,
			String table, Consumer<Expression> parentSet) {
		if (exp instanceof SubSelect) {
			SelectBody sb = ((SubSelect) exp).getSelectBody();
			return sb instanceof PlainSelect && replace((PlainSelect) sb);
		}
		if (exp instanceof Column) {
			Column col = (Column) exp;
			switch (col.getName(false).toLowerCase()) {
			case "type_udt_name":
				if ("\"DTD_IDENTIFIER\"".equals(alias)) {
					parentSet.accept(EMPTY);
					return true;
				}
				break;
			case "indoption":
				if ("pg_index".equals(table)) {
					parentSet.accept(NULL);
					return true;
				}
				break;
			default:
			}
			return false;
		}
		if (exp instanceof Function) {
			switch (((Function) exp).getName().toLowerCase()) {
			case "pg_total_relation_size":
				parentSet.accept(ZERO);
				return true;
			case "col_description":
			case "pg_get_constraintdef":
				parentSet.accept(EMPTY);
				return true;
			default:
			}
			return false;
		}
		if (!(exp instanceof BinaryExpression)) {
			return false;
		}
		BinaryExpression be = (BinaryExpression) exp;
		Expression left = be.getLeftExpression();
		Expression right = be.getRightExpression();
		// value = ANY(array) -> ARRAY_CONTAINS(array, value)
		if (be instanceof EqualsTo) {
			if (left instanceof Column && ((Column) left).getColumnName().
					toLowerCase().equals("event_object_table")) {
				be.setLeftExpression(EMPTY);
				replace(right, be::setRightExpression);
				return true;
			}
			if (right instanceof Function) {
				Function func = (Function) right;
				if (func.getName().toUpperCase().equals("ANY")) {
					List<Expression> exps = func.getParameters().getExpressions();
					if (exps.size() == 1) {
						Expression exp0 = exps.get(0);
						if (!(exp0 instanceof SubSelect)) {
							ExpressionList el = new ExpressionList(exp0, left);
							replace(exp0, e -> el.getExpressions().set(0, e));
							replace(left, e -> el.getExpressions().set(1, e));
							func.setName("ARRAY_CONTAINS");
							func.setParameters(el);
							parentSet.accept(func);
							return true;
						}
					}
				}
			}
		}
		// Use `|` instead of `||` to avoid short-circuit
		return replace(left, be::setLeftExpression) |
				replace(right, be::setRightExpression);
	}

	private static boolean replace(PlainSelect ps) {
		boolean replaced = false;
		String table = null;
		FromItem fi = ps.getFromItem();
		if (fi instanceof Table) {
			table = ((Table) fi).getName();
		}
		for (SelectItem si : ps.getSelectItems()) {
			if (si instanceof SelectExpressionItem) {
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Alias alias = sei.getAlias();
				replaced |= replace(sei.getExpression(), alias == null ?
						null : alias.getName(), table, sei::setExpression);
			}
		}
		replaced |= replace(ps.getWhere(), ps::setWhere);
		return replaced;
	}

	private static final String[] REPLACE_FROM = {
		"(indpred IS NOT NULL)",
		", condeferrable::int AS deferrable, "
	};
	private static final String[] REPLACE_TO = {
		"(NVL2(indpred, TRUE, FALSE))",
		", 0 AS \"deferrable\", " // "deferrable" is a keyword in JSqlParser
	};
	private static final int[] REPLACE_FROM_LEN = {
		21,
		36,
	};

	private static String getSQL(String s) {
		String sql = s;
		for (int i = 0; i < REPLACE_FROM.length; i ++) {
			int index;
			while ((index = sql.indexOf(REPLACE_FROM[i])) >= 0) {
				sql = sql.substring(0, index) + REPLACE_TO[i] +
						sql.substring(index + REPLACE_FROM_LEN[i]);
			}
		}

		try {
			CCJSqlParser parser = new CCJSqlParser(new StringProvider(sql));
			Statement st = parser.Statement();
			if (st instanceof Select) {
				Select sel = (Select) st;
				SelectBody sb = sel.getSelectBody();
				if (sb instanceof PlainSelect) {
					if (replace((PlainSelect) sb)) {
						return sb.toString();
					}
				}
			}
			if (st instanceof ShowStatement) {
				ShowStatement ss = (ShowStatement) st;
				switch (ss.getName().toLowerCase()) {
				case "lc_collate":
					ss.setName("client_encoding");
					return ss.toString();
				default:
				}
			}
		} catch (ParseException e) {
			// Ignored
			// System.err.println(s + ": " + e);
		}
		return null;
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