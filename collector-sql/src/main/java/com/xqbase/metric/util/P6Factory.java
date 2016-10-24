package com.xqbase.metric.util;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import com.p6spy.engine.common.PreparedStatementInformation;
import com.p6spy.engine.common.StatementInformation;
import com.p6spy.engine.event.JdbcEventListener;
import com.p6spy.engine.event.SimpleJdbcEventListener;
import com.p6spy.engine.spy.P6SpyDriver;
import com.p6spy.engine.spy.P6SpyFactory;
import com.xqbase.metric.common.Metric;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;

public class P6Factory extends P6SpyFactory {
	private static final int SLOW_THRESHOLD = 200;

	public static class Driver extends P6SpyDriver {
		static {
			for (String className : Conf.load("spy").
					getProperty("driverlist", "").split(",")) {
				try {
					Class.forName(className);
				} catch (ReflectiveOperationException e) {
					// Ignored
				}
			}
		}
	}

	private static String removeQuote(String s) {
		int len = s.length();
		return len > 2 && s.charAt(0) == '`' && s.charAt(len - 1) == '`' ?
				s.substring(1, len - 1) : s;
	}

	static void log(String sql_, int elapsed, int rowCount, SQLException e) {
		// Handle Error
		if (e != null) {
			if (e instanceof SQLIntegrityConstraintViolationException) {
				// Log.d("Update Failure: " + sql_ + ", " + elapsed + "ms, " + e);
			} else {
				Log.w("Execute Failure: " + sql_ + ", " + elapsed + "ms, " + e);
			}
			return;
		}

		// Parse Command
		String sql = sql_.trim().replaceAll("\\s+", " ");
		if (sql.isEmpty()) {
			return;
		}
		int space = sql.indexOf(' ');
		if (space < 0) {
			Log.w("Invalid SQL: " + sql);
			return;
		}
		if (elapsed > SLOW_THRESHOLD) {
			Log.w("Slow SQL (" + elapsed + "ms): " + sql);
		}
		String cmd = sql.substring(0, space).toUpperCase();
		switch (cmd) {
		case "SELECT":
		case "DELETE":
			int from = sql.toUpperCase().indexOf(" FROM ", space - 1);
			if (from < 0) {
				Log.w("Invalid " + cmd + ": " + sql);
				return;
			}
			space = from + 6;
			break;
		case "INSERT":
		case "UPDATE":
		case "REPLACE":
			int into = sql.toUpperCase().indexOf(" INTO ", space - 1);
			space = into < 0 ? space + 1 : into + 6;
			break;
		default:
			Log.w("Unknown Command " + cmd + ": " + sql);
			return;
		}

		// Parse Schema and Table
		String table;
		int comma = sql.indexOf(',', space);
		int space2 = sql.indexOf(' ', space);
		if (comma < 0) {
			if (space2 < 0) {
				table = sql.substring(space);
			} else {
				table = sql.substring(space, space2);
			}
		} else if (space2 < 0) {
			table = sql.substring(space, comma);
		} else {
			table = sql.substring(space, Math.min(comma, space2));
		}
		table = table.toLowerCase();
		String schema = "_";
		int dot = table.indexOf('.');
		if (dot > 0) {
			schema = removeQuote(table.substring(0, dot));
			table = table.substring(dot + 1);
		}
		table = removeQuote(table);
		Metric.put("metric.sql.elapsed", elapsed,
				"schema", schema, "table", table, "command", cmd);
		if (!cmd.equals("SELECT")) {
			Metric.put("metric.sql.rowcount", rowCount,
					"schema", schema, "table", table, "command", cmd);
		}
	}

	static int MILLIS(long timeElapsedNanos) {
		return (int) (timeElapsedNanos / 1_000_000);
	}

	@Override
	public JdbcEventListener getJdbcEventListener() {
		return new SimpleJdbcEventListener() {
			@Override
			public void onAfterExecuteUpdate(PreparedStatementInformation psi,
					long timeElapsedNanos, int rowCount, SQLException e) {
				log(psi.getSqlWithValues(), MILLIS(timeElapsedNanos), rowCount, e);
			}

			@Override
			public void onAfterExecuteUpdate(StatementInformation si,
					long timeElapsedNanos, String sql, int rowCount, SQLException e) {
				log(sql, MILLIS(timeElapsedNanos), rowCount, e);
			}

			@Override
			public void onAfterAnyExecute(StatementInformation si,
					long timeElapsedNanos, SQLException e) {
				log(si instanceof PreparedStatementInformation ? si.getSqlWithValues() :
						si.getSql(), MILLIS(timeElapsedNanos), 0, e);
			}
		};
	}
}