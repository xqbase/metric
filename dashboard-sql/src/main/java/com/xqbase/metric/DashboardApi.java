package com.xqbase.metric;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Strings;
import com.xqbase.util.Time;
import com.xqbase.util.db.ConnectionPool;
import com.xqbase.util.db.Row;
import com.xqbase.util.function.ConsumerEx;

class GroupKey {
	String tag;
	int index;

	GroupKey(String tag, int index) {
		this.tag = tag;
		this.index = index;
	}

	@Override
	public boolean equals(Object obj) {
		GroupKey key = (GroupKey) obj;
		return index == key.index && tag.equals(key.tag);
	}

	@Override
	public int hashCode() {
		return tag.hashCode() * 31 + index;
	}
}

public class DashboardApi extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final String QUERY_NAMES = "SELECT name FROM metric_name";
	private static final String QUERY_TAGS = "SELECT tags FROM metric_name WHERE name = ?";
	private static final String QUERY_ID = "SELECT id FROM metric_name WHERE name = ?";
	private static final String AGGREGATE_MINUTE =
			"SELECT time, metrics FROM metric_minute WHERE id = ? AND time >= ? AND time <= ?";
	private static final String AGGREGATE_QUARTER =
			"SELECT time, metrics FROM metric_quarter WHERE id = ? AND time >= ? AND time <= ?";

	private int maxTagValues = 0;
	private ConnectionPool db = null;

	@Override
	public void init() throws ServletException {
		try {
			Properties p = Conf.load("jdbc");
			Driver driver = (Driver) Class.forName(p.
					getProperty("driver")).newInstance();
			db = new ConnectionPool(driver, p.getProperty("url", ""),
					p.getProperty("user"), p.getProperty("password"));

			maxTagValues = Numbers.parseInt(Conf.
					load("Dashboard").getProperty("max_tag_values"));
		} catch (ReflectiveOperationException e) {
			throw new ServletException(e);
		}
	}

	@Override
	public void destroy() {
		if (db != null) {
			db.close();
		}
	}

	private static double __(String s) {
		double d = Numbers.parseDouble(s);
		return Double.isFinite(d) ? d : 0;
	}

	private static Class<?> pgConnection = null;
	private static Map<String, ToDoubleFunction<MetricValue>>
			methodMap = new HashMap<>();
	private static final ToDoubleFunction<MetricValue> NAMES_METHOD = value -> 0;
	private static final ToDoubleFunction<MetricValue> TAGS_METHOD = value -> 1;

	static {
		try {
			pgConnection = Class.forName("org.postgresql.jdbc.PgConnection");
		} catch (ClassNotFoundException e) {
			// Ignored
		}
		methodMap.put("count", MetricValue::getCount);
		methodMap.put("sum", MetricValue::getSum);
		methodMap.put("max", MetricValue::getMax);
		methodMap.put("min", MetricValue::getMin);
		methodMap.put("avg", MetricValue::getAvg);
		methodMap.put("std", MetricValue::getStd);
		methodMap.put("names", NAMES_METHOD);
		methodMap.put("tags", TAGS_METHOD);
	}

	private static void error400(HttpServletResponse resp) {
		try {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
		} catch (IOException e) {/**/}
	}

	private static void error500(HttpServletResponse resp, Throwable e) {
		Log.e(e);
		try {
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		} catch (IOException e_) {/**/}
	}

	private static void copyHeader(HttpServletRequest req,
			HttpServletResponse resp, String reqHeader, String respHeader) {
		String value = req.getHeader(reqHeader);
		if (value != null) {
			resp.setHeader(respHeader, value);
		}
	}

	private static void outputJson(HttpServletRequest req,
			HttpServletResponse resp, Object data) {
		resp.setCharacterEncoding("UTF-8");
		PrintWriter out;
		try {
			out = resp.getWriter();
		} catch (IOException e) {
			Log.d("" + e);
			return;
		}
		String json = (data instanceof String ?
				(String) data : JSONObject.valueToString(data));
		String callback = req.getParameter("_callback");
		if (callback == null) {
			copyHeader(req, resp, "Origin", "Access-Control-Allow-Origin");
			copyHeader(req, resp, "Access-Control-Request-Methods",
					"Access-Control-Allow-Methods");
			copyHeader(req, resp, "Access-Control-Request-Headers",
					"Access-Control-Allow-Headers");
			resp.setHeader("Access-Control-Allow-Credentials", "true");
			resp.setContentType("application/json");
			out.print(json);
		} else {
			resp.setContentType("text/javascript");
			out.print(callback + "(" + json + ");");
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
		// Find Metric Collection and Aggregation Method
		String path = req.getPathInfo();
		if (path == null) {
			error400(resp);
			return;
		}
		while (!path.isEmpty() && path.charAt(0) == '/') {
			path = path.substring(1);
		}
		int slash = path.indexOf('/');
		if (slash < 0) {
			error400(resp);
			return;
		}
		ToDoubleFunction<MetricValue> method =
				methodMap.get(path.substring(slash + 1));
		if (method == null) {
			error400(resp);
			return;
		}
		if (method == NAMES_METHOD) {
			try {
				Set<String> names = new TreeSet<>();
				db.queryEx(row -> names.add(row.getString("name")), QUERY_NAMES);
				outputJson(req, resp, names);
			} catch (SQLException e) {
				error500(resp, e);
			}
			return;
		}
		String metricName = path.substring(0, slash);
		if (method == TAGS_METHOD) {
			Row row;
			try {
				row = db.queryEx(QUERY_TAGS, metricName);
			} catch (SQLException e) {
				error500(resp, e);
				return;
			}
			if (row == null) {
				outputJson(req, resp, "{}");
				return;
			}
			String s = row.getString("tags");
			outputJson(req, resp, s == null ? "{}" : s);
			return;
		}

		boolean quarter = metricName.startsWith("_quarter.");
		if (quarter) {
			metricName = metricName.substring(9);
		}
		int id;
		try {
			Row row = db.queryEx(QUERY_ID, metricName);
			if (row == null) {
				outputJson(req, resp, "{}");
				return;
			}
			id = row.getInt("id");
		} catch (SQLException e) {
			error500(resp, e);
			return;
		}

		// Query Condition
		Map<String, String> query = new HashMap<>();
		Enumeration<String> names = req.getParameterNames();
		while (names.hasMoreElements()) {
			String name = names.nextElement();
			if (!name.isEmpty() && name.charAt(0) != '_') {
				query.put(name, req.getParameter(name));
			}
		}
		// Other Query Parameters
		int end = Numbers.parseInt(req.getParameter("_end"),
				(int) (System.currentTimeMillis() /
				(quarter ? Time.MINUTE / 15 : Time.MINUTE)));
		int interval = Numbers.parseInt(req.getParameter("_interval"), 1, 1440);
		int length = Numbers.parseInt(req.getParameter("_length"), 1, 1024);
		int begin = end - interval * length + 1;

		String groupBy_ = req.getParameter("_group_by");
		Function<Map<String, String>, String> groupBy = groupBy_ == null ?
				tags -> "_" : tags -> {
			String value = tags.get(groupBy_);
			return Strings.isEmpty(value) ? "_" : value;
		};
		// Query Time Range by SQL, Query and Group Tags by Java
		Map<GroupKey, MetricValue> result = new HashMap<>();
		ConsumerEx<ResultSet, SQLException> consumer = rs -> {
			int index = (rs.getInt("time") - begin) / interval;
			if (index < 0 || index >= length) {
				Log.w("Key " + rs.getInt("time") + " out of range, end = " + end +
						", interval = " + interval + ", length = " + length);
				return;
			}
			String s = rs.getString("metrics");
			for (String line : s.split("\n")) {
				String[] paths;
				Map<String, String> tags = new HashMap<>();
				int i = line.indexOf('?');
				if (i < 0) {
					paths = line.split("/");
				} else {
					paths = line.substring(0, i).split("/");
					String q = line.substring(i + 1);
					for (String tag : q.split("&")) {
						i = tag.indexOf('=');
						if (i > 0) {
							tags.put(Strings.decodeUrl(tag.substring(0, i)),
									Strings.decodeUrl(tag.substring(i + 1)));
						}
					}
				}
				// Query Tags
				boolean skip = false;
				for (Map.Entry<String, String> entry : query.entrySet()) {
					String value = tags.get(entry.getKey());
					if (!entry.getValue().equals(value)) {
						skip = true;
						break;
					}
				}
				if (skip || paths.length <= 4) {
					continue;
				}
				// Group Tags
				GroupKey key = new GroupKey(groupBy.apply(tags), index);
				MetricValue newValue = new MetricValue(Numbers.parseLong(paths[0]),
						__(paths[1]), __(paths[2]), __(paths[3]), __(paths[4]));
				MetricValue value = result.get(key);
				if (value == null) {
					result.put(key, newValue);
				} else {
					value.add(newValue);
				}
			}
		};
		try (ConnectionPool.Entry entry = db.borrow()) {
			Connection conn = entry.getObject();
			boolean pg = pgConnection != null &&
					pgConnection.isAssignableFrom(conn.getClass());
			// db.query() does not support setAutoCommit
			if (pg) {
				conn.setAutoCommit(false);
			}
			try (PreparedStatement ps = conn.prepareStatement(quarter ?
					AGGREGATE_QUARTER : AGGREGATE_MINUTE)) {
				ps.setInt(1, id);
				ps.setInt(2, begin);
				ps.setInt(3, end);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						consumer.accept(rs);
					}
				}
			}
			if (pg) {
				conn.setAutoCommit(true);
			}
			entry.setValid(true);
		} catch (SQLException e) {
			error500(resp, e);
			return;
		}
		// Generate Data
		Map<String, double[]> data = new HashMap<>();
		result.forEach((key, value) -> {
			/* Already Filtered during Grouping
			if (key.index < 0 || key.index >= length) {
				continue;
			} */
			double[] values = data.get(key.tag);
			if (values == null) {
				values = new double[length];
				Arrays.fill(values, 0);
				data.put(key.tag, values);
			}
			double d = method.applyAsDouble(value);
			values[key.index] = Double.isFinite(d) ? d : 0;
		});
		if (maxTagValues > 0 && data.size() > maxTagValues) {
			outputJson(req, resp, CollectionsEx.toMap(CollectionsEx.max(data.entrySet(),
					Comparator.comparingDouble(entry ->
					DoubleStream.of((double[]) entry.getValue()).sum()),
					maxTagValues)));
		} else {
			outputJson(req, resp, data);
		}
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
		doGet(req, resp);
	}
}