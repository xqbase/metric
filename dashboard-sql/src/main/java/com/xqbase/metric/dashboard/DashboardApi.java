package com.xqbase.metric.dashboard;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.metric.util.Kryos;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Strings;
import com.xqbase.util.Time;
import com.xqbase.util.db.ConnectionPool;
import com.xqbase.util.db.Row;

class GroupKey {
	String tag;
	int index;

	GroupKey(String tag, int index) {
		this.tag = tag;
		this.index = index;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof GroupKey)) {
			return false;
		}
		GroupKey key = (GroupKey) obj;
		return tag.equals(key.tag) && index == key.index;
	}

	@Override
	public int hashCode() {
		return tag.hashCode() + index;
	}
}

class MetricRow {
	int time;
	long count;
	double sum, max, min, sqr;
	HashMap<String, String> tags;
}

public class DashboardApi extends HttpServlet {
	private static final long serialVersionUID = 1L;

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

	private static HashMap<String, ToDoubleFunction<MetricValue>>
			methodMap = new HashMap<>();
	private static final ToDoubleFunction<MetricValue> TAGS_METHOD = value -> 0;

	static {
		methodMap.put("count", MetricValue::getCount);
		methodMap.put("sum", MetricValue::getSum);
		methodMap.put("max", MetricValue::getMax);
		methodMap.put("min", MetricValue::getMin);
		methodMap.put("avg", MetricValue::getAvg);
		methodMap.put("std", MetricValue::getStd);
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

	private static void outputJson(HttpServletRequest req,
			HttpServletResponse resp, Object data) {
		resp.setCharacterEncoding("UTF-8");
		PrintWriter out;
		try {
			out = resp.getWriter();
		} catch (IOException e) {
			Log.d(e.getMessage());
			return;
		}
		String callback = req.getParameter("_callback");
		if (callback == null) {
			String origin = req.getHeader("Origin");
			if (origin != null) {
				resp.setHeader("Access-Control-Allow-Origin", origin);
			}
			resp.setHeader("Access-Control-Allow-Credentials", "true");
			resp.setContentType("application/json");
			out.print(JSONObject.wrap(data));
		} else {
			resp.setContentType("text/javascript");
			out.print(callback + "(" + JSONObject.wrap(data) + ");");
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
		String metricName = path.substring(0, slash);
		if (method == TAGS_METHOD) {
			Row row;
			try {
				row = db.queryEx("SELECT tags FROM metric_tags_all WHERE name = ?", metricName);
			} catch (SQLException e) {
				error500(resp, e);
				return;
			}
			if (row == null) {
				outputJson(req, resp, Collections.emptyMap());
				return;
			}
			@SuppressWarnings("unchecked")
			HashMap<String, HashMap<String, MetricValue>> tags =
					Kryos.deserialize(row.getBytes(1), HashMap.class);
			if (tags == null) {
				outputJson(req, resp, Collections.emptyMap());
				return;
			}
			JSONObject json = new JSONObject();
			tags.forEach((tagKey, tagValues) -> {
				if (tagKey.isEmpty() || tagKey.charAt(0) == '_') {
					return;
				}
				Collection<Map.Entry<String, MetricValue>> tagValues_ =
						tagValues.entrySet();
				if (maxTagValues > 0 && tagValues.size() > maxTagValues) {
					tagValues_ = CollectionsEx.max(tagValues_,
							Comparator.comparingLong(o -> o.getValue().getCount()),
							maxTagValues);
				}
				JSONArray arr = new JSONArray();
				for (Map.Entry<String, MetricValue> tagValue : tagValues_) {
					MetricValue metric = tagValue.getValue();
					JSONObject obj = new JSONObject();
					obj.put("_value", tagValue.getKey());
					obj.put("_count", metric.getCount());
					obj.put("_sum", metric.getSum());
					obj.put("_max", metric.getMax());
					obj.put("_min", metric.getMin());
					obj.put("_sqr", metric.getSqr());
					arr.put(obj);
				}
				json.put(tagKey, arr);
			});
			outputJson(req, resp, json);
			return;
		}
		boolean quarter = metricName.startsWith("_quarter.");
		if (quarter) {
			metricName = metricName.substring(9);
		}
		// Query Condition
		HashMap<String, String> query = new HashMap<>();
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
		Function<HashMap<String, String>, String> groupBy = groupBy_ == null ?
				tags -> "_" : tags -> {
			String value = tags.get(groupBy_);
			return Strings.isEmpty(value) ? "_" : value;
		};
		//Query Time Range by SQL, Query and Group Tags by Java
		HashMap<GroupKey, MetricValue> result = new HashMap<>();
		String sql = "SELECT time, _count, _sum, _max, _min, _sqr, tags " +
				"FROM metric_" + (quarter ? "quarter" : "minute") +
				" WHERE name = ? AND time >= ? AND time <= ?";
		try {
			db.queryEx(row -> {
				int index = (row.getInt(1) - begin) / interval;
				if (index < 0 || index >= length) {
					return;
				}
				@SuppressWarnings("unchecked")
				HashMap<String, String> tags = Kryos.
						deserialize(row.getBytes(7), HashMap.class);
				if (tags == null) {
					tags = new HashMap<>();
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
				if (skip) {
					return;
				}
				// Group Tags
				GroupKey key = new GroupKey(groupBy.apply(tags), index);
				MetricValue newValue = new MetricValue(row.getLong(2),
						((Number) row.get(3)).doubleValue(),
						((Number) row.get(4)).doubleValue(),
						((Number) row.get(5)).doubleValue(),
						((Number) row.get(6)).doubleValue());
				MetricValue value = result.get(key);
				if (value == null) {
					result.put(key, newValue);
				} else {
					value.add(newValue);
				}
			}, sql, metricName, Integer.valueOf(begin), Integer.valueOf(end));
		} catch (SQLException e) {
			error500(resp, e);
			return;
		}
		// Generate Data
		HashMap<String, double[]> data = new HashMap<>();
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
			values[key.index] = method.applyAsDouble(value);
		});
		if (maxTagValues > 0 && data.size() > maxTagValues) {
			outputJson(req, resp, CollectionsEx.toMap(CollectionsEx.max(data.entrySet(),
					Comparator.comparingDouble(entry -> DoubleStream.of((double[]) entry.getValue()).sum()),
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