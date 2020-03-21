package com.xqbase.metric.dashboard;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Time;

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
		return index == key.index && tag.equals(key.tag);
	}

	@Override
	public int hashCode() {
		return tag.hashCode() + index;
	}
}

public class DashboardApi extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private int maxTagValues = 0;
	private MongoClient mongo = null;
	private MongoDatabase db = null;

	@Override
	public void init() throws ServletException {
		Properties p = Conf.load("Mongo");
		mongo = new MongoClient(new MongoClientURI(p.getProperty("uri")));
		db = mongo.getDatabase(p.getProperty("database", "metric"));

		maxTagValues = Numbers.parseInt(Conf.
				load("Dashboard").getProperty("max_tag_values"));
	}

	@Override
	public void destroy() {
		if (mongo != null) {
			mongo.close();
			mongo = null;
			db = null;
		}
	}

	private static String escape(String s) {
		return s.replace("\\", "\\\\").replace(".", "\\_");
	}

	private static String unescape(String s) {
		return s.replace("\\-", "\\").replace("\\_", ".");
	}

	private static Document __(String key, Object value) {
		return new Document(key, value);
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

	private static int getInt(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).intValue() : 0;
	}

	private static long getLong(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).longValue() : 0;
	}

	private static double getDouble(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).doubleValue() : 0;
	}

	private static String getString(Document row, String key) {
		Object value = row.get(key);
		return value instanceof String ? (String) value : "_";
	}

	private static Document getDocument(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Document ? (Document) value : new Document();
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
			Log.d(e.getMessage());
			return;
		}
		String json = data instanceof Document ? ((Document) data).toJson() :
				JSONObject.wrap(data).toString();
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
		String metricName = path.substring(0, slash);
		if (method == TAGS_METHOD) {
			Document tagsRow;
			try {
				tagsRow = db.getCollection("_meta.aggregated").
						find(__("name", metricName)).first();
			} catch (MongoException e) {
				error500(resp, e);
				return;
			}
			outputJson(req, resp, tagsRow == null ?
					Collections.emptyMap() : getDocument(tagsRow, "tags"));
			return;
		}
		// Query Condition
		Document query = new Document();
		Enumeration<String> names = req.getParameterNames();
		while (names.hasMoreElements()) {
			String name = names.nextElement();
			if (!name.isEmpty() && name.charAt(0) != '_') {
				query.put("tags." + escape(name), req.getParameter(name));
			}
		}
		// Other Query Parameters
		int end;
		String rangeColumn;
		if (metricName.startsWith("_quarter.")) {
			end = Numbers.parseInt(req.getParameter("_end"),
					(int) (System.currentTimeMillis() / Time.MINUTE / 15));
			rangeColumn = "quarter";
		} else {
			end = Numbers.parseInt(req.getParameter("_end"),
					(int) (System.currentTimeMillis() / Time.MINUTE));
			rangeColumn = "minute";
		}
		int interval = Numbers.parseInt(req.getParameter("_interval"), 1, 1440);
		int length = Numbers.parseInt(req.getParameter("_length"), 1, 1024);
		int begin = end - interval * length + 1;
		Document range = __("$gte", Integer.valueOf(begin));
		range.put("$lte", Integer.valueOf(end));
		query.put(rangeColumn, range);
		String groupBy_ = req.getParameter("_group_by");
		Function<Document, String> groupBy = groupBy_ == null ? row -> "_" :
				row -> getString(getDocument(row, "tags"), escape(groupBy_));
		// Query by MongoDB and Group by Java
		HashMap<GroupKey, MetricValue> result = new HashMap<>();
		try {
			for (Document row : db.getCollection(metricName).find(query)) {
				int index = (getInt(row, rangeColumn) - begin) / interval;
				if (index < 0 || index >= length) {
					continue;
				}
				GroupKey key = new GroupKey(groupBy.apply(row), index);
				MetricValue newValue = new MetricValue(getLong(row, "count"),
						getDouble(row, "sum"), getDouble(row, "max"),
						getDouble(row, "min"), getDouble(row, "sqr"));
				MetricValue value = result.get(key);
				if (value == null) {
					result.put(key, newValue);
				} else {
					value.add(newValue);
				}
			}
		} catch (MongoException e) {
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
			double d = method.applyAsDouble(value);
			values[key.index] = Double.isFinite(d) ? d : 0;
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