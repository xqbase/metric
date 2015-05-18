package com.xqbase.metric.dashboard;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.xqbase.metric.common.MetricValue;
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
		return tag.equals(key.tag) && index == key.index;
	}

	@Override
	public int hashCode() {
		return tag.hashCode() + index;
	}
}

public class DashboardApi extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private MongoClient mongo = null;
	private DB db = null;

	@Override
	public void init() throws ServletException {
		Properties p = Conf.load("Mongo");
		try {
			ServerAddress addr = new ServerAddress(p.getProperty("host"),
					Numbers.parseInt(p.getProperty("port"), 27017));
			String database = p.getProperty("db");
			String username = p.getProperty("username");
			String password = p.getProperty("password");
			if (username == null || password == null) {
				mongo = new MongoClient(addr);
			} else {
				mongo = new MongoClient(addr, Collections.singletonList(MongoCredential.
						createMongoCRCredential(username, database, password.toCharArray())));
			}
			db = mongo.getDB(database);
		} catch (IOException e) {
			throw new ServletException(e);
		}
	}

	@Override
	public void destroy() {
		if (mongo != null) {
			mongo.close();
			mongo = null;
			db = null;
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

	private static int getInt(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).intValue() : 0;
	}

	private static double getDouble(DBObject row, String key) {
		Object value = row.get(key);
		double d = value instanceof Number ? ((Number) value).doubleValue() : 0;
		return Double.isFinite(d) ? d : 0;
	}

	private static String getString(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof String ? (String) value : "_";
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
			DBObject tagsRow;
			try {
				tagsRow = db.getCollection("_meta.tags_all").
						findOne(new BasicDBObject("_name", metricName));
			} catch (MongoException e) {
				error500(resp, e);
				return;
			}
			if (tagsRow == null) {
				outputJson(req, resp, Collections.emptyMap());
				return;
			}
			Iterator<String> it = tagsRow.keySet().iterator();
			while (it.hasNext()) {
				String tag = it.next();
				if (tag.isEmpty() || tag.charAt(0) == '_') {
					it.remove();
				}
			}
			outputJson(req, resp, tagsRow);
			return;
		}
		// Query Condition
		BasicDBObject query = new BasicDBObject();
		Enumeration<String> names = req.getParameterNames();
		while (names.hasMoreElements()) {
			String name = names.nextElement();
			if (!name.isEmpty() && name.charAt(0) != '_') {
				query.put(name, req.getParameter(name));
			}
		}
		// Other Query Parameters
		int end;
		String rangeColumn;
		if (metricName.startsWith("_quarter.")) {
			end = Numbers.parseInt(req.getParameter("_end"),
					(int) (System.currentTimeMillis() / Time.MINUTE / 15));
			rangeColumn = "_quarter";
		} else {
			end = Numbers.parseInt(req.getParameter("_end"),
					(int) (System.currentTimeMillis() / Time.MINUTE));
			rangeColumn = "_minute";
		}
		int interval = Numbers.parseInt(req.getParameter("_interval"), 1, 1440);
		int length = Numbers.parseInt(req.getParameter("_length"), 1, 1024);
		int begin = end - interval * length + 1;
		BasicDBObject range = new BasicDBObject("$gte", Integer.valueOf(begin));
		range.put("$lte", Integer.valueOf(end));
		query.put(rangeColumn, range);
		String groupBy_ = req.getParameter("_group_by");
		Function<DBObject, String> groupBy = groupBy_ == null ?
				row -> "_" : row -> getString(row, groupBy_);
		// Query by MongoDB and Group by Java
		HashMap<GroupKey, MetricValue> result = new HashMap<>();
		try {
			for (DBObject row : db.getCollection(metricName).find(query)) {
				int index = (getInt(row, rangeColumn) - begin) / interval;
				if (index < 0 || index >= length) {
					continue;
				}
				GroupKey key = new GroupKey(groupBy.apply(row), index);
				MetricValue newValue = new MetricValue(getInt(row, "_count"),
						getDouble(row, "_sum"), getDouble(row, "_max"),
						getDouble(row, "_min"), getDouble(row, "_sqr"));
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
			values[key.index] = method.applyAsDouble(value);
		});
		outputJson(req, resp, data);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
		doGet(req, resp);
	}
}