package com.xqbase.metric.dashboard;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
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
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;

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
			mongo = new MongoClient(p.getProperty("host"),
					Numbers.parseInt(p.getProperty("port")));
			db = mongo.getDB(p.getProperty("db"));
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

	static {
		methodMap.put("count", MetricValue::getCount);
		methodMap.put("sum", MetricValue::getSum);
		methodMap.put("max", MetricValue::getMax);
		methodMap.put("min", MetricValue::getMin);
		methodMap.put("avg", MetricValue::getAvg);
		methodMap.put("std", MetricValue::getStd);
	}

	private static void badRequest(HttpServletResponse resp) {
		try {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
		} catch (IOException e) {/**/}
	}

	private static int getInt(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).intValue() : 0;
	}

	private static double getDouble(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).doubleValue() : 0;
	}

	private static String getString(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof String ? (String) value : "_";
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
		// Find Metric Collection and Aggregation Method
		String path = req.getPathInfo();
		if (path == null) {
			badRequest(resp);
			return;
		}
		while (!path.isEmpty() && path.charAt(0) == '/') {
			path = path.substring(1);
		}
		int slash = path.indexOf('/');
		if (slash < 0) {
			badRequest(resp);
			return;
		}
		ToDoubleFunction<MetricValue> method =
				methodMap.get(path.substring(slash + 1));
		if (method == null) {
			badRequest(resp);
			return;
		}
		DBCollection collection = db.getCollection(path.substring(0, slash));
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
		int end = Numbers.parseInt(req.getParameter("_end"),
				(int) (System.currentTimeMillis() / 60000));
		int interval = Numbers.parseInt(req.getParameter("_interval"), 1, 1440);
		int length = Numbers.parseInt(req.getParameter("_length"), 1, 1024);
		int begin = end - interval * length;
		BasicDBObject range = new BasicDBObject("$gte", Integer.valueOf(begin));
		range.put("$lt", Integer.valueOf(end));
		query.put("_minute", range);
		String groupBy_ = req.getParameter("_group_by");
		Function<DBObject, String> groupBy = groupBy_ == null ?
				row -> "_" : row -> getString(row, groupBy_);
		// Query by MongoDB and Group by Java
		HashMap<GroupKey, MetricValue> result = new HashMap<>();
		try {
			for (DBObject row : collection.find(query)) {
				int index = (getInt(row, "_minute") - begin) / interval;
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
			Log.e(e);
			try {
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			} catch (IOException e_) {/**/}
			return;
		}
		// Generate Data
		HashMap<String, double[]> data = new HashMap<>();
		for (Map.Entry<GroupKey, MetricValue> entry : result.entrySet()) {
			GroupKey key = entry.getKey();
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
			values[key.index] = method.applyAsDouble(entry.getValue());
		}
		// Output JSON
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
			out.print(new JSONObject(data));
		} else {
			resp.setContentType("text/javascript");
			out.print(callback + "(" + new JSONObject(data) + ");");
		}
	}
}