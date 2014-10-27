package com.xqbase.metric.dashboard;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

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
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;

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

	private static BasicDBObject __(String key, Object value) {
		return new BasicDBObject(key, value);
	}

	private static BasicDBObject __(String key, Object value, String key2, Object value2) {
		BasicDBObject obj = __(key, value);
		obj.put(key2, value2);
		return obj;
	}

	private static BasicDBObject __(String operator, Object value1, Object value2) {
		return __(operator, Arrays.asList(value1, value2));
	}

	private static HashMap<String, BasicDBObject> groupMap = new HashMap<>();

	static {
		groupMap.put("count", __("$sum", "$_count"));
		groupMap.put("sum", __("$sum", "$_sum"));
		groupMap.put("max", __("$max", "$_max"));
		groupMap.put("min", __("$min", "$_min"));
		groupMap.put("avg", __("$sum", "$_sum"));
		groupMap.put("std", __("$sum", "$_sqr"));
	}

	private static void badRequest(HttpServletResponse resp) {
		try {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
		} catch (IOException e) {/**/}
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
		String method = path.substring(slash + 1);
		BasicDBObject groupValue = groupMap.get(method);
		if (groupValue == null) {
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
		int interval = Numbers.parseInt(req.getParameter("_interval"), 1);
		int length = Numbers.parseInt(req.getParameter("_length"), 1, 1024);
		int begin = end - interval * length;
		// Aggregation Pipeline: Match
		query.put("_minute", __("$gte", Integer.valueOf(begin),
				"$lt", Integer.valueOf(end)));
		// Aggregation Pipeline: Group
		BasicDBObject groupId = __("_index", __("$divide",
			__("$subtract",
				__("$subtract", "$_minute", Integer.valueOf(begin)),
				__("$mod",
					__("$subtract", "$_minute", Integer.valueOf(begin)),
					Integer.valueOf(interval)
				)
			),
			Integer.valueOf(interval)
		));
		String groupBy = req.getParameter("_group_by");
		if (groupBy != null) {
			groupId.put("_group", "$" + groupBy);
		}
		BasicDBObject group = __("_id", groupId, "_value", groupValue);
		boolean avg = false, std = false;
		if (method.equals("avg")) {
			avg = true;
			group.put("_count", groupMap.get("count"));
		} else if (method.equals("std")) {
			avg = std = true;
			group.put("_count", groupMap.get("count"));
			group.put("_sum", groupMap.get("sum"));
		}
		// Aggregation by MongoDB
		List<DBObject> stages = Arrays.<DBObject>
				asList(__("$match", query), __("$group", group));
		Iterable<DBObject> results_;
		try {
			results_ = collection.aggregate(stages).results();
		} catch (MongoException e) {
			Log.e(e);
			try {
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			} catch (IOException e_) {/**/}
			return;
		}
		// Aggregation by Java
		HashMap<String, double[]> data = new HashMap<>();
		for (DBObject result : results_) {
			Object id = result.get("_id");
			if (!(id instanceof DBObject)) {
				continue;
			}
			DBObject id_ = (DBObject) id;
			Object indexKey = id_.get("_index");
			Object groupKey = id_.get("_group");
			Object value_ = result.get("_value");
			if (!(indexKey instanceof Number) || !(value_ instanceof Number)) {
				continue;
			}
			int index = ((Number) indexKey).intValue();
			if (index < 0 || index >= length) {
				continue;
			}
			String groupKey_ = groupKey instanceof String ?
					(String) groupKey : "_";
			double value = ((Number) value_).doubleValue();
			if (avg) {
				Object count_ = result.get("_count");
				if (!(count_ instanceof Integer)) {
					continue;
				}
				int count = ((Integer) count_).intValue();
				if (count == 0) {
					continue;
				}
				if (!std) {
					// avg=sum(x)/n
					value /= count;
					continue;
				}
				// std=sqrt(sum(x^2)/n-avg^2)
				Object sum_ = result.get("_sum");
				if (!(sum_ instanceof Double)) {
					continue;
				}
				double sum = ((Double) sum_).doubleValue();
				double a = value * count - sum * sum;
				if (a < 0) {
					continue;
				}
				value = Math.sqrt(a) / count;
			}
			double[] values = data.get(groupKey_);
			if (values == null) {
				values = new double[length];
				Arrays.fill(values, 0);
				data.put(groupKey_, values);
			}
			values[index] = value;
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