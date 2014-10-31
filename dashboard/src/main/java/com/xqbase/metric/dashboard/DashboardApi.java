package com.xqbase.metric.dashboard;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

	private static HashMap<String, Collector<DBObject, ?, double[]>> groupMap = new HashMap<>();

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

	private static <R> BinaryOperator<R> combiner(BiConsumer<R, R> consumer) {
		return (result, another) -> {
			consumer.accept(result, another);
			return result;
		};
	}

	static {
		groupMap.put("count", Collector.of(() -> new double[] {0},
				(result, row) -> result[0] += getDouble(row, "_count"),
				combiner((result, another) -> result[0] += another[0])));
		groupMap.put("sum", Collector.of(() -> new double[] {0},
				(result, row) -> result[0] += getDouble(row, "_sum"),
				combiner((result, another) -> result[0] += another[0])));
		groupMap.put("max", Collector.<DBObject, double[], double[]>
				of(() -> new double[] {Double.NEGATIVE_INFINITY},
				(result, row) -> result[0] = Math.max(result[0], getDouble(row, "_max")),
				combiner((result, another) -> result[0] = Math.max(result[0], another[0])),
				result -> result[0] == Double.NEGATIVE_INFINITY ? new double[] {0} : result));
		groupMap.put("min", Collector.<DBObject, double[], double[]>
				of(() -> new double[] {Double.POSITIVE_INFINITY},
				(result, row) -> result[0] = Math.min(result[0], getDouble(row, "_min")),
				combiner((result, another) -> result[0] = Math.min(result[0], another[0])),
				result -> result[0] == Double.POSITIVE_INFINITY ? new double[] {0} : result));
		groupMap.put("avg", Collector.<DBObject, double[], double[]>
				of(() -> new double[] {0, 0},
				(result, row) -> {
					result[0] += getDouble(row, "_sum");
					result[1] += getDouble(row, "_count");
				}, combiner((result, another) -> {
					result[0] += another[0];
					result[1] += another[1];
				}), result -> new double[] {result[1] == 0 ? 0 : result[0] / result[1]}));
		groupMap.put("std", Collector.<DBObject, double[], double[]>
				of(() -> new double[] {0, 0, 0},
				(result, row) -> {
					result[0] += getDouble(row, "_sqr");
					result[1] += getDouble(row, "_sum");
					result[2] += getDouble(row, "_count");
				}, combiner((result, another) -> {
					result[0] += another[0];
					result[1] += another[1];
					result[2] += another[2];
				}), result -> {
					double count = result[2];
					if (count == 0) {
						return new double[] {0};
					}
					double a = result[0] * count - result[1] * result[1];
					return new double[] {a < 0 ? 0 : Math.sqrt(a) / count};
				}));
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
		Collector<DBObject, ?, double[]> downstream = groupMap.get(method);
		if (downstream == null) {
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
		String groupBy = req.getParameter("_group_by");
		Function<DBObject, GroupKey> classifier;
		if (groupBy == null) {
			classifier = row -> new GroupKey("_",
					(getInt(row, "_minute") - begin) / interval);
		} else {
			classifier = row -> new GroupKey(getString(row, groupBy),
					(getInt(row, "_minute") - begin) / interval);
		}
		// Query by MongoDB and Group by Java
		Map<GroupKey, double[]> result;
		try {
			result = StreamSupport.stream(Spliterators.
					spliteratorUnknownSize(collection.find(query).iterator(), 0), false).
					collect(Collectors.groupingBy(classifier, downstream));
		} catch (MongoException e) {
			Log.e(e);
			try {
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			} catch (IOException e_) {/**/}
			return;
		}
		HashMap<String, double[]> data = new HashMap<>();
		for (Map.Entry<GroupKey, double[]> entry : result.entrySet()) {
			GroupKey key = entry.getKey();
			if (key.index < 0 || key.index >= length) {
				continue;
			}
			double[] values = data.get(key.tag);
			if (values == null) {
				values = new double[length];
				Arrays.fill(values, 0);
				data.put(key.tag, values);
			}
			values[key.index] = entry.getValue()[0];
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