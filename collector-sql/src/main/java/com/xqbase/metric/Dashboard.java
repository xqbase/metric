package com.xqbase.metric;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.zip.GZIPOutputStream;

import org.json.JSONArray;
import org.json.JSONObject;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.metric.util.Kryos;
import com.xqbase.util.ByteArrayQueue;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Streams;
import com.xqbase.util.Strings;
import com.xqbase.util.Time;
import com.xqbase.util.db.ConnectionPool;
import com.xqbase.util.db.Row;

class Resource {
	String mime;
	byte[] body;
}

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

public class Dashboard {
	private static final String[] RESOURCES = {
		"css/bootstrap.min.css",
		"css/datepicker.css",
		"js/bootstrap-datepicker.js",
		"js/bootstrap.min.js",
		"js/highcharts.js",
		"js/jquery.min.js",
		"all.html",
		"all.js",
		// "config.js",
		"dashboard.html",
		"dashboard.js",
		"index.css",
		"index.html",
		"index.js",
	};
	private static final String QUERY_TAGS = "SELECT tags FROM metric_name WHERE name = ?";
	private static final String QUERY_ID = "SELECT id FROM metric_name WHERE name = ?";
	private static final String AGGREGATE_MINUTE =
			"SELECT time, _count, _sum, _max, _min, _sqr, tags " +
			"FROM metric_minute WHERE id = ? AND time >= ? AND time <= ?";
	private static final String AGGREGATE_QUARTER =
			"SELECT time, _count, _sum, _max, _min, _sqr, tags " +
			"FROM metric_quarter WHERE id = ? AND time >= ? AND time <= ?";

	private static ThreadLocal<SimpleDateFormat> format =
			new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			// https://stackoverflow.com/a/8642463/4260959
			SimpleDateFormat format_ = new SimpleDateFormat(
		    		"EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
			format_.setTimeZone(TimeZone.getTimeZone("GMT"));
			return format_;
		}
	};

	private static HashMap<String, ToDoubleFunction<MetricValue>>
			methodMap = new HashMap<>();
	private static final ToDoubleFunction<MetricValue> TAGS_METHOD = value -> 0;

	private static ConnectionPool db;
	private static HttpServer server;
	private static Resource config;
	private static HashMap<String, Resource> resources = new HashMap<>();
	private static String resourcesModified;
	private static long configModified = 0;
	private static int maxTagValues;

	static {
		methodMap.put("count", MetricValue::getCount);
		methodMap.put("sum", MetricValue::getSum);
		methodMap.put("max", MetricValue::getMax);
		methodMap.put("min", MetricValue::getMin);
		methodMap.put("avg", MetricValue::getAvg);
		methodMap.put("std", MetricValue::getStd);
		methodMap.put("tags", TAGS_METHOD);
	}

	private static void error400(HttpExchange exchange) {
		try {
			exchange.sendResponseHeaders(400, -1);
		} catch (IOException e_) {/**/}
	}

	private static void error500(HttpExchange exchange, Throwable e) {
		Log.e(e);
		try {
			exchange.sendResponseHeaders(500, -1);
		} catch (IOException e_) {/**/}
	}

	private static void copyHeader(HttpExchange exchange,
			String reqHeader, String respHeader) {
		String value = exchange.getRequestHeaders().getFirst(reqHeader);
		if (value != null) {
			exchange.getResponseHeaders().set(respHeader, value);
		}
	}

	private static void outputJson(HttpExchange exchange, Object data) {
		exchange.getRequestHeaders().getFirst("UTF-8");
		ByteArrayQueue body = new ByteArrayQueue();
		// String callback = exchange.getParameter("_callback");
		String callback = null;
		Headers headers = exchange.getResponseHeaders();
		String out;
		if (callback == null) {
			copyHeader(exchange, "Origin", "Access-Control-Allow-Origin");
			copyHeader(exchange, "Access-Control-Request-Methods",
					"Access-Control-Allow-Methods");
			copyHeader(exchange, "Access-Control-Request-Headers",
					"Access-Control-Allow-Headers");
			headers.set("Access-Control-Allow-Credentials", "true");
			headers.set("Content-Type", "application/json; charset=utf-8");
			out = JSONObject.wrap(data).toString();
		} else {
			headers.set("Content-Type", "text/javascript; charset=utf-8");
			out = callback + "(" + JSONObject.wrap(data) + ");";
		}
		body.add(out.getBytes(StandardCharsets.UTF_8));
		try {
			exchange.sendResponseHeaders(200, body.length());
			exchange.getResponseBody().write(body.getBytes());
		} catch (IOException e) {
			Log.w(e.getMessage());
		}
	}

	private static void doService(HttpExchange exchange) {
		URI uri = exchange.getRequestURI();
		String path = uri.getPath();
		if (path == null) {
			error400(exchange);
			return;
		}

		Resource resource;
		String lastModified;
		if (path.equals("/config.js")) {
			synchronized (config) {
				long now = System.currentTimeMillis();
				if (now > configModified + Time.MINUTE) {
					configModified = now;
					ByteArrayQueue baq = new ByteArrayQueue();
					try (
						FileInputStream in = new FileInputStream(Conf.
								getAbsolutePath("../webapp/config.js"));
						GZIPOutputStream out = new GZIPOutputStream(baq.
								getOutputStream());
					) {
						Streams.copy(in, out);
					} catch (IOException e) {
						if (!(e instanceof FileNotFoundException)) {
							Log.w(e.getMessage());
						}
						return;
					}
					config.body = baq.getBytes();
				}
				resource = config;
				lastModified = format.get().format(new Date(now));
			}
		} else {
			resource = resources.get(path);
			lastModified = resourcesModified;
		}

		if (resource != null) {
			Headers headers = exchange.getResponseHeaders();
			headers.set("Content-Encoding", "gzip");
			headers.set("Content-Type", resource.mime);
			headers.set("Last-Modified", lastModified);
			try {
				exchange.sendResponseHeaders(200, resource.body.length);
				exchange.getResponseBody().write(resource.body);
			} catch (IOException e) {
				Log.w(e.getMessage());
			}
			return;
		}

		while (!path.isEmpty() && path.charAt(0) == '/') {
			path = path.substring(1);
		}
		int slash = path.indexOf('/');
		if (slash < 0) {
			error400(exchange);
			return;
		}
		ToDoubleFunction<MetricValue> method =
				methodMap.get(path.substring(slash + 1));
		if (method == null) {
			error400(exchange);
			return;
		}
		String metricName = path.substring(0, slash);
		if (method == TAGS_METHOD) {
			Row row;
			try {
				row = db.queryEx(QUERY_TAGS, metricName);
			} catch (SQLException e) {
				error500(exchange, e);
				return;
			}
			if (row == null) {
				outputJson(exchange, Collections.emptyMap());
				return;
			}
			byte[] b = row.getBytes("tags");
			if (b == null) {
				outputJson(exchange, Collections.emptyMap());
				return;
			}
			@SuppressWarnings("unchecked")
			HashMap<String, HashMap<String, MetricValue>> tags =
					Kryos.deserialize(b, HashMap.class);
			if (tags == null) {
				outputJson(exchange, Collections.emptyMap());
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
			outputJson(exchange, json);
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
				outputJson(exchange, Collections.emptyMap());
				return;
			}
			id = row.getInt("id");
		} catch (SQLException e) {
			error500(exchange, e);
			return;
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
		// Query Time Range by SQL, Query and Group Tags by Java
		HashMap<GroupKey, MetricValue> result = new HashMap<>();
		try {
			db.query(row -> {
				int index = (row.getInt("time") - begin) / interval;
				if (index < 0 || index >= length) {
					return;
				}
				@SuppressWarnings("unchecked")
				HashMap<String, String> tags = Kryos.
						deserialize(row.getBytes("tags"), HashMap.class);
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
				MetricValue newValue = new MetricValue(row.getLong("_count"),
						((Number) row.get("_sum")).doubleValue(),
						((Number) row.get("_max")).doubleValue(),
						((Number) row.get("_min")).doubleValue(),
						((Number) row.get("_sqr")).doubleValue());
				MetricValue value = result.get(key);
				if (value == null) {
					result.put(key, newValue);
				} else {
					value.add(newValue);
				}
			}, quarter ? AGGREGATE_QUARTER : AGGREGATE_MINUTE, id, begin, end);
		} catch (SQLException e) {
			error500(exchange, e);
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
			outputJson(exchange, CollectionsEx.toMap(CollectionsEx.max(data.entrySet(),
					Comparator.comparingDouble(entry ->
					DoubleStream.of((double[]) entry.getValue()).sum()),
					maxTagValues)));
		} else {
			outputJson(exchange, data);
		}
	}

	public static void startup(ConnectionPool db_) {
		db = db_;
		Properties p = Conf.load("Dashboard");
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		String host = p.getProperty("host");
		host = host == null || host.isEmpty() ? "0.0.0.0" : host;
		if (port <= 0) {
			server = null;
			return;
		}
		
		try {
			server = HttpServer.create(new InetSocketAddress(host, port), 50);
			server.createContext("/", Dashboard::doService);
			server.start();
			Log.i("Metric Dashboard Started on " + host + ":" + port);
		} catch (IOException e) {
			Log.w("Unable to start HttpServer (" +
					host + ":" + port + "): " + e.getMessage());
			server = null;
			return;
		}

		maxTagValues = Numbers.parseInt(p.getProperty("max_tag_values"));
		for (String file : RESOURCES) {
			String path = "/webapps/" + file;
			ByteArrayQueue baq = new ByteArrayQueue();
			try (
				InputStream in = Dashboard.class.getResourceAsStream(path);
				GZIPOutputStream out = new GZIPOutputStream(baq.getOutputStream());
			) {
				Streams.copy(in, out);
			} catch (IOException e) {
				Log.w(e.getMessage());
				continue;
			}
			Resource resource = new Resource();
			resource.mime = path.endsWith(".css") ? "text/css" :
					path.endsWith(".html") ? "text/html" :
					path.endsWith(".js") ? "application/javascript" :
					"application/octet-stream";
			resources.put(path, resource);
		}
		resourcesModified = format.get().format(new Date());
	}

	public static void shutdown() {
		if (server != null) {
			server.stop(0);
		}
	}
}