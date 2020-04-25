package com.xqbase.metric;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.zip.GZIPOutputStream;

import org.h2.mvstore.MVStore;
import org.json.JSONObject;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.util.ByteArrayQueue;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Strings;
import com.xqbase.util.Time;

class Resource {
	String mime;
	byte[] body, gzip;
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
		GroupKey key = (GroupKey) obj;
		return index == key.index && tag.equals(key.tag);
	}

	@Override
	public int hashCode() {
		return tag.hashCode() * 31 + index;
	}
}

public class Dashboard {
	private static final String[] RESOURCES = {
		"/css/bootstrap.min.css",
		"/css/datepicker.css",
		"/js/bootstrap-datepicker.js",
		"/js/bootstrap.min.js",
		"/js/highcharts.js",
		"/js/jquery.min.js",
		"/all.html",
		"/all.js",
		"/config.js",
		"/dashboard.html",
		"/dashboard.js",
		"/index.css",
		"/index.html",
		"/index.js",
	};

	private static ThreadLocal<SimpleDateFormat> format =
			ThreadLocal.withInitial(() -> {
		// https://stackoverflow.com/a/8642463/4260959
		SimpleDateFormat format_ = new SimpleDateFormat(
				"EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
		format_.setTimeZone(TimeZone.getTimeZone("GMT"));
		return format_;
	});

	private static Map<String, ToDoubleFunction<MetricValue>>
			methodMap = new HashMap<>();
	private static final ToDoubleFunction<MetricValue> TAGS_METHOD = value -> 0;

	private static MVStore mv;
	private static HttpServer server;
	private static Map<String, Resource> resources = new HashMap<>();
	private static String resourcesModified;
	private static Resource config;
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

	private static void response(HttpExchange exchange, int status) {
		try {
			exchange.sendResponseHeaders(status, -1);
		} catch (IOException e_) {/**/}
		exchange.close();
	}

	private static void copyHeader(HttpExchange exchange,
			String reqHeader, String respHeader) {
		String value = exchange.getRequestHeaders().getFirst(reqHeader);
		if (value != null) {
			exchange.getResponseHeaders().set(respHeader, value);
		}
	}

	private static void response(HttpExchange exchange,
			Object data, boolean acceptGzip) {
		Headers headers = exchange.getResponseHeaders();
		String out = (data instanceof String ?
				(String) data : JSONObject.valueToString(data));
		String callback = getParameters(exchange.getRequestURI()).get("_callback");
		if (callback == null) {
			copyHeader(exchange, "Origin", "Access-Control-Allow-Origin");
			copyHeader(exchange, "Access-Control-Request-Methods",
					"Access-Control-Allow-Methods");
			copyHeader(exchange, "Access-Control-Request-Headers",
					"Access-Control-Allow-Headers");
			headers.set("Access-Control-Allow-Credentials", "true");
			headers.set("Content-Type", "application/json; charset=utf-8");
		} else {
			headers.set("Content-Type", "text/javascript; charset=utf-8");
			out = callback + "(" + out + ");";
		}
		byte[] body = out.getBytes(StandardCharsets.UTF_8);
		if (acceptGzip && body.length > 1024) {
			headers.set("Content-Encoding", "gzip");
			ByteArrayQueue gzipBody = new ByteArrayQueue();
			try (GZIPOutputStream gzip = new
					GZIPOutputStream(gzipBody.getOutputStream())) {
				gzip.write(body);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			try {
				exchange.sendResponseHeaders(200, gzipBody.length());
				gzipBody.writeTo(exchange.getResponseBody());
			} catch (IOException e) {/**/}
		} else {
			try {
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
			} catch (IOException e) {/**/}
		}
		exchange.close();
	}

	private static Map<String, String> getParameters(URI uri) {
		Map<String, String> parameters = new HashMap<>();
		String query = uri.getRawQuery();
		if (query == null) {
			return parameters;
		}
		for (String pair : query.split("&")) {
			int eq = pair.indexOf('=');
			if (eq >= 0) {
				parameters.put(Strings.decodeUrl(pair.substring(0, eq)),
						Strings.decodeUrl(pair.substring(eq + 1)));
			}
		}
		return parameters;
	}

	private static void doService(HttpExchange exchange) {
		URI uri = exchange.getRequestURI();
		String path = uri.getPath();
		if (path == null) {
			response(exchange, 400);
			return;
		}
		if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
			response(exchange, 405);
			return;
		}

		boolean acceptGzip = false;
		String encodings = exchange.getRequestHeaders().getFirst("Accept-Encoding");
		if (encodings != null) {
			for (String encoding : encodings.split(",")) {
				if (encoding.trim().toLowerCase().equals("gzip")) {
					acceptGzip = true;
					break;
				}
			}
		}

		String lastModified = resourcesModified;
		Resource resource;
		if (path.equals("/config.js")) {
			synchronized (Dashboard.class) {
				long now = System.currentTimeMillis();
				if (now > configModified + Time.SECOND * 10) {
					configModified = now;
					config = new Resource();
					config.mime = "application/javascript";
					ByteArrayQueue body = new ByteArrayQueue();
					ByteArrayQueue gzip = new ByteArrayQueue();
					try (FileInputStream in = new FileInputStream(Conf.
							getAbsolutePath("webapp/config.js"))) {
						body.readFrom(in);
						config.body = body.getBytes();
						try (GZIPOutputStream out =
								new GZIPOutputStream(gzip.getOutputStream())) {
							body.writeTo(out);
						}
						config.gzip = gzip.getBytes();
						lastModified = format.get().format(new Date(now));
					} catch (IOException e) {
						if (!(e instanceof FileNotFoundException)) {
							Log.w(e.getMessage());
						}
						config = resources.get(path);
					}
				}
			}
			resource = config;
		} else {
			resource = resources.get(path);
		}

		if (resource != null) {
			Headers headers = exchange.getResponseHeaders();
			byte[] body;
			if (acceptGzip) {
				headers.set("Content-Encoding", "gzip");
				body = resource.gzip;
			} else {
				body = resource.body;
			}
			headers.set("Content-Type", resource.mime);
			headers.set("Last-Modified", lastModified);
			try {
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
			} catch (IOException e) {/**/}
			exchange.close();
			return;
		}

		if (!path.startsWith("/api/")) {
			response(exchange, 404);
			return;
		}
		path = path.substring(5);
		int slash = path.indexOf('/');
		if (slash < 0) {
			response(exchange, 400);
			return;
		}
		ToDoubleFunction<MetricValue> method =
				methodMap.get(path.substring(slash + 1));
		if (method == null) {
			response(exchange, 400);
			return;
		}
		String metricName = path.substring(0, slash);
		if (method == TAGS_METHOD) {
			response(exchange, mv.<String, String>openMap("_meta.tags").
					getOrDefault(metricName, "{}"), acceptGzip);
			return;
		}

		boolean quarter = metricName.startsWith("_quarter.");
		// Query Condition
		Map<String, String> query = getParameters(uri);
		String end_ = query.remove("_end");
		String interval_ = query.remove("_interval");
		String length_ = query.remove("_length");
		String groupBy_ = query.remove("_group_by");
		query.remove("_r");
		// Other Query Parameters
		int end = Numbers.parseInt(end_, (int) (System.currentTimeMillis() /
				(quarter ? Time.MINUTE / 15 : Time.MINUTE)));
		int interval = Numbers.parseInt(interval_, 1, 1440);
		int length = Numbers.parseInt(length_, 1, 1024);
		int begin = end - interval * length + 1;

		Function<Map<String, String>, String> groupBy = groupBy_ == null ?
				tags -> "_" : tags -> {
			String value = tags.get(groupBy_);
			return Strings.isEmpty(value) ? "_" : value;
		};
		// Query Time Range by SQL, Query and Group Tags by Java
		Map<GroupKey, MetricValue> result = new HashMap<>();
		Map<Integer, byte[]> metricTable = mv.openMap(metricName);
		for (int time = begin; time <= end; time ++) {
			int index = (time - begin) / interval;
			if (time < begin || index >= length) {
				continue;
			}
			byte[] b = metricTable.get(Integer.valueOf(time));
			if (b == null) {
				continue;
			}
			Map<Map<String, String>, MetricValue> metricMap =
					Codecs.deserialize(b, METRIC_TYPE);
			metricMap.forEach((tags, newValue) -> {
				// Query Tags
				for (Map.Entry<String, String> entry : query.entrySet()) {
					String tagValue = tags.get(entry.getKey());
					if (!entry.getValue().equals(tagValue)) {
						return;
					}
				}
				// Group Tags
				GroupKey key = new GroupKey(groupBy.apply(tags), index);
				MetricValue value = result.get(key);
				if (value == null) {
					result.put(key, newValue);
				} else {
					value.add(newValue);
				}
			});
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
			response(exchange, CollectionsEx.toMap(CollectionsEx.max(data.entrySet(),
					Comparator.comparingDouble(entry ->
					DoubleStream.of((double[]) entry.getValue()).sum()),
					maxTagValues)), acceptGzip);
		} else {
			response(exchange, data, acceptGzip);
		}
	}

	public static void startup(MVStore mv_) {
		mv = mv_;

		Properties p = Conf.load("Dashboard");
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		String host = p.getProperty("host");
		host = host == null || host.isEmpty() ? "0.0.0.0" : host;
		if (port <= 0) {
			server = null;
			return;
		}
		
		for (String path : RESOURCES) {
			Resource resource = new Resource();
			resource.mime = path.endsWith(".css") ? "text/css" :
					path.endsWith(".html") ? "text/html" :
					path.endsWith(".js") ? "application/javascript" :
					"application/octet-stream";
			ByteArrayQueue body = new ByteArrayQueue();
			ByteArrayQueue gzip = new ByteArrayQueue();
			try (InputStream in = Dashboard.class.
					getResourceAsStream("/webapp" + path)) {
				body.readFrom(in);
				resource.body = body.getBytes();
				try (GZIPOutputStream out = new
						GZIPOutputStream(gzip.getOutputStream())) {
					body.writeTo(out);
				}
				resource.gzip = gzip.getBytes();
				resources.put(path, resource);
			} catch (IOException e) {
				Log.w(e.getMessage());
			}
		}
		Resource index = resources.get("/index.html");
		if (index != null) {
			resources.put("/", index);
		}
		resourcesModified = format.get().format(new Date());
		maxTagValues = Numbers.parseInt(p.getProperty("max_tag_values"));

		try {
			server = HttpServer.create(new InetSocketAddress(host, port), 50);
			server.createContext("/", exchange -> {
				try {
					doService(exchange);
				} catch (Error | RuntimeException e) {
					Log.e(e);
					response(exchange, 500);
				}
			});
			server.start();
		} catch (IOException e) {
			Log.w("Unable to start HttpServer (" +
					host + ":" + port + "): " + e.getMessage());
			server = null;
			return;
		}

		Log.i("Metric Dashboard Started on " + host + ":" + port);
	}

	public static void shutdown() {
		if (server != null) {
			server.stop(0);
		}
	}
}