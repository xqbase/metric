package com.xqbase.metric;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FilePath;
import org.json.JSONObject;

import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Strings;
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

	private static Field fileField, fileNameField, readOnlyField, fileSizeField;

	private static Field getField(String name)
			throws ReflectiveOperationException {
		Field field = FileStore.class.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}

	static {
		try {
			fileField = getField("file");
			fileNameField = getField("fileName");
			readOnlyField = getField("readOnly");
			fileSizeField = getField("fileSize");
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	private static double __(String s) {
		double d = Numbers.parseDouble(s);
		return Double.isFinite(d) ? d : 0;
	}

	private static MVStore open() {
		FileStore fs = new FileStore();
		String fileName = Conf.load("Dashboard").getProperty("data_file",
				Conf.getAbsolutePath("data/metric.mv"));
		try {
			FileChannel fc = FilePath.get(fileName).open("r");
			fileField.set(fs, fc);
			fileNameField.set(fs, fileName);
			fileSizeField.set(fs, Long.valueOf(fc.size()));
			readOnlyField.set(fs, Boolean.TRUE);
		} catch (IOException | ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
		return new MVStore.Builder().fileStore(fs).cacheSize(0).open();
	}

	private int maxTagValues = 0;

	@Override
	public void init() throws ServletException {
		maxTagValues = Numbers.parseInt(Conf.
				load("Dashboard").getProperty("max_tag_values"));
	}

	private static Map<String, ToDoubleFunction<MetricValue>>
			methodMap = new HashMap<>();
	private static final ToDoubleFunction<MetricValue> NAMES_METHOD = value -> 0;
	private static final ToDoubleFunction<MetricValue> TAGS_METHOD = value -> 1;

	static {
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
			Set<String> names = new TreeSet<>();
			try (MVStore mv = open()) {
				for (String name : mv.getMapNames()) {
					if (!(name.startsWith("_tags_quarter.") || name.startsWith("_meta."))) {
						names.add(name.startsWith("_quarter.") ? name.substring(9) : name);
					}
				}
			}
			outputJson(req, resp, names);
			return;
		}
		String metricName = path.substring(0, slash);
		if (method == TAGS_METHOD) {
			String s;
			try (MVStore mv = open()) {
				s = mv.<String, String>openMap("_meta.tags").getOrDefault(metricName, "{}");
			}
			outputJson(req, resp, s);
			return;
		}

		boolean quarter = metricName.startsWith("_quarter.");
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
		try (MVStore mv = open()) {
			MVMap<Number, String> metricTable = mv.openMap(metricName);
			Iterator<Number> it;
			long to;
			if (quarter) {
				it = metricTable.keyIterator(Integer.valueOf(begin));
				to = end;
			} else {
				it = metricTable.keyIterator(Long.valueOf((long) begin << 32));
				to = ((long) (end + 1) << 32) - 1;
			}
			while (it.hasNext()) {
				Number time = it.next();
				if (time.longValue() > to) {
					break;
				}
				int index = ((quarter ? time.intValue() :
						(int) (time.longValue() >> 32)) - begin) / interval;
				if (index < 0 || index >= length) {
					Log.w("Key " + time + " out of range, end = " + end +
							", interval = " + interval + ", length = " + length);
					return;
				}
				String s = metricTable.get(time);
				if (s == null) {
					Log.w("Unable to get key " + time + " from table " + metricName);
					continue;
				}
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
			}
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