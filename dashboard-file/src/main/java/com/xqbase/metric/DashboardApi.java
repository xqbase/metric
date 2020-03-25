package com.xqbase.metric;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.zip.GZIPInputStream;

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

	private int maxTagValues = 0;
	private String dataDir = null;

	@Override
	public void init() throws ServletException {
		Properties p = Conf.load("Dashboard");
		maxTagValues = Numbers.parseInt(p.getProperty("max_tag_values"));
		dataDir = p.getProperty("data_dir", Conf.getAbsolutePath("data"));
		if (!dataDir.endsWith(File.separator) && !dataDir.endsWith("\\")) {
			dataDir = dataDir + "/";
		}
	}

	private static double __(String s) {
		double d = Numbers.parseDouble(s);
		return Double.isFinite(d) ? d : 0;
	}

	private static Map<String, ToDoubleFunction<MetricValue>>
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
		String metricName = path.substring(0, slash);
		if (method == TAGS_METHOD) {
			File file = new File(dataDir + "Tags.properties");
			if (!file.exists()) {
				outputJson(req, resp, Collections.emptyMap());
				return;
			}
			Properties p = new Properties();
			try (FileInputStream in = new FileInputStream(file)) {
				p.load(in);
			} catch (IOException e) {
				Log.e(e);
			}
			String json = p.getProperty(metricName);
			if (json == null) {
				outputJson(req, resp, Collections.emptyMap());
				return;
			}
			outputJson(req, resp, json);
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
		String[] filenames = new File(dataDir + metricName).list();
		if (filenames == null) {
			outputJson(req, resp, Collections.emptyMap());
			return;
		}
		for (String filename : filenames) {
			boolean gzip = filename.endsWith(".gz");
			int time = Numbers.parseInt(gzip ?
					filename.substring(0, filename.length() - 3) : filename);
			int index = (time - begin) / interval;
			if (time < begin || index >= length) {
				continue;
			}
			File file = new File(dataDir + metricName + "/" + filename);
			if (!file.exists()) {
				continue;
			}
			try (
				FileInputStream fis = new FileInputStream(file);
				BufferedReader in = new BufferedReader(new
						InputStreamReader(gzip ? new GZIPInputStream(fis) : fis));
			) {
				String line;
				while ((line = in.readLine()) != null) {
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
			} catch (IOException e) {
				Log.e(e);
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