package com.xqbase.metric.sleepycat;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.logging.Logger;
import java.util.stream.DoubleStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.xqbase.metric.client.MetricClient;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.sleepycat.model.AllTags;
import com.xqbase.metric.sleepycat.model.Row;
import com.xqbase.metric.sleepycat.model.TagValue;
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

	private static AtomicInteger count = new AtomicInteger(0);
	private static int maxTagValues;
	private static Logger logger;
	private static HashMap<String, EntityStore> tableMap;
	private static Collector collector;
	private static Thread thread;

	@Override
	public void init() throws ServletException {
		if (count.getAndIncrement() > 0) {
			return;
		}

		logger = Log.getAndSet(Conf.openLogger("Dashboard.", 16777216, 10));
		Properties p = Conf.load("Dashboard");
		ArrayList<InetSocketAddress> addrs = new ArrayList<>();
		String addresses = p.getProperty("metric.collectors", "");
		for (String s : addresses.split("[,;]")) {
			String[] ss = s.split("[:/]");
			if (ss.length > 1) {
				addrs.add(new InetSocketAddress(ss[0],
						Numbers.parseInt(ss[1], 5514, 0, 65535)));
			}
		}
		MetricClient.startup(addrs.toArray(new InetSocketAddress[0]));
		maxTagValues = Numbers.parseInt(p.getProperty("max_tag_values"));

		tableMap = new HashMap<>();
		collector = new Collector();
		thread = new Thread(collector);
		thread.start();
	}

	@Override
	public void destroy() {
		if (count.decrementAndGet() > 0) {
			return;
		}

		for (EntityStore table : tableMap.values()) {
			table.close();
		}
		collector.getInterrupted().set(true);
		if (collector.getSocket() != null) {
			collector.getSocket().close();
		}
		try {
			thread.join();
		} catch (InterruptedException e) {/**/}
		MetricClient.shutdown();
		Conf.closeLogger(Log.getAndSet(logger));
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

	private static String getString(Row row, String key) {
		String value = row.tags.get(key);
		return Strings.isEmpty(value) ? "_" : value;
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
			AllTags allTags = collector.getStore("_meta.tags_all").
					getPrimaryIndex(String.class, AllTags.class).get(metricName);
			if (allTags == null) {
				outputJson(req, resp, Collections.emptyMap());
				return;
			}
			Iterator<String> it = allTags.tags.keySet().iterator();
			JSONObject json = new JSONObject();
			while (it.hasNext()) {
				String tag = it.next();
				if (tag.isEmpty() || tag.charAt(0) == '_') {
					it.remove();
					continue;
				}
				if (maxTagValues <= 0) {
					continue;
				}
				Collection<TagValue> tagValues = allTags.tags.get(tag);
				if (tagValues.size() > maxTagValues) {
					tagValues = CollectionsEx.max(tagValues,
							Comparator.comparingLong(o -> o.count), maxTagValues);
				}
				JSONArray arr = new JSONArray();
				for (TagValue tagValue : tagValues) {
					JSONObject obj = new JSONObject();
					obj.put("_value", tagValue.value);
					obj.put("_count", tagValue.count);
					obj.put("_sum", tagValue.sum);
					obj.put("_max", tagValue.max);
					obj.put("_min", tagValue.min);
					obj.put("_sqr", tagValue.sqr);
					arr.put(obj);
				}
				json.put(tag, arr);
			}
			outputJson(req, resp, json);
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
				(metricName.startsWith("_quarter.") ? Time.MINUTE / 15 : Time.MINUTE)));
		int interval = Numbers.parseInt(req.getParameter("_interval"), 1, 1440);
		int length = Numbers.parseInt(req.getParameter("_length"), 1, 1024);
		int begin = end - interval * length + 1;
		
		String groupBy_ = req.getParameter("_group_by");
		Function<Row, String> groupBy = groupBy_ == null ?
				row -> "_" : row -> getString(row, groupBy_);
		// Query Time Range by Sleepycat, Query and Group Tags by Java
		HashMap<GroupKey, MetricValue> result = new HashMap<>();
		EntityStore store = collector.getStore(metricName);
		PrimaryIndex<Long, Row> pk =
				store.getPrimaryIndex(Long.class, Row.class);
		SecondaryIndex<Integer, Long, Row> sk =
				store.getSecondaryIndex(pk, Integer.class, "time");
		try (EntityCursor<Row> rows = sk.entities(Integer.valueOf(begin),
				true, Integer.valueOf(end), true)) {
			for (Row row : rows) {
				int index = (row.time - begin) / interval;
				if (index < 0 || index >= length) {
					continue;
				}
				// Query Tags
				boolean skip = false;
				for (Map.Entry<String, String> entry : query.entrySet()) {
					String value = row.tags.get(entry.getKey());
					if (!entry.getValue().equals(value)) {
						skip = true;
						break;
					}
				}
				if (skip) {
					continue;
				}
				// Group Tags
				GroupKey key = new GroupKey(groupBy.apply(row), index);
				MetricValue newValue = new MetricValue(row.count,
						row.sum, row.max, row.min, row.sqr);
				MetricValue value = result.get(key);
				if (value == null) {
					result.put(key, newValue);
				} else {
					value.add(newValue);
				}
			}
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