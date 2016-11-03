package com.xqbase.metric.collector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.zip.InflaterInputStream;

import com.xqbase.metric.common.Metric;
import com.xqbase.metric.common.MetricEntry;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.metric.util.Kryos;
import com.xqbase.util.ByteArrayQueue;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Runnables;
import com.xqbase.util.Service;
import com.xqbase.util.Strings;
import com.xqbase.util.Time;
import com.xqbase.util.db.ConnectionPool;
import com.xqbase.util.db.Row;

class MetricRow {
	int time;
	long count;
	double sum, max, min, sqr;
	HashMap<String, String> tags;
}

class Name {
	int id, minuteSize, quarterSize, aggregatedTime;
	String name;
}

public class Collector {
	private static final int MAX_BUFFER_SIZE = 64000;
	private static final int MAX_METRIC_LEN = 64;

	private static final String QUERY_ID = "SELECT id FROM metric_name WHERE name = ?";
	private static final String CREATE_ID = "INSERT INTO metric_name (name) VALUES (?)";
	private static final String QUERY_NAME =
			"SELECT id, name, minute_size, quarter_size, aggregated_time FROM metric_name";
	private static final String INSERT_MINUTE = "INSERT INTO metric_minute " +
			"(id, time, _count, _sum, _max, _min, _sqr, tags) VALUES ";
	private static final String INSERT_QUARTER = "INSERT INTO metric_quarter " +
			"(id, time, _count, _sum, _max, _min, _sqr, tags) VALUES ";
	private static final String INCREMENT_MINUTE =
			"UPDATE metric_name SET minute_size = minute_size + ? WHERE id = ?";
	private static final String INCREMENT_QUARTER =
			"UPDATE metric_name SET quarter_size = quarter_size + ? WHERE id = ?";

	private static final String DELETE_TAGS_BY_ID =
			"DELETE FROM metric_tags_quarter WHERE id = ?";
	private static final String DELETE_TAGS_BY_TIME =
			"DELETE FROM metric_tags_quarter WHERE time <= ?";
	private static final String DELETE_NAME = "DELETE FROM metric_name WHERE id = ?";

	private static final String DELETE_MINUTE =
			"DELETE FROM metric_minute WHERE id = ? AND time <= ?";
	private static final String DELETE_QUARTER =
			"DELETE FROM metric_quarter WHERE id = ? AND time <= ?";
	private static final String AGGREGATE_FROM =
			"SELECT time, _count, _sum, _max, _min, _sqr, tags " +
			"FROM metric_minute WHERE id = ? AND time >= ? AND time <= ?";
	private static final String AGGREGATE_TO =
			"INSERT INTO metric_tags_quarter (id, time, tags) VALUES (?, ?, ?)";
	private static final String AGGREGATE_TAGS_FROM =
			"SELECT tags FROM metric_tags_quarter WHERE id = ?";
	private static final String UPDATE_NAME =
			"UPDATE metric_name SET minute_size = minute_size - ?, " +
			"quarter_size = quarter_size - ?, aggregated_time = ?, tags = ? WHERE id = ?";

	private static String decode(String s, int limit) {
		String result = Strings.decodeUrl(s);
		return limit > 0 ? Strings.truncate(result, limit) : result;
	}

	private static void put(HashMap<String, ArrayList<MetricRow>> rowsMap,
			String name, MetricRow row) {
		ArrayList<MetricRow> rows = rowsMap.get(name);
		if (rows == null) {
			rows = new ArrayList<>();
			rowsMap.put(name, rows);
		}
		rows.add(row);
	}

	private static void insert(String name, ArrayList<MetricRow> rows,
			boolean quarter) throws SQLException {
		if (rows.isEmpty()) {
			return;
		}
		int id;
		while (true) {
			Row row = DB.queryEx(QUERY_ID, name);
			if (row != null) {
				id = row.getInt(1);
				break;
			}
			long[] id_ = new long[1];
			if (DB.updateEx(id_, CREATE_ID, name) > 0) {
				id = (int) id_[0];
				break;
			}
		}
		Integer id_ = Integer.valueOf(id);
		ArrayList<Object> ins = new ArrayList<>();
		StringBuilder sb = new StringBuilder(quarter ? INSERT_QUARTER : INSERT_MINUTE);
		for (MetricRow row : rows) {
			sb.append("(?, ?, ?, ?, ?, ?, ?, ?), ");
			ins.add(id_);
			ins.add(Integer.valueOf(row.time));
			ins.add(Long.valueOf(row.count));
			ins.add(Double.valueOf(row.sum));
			ins.add(Double.valueOf(row.max));
			ins.add(Double.valueOf(row.min));
			ins.add(Double.valueOf(row.sqr));
			ins.add(Kryos.serialize(row.tags));
		}
		DB.updateEx(sb.substring(0, sb.length() - 2), ins.toArray());
		DB.update(quarter ? INCREMENT_QUARTER : INCREMENT_MINUTE, rows.size(), id);
	}

	private static void insert(HashMap<String, ArrayList<MetricRow>> rowsMap) throws SQLException {
		for (Map.Entry<String, ArrayList<MetricRow>> entry : rowsMap.entrySet()) {
			insert(entry.getKey(), entry.getValue(), false);
		}
	}

	private static Service service = new Service();
	private static ConnectionPool DB = null;
	private static int serverId, expire, tagsExpire, maxTags, maxTagValues,
			maxTagCombinations, maxTagNameLen, maxTagValueLen;
	private static boolean verbose;

	private static MetricRow row(HashMap<String, String> tagMap, int now,
			long count, double sum, double max, double min, double sqr) {
		MetricRow row = new MetricRow();
		if (maxTags > 0 && tagMap.size() > maxTags) {
			row.tags = new HashMap<>();
			CollectionsEx.forEach(CollectionsEx.min(tagMap.entrySet(),
					Comparator.comparing(Map.Entry::getKey), maxTags), row.tags::put);
		} else {
			row.tags = tagMap;
		}
		row.time = now;
		row.count = count;
		row.sum = sum;
		row.max = max;
		row.min = min;
		row.sqr = sqr;
		return row;
	}

	private static ArrayList<Name> getNames() throws SQLException {
		ArrayList<Name> names = new ArrayList<>();
		DB.query(row -> {
			Name name = new Name();
			name.id = row.getInt(1);
			name.name = row.getString(2);
			name.minuteSize = row.getInt(3);
			name.quarterSize = row.getInt(4);
			name.aggregatedTime = row.getInt(5);
		}, QUERY_NAME);
		return names;
	}

	private static void minutely(int minute) throws SQLException {
		// Insert aggregation-during-collection metrics
		HashMap<String, ArrayList<MetricRow>> rowsMap = new HashMap<>();
		for (MetricEntry entry : Metric.removeAll()) {
			MetricRow row = row(entry.getTagMap(), minute, entry.getCount(),
					entry.getSum(), entry.getMax(), entry.getMin(), entry.getSqr());
			put(rowsMap, entry.getName(), row);
		}
		if (!rowsMap.isEmpty()) {
			insert(rowsMap);
		}
		// Ensure index and calculate metric size by master collector
		if (serverId != 0) {
			return;
		}
		for (Name name : getNames()) {
			Metric.put("metric.size", name.minuteSize, "name", name.name);
			Metric.put("metric.size", name.quarterSize, "name", "_quarter." + name.name);
			if (name.minuteSize <= 0 && name.quarterSize <= 0) {
				DB.update(DELETE_TAGS_BY_ID, name.id);
				DB.update(DELETE_NAME, name.id);
			}
		}
	}

	private static void putTagValue(HashMap<String, HashMap<String, MetricValue>> tagMap,
			String tagKey, String tagValue, MetricValue value) {
		HashMap<String, MetricValue> tagValues = tagMap.get(tagKey);
		if (tagValues == null) {
			tagValues = new HashMap<>();
			tagMap.put(tagKey, tagValues);
			// Must use "value.clone()" here, because many tags may share one "value" 
			tagValues.put(tagValue, value.clone());
		} else {
			MetricValue oldValue = tagValues.get(tagValue);
			if (oldValue == null) {
				// Must use "value.clone()" here
				tagValues.put(tagValue, value.clone());
			} else {
				oldValue.add(value);
			}
		}
	}

	private static HashMap<String, HashMap<String, MetricValue>>
			limit(HashMap<String, HashMap<String, MetricValue>> tagMap) {
		HashMap<String, HashMap<String, MetricValue>> tags = new HashMap<>();
		BiConsumer<String, HashMap<String, MetricValue>> action = (tagName, valueMap) -> {
			HashMap<String, MetricValue> tagValues = new HashMap<>();
			if (maxTagValues > 0 && valueMap.size() > maxTagValues) {
				CollectionsEx.forEach(CollectionsEx.max(valueMap.entrySet(),
						Comparator.comparingLong(metricValue ->
						metricValue.getValue().getCount()), maxTagValues), tagValues::put);
			} else {
				tagValues.putAll(valueMap);
			}
			tags.put(tagName, tagValues);
		};
		if (maxTags > 0 && tagMap.size() > maxTags) {
			CollectionsEx.forEach(CollectionsEx.min(tagMap.entrySet(),
					Comparator.comparing(Map.Entry::getKey), maxTags), action);
		} else {
			tagMap.forEach(action);
		}
		return tags;
	}

	private static void quarterly(int quarter) throws SQLException {
		DB.update(DELETE_TAGS_BY_TIME, quarter - tagsExpire);

		for (Name name : getNames()) {
			int deletedMinute = DB.update(DELETE_MINUTE, name.id, quarter * 15 - expire);
			int deletedQuarter = DB.update(DELETE_QUARTER, name.id, quarter - expire);
			int start = name.aggregatedTime == 0 ? quarter - expire : name.aggregatedTime;
			for (int i = start + 1; i <= quarter; i ++) {
				ArrayList<MetricRow> rows = new ArrayList<>();
				HashMap<HashMap<String, String>, MetricValue> result = new HashMap<>();
				DB.query(row -> {
					@SuppressWarnings("unchecked")
					HashMap<String, String> tags =
							Kryos.deserialize(row.getBytes(7), HashMap.class);
					if (tags == null) {
						tags = new HashMap<>();
					}
					// Aggregate to "_quarter.*"
					MetricValue newValue = new MetricValue(row.getLong(2),
							((Number) row.get(3)).doubleValue(),
							((Number) row.get(4)).doubleValue(),
							((Number) row.get(5)).doubleValue(),
							((Number) row.get(6)).doubleValue());
					MetricValue value = result.get(tags);
					if (value == null) {
						result.put(tags, newValue);
					} else {
						value.add(newValue);
					}
				}, AGGREGATE_FROM, name.id, i * 15 - 14, i * 15);
				if (result.isEmpty()) {
					continue;
				}
				int combinations = result.size();
				Metric.put("metric.tags.combinations", combinations, "name", name.name);
				HashMap<String, HashMap<String, MetricValue>> tagMap = new HashMap<>();
				int i_ = i;
				BiConsumer<HashMap<String, String>, MetricValue> action = (tags, value) -> {
					// {"_quarter": i}, but not {"_quarter": quarter} !
					rows.add(row(tags, i_, value.getCount(), value.getSum(),
							value.getMax(), value.getMin(), value.getSqr()));
					// Aggregate to "_meta.tags_quarter"
					tags.forEach((tagKey, tagValue) ->
							putTagValue(tagMap, tagKey, tagValue, value));
				};
				if (maxTagCombinations > 0 && combinations > maxTagCombinations) {
					CollectionsEx.forEach(CollectionsEx.max(result.entrySet(),
							Comparator.comparingLong(entry -> entry.getValue().getCount()),
							maxTagCombinations), action);
				} else {
					result.forEach(action);
				}
				insert(name.name, rows, true);
				// Aggregate to "_meta.tags_quarter"
				tagMap.forEach((tagKey, tagValue) -> {
					Metric.put("metric.tags.values", tagValue.size(), "name", name.name, "key", tagKey);
				});
				// {"_quarter": i}, but not {"_quarter": quarter} !
				DB.updateEx(AGGREGATE_TO, Integer.valueOf(name.id),
						Integer.valueOf(i), Kryos.serialize(limit(tagMap)));
			}

			// Aggregate "_meta.tags_quarter" to "_meta.tags_all";
			HashMap<String, HashMap<String, MetricValue>> tagMap = new HashMap<>();
			DB.queryEx(row -> {
				@SuppressWarnings("unchecked")
				HashMap<String, HashMap<String, MetricValue>> tags =
						Kryos.deserialize(row.getBytes(1), HashMap.class);
				if (tags == null) {
					return;
				}
				tags.forEach((tagKey, tagValues) -> {
					tagValues.forEach((value, metric) -> {
						putTagValue(tagMap, tagKey, value, metric);
					});
				});
			}, AGGREGATE_TAGS_FROM, name);

			byte[] b = Kryos.serialize(limit(tagMap));
			DB.updateEx(UPDATE_NAME, Integer.valueOf(deletedMinute),
					Integer.valueOf(deletedQuarter), Integer.valueOf(quarter),
					b, Integer.valueOf(name.id));
		}
	}

	public static void main(String[] args) {
		if (!service.startup(args)) {
			return;
		}
		System.setProperty("java.util.logging.SimpleFormatter.format",
				"%1$tY-%1$tm-%1$td %1$tk:%1$tM:%1$tS.%1$tL %2$s%n%4$s: %5$s%6$s%n");
		Logger logger = Log.getAndSet(Conf.openLogger("Collector.", 16777216, 10));
		ExecutorService executor = Executors.newCachedThreadPool();
		ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);

		Properties p = Conf.load("Collector");
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		String host = p.getProperty("host");
		host = host == null || host.isEmpty() ? "0.0.0.0" : host;
		serverId = Numbers.parseInt(p.getProperty("server_id"), 0);
		expire = Numbers.parseInt(p.getProperty("expire"), 2880);
		tagsExpire = Numbers.parseInt(p.getProperty("tags_expire"), 96);
		maxTags = Numbers.parseInt(p.getProperty("max_tags"));
		maxTagValues = Numbers.parseInt(p.getProperty("max_tag_values"));
		maxTagCombinations = Numbers.parseInt(p.getProperty("max_tag_combinations"));
		maxTagNameLen = Numbers.parseInt(p.getProperty("max_tag_name_len"));
		maxTagValueLen = Numbers.parseInt(p.getProperty("max_tag_value_len"));
		int quarterDelay = Numbers.parseInt(p.getProperty("quarter_delay"), 2);
		boolean enableRemoteAddr = Conf.getBoolean(p.getProperty("remote_addr"), true);
		String allowedRemote = p.getProperty("allowed_remote");
		HashSet<String> allowedRemotes = null;
		if (allowedRemote != null) {
			allowedRemotes = new HashSet<>(Arrays.asList(allowedRemote.split("[,;]")));
		}
		verbose = Conf.getBoolean(p.getProperty("verbose"), false);
		long start = System.currentTimeMillis();
		AtomicInteger currentMinute = new AtomicInteger((int) (start / Time.MINUTE));
		p = Conf.load("jdbc");
		Runnable minutely = null;
		try (DatagramSocket socket = new DatagramSocket(new
				InetSocketAddress(host, port))) {
			Driver driver = (Driver) Class.forName(p.
					getProperty("driver")).newInstance();
			DB = new ConnectionPool(driver, p.getProperty("url", ""),
					p.getProperty("user"), p.getProperty("password"));
			minutely = Runnables.wrap(() -> {
				int minute = currentMinute.incrementAndGet();
				try {
					minutely(minute);
					if (serverId == 0 && !service.isInterrupted() && minute % 15 == quarterDelay) {
						// Skip "quarterly" when shutdown
						quarterly(minute / 15);
					}
				} catch (SQLException e) {
					Log.e(e);
				}
			});
			timer.scheduleAtFixedRate(minutely, Time.MINUTE - start % Time.MINUTE,
					Time.MINUTE, TimeUnit.MILLISECONDS);
			service.register(socket);

			Log.i("Metric Collector Started on UDP " + host + ":" + port);
			while (!Thread.interrupted()) {
				// Receive
				byte[] buf = new byte[65536];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				// Blocked, or closed by shutdown handler
				socket.receive(packet);
				int len = packet.getLength();
				String remoteAddr = packet.getAddress().getHostAddress();
				if (allowedRemotes != null && !allowedRemotes.contains(remoteAddr)) {
					Log.w(remoteAddr + " not allowed");
					continue;
				}
				if (enableRemoteAddr) {
					Metric.put("metric.throughput", len,
							"remote_addr", remoteAddr, "server_id", "" + serverId);
				} else {
					Metric.put("metric.throughput", len, "server_id", "" + serverId);
				}
				// Inflate
				ByteArrayQueue baq = new ByteArrayQueue();
				byte[] buf_ = new byte[2048];
				try (InflaterInputStream inflater = new InflaterInputStream(new
						ByteArrayInputStream(buf, 0, len))) {
					int bytesRead;
					while ((bytesRead = inflater.read(buf_)) > 0) {
						baq.add(buf_, 0, bytesRead);
						// Prevent attack
						if (baq.length() > MAX_BUFFER_SIZE) {
							break;
						}
					}
				} catch (IOException e) {
					Log.w("Unable to inflate packet from " + remoteAddr);
					// Continue to parse rows
				}

				HashMap<String, ArrayList<MetricRow>> rowsMap = new HashMap<>();
				HashMap<String, Integer> countMap = new HashMap<>();
				for (String line : baq.toString().split("\n")) {
					// Truncate tailing '\r'
					int length = line.length();
					if (length > 0 && line.charAt(length - 1) == '\r') {
						line = line.substring(0, length - 1);
					}
					// Parse name, aggregation, value and tags
					// <name>/<aggregation>/<value>[?<tag>=<value>[&...]]
					String[] paths;
					HashMap<String, String> tagMap = new HashMap<>();
					int index = line.indexOf('?');
					if (index < 0) {
						paths = line.split("/");
					} else {
						paths = line.substring(0, index).split("/");
						String tags = line.substring(index + 1);
						for (String tag : tags.split("&")) {
							index = tag.indexOf('=');
							if (index > 0) {
								tagMap.put(decode(tag.substring(0, index), maxTagNameLen),
										decode(tag.substring(index + 1), maxTagValueLen));
							}
						}
					}
					if (paths.length < 2) {
						Log.w("Incorrect format: [" + line + "]");
						continue;
					}
					String name = decode(paths[0], MAX_METRIC_LEN);
					if (name.isEmpty()) {
						Log.w("Incorrect format: [" + line + "]");
						continue;
					}
					if (enableRemoteAddr) {
						tagMap.put("remote_addr", remoteAddr);
						Metric.put("metric.rows", 1, "name", name,
								"remote_addr", remoteAddr, "server_id", "" + serverId);
					} else {
						Metric.put("metric.rows", 1, "name", name,
								"server_id", "" + serverId);
					}
					if (paths.length > 6) {
						// For aggregation-before-collection metric, insert immediately
						long count = Numbers.parseLong(paths[2]);
						double sum = Numbers.parseDouble(paths[3]);
						put(rowsMap, name, row(tagMap,
								Numbers.parseInt(paths[1], currentMinute.get()), count, sum,
								Numbers.parseDouble(paths[4]), Numbers.parseDouble(paths[5]),
								Numbers.parseDouble(paths[6])));
					} else {
						// For aggregation-during-collection metric, aggregate first
						Metric.put(name, Numbers.parseDouble(paths[1]), tagMap);
					}
					if (verbose) {
						Integer count = countMap.get(name);
						countMap.put(name, Integer.valueOf(count == null ?
								1 : count.intValue() + 1));
					}
				}
				if (!countMap.isEmpty()) {
					Log.d("Metrics received from " + remoteAddr + ": " + countMap);
				}
				// Insert aggregation-before-collection metrics
				if (!rowsMap.isEmpty()) {
					executor.execute(Runnables.wrap(() -> {
						try {
							insert(rowsMap);
						} catch (SQLException e) {
							Log.e(e);
						}
					}));
				}
			}
		} catch (IOException | ReflectiveOperationException e) {
			Log.w(e.getMessage());
		} catch (Error | RuntimeException e) {
			Log.e(e);
		}
		// Do not do SQL operations in main thread (may be interrupted)
		if (minutely != null) {
			executor.execute(minutely);
		}
		Runnables.shutdown(executor);
		Runnables.shutdown(timer);
		if (DB != null) {
			DB.close();
		}

		Log.i("Metric Collector Stopped");
		Conf.closeLogger(Log.getAndSet(logger));
		service.shutdown();
	}
}