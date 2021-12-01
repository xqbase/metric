package com.xqbase.metric;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.zip.InflaterInputStream;

import org.json.JSONObject;

import com.xqbase.metric.client.ManagementMonitor;
import com.xqbase.metric.common.Metric;
import com.xqbase.metric.common.MetricEntry;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.util.CollectionsEx;
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

class NameTime {
	String name;
	int time;

	@Override
	public int hashCode() {
		return name.hashCode() * 31 + time;
	}

	@Override
	public boolean equals(Object obj) {
		NameTime o = (NameTime) obj;
		return time == o.time && name.equals(o.name);
	}
}

class MetricName {
	int id, minuteSize, quarterSize, aggregatedTime;
	String name;
}

public class Collector {
	private static final int MAX_BUFFER_SIZE = 1048576;
	private static final int MAX_METRIC_LEN = 64;

	private static final String QUERY_ID =
			"SELECT id FROM metric_name WHERE name = ?";
	private static final String CREATE_ID =
			"INSERT INTO metric_name (name) VALUES (?)";
	private static final String QUERY_NAME =
			"SELECT id, name, minute_size, quarter_size, aggregated_time FROM metric_name";
	private static final String QUERY_SEQ =
			"SELECT seq FROM metric_seq WHERE time = ?";
	private static final String INSERT_SEQ =
			"INSERT INTO metric_seq (time, seq) VALUES (?, 0)";
	private static final String INCREMENT_SEQ =
			"UPDATE metric_seq SET seq = seq + 1 WHERE time = ? AND seq = ?";
	private static final String INSERT_MINUTE =
			"INSERT INTO metric_minute (id, time, seq, metrics) VALUES (?, ?, ?, ?)";
	private static final String INSERT_QUARTER =
			"INSERT INTO metric_quarter (id, time, metrics) VALUES (?, ?, ?)";
	private static final String INCREMENT_MINUTE =
			"UPDATE metric_name SET minute_size = minute_size + ? WHERE id = ?";
	private static final String INCREMENT_QUARTER =
			"UPDATE metric_name SET quarter_size = quarter_size + ? WHERE id = ?";

	private static final String DELETE_SEQ =
			"DELETE FROM metric_seq WHERE time <= ?";
	private static final String DELETE_TAGS_QUARTER_BY_ID =
			"DELETE FROM metric_tags_quarter WHERE id = ?";
	private static final String DELETE_TAGS_QUARTER =
			"DELETE FROM metric_tags_quarter WHERE id = ? AND time <= ?";
	private static final String DELETE_NAME =
			"DELETE FROM metric_name WHERE id = ?";

	private static final String QUERY_MINUTE_SIZE =
			"SELECT COUNT(*) c, SUM(LENGTH(metrics)) s FROM metric_minute WHERE id = ? AND time <= ?";
	private static final String QUERY_QUARTER_SIZE =
			"SELECT COUNT(*) c, SUM(LENGTH(metrics)) s FROM metric_quarter WHERE id = ? AND time <= ?";
	private static final String DELETE_MINUTE =
			"DELETE FROM metric_minute WHERE id = ? AND time <= ?";
	private static final String DELETE_QUARTER =
			"DELETE FROM metric_quarter WHERE id = ? AND time <= ?";
	private static final String AGGREGATE_FROM =
			"SELECT metrics FROM metric_minute WHERE id = ? AND time >= ? AND time <= ?";
	private static final String AGGREGATE_TO =
			"INSERT INTO metric_tags_quarter (id, time, tags) VALUES (?, ?, ?)";
	private static final String AGGREGATE_TAGS_FROM =
			"SELECT tags FROM metric_tags_quarter WHERE id = ?";
	private static final String UPDATE_NAME =
			"UPDATE metric_name SET minute_size = minute_size - ?, " +
			"quarter_size = quarter_size - ?, aggregated_time = ?, tags = ? WHERE id = ?";

	private static double __(String s) {
		double d = Numbers.parseDouble(s);
		return Double.isNaN(d) ? 0 : d;
	}

	private static String decode(String s, int limit) {
		String result = Strings.decodeUrl(s);
		return limit > 0 ? Strings.truncate(result, limit) : result;
	}

	private static Service service = new Service();
	private static ConnectionPool DB = null;
	private static int serverId, expire, tagsExpire, maxTags, maxTagValues,
			maxTagCombinations, maxTagNameLen, maxTagValueLen;
	private static boolean verbose;
	private static File mvStoreFile;
	private static ConnectionPool.Entry h2PoolEntry;
	private static Object mvStore;

	private static boolean insert(String sql, Object... in) throws SQLException {
		return insert(null, sql, in);
	}

	private static boolean insert(long[] insertId,
			String sql, Object... in) throws SQLException {
		try {
			if (DB.updateEx(insertId, sql, in) <= 0) {
				return false;
			}
		} catch (SQLException e) {
			if (e.getErrorCode() == 19 &&
					e.getClass().getSimpleName().equals("SQLiteException")) {
				return false;
			}
			if ("23505".equals(e.getSQLState()) &&
					e.getClass().getSimpleName().equals("PSQLException")) {
				return false;
			}
			throw e;
		}
		return true;
	}

	private static List<MetricName> getNames() throws SQLException {
		List<MetricName> names = new ArrayList<>();
		DB.query(row -> {
			MetricName name = new MetricName();
			name.id = row.getInt("id");
			name.name = row.getString("name");
			name.minuteSize = row.getInt("minute_size");
			name.quarterSize = row.getInt("quarter_size");
			name.aggregatedTime = row.getInt("aggregated_time");
			names.add(name);
		}, QUERY_NAME);
		return names;
	}

	private static int nextSeq(int time) throws SQLException {
		int lastSeq;
		Row row = DB.query(QUERY_SEQ, time);
		if (row == null) {
			if (insert(INSERT_SEQ, Integer.valueOf(time))) {
				Metric.put("metric.seq_increment", 0, "server_id", "" + serverId);
				return 0;
			}
			Log.i("Simultaneously inserted seq 0 for time " + time);
			lastSeq = 0;
		} else {
			lastSeq = row.getInt("seq");
		}
		int seq = lastSeq;
		while (DB.update(INCREMENT_SEQ, time, seq) <= 0) {
			row = DB.query(QUERY_SEQ, time);
			if (row == null) {
				Log.w("Unable to query seq by time " + time + ", expected " + seq);
				break;
			}
			seq = row.getInt("seq");
		}
		seq ++;
		Metric.put("metric.seq_increment", seq - lastSeq, "server_id", "" + serverId);
		return seq;
	}

	private static void insert(String name, int time,
			int seq, StringBuilder sb) throws SQLException {
		int id;
		Row row = DB.queryEx(QUERY_ID, name);
		if (row == null) {
			long[] id_ = new long[1];
			if (insert(id_, CREATE_ID, name)) {
				id = (int) id_[0];
			} else {
				Log.i("Simultaneously inserted name " + name);
				row = DB.queryEx(QUERY_ID, name);
				if (row == null) {
					Log.w("Unable to create name " + name);
					return;
				}
				id = row.getInt("id");
			}
		} else {
			id = row.getInt("id");
		}
		if (!insert(INSERT_MINUTE, Integer.valueOf(id), Integer.valueOf(time),
				Integer.valueOf(seq), sb.toString())) {
			Log.w("Duplicate key " + time + "-" + seq + " in " + name);
		}
		DB.update(INCREMENT_MINUTE, sb.length(), id);
	}

	private static void putTagValue(Map<String, Map<String, MetricValue>> tagMap,
			String tagKey, String tagValue, MetricValue value) {
		Map<String, MetricValue> tagValues = tagMap.get(tagKey);
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

	private static Map<String, Map<String, MetricValue>>
			limit(Map<String, Map<String, MetricValue>> tagMap) {
		Map<String, Map<String, MetricValue>> tags = new HashMap<>();
		BiConsumer<String, Map<String, MetricValue>> action = (tagName, valueMap) -> {
			Map<String, MetricValue> tagValues = new HashMap<>();
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

	private static Method getFillRate, getChunksFillRate, getRewritableChunksFillRate;
	private static Method getCacheSizeUsed, getCacheHitRatio;

	private static void minutely(int minute) throws SQLException {
		// Insert aggregation-during-collection metrics
		Map<String, StringBuilder> metricMap = new HashMap<>();
		for (MetricEntry entry : Metric.removeAll()) {
			StringBuilder sb = metricMap.computeIfAbsent(entry.getName(),
					k -> new StringBuilder());
			sb.append(entry.getCount()).
					append('/').append(entry.getSum()).
					append('/').append(entry.getMax()).
					append('/').append(entry.getMin()).
					append('/').append(entry.getSqr());
			int question = sb.length();
			Map<String, String> tags = entry.getTagMap();
			Map<String, String> limitedTags;
			if (maxTags > 0 && tags.size() > maxTags) {
				limitedTags = new HashMap<>();
				CollectionsEx.forEach(CollectionsEx.min(tags.entrySet(),
						Comparator.comparing(Map.Entry::getKey), maxTags), limitedTags::put);
			} else {
				limitedTags = tags;
			}
			limitedTags.forEach((k, v) -> {
				sb.append('&').append(Strings.encodeUrl(k)).
						append('=').append(Strings.encodeUrl(v));
			});
			if (!limitedTags.isEmpty()) {
				sb.setCharAt(question, '?');
			}
			sb.append('\n');
		}
		int seq = nextSeq(minute);
		for (Map.Entry<String, StringBuilder> entry : metricMap.entrySet()) {
			insert(entry.getKey(), minute, seq, entry.getValue());
		}
		// Calculate metric size by master collector
		if (serverId != 0) {
			return;
		}
		for (MetricName name : getNames()) {
			if (name.minuteSize > 0) {
				Metric.put("metric.size", name.minuteSize, "name", name.name);
			}
			if (name.quarterSize > 0) {
				Metric.put("metric.size", name.quarterSize, "name", "_quarter." + name.name);
			}
		}
		// MVStore: size, fill rate, cache used and hit ratio
		if (mvStoreFile != null) {
			Metric.put("metric.mvstore.size", mvStoreFile.length());
		}
		if (mvStore == null) {
			return;
		}
		try {
			Metric.put("metric.mvstore.fill_rate", ((Number) getFillRate.
					invoke(mvStore)).doubleValue(), "type", "store");
			Metric.put("metric.mvstore.fill_rate", ((Number) getChunksFillRate.
					invoke(mvStore)).doubleValue(), "type", "chunks");
			Metric.put("metric.mvstore.fill_rate", ((Number) getRewritableChunksFillRate.
					invoke(mvStore)).doubleValue(), "type", "rewritable_chunks");
			Metric.put("metric.mvstore.cache_size_used",
					((Number) getCacheSizeUsed.invoke(mvStore)).doubleValue());
			Metric.put("metric.mvstore.cache_hit_ratio",
					((Number) getCacheHitRatio.invoke(mvStore)).doubleValue());
		} catch (ReflectiveOperationException e) {
			Log.w("" + e);
		}
	}

	private static void quarterly(int quarter) throws SQLException {
		DB.update(DELETE_SEQ, quarter * 15);
		for (MetricName name : getNames()) {
			// Delete meta data
			if (name.minuteSize <= 0 && name.quarterSize <= 0) {
				DB.update(DELETE_TAGS_QUARTER_BY_ID, name.id);
				DB.update(DELETE_NAME, name.id);
				continue;
			}
			// Delete tags_quarter
			DB.update(DELETE_TAGS_QUARTER, name.id, quarter - tagsExpire);
			// Delete minutely metrics
			Row sizeRow = DB.query(QUERY_MINUTE_SIZE, name.id, quarter * 15 - expire);
			if (sizeRow == null) {
				Log.w("Unable to get minute size for metric " + name.name);
				continue;
			}
			int deletedRows = DB.update(DELETE_MINUTE, name.id, quarter * 15 - expire);
			if (deletedRows != sizeRow.getInt("c")) {
				Log.w(deletedRows + " (deleted rows) != " + sizeRow.getInt("c") +
						" (queried rows), for minute metric " + name.name);
			}
			long deletedMinute = sizeRow.getLong("s");
			// Delete quarterly metrics
			sizeRow = DB.query(QUERY_QUARTER_SIZE, name.id, quarter - expire);
			if (sizeRow == null) {
				Log.w("Unable to get quarter size for metric " + name.name);
				continue;
			}
			deletedRows = DB.update(DELETE_QUARTER, name.id, quarter - expire);
			if (deletedRows != sizeRow.getInt("c")) {
				Log.w(deletedRows + " (deleted rows) != " + sizeRow.getInt("c") +
						" (queried rows), for quarter metric " + name.name);
			}
			long deletedQuarter = sizeRow.getLong("s");
			// Aggregate minute to quarter
			int start = name.aggregatedTime == 0 ? quarter - expire : name.aggregatedTime;
			for (int i = start + 1; i <= quarter; i ++) {
				Map<Map<String, String>, MetricValue> accMetricMap = new HashMap<>();
				DB.query(row -> {
					String s = row.getString("metrics");
					for (String line : s.split("\n")) {
						String[] paths;
						Map<String, String> tags = new HashMap<>();
						int index = line.indexOf('?');
						if (index < 0) {
							paths = line.split("/");
						} else {
							paths = line.substring(0, index).split("/");
							String query = line.substring(index + 1);
							for (String tag : query.split("&")) {
								index = tag.indexOf('=');
								if (index > 0) {
									tags.put(decode(tag.substring(0, index), maxTagNameLen),
											decode(tag.substring(index + 1), maxTagValueLen));
								}
							}
						}
						if (paths.length <= 4) {
							continue;
						}
						MetricValue newValue = new MetricValue(Numbers.parseLong(paths[0]),
								__(paths[1]), __(paths[2]), __(paths[3]), __(paths[4]));
						MetricValue value = accMetricMap.get(tags);
						if (value == null) {
							accMetricMap.put(tags, newValue);
						} else {
							value.add(newValue);
						}
					}
				}, AGGREGATE_FROM, name.id, i * 15 - 14, i * 15);
				if (accMetricMap.isEmpty()) {
					continue;
				}
				int combinations = accMetricMap.size();
				Metric.put("metric.tags.combinations", combinations, "name", name.name);
				// 3'. Aggregate to "_quarter.*"
				// 5. Aggregate to "_tags_quarter.*"
				StringBuilder sb = new StringBuilder();
				Map<String, Map<String, MetricValue>> tagMap = new HashMap<>();
				BiConsumer<Map<String, String>, MetricValue> action = (tags, value) -> {
					sb.append(value.getCount()).append('/').
							append(value.getSum()).append('/').
							append(value.getMax()).append('/').
							append(value.getMin()).append('/').
							append(value.getSqr());
					if (tags.isEmpty()) {
						return;
					}
					int question = sb.length();
					tags.forEach((tagKey, tagValue) -> {
						sb.append('&').append(Strings.encodeUrl(tagKey)).
								append('=').append(Strings.encodeUrl(tagValue));
						putTagValue(tagMap, tagKey, tagValue, value);
					});
					sb.setCharAt(question, '?');
					sb.append('\n');
				};
				if (maxTagCombinations > 0 && combinations > maxTagCombinations) {
					CollectionsEx.forEach(CollectionsEx.max(accMetricMap.entrySet(),
							Comparator.comparingLong(entry -> entry.getValue().getCount()),
							maxTagCombinations), action);
				} else {
					accMetricMap.forEach(action);
				}
				// 3'. Aggregate to "_quarter.*"
				if (!insert(INSERT_QUARTER, Integer.valueOf(name.id),
						Integer.valueOf(i), sb.toString())) {
					Log.w("Duplicate key " + i + " in " + name.name);
				}
				DB.update(INCREMENT_QUARTER, sb.length(), name.id);
				// 5. Aggregate to "_tags_quarter.*"
				tagMap.forEach((tagKey, tagValue) -> {
					Metric.put("metric.tags.values", tagValue.size(),
							"name", name.name, "key", tagKey);
				});
				// {"_quarter": i}, but not {"_quarter": quarter} !
				if (!insert(AGGREGATE_TO, Integer.valueOf(name.id),
						Integer.valueOf(i), new JSONObject(limit(tagMap)).toString())) {
					Log.w("Duplicate key " + i + " in tags_quarter " + name.name);
				}
			}
			// Aggregate "tags_quarter.tags" to "name.tags";
			Map<String, Map<String, MetricValue>> tagMap = new HashMap<>();
			DB.query(row -> {
				JSONObject json = new JSONObject(row.getString("tags"));
				for (String tagKey : json.keySet()) {
					JSONObject tagsJson = json.optJSONObject(tagKey);
					if (tagsJson == null) {
						continue;
					}
					for (String tagValue : tagsJson.keySet()) {
						JSONObject j = tagsJson.optJSONObject(tagValue);
						if (j == null) {
							continue;
						}
						putTagValue(tagMap, tagKey, tagValue,
								new MetricValue(j.optLong("count"), j.optDouble("sum"),
								j.optDouble("max"), j.optDouble("min"), j.optDouble("sqr")));
					}
				}
			}, AGGREGATE_TAGS_FROM, name.id);
			DB.updateEx(UPDATE_NAME, Long.valueOf(deletedMinute),
					Long.valueOf(deletedQuarter), Integer.valueOf(quarter),
					new JSONObject(limit(tagMap)).toString(), Integer.valueOf(name.id));
		}
	}

	private static void insert(Map<NameTime, StringBuilder> metricMap)
			throws SQLException {
		Map<Integer, Map<String, StringBuilder>> timeMap = new HashMap<>();
		metricMap.forEach((key, sb) -> {
			timeMap.computeIfAbsent(Integer.valueOf(key.time),
					k -> new HashMap<>()).put(key.name, sb);
		});
		for (Map.Entry<Integer, Map<String, StringBuilder>> timeEntry :
				timeMap.entrySet()) {
			int time = timeEntry.getKey().intValue();
			int seq = nextSeq(time);
			for (Map.Entry<String, StringBuilder> nameEntry :
					timeEntry.getValue().entrySet()) {
				insert(nameEntry.getKey(), time, seq, nameEntry.getValue());
			}
		}
	}

	private static Object startServer(int port, String type, String h2DataDir) {
		Object server = null;
		// server = Server.createTcpServer(...);
		// server.start();
		String _type = "-" + type.toLowerCase();
		try {
			Class<?> serverCls = Class.forName("org.h2.tools.Server");
			server = serverCls.getMethod("create" + type + "Server", String[].class).
					invoke(null, (Object) new String[] {
				_type + "AllowOthers", _type + "Port", "" + port, "-baseDir", h2DataDir
			});
			serverCls.getMethod("start").invoke(server);
			Log.i("" + serverCls.getMethod("getStatus").invoke(server));
		} catch (ReflectiveOperationException e) {
			Log.e(e);
		}
		return server;
	}

	private static void stopServer(Object server) {
		if (server == null) {
			return;
		}
		try {
			Class.forName("org.h2.tools.Server").
					getMethod("stop").invoke(server);
		} catch (ReflectiveOperationException e) {
			Log.e(e);
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
		ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(2);

		try {
			Class<?> mvStoreClass = Class.forName("org.h2.mvstore.MVStore");
			getFillRate = mvStoreClass.getMethod("getFillRate");
			getChunksFillRate = mvStoreClass.getMethod("getChunksFillRate");
			getRewritableChunksFillRate = mvStoreClass.getMethod("getRewritableChunksFillRate");
			getCacheSizeUsed = mvStoreClass.getMethod("getCacheSizeUsed");
			getCacheHitRatio = mvStoreClass.getMethod("getCacheHitRatio");
		} catch (ReflectiveOperationException e) {
			Log.w("" + e);
		}

		Properties p = Conf.load("Collector");
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		int h2Port = Numbers.parseInt(p.getProperty("h2_port"), 5513);
		int pgPort = Numbers.parseInt(p.getProperty("pg_port"), 5512);
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
		Set<String> allowedRemotes = null;
		if (allowedRemote != null) {
			allowedRemotes = new HashSet<>(Arrays.asList(allowedRemote.split("[,;]")));
		}
		verbose = Conf.getBoolean(p.getProperty("verbose"), false);
		long start = System.currentTimeMillis();
		AtomicInteger currentMinute = new AtomicInteger((int) (start / Time.MINUTE));
		p = Conf.load("jdbc");
		Runnable minutely = null;
		String h2DataDir = null;
		Object h2Server = null;
		Object pgServer = null;
		try (
			DatagramSocket socket = new DatagramSocket(new
					InetSocketAddress(host, port));
			ManagementMonitor monitor = new ManagementMonitor("metric.server",
					"server_id", "" + serverId);
		) {
			mvStoreFile = null;
			boolean createTable = false, derby = false, sqlite = false;
			Driver driver = (Driver) Class.forName(p.
					getProperty("driver")).newInstance();
			String url = p.getProperty("url", "");
			if (url.endsWith(":h2:metric")) {
				h2DataDir = Conf.getAbsolutePath("data");
				new File(h2DataDir).mkdir();
				mvStoreFile = new File(h2DataDir + "/metric.mv.db");
				createTable = !mvStoreFile.exists();
				h2DataDir = h2DataDir.replace('\\', '/');
				url = url.substring(0, url.length() - 6) + "file:" + h2DataDir +
						"/metric;mode=postgresql;database_to_lower=true;" +
						"compress=true;cache_size=32768;lazy_query_execution=1;" +
						"db_close_on_exit=false;write_delay=10000;" +
						"max_compact_time=0;auto_compact_fill_rate=80";
			} else if (url.endsWith(":derby:metric")) {
				String dataDir = Conf.getAbsolutePath("data");
				new File(dataDir).mkdir();
				String dataFile = dataDir + "/metric";
				createTable = !new File(dataFile).exists();
				url = url.substring(0, url.length() - 6) + dataFile.replace('\\', '/') +
						(createTable ? ";create=true" : "");
				derby = true;
			} else if (url.endsWith(":sqlite:metric")) {
				String dataDir = Conf.getAbsolutePath("data");
				new File(dataDir).mkdir();
				String dataFile = dataDir + "/metric.db";
				createTable = !new File(dataFile).exists();
				url = url.substring(0, url.length() - 6) + "file:" +
						dataFile + "?busy_timeout=30000";
				sqlite = true;
			}

			DB = new ConnectionPool(driver, url,
					p.getProperty("user"), p.getProperty("password"));
			h2PoolEntry = null;
			mvStore = null;
			try {
				if (h2DataDir != null) {
					// An embedded connection must be created first, see:
					// https://github.com/h2database/h2database/issues/2294
					h2PoolEntry = DB.borrow();
					Class<?> connClz = Class.forName("org.h2.jdbc.JdbcConnection");
					Object conn = h2PoolEntry.getObject().unwrap(connClz);
					Object session = connClz.getMethod("getSession").invoke(conn);
					Object database = Class.forName("org.h2.engine.SessionLocal").
							getMethod("getDatabase").invoke(session);
					Object store = Class.forName("org.h2.engine.Database").
							getMethod("getStore").invoke(database);
					mvStore = Class.forName("org.h2.mvstore.db.Store").
							getMethod("getMvStore").invoke(store);
					if (h2Port > 0) {
						h2Server = startServer(h2Port, "Tcp", h2DataDir);
					}
					if (pgPort > 0) {
						// pgServer = startServer(pgPort, "Pg", h2DataDir);
						Class<?> serverCls = Class.forName("org.h2.tools.Server");
						Class<?> serviceCls = Class.forName("org.h2.server.Service");
						Class<?> pgServerCls = Class.forName("com.xqbase.h2pg.PgServerCompat");
						pgServer = serverCls.getConstructor(serviceCls, String[].class).
								newInstance(pgServerCls.newInstance(), new String[] {
							"-pgAllowOthers", "-pgPort", "" + pgPort, "-baseDir", h2DataDir,
						});
						serverCls.getMethod("start").invoke(pgServer);
						Log.i("" + serverCls.getMethod("getStatus").invoke(pgServer));
					}
				}
				if (createTable) {
					ByteArrayQueue baq = new ByteArrayQueue();
					try (InputStream in = Collector.class.
								getResourceAsStream("/sql/metric.sql")) {
						baq.readFrom(in);
						String[] sqls = baq.toString().split(";");
						for (String s : sqls) {
							String sql = s.trim();
							if (!sql.isEmpty()) {
								if (derby) {
									sql = sql.replace(" AUTO_INCREMENT,",
											" GENERATED BY DEFAULT AS IDENTITY,").
											replace(" LONGTEXT ", " CLOB ");
								} else if (sqlite) {
									sql = sql.replace(" AUTO_INCREMENT,",
											" AUTOINCREMENT,");
								} else { // H2PG
									sql = sql.replace(" INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,",
											" SERIAL PRIMARY KEY,");
								}
								DB.update(sql);
							}
						}
					}
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			Dashboard.startup(DB);

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
			timer.scheduleAtFixedRate(monitor, 5, 5, TimeUnit.SECONDS);
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

				Map<NameTime, StringBuilder> metricMap = new HashMap<>();
				Map<String, Integer> countMap = new HashMap<>();
				for (String line : baq.toString().split("\n")) {
					// Truncate tailing '\r'
					int length = line.length();
					if (length > 0 && line.charAt(length - 1) == '\r') {
						line = line.substring(0, length - 1);
					}
					// Parse name, time, value and tags
					int slash0 = line.indexOf('/');
					if (slash0 < 0) {
						Log.w("Incorrect format: [" + line + "]");
						continue;
					}
					String name = decode(line.substring(0, slash0), MAX_METRIC_LEN);
					Integer count = countMap.get(name);
					countMap.put(name, Integer.valueOf(count == null ?
							1 : count.intValue() + 1));
					int slash1 = line.indexOf('/', slash0 + 1);
					if (slash1 > 0) {
						// <name>/<time>/<count>/<sum>/<max>/<min>/<sqr>[?<tag>=<value>[&...]]
						// Aggregation-before-collection metric, insert immediately
						NameTime key = new NameTime();
						key.name = name;
						key.time = Numbers.parseInt(line.substring(slash0 + 1, slash1),
								currentMinute.get());
						line = line.substring(slash1 + 1);
						if (enableRemoteAddr) {
							int index = line.indexOf('?');
							line += (index < 0 ? '?' : '&') + "remote_addr=" + remoteAddr;
						}
						metricMap.computeIfAbsent(key, k -> new StringBuilder()).
								append(line).append('\n');
						continue;
					}
					// <name>/<value>[?<tag>=<value>[&...]]
					// Aggregation-during-collection metric, aggregate first
					Map<String, String> tagMap = new HashMap<>();
					int index = line.indexOf('?');
					double value;
					if (index < 0) {
						value = __(line.substring(slash0 + 1));
					} else {
						value = __(line.substring(slash0 + 1, index));
						String query = line.substring(index + 1);
						for (String tag : query.split("&")) {
							index = tag.indexOf('=');
							if (index >= 0) {
								tagMap.put(decode(tag.substring(0, index), maxTagNameLen),
										decode(tag.substring(index + 1), maxTagValueLen));
							}
						}
					}
					if (enableRemoteAddr) {
						tagMap.put("remote_addr", remoteAddr);
					}
					Metric.put(name, value, tagMap);
				}
				if (verbose) {
					Log.d("Metrics received from " + remoteAddr + ": " + countMap);
				}
				if (enableRemoteAddr) {
					Metric.put("metric.throughput", len,
							"remote_addr", remoteAddr, "server_id", "" + serverId);
					countMap.forEach((name, value) -> {
						Metric.put("metric.rows", value.intValue(), "name", name,
								"remote_addr", remoteAddr, "server_id", "" + serverId);
					});
				} else {
					Metric.put("metric.throughput", len, "server_id", "" + serverId);
					countMap.forEach((name, value) -> {
						Metric.put("metric.rows", value.intValue(), "name", name,
								"server_id", "" + serverId);
					});
				}
				// Insert aggregation-before-collection metrics
				if (!metricMap.isEmpty()) {
					executor.execute(Runnables.wrap(() -> {
						try {
							insert(metricMap);
						} catch (SQLException e) {
							Log.e(e);
						}
					}));
				}
			}
		} catch (IOException | ReflectiveOperationException e) {
			Log.w("" + e);
		} catch (Error | RuntimeException e) {
			Log.e(e);
		}
		Runnables.shutdown(timer);
		// Do not do SQL operations in main thread (may be interrupted)
		if (minutely != null) {
			executor.execute(minutely);
		}
		Runnables.shutdown(executor);
		Dashboard.shutdown();
		if (DB != null) {
			if (h2DataDir != null) {
				stopServer(h2Server);
				stopServer(pgServer);
				if (h2PoolEntry != null) {
					h2PoolEntry.close();
				}
			}
			// H2DB shutdown here, see:
			// http://www.h2database.com/html/features.html#closing_a_database
			DB.close();
		}

		Log.i("Metric Collector Stopped");
		Conf.closeLogger(Log.getAndSet(logger));
		service.shutdown();
	}
}