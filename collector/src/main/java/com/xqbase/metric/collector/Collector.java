package com.xqbase.metric.collector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.zip.InflaterInputStream;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.xqbase.metric.common.Metric;
import com.xqbase.metric.common.MetricEntry;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.util.ByteArrayQueue;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Runnables;
import com.xqbase.util.Service;
import com.xqbase.util.Time;

public class Collector {
	private static final int MAX_BUFFER_SIZE = 64000;

	private static String decode(String s) {
		try {
			return URLDecoder.decode(s, "UTF-8");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static BasicDBObject row(Map<String, String> tagMap, String type,
			int now, int count, double sum, double max, double min, double sqr) {
		BasicDBObject row = new BasicDBObject(tagMap);
		if (type != null) {
			row.put(type, Integer.valueOf(now));
		}
		row.put("_count", Integer.valueOf(count));
		row.put("_sum", Double.valueOf(sum));
		row.put("_max", Double.valueOf(max));
		row.put("_min", Double.valueOf(min));
		row.put("_sqr", Double.valueOf(sqr));
		return row;
	}

	private static void put(HashMap<String, ArrayList<DBObject>> rowsMap,
			String name, DBObject row) {
		ArrayList<DBObject> rows = rowsMap.get(name);
		if (rows == null) {
			rows = new ArrayList<>();
			rowsMap.put(name, rows);
		}
		rows.add(row);
	}

	private static void insert(DB db, HashMap<String, ArrayList<DBObject>> rowsMap) {
		rowsMap.forEach((k, v) -> db.getCollection(k).insert(v));
	}

	private static boolean isTag(String key) {
		return !key.isEmpty() && key.charAt(0) != '_';
	}

	private static int getInt(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).intValue() : 0;
	}

	private static double getDouble(DBObject row, String key) {
		Object value = row.get(key);
		double d = value instanceof Number ? ((Number) value).doubleValue() : 0;
		return Double.isFinite(d) ? d : 0;
	}

	private static String getString(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof String ? (String) value : "_";
	}

	private static List<?> getList(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof List ? (List<?>) value : Collections.emptyList();
	}

	private static BasicDBObject __(String key, Object value) {
		return new BasicDBObject(key, value);
	}

	private static final BasicDBObject
			INDEX_MINUTE = __("_minute", Integer.valueOf(1)),
			INDEX_QUARTER = __("_quarter", Integer.valueOf(1)),
			INDEX_NAME = __("_name", Integer.valueOf(1));

	private static Service service = new Service();
	private static int serverId, expire, tagsExpire;
	private static boolean verbose;

	private static void minutely(DB db, int minute) {
		// Insert aggregation-during-collection metrics
		HashMap<String, ArrayList<DBObject>> rowsMap = new HashMap<>();
		for (MetricEntry entry : Metric.removeAll()) {
			BasicDBObject row = row(entry.getTagMap(), "_minute", minute, entry.getCount(),
					entry.getSum(), entry.getMax(), entry.getMin(), entry.getSqr());
			put(rowsMap, entry.getName(), row);
		}
		if (!rowsMap.isEmpty()) {
			insert(db, rowsMap);
		}
		// Ensure index and calculate metric size by master collector
		if (serverId != 0) {
			return;
		}
		ArrayList<DBObject> rows = new ArrayList<>();
		for (String name : db.getCollectionNames()) {
			if (name.startsWith("system.") || name.startsWith("_meta.")) {
				continue;
			}
			DBCollection collection = db.getCollection(name);
			int count = (int) collection.count();
			if (count == 0) {
				// Remove disappeared metric collections
				collection.drop();
				if (name.startsWith("_quarter.")) {
					// Remove disappeared quarterly metric from meta collections
					BasicDBObject query = __("name", name.substring(9));
					db.getCollection("_meta.aggregated").remove(query);
					db.getCollection("_meta.tags_quarter").remove(query);
					db.getCollection("_meta.tags_all").remove(query);
				}
			} else {
				if (!name.startsWith("_quarter.")) {
					collection.createIndex(INDEX_MINUTE);
				}
				rows.add(row(Collections.singletonMap("name", name), "_minute",
						minute, 1, count, count, count, count * count));
			}
		}
		if (!rows.isEmpty()) {
			db.getCollection("metric.size").insert(rows);
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

	private static BasicDBObject getTagRow(HashMap<String, HashMap<String, MetricValue>> tagMap) {
		BasicDBObject row = new BasicDBObject();
		tagMap.forEach((k, v) -> {
			ArrayList<BasicDBObject> tagValues = new ArrayList<>();
			v.forEach((tagValue, metricValue) -> {
				tagValues.add(row(Collections.singletonMap("_value", tagValue),
						null, 0, metricValue.getCount(), metricValue.getSum(),
						metricValue.getMax(), metricValue.getMin(), metricValue.getSqr()));
			});
			row.put(k, tagValues);
		});
		return row;
	}

	private static void quarterly(DB db, int quarter) {
		BasicDBObject removeBefore = __("_minute", __("$lte",
				Integer.valueOf(quarter * 15 - expire)));
		BasicDBObject removeAfter = __("_minute", __("$gte",
				Integer.valueOf(quarter * 15 + expire)));
		BasicDBObject removeBeforeQuarter = __("_quarter", __("$lte",
				Integer.valueOf(quarter - expire)));
		BasicDBObject removeAfterQuarter = __("_quarter", __("$gte",
				Integer.valueOf(quarter + expire)));
		// Ensure index on meta collections
		DBCollection aggregated = db.getCollection("_meta.aggregated");
		aggregated.createIndex(INDEX_NAME);
		DBCollection tagsQuarter = db.getCollection("_meta.tags_quarter");
		tagsQuarter.createIndex(INDEX_NAME);
		tagsQuarter.createIndex(INDEX_QUARTER);
		tagsQuarter.remove(__("_quarter", __("$lte",
				Integer.valueOf(quarter - tagsExpire))));
		tagsQuarter.remove(__("_quarter", __("$gte",
				Integer.valueOf(quarter + tagsExpire))));
		DBCollection tagsAll = db.getCollection("_meta.tags_all");
		tagsAll.createIndex(INDEX_NAME);
		// Scan minutely collections
		for (String name : db.getCollectionNames()) {
			if (name.startsWith("system.") || name.startsWith("_meta.") ||
					name.startsWith("_quarter.")) {
				continue;
			}
			DBCollection collection = db.getCollection(name);
			DBCollection quarterCollection = db.getCollection("_quarter." + name);
			// Remove stale
			collection.remove(removeBefore);
			collection.remove(removeAfter);
			// Aggregate to quarter
			int start = quarter - expire;
			BasicDBObject query = __("_name", name);
			DBObject aggregatedRow = aggregated.findOne(query);
			start = aggregatedRow == null ? 0 : getInt(aggregatedRow, "_quarter");
			for (int i = (start == 0 ? quarter - expire : start) + 1; i <= quarter; i ++) {
				ArrayList<DBObject> rows = new ArrayList<>();
				HashMap<HashMap<String, String>, MetricValue> result = new HashMap<>();
				BasicDBObject range = __("$gte", Integer.valueOf(i * 15 - 14));
				range.put("$lte", Integer.valueOf(i * 15));
				for (DBObject row : collection.find(__("_minute", range))) {
					HashMap<String, String> tags = new HashMap<>();
					for (String tagKey : row.keySet()) {
						if (isTag(tagKey)) {
							tags.put(tagKey, getString(row, tagKey));
						}
					}
					// Aggregate to "_quarter.*"
					MetricValue newValue = new MetricValue(getInt(row, "_count"),
							getDouble(row, "_sum"), getDouble(row, "_max"),
							getDouble(row, "_min"), getDouble(row, "_sqr"));
					MetricValue value = result.get(tags);
					if (value == null) {
						result.put(tags, newValue);
					} else {
						value.add(newValue);
					}
				}
				if (result.isEmpty()) {
					continue;
				}
				HashMap<String, HashMap<String, MetricValue>> tagMap = new HashMap<>();
				int i_ = i;
				result.forEach((tags, value) -> {
					// {"_quarter": i}, but not {"_quarter": quarter} !
					rows.add(row(tags, "_quarter", i_,
							value.getCount(), value.getSum(),
							value.getMax(), value.getMin(), value.getSqr()));
					// Aggregate to "_meta.tags_quarter"
					tags.forEach((k, v) -> putTagValue(tagMap, k, v, value));
				});
				quarterCollection.insert(rows);
				// Aggregate to "_meta.tags_quarter"
				BasicDBObject row = getTagRow(tagMap);
				row.put("_name", name);
				// {"_quarter": i}, but not {"_quarter": quarter} !
				row.put("_quarter", Integer.valueOf(i));
				tagsQuarter.insert(row);
			}
			BasicDBObject update = __("$set", __("_quarter", Integer.valueOf(quarter)));
			aggregated.update(query, update, true, false);
		}
		// Scan quarterly collections
		for (String name : db.getCollectionNames()) {
			if (!name.startsWith("_quarter.")) {
				continue;
			}
			DBCollection collection = db.getCollection(name);
			// Ensure index
			collection.createIndex(INDEX_QUARTER);
			// Remove stale
			collection.remove(removeBeforeQuarter);
			collection.remove(removeAfterQuarter);
			// Aggregate "_meta.tags_quarter" to "_meta.tags_all";
			String minuteName = name.substring(9);
			BasicDBObject query = __("_name", minuteName);
			HashMap<String, HashMap<String, MetricValue>> tagMap = new HashMap<>();
			for (DBObject row : tagsQuarter.find(query)) {
				for (String tagKey : row.keySet()) {
					if (!isTag(tagKey)) {
						continue;
					}
					for (Object o : getList(row, tagKey)) {
						if (!(o instanceof DBObject)) {
							continue;
						}
						DBObject oo = (DBObject) o;
						putTagValue(tagMap, tagKey, getString(oo, "_value"),
								new MetricValue(getInt(oo, "_count"),
								getDouble(oo, "_sum"), getDouble(oo, "_max"),
								getDouble(oo, "_min"), getDouble(oo, "_sqr")));
					}
				}
			}
			BasicDBObject row = getTagRow(tagMap);
			row.put("_name", minuteName);
			tagsAll.update(query, row, true, false);
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
		p = Conf.load("Mongo");
		MongoClient mongo = null;
		Runnable minutely = null;
		try (DatagramSocket socket = new DatagramSocket(new
				InetSocketAddress(host, port))) {
			ServerAddress addr = new ServerAddress(p.getProperty("host"),
					Numbers.parseInt(p.getProperty("port"), 27017));
			String database = p.getProperty("db");
			String username = p.getProperty("username");
			String password = p.getProperty("password");
			if (username == null || password == null) {
				mongo = new MongoClient(addr);
			} else {
				mongo = new MongoClient(addr, Collections.singletonList(MongoCredential.
						createMongoCRCredential(username, database, password.toCharArray())));
			}
			DB db = mongo.getDB(database);
			minutely = Runnables.wrap(() -> {
				int minute = currentMinute.incrementAndGet();
				minutely(db, minute);
				if (serverId == 0 && minute % 15 == quarterDelay) {
					quarterly(db, minute / 15);
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

				HashMap<String, ArrayList<DBObject>> rowsMap = new HashMap<>();
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
								tagMap.put(decode(tag.substring(0, index)),
										decode(tag.substring(index + 1)));
							}
						}
					}
					if (paths.length < 2) {
						Log.w("Incorrect format: [" + line + "]");
						continue;
					}
					String name = decode(paths[0]);
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
					if (paths.length < 6) {
						// For aggregation-during-collection metric, aggregate first
						Metric.put(name, Numbers.parseDouble(paths[1]), tagMap);
					} else {
						// For aggregation-before-collection metric, insert immediately
						int count = Numbers.parseInt(paths[2]);
						double sum = Numbers.parseDouble(paths[3]);
						put(rowsMap, name, row(tagMap, "_minute",
								Numbers.parseInt(paths[1], currentMinute.get()), count, sum,
								Numbers.parseDouble(paths[4]), Numbers.parseDouble(paths[5]),
								paths.length == 6 ? (count == 0 ? 0 : sum * sum / count) :
								Numbers.parseDouble(paths[6])));
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
					executor.execute(Runnables.wrap(() -> insert(db, rowsMap)));
				}
			}
		} catch (IOException e) {
			Log.w(e.getMessage());
		} catch (Error | RuntimeException e) {
			Log.e(e);
		} finally {
			// Do not do Mongo operations in main thread (may be interrupted)
			if (minutely != null) {
				executor.execute(minutely);
			}
			Runnables.shutdown(executor);
			Runnables.shutdown(timer);
			if (mongo != null) {
				mongo.close();
			}
		}

		Log.i("Metric Collector Stopped");
		Conf.closeLogger(Log.getAndSet(logger));
		service.shutdown();
	}
}
