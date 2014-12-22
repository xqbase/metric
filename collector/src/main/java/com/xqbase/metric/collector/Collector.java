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
	private static final int QUARTER = 15;

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
		row.put(type, Integer.valueOf(now));
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
		for (Map.Entry<String, ArrayList<DBObject>> entry :
				rowsMap.entrySet()) {
			String name = entry.getKey();
			ArrayList<DBObject> rows = entry.getValue();
			db.getCollection(name).insert(rows);
			if (verbose) {
				Log.d("Inserted " + rows.size() + " rows into metric \"" + name + "\".");
			}
		}
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
		return value instanceof Number ? ((Number) value).doubleValue() : 0;
	}

	private static String getString(DBObject row, String key) {
		Object value = row.get(key);
		return value instanceof String ? (String) value : "_";
	}

	private static BasicDBObject __(String key, Object value) {
		return new BasicDBObject(key, value);
	}

	private static final BasicDBObject INDEX_MINUTE = __("_minute", Integer.valueOf(1));
	private static final BasicDBObject INDEX_QUARTER = __("_quarter", Integer.valueOf(1));
	private static final BasicDBObject INDEX_NAME = __("_name", Integer.valueOf(1));

	private static Service service = new Service();
	private static AtomicInteger minuteNow = new AtomicInteger();
	private static AtomicInteger quarterNow = new AtomicInteger();
	private static int serverId, expire, tagsExpire;
	private static boolean verbose;

	private static void minutely(DB db) {
		int minute = minuteNow.incrementAndGet();
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
			if (name.startsWith("system.") || name.equals("_tags")) {
				continue;
			}
			DBCollection collection = db.getCollection(name);
			int count = (int) collection.count();
			if (count == 0) {
				collection.drop();
				if (name.startsWith("_quarter.")) {
					db.getCollection("_tags").remove(__("name", name.substring(9)));
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

	private static void quarterly(DB db) {
		int quarter = quarterNow.incrementAndGet();
		BasicDBObject removeBefore = __("_minute", __("$lte",
				Integer.valueOf(quarter * QUARTER - expire)));
		BasicDBObject removeAfter = __("_minute", __("$gte",
				Integer.valueOf(quarter * QUARTER + expire)));
		BasicDBObject removeBeforeQuarter = __("_quarter", __("$lte",
				Integer.valueOf(quarter - expire)));
		BasicDBObject removeAfterQuarter = __("_quarter", __("$gte",
				Integer.valueOf(quarter + expire)));
		// Ensure index on tags
		DBCollection tags = db.getCollection("_tags");
		tags.createIndex(INDEX_NAME);
		// Scan minutely collections
		for (String name : db.getCollectionNames()) {
			if (name.startsWith("system.") || name.equals("_tags") ||
					name.startsWith("_quarter.")) {
				continue;
			}
			DBCollection collection = db.getCollection(name);
			// Remove stale
			collection.remove(removeBefore);
			collection.remove(removeAfter);
			// Aggregate to quarter
			ArrayList<DBObject> rows = new ArrayList<>();
			int start = quarter - expire;
			BasicDBObject tagsQuery = __("_name", name);
			DBObject tagsRow = tags.findOne(tagsQuery);
			start = tagsRow == null ? 0 : getInt(tagsRow, "_quarter");
			for (int i = (start == 0 ? quarter - expire : start) + 1; i <= quarter; i ++) {
				HashMap<HashMap<String, String>, MetricValue> result = new HashMap<>();
				BasicDBObject range = __("$gte", Integer.valueOf((i - 1) * QUARTER + 1));
				range.put("$lte", Integer.valueOf(i * QUARTER));
				for (DBObject row : collection.find(__("_minute", range))) {
					HashMap<String, String> tag = new HashMap<>();
					for (String tagKey : row.keySet()) {
						if (isTag(tagKey)) {
							tag.put(tagKey, getString(row, tagKey));
						}
					}
					MetricValue newValue = new MetricValue(getInt(row, "_count"),
							getDouble(row, "_sum"), getDouble(row, "_max"),
							getDouble(row, "_min"), getDouble(row, "_sqr"));
					MetricValue value = result.get(tag);
					if (value == null) {
						result.put(tag, newValue);
					} else {
						value.add(newValue);
					}
				}
				for (Map.Entry<HashMap<String, String>, MetricValue> entry :
						result.entrySet()) {
					MetricValue value = entry.getValue();
					rows.add(row(entry.getKey(), "_quarter", i,
							value.getCount(), value.getSum(),
							value.getMax(), value.getMin(), value.getSqr()));
				}
			}
			BasicDBObject update = __("$set", __("_quarter", Integer.valueOf(quarter)));
			tags.update(tagsQuery, update, true, false);
			if (!rows.isEmpty()) {
				db.getCollection("_quarter." + name).insert(rows);
			}
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
			// Arrange tags
			String minuteName = name.substring(9);
			BasicDBObject unsetTags = new BasicDBObject();
			BasicDBObject tagsQuery = __("_name", minuteName);
			DBObject tagsRow = tags.findOne(tagsQuery);
			if (tagsRow != null) {
				for (String tagKey : tagsRow.keySet()) {
					if (isTag(tagKey)) {
						unsetTags.put(tagKey, Integer.valueOf(1));
					}
				}
			}
			HashMap<String, HashSet<String>> tagMap = new HashMap<>();
			BasicDBObject range = __("$gte",
					Integer.valueOf(quarter - tagsExpire / QUARTER + 1));
			range.put("$lte", Integer.valueOf(quarter)); // last 24 hours
			for (DBObject row : db.getCollection(name).
					find(__("_quarter", range))) {
				for (String tagKey : row.keySet()) {
					if (!isTag(tagKey)) {
						continue;
					}
					HashSet<String> tagValues = tagMap.get(tagKey);
					if (tagValues == null) {
						tagValues = new HashSet<>();
						tagMap.put(tagKey, tagValues);
					}
					tagValues.add(getString(row, tagKey));
				}
			}
			unsetTags.keySet().removeAll(tagMap.keySet());
			BasicDBObject update = __("$set", new BasicDBObject(tagMap));
			tags.update(tagsQuery, update, true, false);
			if (!unsetTags.isEmpty()) {
				update = __("$unset", unsetTags);
				tags.update(tagsQuery, update, true, false);
			}
		}
	}

	public static void main(String[] args) {
		if (!service.startup(args)) {
			return;
		}
		Logger logger = Log.getAndSet(Conf.openLogger("Collector.", 16777216, 10));
		ExecutorService executor = Executors.newCachedThreadPool();
		ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
		Log.i("Metric Collector Started");

		Properties p = Conf.load("Collector");
		String host = p.getProperty("host");
		host = host == null || host.isEmpty() ? "0.0.0.0" : host;
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		serverId = Numbers.parseInt(p.getProperty("server_id"), 0);
		expire = Numbers.parseInt(p.getProperty("expire"), 2880);
		tagsExpire = Numbers.parseInt(p.getProperty("tags_expire"), 360);
		boolean enableRemoteAddr = Conf.getBoolean(p.getProperty("remote_addr"), true);
		String allowedRemote = p.getProperty("allowed_remote");
		HashSet<String> allowedRemotes = null;
		if (allowedRemote != null) {
			allowedRemotes = new HashSet<>(Arrays.asList(allowedRemote.split("[,;]")));
		}
		verbose = Conf.getBoolean(p.getProperty("verbose"), false);
		long start = System.currentTimeMillis();
		minuteNow.set((int) (start / Time.MINUTE));
		quarterNow.set((int) (start / Time.MINUTE / QUARTER));
		p = Conf.load("Mongo");
		MongoClient mongo = null;
		Runnable minutely = null;
		try (DatagramSocket socket = new DatagramSocket(new
				InetSocketAddress(host, port))) {
			mongo = new MongoClient(p.getProperty("host"),
					Numbers.parseInt(p.getProperty("port"), 27017));
			DB db = mongo.getDB(p.getProperty("db"));
			minutely = Runnables.wrap(() -> minutely(db));
			timer.scheduleAtFixedRate(minutely, Time.MINUTE - start % Time.MINUTE,
					Time.MINUTE, TimeUnit.MILLISECONDS);
			if (serverId == 0) {
				long period = Time.MINUTE * QUARTER;
				timer.scheduleAtFixedRate(Runnables.wrap(() -> quarterly(db)),
						period - (start - period / 2) % period,
						period, TimeUnit.MILLISECONDS);
			}

			service.register(socket);
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
						put(rowsMap, name, row(tagMap, "_minute", Numbers.parseInt(paths[1], minuteNow.get()),
								count, sum, Numbers.parseDouble(paths[4]), Numbers.parseDouble(paths[5]),
								paths.length == 6 ? sum * sum / count : Numbers.parseDouble(paths[6])));
					}
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