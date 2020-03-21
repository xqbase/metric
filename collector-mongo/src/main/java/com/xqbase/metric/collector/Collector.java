package com.xqbase.metric.collector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
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

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
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

public class Collector {
	private static final int MAX_BUFFER_SIZE = 64000;

	private static Document aggregatedProj = __("quarter", Boolean.TRUE);

	private static double __(String s) {
		double d = Numbers.parseDouble(s);
		return Double.isNaN(d) ? 0 : d;
	}

	private static String escape(String s) {
		return s.replace("\\", "\\-").replace(".", "\\_");
	}

	private static String unescape(String s) {
		return s.replace("\\-", "\\").replace("\\_", ".");
	}

	private static String decode(String s, int limit) {
		String result = Strings.decodeUrl(s);
		return limit > 0 ? Strings.truncate(result, limit) : result;
	}

	private static void put(HashMap<String, ArrayList<Document>> rowsMap,
			String name, Document row) {
		rowsMap.computeIfAbsent(name, k -> new ArrayList<>()).add(row);
	}

	private static void insert(MongoDatabase db,
			HashMap<String, ArrayList<Document>> rowsMap) {
		rowsMap.forEach((name, rows) -> db.getCollection(name).insertMany(rows));
	}

	private static int getInt(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).intValue() : 0;
	}

	private static long getLong(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).longValue() : 0;
	}

	private static double getDouble(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Number ? ((Number) value).doubleValue() : 0;
	}

	private static Document getDocument(Document row, String key) {
		Object value = row.get(key);
		return value instanceof Document ? (Document) value : new Document();
	}

	private static void put(Document row, String key, double d) {
		row.put(key, Double.valueOf(d));
	}

	private static Document __(String key, Object value) {
		return new Document(key, value);
	}

	private static final Document
			INDEX_MINUTE = __("minute", Integer.valueOf(1)),
			INDEX_QUARTER = __("quarter", Integer.valueOf(1)),
			INDEX_NAME = __("name", Integer.valueOf(1));
	private static final UpdateOptions UPSERT = new UpdateOptions().upsert(true);

	private static Service service = new Service();
	private static int serverId, expire, aggrExpire, tagsExpire, maxTags, maxTagValues,
			maxTagCombinations, maxMetricLen, maxTagNameLen, maxTagValueLen;
	private static boolean verbose;

	private static Document row(Map<String, String> tagMap, String type,
			int now, long count, double sum, double max, double min, double sqr) {
		Document row = new Document();
		if (tagMap != null) {
			Document tags = new Document();
			BiConsumer<String, String> action = (k, v) -> tags.put(escape(k), v);
			if (maxTags > 0 && tagMap.size() > maxTags) {
				CollectionsEx.forEach(CollectionsEx.min(tagMap.entrySet(),
						Comparator.comparing(Map.Entry::getKey), maxTags), action);
			} else {
				tagMap.forEach(action);
			}
			row.put("tags", tags);
		}
		if (type != null) {
			row.put(type, Integer.valueOf(now));
		}
		row.put("count", Long.valueOf(count));
		put(row, "sum", sum);
		put(row, "max", max);
		put(row, "min", min);
		put(row, "sqr", sqr);
		return row;
	}

	private static void minutely(MongoDatabase db, int minute) {
		// Insert aggregation-during-collection metrics
		HashMap<String, ArrayList<Document>> rowsMap = new HashMap<>();
		for (MetricEntry entry : Metric.removeAll()) {
			Document row = row(entry.getTagMap(), "minute", minute, entry.getCount(),
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
		for (String name : db.listCollectionNames()) {
			if (name.startsWith("system.") || name.startsWith("_meta.")) {
				continue;
			}
			MongoCollection<Document> collection = db.getCollection(name);
			long count = collection.countDocuments();
			if (count == 0) {
				// Remove disappeared metric collections
				collection.drop();
				if (name.startsWith("_quarter.")) {
					// Remove disappeared quarterly metric from meta collections
					Document query = __("name", name.substring(9));
					db.getCollection("_meta.aggregated").deleteMany(query);
					db.getCollection("_meta.tags_quarter").deleteMany(query);
				}
			} else {
				if (!name.startsWith("_quarter.")) {
					collection.createIndex(INDEX_MINUTE);
				}
				// Will be inserted next minute
				Metric.put("metric.size", count, "name", name);
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

	private static Document getTags(HashMap<String, HashMap<String, MetricValue>> tagMap) {
		Document tags = new Document();
		BiConsumer<String, HashMap<String, MetricValue>> mainAction = (tagName, valueMap) -> {
			Document tagValues = new Document();
			BiConsumer<String, MetricValue> action = (tagValue, value) -> {
				tagValues.put(escape(tagValue), row(null, null, 0, value.getCount(),
						value.getSum(), value.getMax(), value.getMin(), value.getSqr()));
			};
			if (maxTagValues > 0 && valueMap.size() > maxTagValues) {
				CollectionsEx.forEach(CollectionsEx.max(valueMap.entrySet(),
						Comparator.comparingLong(metricValue ->
						metricValue.getValue().getCount()), maxTagValues), action);
			} else {
				valueMap.forEach(action);
			}
			tags.put(escape(tagName), tagValues);
		};
		if (maxTags > 0 && tagMap.size() > maxTags) {
			CollectionsEx.forEach(CollectionsEx.min(tagMap.entrySet(),
					Comparator.comparing(Map.Entry::getKey), maxTags), mainAction);
		} else {
			tagMap.forEach(mainAction);
		}
		return tags;
	}

	private static void quarterly(MongoDatabase db, int quarter) {
		Document removeBefore = __("minute", __("$lte",
				Integer.valueOf(quarter * 15 - expire)));
		Document removeAfter = __("minute", __("$gte",
				Integer.valueOf(quarter * 15 + expire)));
		Document removeBeforeQuarter = __("quarter", __("$lte",
				Integer.valueOf(quarter - expire)));
		Document removeAfterQuarter = __("quarter", __("$gte",
				Integer.valueOf(quarter + expire)));
		// Ensure index on meta collections
		MongoCollection<Document> aggregated = db.getCollection("_meta.aggregated");
		aggregated.createIndex(INDEX_NAME);
		MongoCollection<Document> tagsQuarter = db.getCollection("_meta.tags_quarter");
		tagsQuarter.createIndex(INDEX_NAME);
		tagsQuarter.createIndex(INDEX_QUARTER);
		tagsQuarter.deleteMany(__("quarter", __("$lte",
				Integer.valueOf(quarter - tagsExpire))));
		tagsQuarter.deleteMany(__("quarter", __("$gte",
				Integer.valueOf(quarter + tagsExpire))));
		// Scan minutely collections
		for (String name : db.listCollectionNames()) {
			if (name.startsWith("system.") || name.startsWith("_meta.") ||
					name.startsWith("_quarter.")) {
				continue;
			}
			MongoCollection<Document> collection = db.getCollection(name);
			MongoCollection<Document> quarterCollection = db.getCollection("_quarter." + name);
			// Remove stale
			collection.deleteMany(removeBefore);
			collection.deleteMany(removeAfter);
			// Aggregate to quarter
			Document query = __("name", name);
			Document aggregatedRow = aggregated.find(query).projection(aggregatedProj).first();
			int start = aggregatedRow == null ? quarter - aggrExpire :
					getInt(aggregatedRow, "quarter");
			for (int i = start + 1; i <= quarter; i ++) {
				ArrayList<Document> rows = new ArrayList<>();
				HashMap<HashMap<String, String>, MetricValue> result = new HashMap<>();
				Document range = __("$gte", Integer.valueOf(i * 15 - 14));
				range.put("$lte", Integer.valueOf(i * 15));
				for (Document row : collection.find(__("minute", range))) {
					HashMap<String, String> tags = new HashMap<>();
					getDocument(row, "tags").forEach((k, v) ->
							tags.put(unescape(k), String.valueOf(v)));
					// Aggregate to "_quarter.*"
					MetricValue newValue = new MetricValue(getLong(row, "count"),
							getDouble(row, "sum"), getDouble(row, "max"),
							getDouble(row, "min"), getDouble(row, "sqr"));
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
				int combinations = result.size();
				Metric.put("metric.tags.combinations", combinations, "name", name);
				HashMap<String, HashMap<String, MetricValue>> tagMap = new HashMap<>();
				int i_ = i;
				BiConsumer<Map<String, String>, MetricValue> action = (tags, value) -> {
					// {"quarter": i}, but not {"quarter": quarter} !
					rows.add(row(tags, "quarter", i_, value.getCount(), value.getSum(),
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
				quarterCollection.insertMany(rows);
				// Aggregate to "_meta.tags_quarter"
				tagMap.forEach((tagKey, tagValue) -> {
					Metric.put("metric.tags.values", tagValue.size(), "name", name, "key", tagKey);
				});
				Document row = __("name", name);
				row.put("name", name);
				// {"quarter": i}, but not {"quarter": quarter} !
				row.put("quarter", Integer.valueOf(i));
				row.put("tags", getTags(tagMap));
				tagsQuarter.insertOne(row);
			}
			Document update = __("$set", __("quarter", Integer.valueOf(quarter)));
			aggregated.updateOne(query, update, UPSERT);
		}
		// Scan quarterly collections
		for (String name : db.listCollectionNames()) {
			if (!name.startsWith("_quarter.")) {
				continue;
			}
			MongoCollection<Document> collection = db.getCollection(name);
			// Ensure index
			collection.createIndex(INDEX_QUARTER);
			// Remove stale
			collection.deleteMany(removeBeforeQuarter);
			collection.deleteMany(removeAfterQuarter);
			// Aggregate "_meta.tags_quarter" to "_meta.aggregated";
			String minuteName = name.substring(9);
			Document query = __("name", minuteName);
			HashMap<String, HashMap<String, MetricValue>> tagMap = new HashMap<>();
			for (Document row : tagsQuarter.find(query)) {
				Document tags = getDocument(row, "tags");
				for (String tagKey : tags.keySet()) {
					Document tagValues = getDocument(tags, tagKey);
					String tagKey_ = unescape(tagKey);
					for (String tagValue : tagValues.keySet()) {
						Document v = getDocument(tagValues, tagValue);
						putTagValue(tagMap, tagKey_, unescape(tagValue),
								new MetricValue(getLong(v, "count"),
								getDouble(v, "sum"), getDouble(v, "max"),
								getDouble(v, "min"), getDouble(v, "sqr")));
					}
				}
			}
			aggregated.updateOne(query, __("$set",
					__("tags", getTags(tagMap))), UPSERT);
		}
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		if (!service.startup(args)) {
			return;
		}
		System.setProperty("java.util.logging.SimpleFormatter.format",
				"%1$tY-%1$tm-%1$td %1$tk:%1$tM:%1$tS.%1$tL %2$s%n%4$s: %5$s%6$s%n");
		Logger logger = Log.getAndSet(Conf.openLogger("Collector.", 16777216, 10));
		ExecutorService executor = Executors.newCachedThreadPool();
		ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(2);

		Properties p = Conf.load("Collector");
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		String host = p.getProperty("host");
		host = host == null || host.isEmpty() ? "0.0.0.0" : host;
		serverId = Numbers.parseInt(p.getProperty("server_id"), 0);
		expire = Numbers.parseInt(p.getProperty("expire"), 2880);
		aggrExpire = Numbers.parseInt(p.getProperty("aggr_expire"), 60);
		tagsExpire = Numbers.parseInt(p.getProperty("tags_expire"), 96);
		maxTags = Numbers.parseInt(p.getProperty("max_tags"));
		maxTagValues = Numbers.parseInt(p.getProperty("max_tag_values"));
		maxTagCombinations = Numbers.parseInt(p.getProperty("max_tag_combinations"));
		maxMetricLen = Numbers.parseInt(p.getProperty("max_metric_len"));
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
		MongoClient mongo = null;
		Runnable minutely = null;
		try (
			DatagramSocket socket = new DatagramSocket(new
					InetSocketAddress(host, port));
			ManagementMonitor monitor = new ManagementMonitor("metric.server");
		) {
			p = Conf.load("Mongo");
			mongo = new MongoClient(new MongoClientURI(p.getProperty("uri")));
			MongoDatabase db = mongo.getDatabase(p.getProperty("database", "metric"));
			minutely = Runnables.wrap(() -> {
				int minute = currentMinute.incrementAndGet();
				minutely(db, minute);
				if (serverId == 0 && !service.isInterrupted() && minute % 15 == quarterDelay) {
					// Skip "quarterly" when shutdown
					quarterly(db, minute / 15);
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

				HashMap<String, ArrayList<Document>> rowsMap = new HashMap<>();
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
					String name = decode(paths[0], maxMetricLen);
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
						double sum = __(paths[3]);
						double max = __(paths[4]);
						double min = __(paths[5]);
						double sqr = __(paths[6]);
						put(rowsMap, name, row(tagMap, "minute",
								Numbers.parseInt(paths[1], currentMinute.get()),
								count, sum, max, min, sqr));
					} else {
						// For aggregation-during-collection metric, aggregate first
						Metric.put(name, __(paths[1]), tagMap);
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
		}
		Runnables.shutdown(timer);
		// Do not do Mongo operations in main thread (may be interrupted)
		if (minutely != null) {
			executor.execute(minutely);
		}
		Runnables.shutdown(executor);
		if (mongo != null) {
			mongo.close();
		}

		Log.i("Metric Collector Stopped");
		Conf.closeLogger(Log.getAndSet(logger));
		service.shutdown();
	}
}