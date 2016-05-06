package com.xqbase.metric.sleepycat;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.zip.InflaterInputStream;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.xqbase.metric.client.ManagementMonitor;
import com.xqbase.metric.common.Metric;
import com.xqbase.metric.common.MetricEntry;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.metric.sleepycat.model.Aggregated;
import com.xqbase.metric.sleepycat.model.AllTags;
import com.xqbase.metric.sleepycat.model.QuarterTags;
import com.xqbase.metric.sleepycat.model.Row;
import com.xqbase.metric.sleepycat.model.TagValue;
import com.xqbase.metric.util.CollectionsEx;
import com.xqbase.util.ByteArrayQueue;
import com.xqbase.util.Conf;
import com.xqbase.util.Log;
import com.xqbase.util.Numbers;
import com.xqbase.util.Runnables;
import com.xqbase.util.Strings;
import com.xqbase.util.Time;


public class Collector implements Runnable {
	private static final int MAX_BUFFER_SIZE = 64000;

	private static final StoreConfig STORE_CONFIG = new StoreConfig().setAllowCreate(true);
	private static final Integer MIN_INT = Integer.valueOf(Integer.MIN_VALUE);
	private static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

	private static String decode(String s, int limit) {
		return Strings.truncate(Strings.decodeUrl(s), limit);
	}

	private static void put(HashMap<String, ArrayList<Row>> rowsMap,
			String name, Row row) {
		ArrayList<Row> rows = rowsMap.get(name);
		if (rows == null) {
			rows = new ArrayList<>();
			rowsMap.put(name, rows);
		}
		rows.add(row);
	}

	private static Row row(HashMap<String, String> tagMap, int now,
			long count, double sum, double max, double min, double sqr) {
		Row row = new Row();
		row.time = now;
		row.count = count;
		row.sum = sum;
		row.max = max;
		row.min = min;
		row.sqr = sqr;
		if (maxTags > 0 && tagMap.size() > maxTags) {
			CollectionsEx.forEach(CollectionsEx.min(tagMap.entrySet(),
					Comparator.comparing(Map.Entry::getKey), maxTags), row.tags::put);
		} else {
			row.tags = tagMap;
		}
		return row;
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

	private static HashMap<String, ArrayList<TagValue>>
			getTags(HashMap<String, HashMap<String, MetricValue>> tagMap) {
		HashMap<String, ArrayList<TagValue>> tags = new HashMap<>();
		BiConsumer<String, HashMap<String, MetricValue>> mainAction = (tagName, valueMap) -> {
			ArrayList<TagValue> tagValues = new ArrayList<>();
			BiConsumer<String, MetricValue> action = (value, metric) -> {
				TagValue tagValue = new TagValue();
				tagValue.value = value;
				tagValue.count = metric.getCount();
				tagValue.sum = metric.getSum();
				tagValue.max = metric.getMax();
				tagValue.min = metric.getMin();
				tagValue.sqr = metric.getSqr();
				tagValues.add(tagValue);
			};
			if (maxTagValues > 0 && valueMap.size() > maxTagValues) {
				CollectionsEx.forEach(CollectionsEx.max(valueMap.entrySet(),
						Comparator.comparingLong(metricValue ->
						metricValue.getValue().getCount()), maxTagValues), action);
			} else {
				valueMap.forEach(action);
			}
			tags.put(tagName, tagValues);
		};
		if (maxTags > 0 && tagMap.size() > maxTags) {
			CollectionsEx.forEach(CollectionsEx.min(tagMap.entrySet(),
					Comparator.comparing(Map.Entry::getKey), maxTags), mainAction);
		} else {
			tagMap.forEach(mainAction);
		}
		return tags;
	}

	private static void removeStale(SecondaryIndex<Integer, ?, ?> sk,
			Integer before, Integer after) {
		try (EntityCursor<Integer> cursor = sk.keys(MIN_INT, true, before, true)) {
			while (cursor.next() != null) {
				cursor.delete();
			}
		}
		try (EntityCursor<Integer> cursor = sk.keys(after, true, MAX_INT, true)) {
			while (cursor.next() != null) {
				cursor.delete();
			}
		}
	}

	private static HashSet<String> getMetricNames(List<String> databaseNames) {
		HashSet<String> metricNames = new HashSet<>();
		for (String s : databaseNames) {
			if (s.startsWith("persist#")) {
				metricNames.add(s.substring(8, s.indexOf('#', 8)));
			}
		}
		return metricNames;
	}

	private static int serverId, expire, tagsExpire, maxTags, maxTagValues,
			maxTagCombinations, maxMetricLen, maxTagNameLen, maxTagValueLen;
	private static boolean verbose;

	private void insert(HashMap<String, ArrayList<Row>> rowsMap) {
		rowsMap.forEach((name, rows) -> {
			PrimaryIndex<Long, Row> index = getStore(name).
					getPrimaryIndex(Long.class, Row.class);
			for (Row row : rows) {
				index.put(row);
			}
		});
	}

	private List<String> getDatabaseNames() {
		return env_.getDatabaseNames();
	}

	private HashSet<String> getMetricNames() {
		return getMetricNames(env_.getDatabaseNames());
	}

	private void minutely(int minute) {
		// Insert aggregation-during-collection metrics
		HashMap<String, ArrayList<Row>> rowsMap = new HashMap<>();
		for (MetricEntry entry : Metric.removeAll()) {
			Row row = row(entry.getTagMap(), minute, entry.getCount(),
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
		List<String> databaseNames = getDatabaseNames();
		ArrayList<String> prefixesToRemove = new ArrayList<>();
		for (String name : getMetricNames(databaseNames)) {
			if (name.startsWith("_meta.")) {
				continue;
			}
			EntityStore store = getStore(name);
			long count = store.getPrimaryIndex(Long.class, Row.class).count();
			if (count == 0) {
				// Remove disappeared metric collections
				store.close();
				storeCache.remove(name);
				prefixesToRemove.add("persist#" + name + "#");
				if (name.startsWith("_quarter.")) {
					// Remove disappeared quarterly metric from meta collections
					String minuteName = name.substring(9);
					aggregatedPk.delete(minuteName);
					tagsNameSk.delete(minuteName);
					allTagsPk.delete(minuteName);
				}
			} else {
				// Will be inserted next minute
				Metric.put("metric.size", count, "name", name);
			}
		}
		for (String databaseName : databaseNames) {
			for (String prefix : prefixesToRemove) {
				if (databaseName.startsWith(prefix)) {
					env_.removeDatabase(null, databaseName);
				}
			}
		}
		env_.flushLog(false);
	}

	private void quarterly(int quarter) {
		Integer removeBefore = Integer.valueOf(quarter * 15 - expire);
		Integer removeAfter = Integer.valueOf(quarter * 15 + expire);
		Integer removeBeforeQuarter = Integer.valueOf(quarter - expire);
		Integer removeAfterQuarter = Integer.valueOf(quarter + expire);

		removeStale(tagsTimeSk, Integer.valueOf(quarter - tagsExpire),
				Integer.valueOf(quarter + tagsExpire));
		// Scan minutely collections
		for (String name : getMetricNames()) {
			if (name.startsWith("_meta.") || name.startsWith("_quarter.")) {
				continue;
			}
			EntityStore store = getStore(name);
			PrimaryIndex<Long, Row> pk = store.getPrimaryIndex(Long.class, Row.class);
			SecondaryIndex<Integer, Long, Row> sk =
					store.getSecondaryIndex(pk, Integer.class, "time");
			removeStale(sk, removeBefore, removeAfter);
			EntityStore quarterStore = getStore("_quarter." + name);
			PrimaryIndex<Long, Row> quarterPk = quarterStore.
					getPrimaryIndex(Long.class, Row.class);
			// Aggregate to quarter
			Aggregated aggregated = aggregatedPk.get(name);
			int start = aggregated == null ? quarter - expire : aggregated.time;
			for (int i = start + 1; i <= quarter; i ++) {
				HashMap<HashMap<String, String>, MetricValue> result = new HashMap<>();
				for (Row row : sk.entities(Integer.valueOf(i * 15 - 14),
						true, Integer.valueOf(i * 15), true)) {
					// Aggregate to "_quarter.*"
					MetricValue newValue = new MetricValue(row.count,
							row.sum, row.max, row.min, row.sqr);
					MetricValue value = result.get(row.tags);
					if (value == null) {
						result.put(row.tags, newValue);
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
				BiConsumer<HashMap<String, String>, MetricValue> action = (tags, value) -> {
					// {"_quarter": i}, but not {"_quarter": quarter} !
					quarterPk.put(row(tags, i_, value.getCount(), value.getSum(),
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
				// Aggregate to "_meta.tags_quarter"
				tagMap.forEach((tagKey, tagValue) -> {
					Metric.put("metric.tags.values", tagValue.size(), "name", name, "key", tagKey);
				});
				QuarterTags quarterTags = new QuarterTags();
				quarterTags.name = name;
				// {"_quarter": i}, but not {"_quarter": quarter} !
				quarterTags.time = i;
				quarterTags.tags = getTags(tagMap);
				tagsPk.put(quarterTags);
			}
			aggregated = new Aggregated();
			aggregated.name = name;
			aggregated.time = quarter;
			aggregatedPk.put(aggregated);
		}
		// Scan quarterly collections
		for (String name : getMetricNames()) {
			if (!name.startsWith("_quarter.")) {
				continue;
			}
			// Remove stale
			EntityStore store = getStore(name);
			PrimaryIndex<Long, Row> pk = store.getPrimaryIndex(Long.class, Row.class);
			SecondaryIndex<Integer, Long, Row> sk =
					store.getSecondaryIndex(pk, Integer.class, "time");
			removeStale(sk, removeBeforeQuarter, removeAfterQuarter);
			// Aggregate "_meta.tags_quarter" to "_meta.tags_all";
			String minuteName = name.substring(9);
			HashMap<String, HashMap<String, MetricValue>> tagMap = new HashMap<>();
			EntityCursor<QuarterTags> quarterTagss = tagsNameSk.
					entities(minuteName, true, minuteName, true);
			for (QuarterTags quarterTags : quarterTagss) {
				quarterTags.tags.forEach((tagKey, tagValues) -> {
					for (TagValue tagValue : tagValues) {
						putTagValue(tagMap, tagKey, tagValue.value,
								new MetricValue(tagValue.count, tagValue.sum,
								tagValue.max, tagValue.min, tagValue.sqr));
					}
				});
			}
			AllTags allTags = new AllTags();
			allTags.name = minuteName;
			allTags.tags = getTags(tagMap);
			allTagsPk.put(allTags);
		}
		env_.flushLog(false);
	}

	private AtomicBoolean interrupted = new AtomicBoolean(false);
	private ConcurrentHashMap<String, Future<EntityStore>>
			storeCache = new ConcurrentHashMap<>();
	private PrimaryIndex<String, Aggregated> aggregatedPk;
	private PrimaryIndex<Long, QuarterTags> tagsPk;
	private SecondaryIndex<String, Long, QuarterTags> tagsNameSk;
	private SecondaryIndex<Integer, Long, QuarterTags> tagsTimeSk;
	private PrimaryIndex<String, AllTags> allTagsPk;
	private volatile Environment env_;
	private volatile DatagramSocket socket_;

	public AtomicBoolean getInterrupted() {
		return interrupted;
	}

	public Environment getEnv() {
		return env_;
	}

	public DatagramSocket getSocket() {
		return socket_;
	}

	public EntityStore getStore(String name) {
		Future<EntityStore> f = storeCache.get(name);
		if (f == null) {
			FutureTask<EntityStore> ft = new FutureTask<>(() ->
					new EntityStore(env_, name, STORE_CONFIG));
			f = storeCache.putIfAbsent(name, ft);
			if (f == null) {
				f = ft;
				ft.run();
			}
		}
		try {
			return f.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {
		ExecutorService executor = Executors.newCachedThreadPool();
		ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
		timer.scheduleAtFixedRate(Runnables.wrap(new
				ManagementMonitor("metric.sleepycat")), 0, 5, TimeUnit.SECONDS);

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
		Runnable minutely = null;

		File dataDir = new File(Conf.getAbsolutePath("data"));
		dataDir.mkdir();
		try (
			Environment env = new Environment(dataDir,
					new EnvironmentConfig().setAllowCreate(true));
			Closeable closeable = () -> {
				for (Future<EntityStore> f : storeCache.values()) {
					try {
						f.get().close();
					} catch (InterruptedException | ExecutionException e) {
						throw new RuntimeException(e);
					}
				}
			};
			DatagramSocket socket = new DatagramSocket(new
					InetSocketAddress(host, port));
		) {
			env_ = env;
			socket_ = socket;
			aggregatedPk = getStore("_meta.aggregated").
					getPrimaryIndex(String.class, Aggregated.class);
			EntityStore tagsQuarter = getStore("_meta.tags_quarter");
			tagsPk = tagsQuarter.getPrimaryIndex(Long.class, QuarterTags.class);
			tagsNameSk = tagsQuarter.getSecondaryIndex(tagsPk, String.class, "name");
			tagsTimeSk = tagsQuarter.getSecondaryIndex(tagsPk, Integer.class, "time");
			allTagsPk = getStore("_meta.tags_all").getPrimaryIndex(String.class, AllTags.class);
			minutely = Runnables.wrap(() -> {
				int minute = currentMinute.incrementAndGet();
				minutely(minute);
				if (serverId == 0 && !interrupted.get() && minute % 15 == quarterDelay) {
					// Skip "quarterly" when shutdown
					quarterly(minute / 15);
				}
			});
			timer.scheduleAtFixedRate(minutely, Time.MINUTE - start % Time.MINUTE,
					Time.MINUTE, TimeUnit.MILLISECONDS);

			Log.i("Metric Collector Started on UDP " + host + ":" + port);
			while (!interrupted.get()) {
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

				HashMap<String, ArrayList<Row>> rowsMap = new HashMap<>();
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
					executor.execute(Runnables.wrap(() -> insert(rowsMap)));
				}
			}
		} catch (IOException e) {
			Log.w(e.getMessage());
		} catch (Error | RuntimeException e) {
			Log.e(e);
		}
		// Do not do Mongo operations in main thread (may be interrupted)
		if (minutely != null) {
			executor.execute(minutely);
		}
		Runnables.shutdown(executor);
		Runnables.shutdown(timer);

		Log.i("Metric Collector Stopped");
	}
}