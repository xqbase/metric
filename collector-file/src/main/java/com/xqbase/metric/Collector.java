package com.xqbase.metric;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.zip.GZIPOutputStream;
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
import com.xqbase.util.concurrent.CountLock;
import com.xqbase.util.concurrent.LockMap;

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

public class Collector {
	private static final int MAX_BUFFER_SIZE = 1048576;
	private static final int MAX_METRIC_LEN = 64;

	private static double __(String s) {
		double d = Numbers.parseDouble(s);
		return Double.isNaN(d) ? 0 : d;
	}

	private static String decode(String s, int limit) {
		String result = Strings.decodeUrl(s);
		return limit > 0 ? Strings.truncate(result, limit) : result;
	}

	private static Service service = new Service();
	private static LockMap<NameTime> lockMap = new LockMap<>();
	private static String dataDir;
	private static int expire, expireQuarter, tagsExpire, maxTags, maxTagValues,
			maxTagCombinations, maxTagNameLen, maxTagValueLen;
	private static boolean verbose;
	private static volatile Map<String, long[]> namesCache = Collections.emptyMap();

	private static String dir(String name) {
		String dir = dataDir + name;
		new File(dir).mkdirs();
		return dir + "/";
	}

	private static void insert(NameTime key, StringBuilder sb) {
		long t = System.currentTimeMillis();
		CountLock lock = lockMap.acquire(key);
		lock.lock();
		try (FileOutputStream out = new
				FileOutputStream(dir(key.name) + key.time, true)) {
			out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			Log.e(e);
		} finally {
			lock.unlock();
			lockMap.release(key, lock);
			Metric.put("metric.file.elapsed", System.currentTimeMillis() - t,
					"command", "insert", "name", key.name);
		}
	}

	private static void delete(String name, int time) {
		String prefix = dataDir + name + "/";
		String[] filenames = new File(dataDir + name).list();
		if (filenames == null) {
			return;
		}
		long t = System.currentTimeMillis();
		for (String filename : filenames) {
			String f = filename;
			if (f.endsWith(".gz")) {
				f = f.substring(0, f.length() - 3);
			}
			if (Numbers.parseInt(f) <= time) {
				new File(prefix + filename).delete();
			}
		}
		Metric.put("metric.file.elapsed", System.currentTimeMillis() - t,
				"command", "delete", "name", name);
	}

	private static long getSize(String name) {
		File[] files = new File(dataDir + name).listFiles();
		if (files == null) {
			return 0;
		}
		long size = 0;
		long t = System.currentTimeMillis();
		for (File file : files) {
			size += file.length();
		}
		Metric.put("metric.file.elapsed", System.currentTimeMillis() - t,
				"command", "size", "name", name);
		return size;
	}

	private static Map<String, long[]> getNames() {
		Map<String, long[]> names = new HashMap<>();
		for (String name : new File(dataDir).list()) {
			if (name.startsWith("_tags_quarter.") || name.startsWith("_meta.")) {
				continue;
			}
			if (name.startsWith("_quarter.")) {
				names.computeIfAbsent(name.substring(9),
						k -> new long[] {0, 0, 0})[1] = getSize(name);
			} else {
				names.computeIfAbsent(name,
						k -> new long[] {0, 0, 0})[0] = getSize(name);
			}
		}
		Properties p = new Properties();
		try (FileInputStream in = new FileInputStream(dataDir +
				"_meta.aggregated.properties")) {
			p.load(in);
		} catch (IOException e) {
			Log.w(e.getMessage());
		}
		p.forEach((k, v) -> {
			long[] value = names.get(k);
			if (value != null) {
				value[2] = Numbers.parseInt((String) v);
			}
		});
		namesCache = names;
		return names;
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

	private static void minutely(int minute) {
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
		metricMap.forEach((name, sb) -> {
			NameTime key = new NameTime();
			key.name = name;
			key.time = minute;
			insert(key, sb);
		});
		// Put metric size
		namesCache.forEach((name, size) -> {
			if (size[0] > 0) {
				Metric.put("metric.size", size[0], "name", name);
			}
			if (size[1] > 0) {
				Metric.put("metric.size", size[1], "name", "_quarter." + name);
			}
		});
	}

	private static void quarterly(int quarter) {
		Properties aggregatedProp = new Properties();
		Properties tagsProp = new Properties();

		getNames().forEach((name, sizeAndAggregated) -> {
			// 1. Delete _tags_quarter.*
			delete("_tags_quarter." + name, quarter - tagsExpire);
			// 2. Delete minute and quarter data
			if (sizeAndAggregated[0] == 0 && sizeAndAggregated[1] == 0) {
				// 2.1 Delete folder if metric data does not exist
				new File(dataDir + name).delete();
				new File(dataDir + "_quarter." + name).delete();
				new File(dataDir + "_tags_quarter." + name).delete();
				return;
			}
			int aggregated = (int) sizeAndAggregated[2];
			delete(name, quarter * 15 - expire);
			delete("_quarter." + name, quarter - expire);
			// 3. Aggregate minute to quarter
			int start = aggregated == 0 ? quarter - expireQuarter : aggregated;
			for (int i = start + 1; i <= quarter; i ++) {
				Map<Map<String, String>, MetricValue> accMetricMap = new HashMap<>();
				int i15 = i * 15;
				long t = System.currentTimeMillis();
				for (int j = i15 - 14; j <= i15; j ++) {
					String filename = dataDir + name + "/" + j;
					File file = new File(filename);
					if (!file.exists()) {
						continue;
					}
					File tmp = new File(dataDir + "tmp.gz");
					try (
						BufferedReader in = new BufferedReader(new FileReader(file));
						PrintStream out = new PrintStream(new
								GZIPOutputStream(new FileOutputStream(tmp)));
					) {
						String line;
						while ((line = in.readLine()) != null) {
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
							// 4. Compress minute data
							out.println(line);
						}
					} catch (IOException e) {
						Log.e(e);
					}
					tmp.renameTo(new File(filename + ".gz"));
					// 4'. Remove uncompressed
					new File(filename).delete();
				}
				Metric.put("metric.file.elapsed", System.currentTimeMillis() - t,
						"command", "aggregate", "name", name);
				if (accMetricMap.isEmpty()) {
					continue;
				}
				int combinations = accMetricMap.size();
				Metric.put("metric.tags.combinations", combinations, "name", name);
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
				t = System.currentTimeMillis();
				File tmp = new File(dataDir + "tmp.gz");
				try (GZIPOutputStream out = new
						GZIPOutputStream(new FileOutputStream(tmp))) {
					out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
				} catch (IOException e) {
					Log.e(e);
				}
				String quarterName = "_quarter." + name;
				tmp.renameTo(new File(dir(quarterName) + i + ".gz"));
				Metric.put("metric.file.elapsed", System.currentTimeMillis() - t,
						"command", "insert", "name", quarterName);
				// 5. Aggregate to "_tags_quarter.*"
				tagMap.forEach((tagKey, tagValue) -> {
					Metric.put("metric.tags.values", tagValue.size(),
							"name", name, "key", tagKey);
				});
				// {"_quarter": i}, but not {"_quarter": quarter} !
				try (FileOutputStream out = new
						FileOutputStream(dir("_tags_quarter." + name) + i)) {
					out.write(new JSONObject(limit(tagMap)).toString().
							getBytes(StandardCharsets.UTF_8));
				} catch (IOException e) {
					Log.e(e);
				}
			}
			// 6. Set "aggregated"
			aggregatedProp.setProperty(name, "" + quarter);
			// 7. Aggregate "_tags_quarter" to "_meta.tags.properties";
			File[] files = new File(dataDir + "_tags_quarter." + name).listFiles();
			if (files == null) {
				return;
			}
			Map<String, Map<String, MetricValue>> tagMap = new HashMap<>();
			for (File file : files) {
				byte[] b;
				try (FileInputStream in = new FileInputStream(file)) {
					b = new byte[in.available()];
					in.read(b);
				} catch (IOException e) {
					Log.e(e);
					continue;
				}
				JSONObject json = new JSONObject(new String(b, StandardCharsets.UTF_8));
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
			}
			tagsProp.setProperty(name, new JSONObject(limit(tagMap)).toString());
		});

		try (FileOutputStream out = new FileOutputStream(dataDir +
				"_meta.aggregated.properties")) {
			aggregatedProp.store(out, null);
		} catch (IOException e) {
			Log.e(e);
		}
		try (FileOutputStream out = new FileOutputStream(dataDir +
				"_meta.tags.properties")) {
			tagsProp.store(out, null);
		} catch (IOException e) {
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

		Properties p = Conf.load("Collector");
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		String host = p.getProperty("host");
		host = host == null || host.isEmpty() ? "0.0.0.0" : host;
		expire = Numbers.parseInt(p.getProperty("expire"), 2880);
		expireQuarter = (expire + 14) / 15;
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
		Runnable minutely = null;
		try (
			DatagramSocket socket = new DatagramSocket(new
					InetSocketAddress(host, port));
			ManagementMonitor monitor = new ManagementMonitor("metric.server");
		) {
			dataDir = Conf.getAbsolutePath("data");
			new File(dataDir).mkdirs();
			dataDir += '/';
			getNames();
			Dashboard.startup(dataDir);

			minutely = Runnables.wrap(() -> {
				int minute = currentMinute.incrementAndGet();
				minutely(minute);
				if (!service.isInterrupted() && minute % 15 == quarterDelay) {
					// Skip "quarterly" when shutdown
					quarterly(minute / 15);
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
					Metric.put("metric.throughput", len, "remote_addr", remoteAddr);
					countMap.forEach((name, value) -> {
						Metric.put("metric.rows", value.intValue(), "name", name,
								"remote_addr", remoteAddr);
					});
				} else {
					Metric.put("metric.throughput", len);
					countMap.forEach((name, value) -> {
						Metric.put("metric.rows", value.intValue(), "name", name);
					});
				}
				// Insert aggregation-before-collection metrics
				if (!metricMap.isEmpty()) {
					executor.execute(Runnables.wrap(() -> {
						metricMap.forEach((key, sb) -> {
							insert(key, sb);
						});
					}));
				}
			}
		} catch (IOException e) {
			Log.w(e.getMessage());
		} catch (Error | RuntimeException e) {
			Log.e(e);
		}
		Runnables.shutdown(timer);
		// Do not do file operations in main thread (may be interrupted)
		if (minutely != null) {
			executor.execute(minutely);
		}
		Runnables.shutdown(executor);
		Dashboard.shutdown();

		Log.i("Metric Collector Stopped");
		Conf.closeLogger(Log.getAndSet(logger));
		service.shutdown();
	}
}