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

class FileName {
	String name;
	int time;

	@Override
	public int hashCode() {
		return name.hashCode() * 31 + time;
	}

	@Override
	public boolean equals(Object obj) {
		FileName o = (FileName) obj;
		return time == o.time && name.equals(o.name);
	}
}

class MetricRow {
	Map<String, String> tags;
	MetricValue value;
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

	private static void put(Map<FileName, List<MetricRow>> rowsMap,
			String name, int time, Map<String, String> tags,
			long count, double sum, double max, double min, double sqr) {
		MetricRow row = new MetricRow();
		if (maxTags > 0 && tags.size() > maxTags) {
			row.tags = new HashMap<>();
			CollectionsEx.forEach(CollectionsEx.min(tags.entrySet(),
					Comparator.comparing(Map.Entry::getKey), maxTags), row.tags::put);
		} else {
			row.tags = tags;
		}
		row.value = new MetricValue(count, sum, max, min, sqr);
		FileName filename = new FileName();
		filename.name = name;
		filename.time = time;
		rowsMap.computeIfAbsent(filename, k -> new ArrayList<>()).add(row);
	}

	private static String dir(String name) {
		String dir = dataDir + name;
		new File(dir).mkdirs();
		return dir + "/";
	}

	private static void insert(PrintStream out, List<MetricRow> rows) {
		for (MetricRow row : rows) {
			StringBuilder sb = new StringBuilder();
			MetricValue value = row.value;
			sb.append(value.getCount()).append('/').
					append(value.getSum()).append('/').
					append(value.getMax()).append('/').
					append(value.getMin()).append('/').
					append(value.getSqr());
			if (!row.tags.isEmpty()) {
				int question = sb.length();
				row.tags.forEach((k, v) -> {
					sb.append('&').append(Strings.encodeUrl(k)).
							append('=').append(Strings.encodeUrl(v));
				});
				sb.setCharAt(question, '?');
			}
			out.println(sb);
		}
	}

	private static void insert(Map<FileName, List<MetricRow>> rowsMap) {
		rowsMap.forEach((k, v) -> {
			if (v.isEmpty()) {
				return;
			}
			CountLock lock = lockMap.acquire(k);
			lock.lock();
			try (PrintStream out = new PrintStream(new
					FileOutputStream(dir(k.name) + k.time, true))) {
				insert(out, v);
			} catch (IOException e) {
				Log.e(e);
			} finally {
				lock.unlock();
			}
		});
	}

	private static Service service = new Service();
	private static LockMap<FileName> lockMap = new LockMap<>();
	private static String dataDir;
	private static int expire, tagsExpire, maxTags, maxTagValues,
			maxTagCombinations, maxTagNameLen, maxTagValueLen;
	private static boolean verbose;

	private static int getSize(String name) {
		File[] files = new File(dataDir + name).listFiles();
		if (files == null) {
			return 0;
		}
		int size = 0;
		long t = System.currentTimeMillis();
		for (File file : files) {
			size += file.length();
		}
		Metric.put("metric.file.elapsed", System.currentTimeMillis() - t,
				"command", "size", "name", name);
		return size;
	}

	private static Map<String, int[]> getNames(boolean aggregated) {
		Map<String, int[]> names = new HashMap<>();
		for (String filename : new File(dataDir).list()) {
			if (filename.startsWith("_tags_quarter.")) {
				continue;
			}
			if (filename.startsWith("_quarter.")) {
				String name = filename.substring(9);
				names.computeIfAbsent(name, k -> new int[3])[1] =
						getSize(filename);
				continue;
			}
			if (filename.equals("Aggregated.properties")) {
				if (!aggregated) {
					continue;
				}
				Properties p = new Properties();
				try (FileInputStream in = new FileInputStream(dataDir +
						"Aggregated.properties")) {
					p.load(in);
				} catch (IOException e) {
					Log.e(e);
				}
				p.forEach((k, v) -> {
					int[] value = names.get(k);
					if (value != null) {
						value[2] = Numbers.parseInt((String) v);
					}
				});
				continue;
			}
			if (filename.equals("Tags.properties")) {
				continue;
			}
			names.computeIfAbsent(filename,
					k -> new int[3])[0] = getSize(filename);
		}
		return names;
	}

	private static void minutely(int minute) {
		// Insert aggregation-during-collection metrics
		Map<FileName, List<MetricRow>> rowsMap = new HashMap<>();
		for (MetricEntry entry : Metric.removeAll()) {
			put(rowsMap, entry.getName(), minute, entry.getTagMap(), entry.getCount(),
					entry.getSum(), entry.getMax(), entry.getMin(), entry.getSqr());
		}
		insert(rowsMap);
		// Calculate metric size
		getNames(false).forEach((name, size) -> {
			Metric.put("metric.size", size[0], "name", name);
			Metric.put("metric.size", size[1], "name", "_quarter." + name);
		});
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

	private static void quarterly(int quarter) {
		Properties aggregatedProp = new Properties();
		Properties tagsProp = new Properties();

		getNames(true).forEach((name, sizeAndAggregated) -> {
			// 1. Delete _tags_quarter.*
			delete("_tags_quarter." + name, quarter - tagsExpire);
			// 2. Delete minute and quarter data
			int minuteSize = sizeAndAggregated[0];
			int quarterSize = sizeAndAggregated[1];
			if (minuteSize == 0 && quarterSize == 0) {
				// 2.1 Delete folder if metric data does not exist
				new File(dataDir + name).delete();
				new File(dataDir + "_quarter." + name).delete();
				new File(dataDir + "_tags_quarter." + name).delete();
				return;
			}
			int aggregated = sizeAndAggregated[2];
			delete(name, quarter * 15 - expire);
			delete("_quarter." + name, quarter - expire);
			// 3. Aggregate minute to quarter
			int start = aggregated == 0 ? quarter - expire : aggregated;
			for (int i = start + 1; i <= quarter; i ++) {
				List<MetricRow> rows = new ArrayList<>();
				Map<Map<String, String>, MetricValue> result = new HashMap<>();
				int i15 = i * 15;
				long t = System.currentTimeMillis();
				for (int j = i * 15 - 14; j <= i15; j ++) {
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
							MetricValue value = result.get(tags);
							if (value == null) {
								result.put(tags, newValue);
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
						"command", "compress", "name", name);
				// 3'. Aggregate to "_quarter.*"
				if (result.isEmpty()) {
					continue;
				}
				int combinations = result.size();
				Metric.put("metric.tags.combinations", combinations, "name", name);
				// 5. Aggregate to "_tags_quarter.*"
				Map<String, Map<String, MetricValue>> tagMap = new HashMap<>();
				BiConsumer<Map<String, String>, MetricValue> action = (tags, value) -> {
					// 3'. Aggregate to "_quarter.*"
					MetricRow row = new MetricRow();
					row.tags = tags;
					row.value = value;
					rows.add(row);
					// 5. Aggregate to "_tags_quarter.*"
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
				// 3'. Aggregate to "_quarter.*"
				t = System.currentTimeMillis();
				File tmp = new File(dataDir + "tmp.gz");
				try (PrintStream out = new PrintStream(new
						GZIPOutputStream(new FileOutputStream(tmp)))) {
					insert(out, rows);
				} catch (IOException e) {
					Log.e(e);
				}
				String quarterName = "_quarter." + name;
				tmp.renameTo(new File(dir(quarterName) + i + ".gz"));
				Metric.put("metric.file.elapsed", System.currentTimeMillis() - t,
						"command", "compress", "name", quarterName);
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
			// 7. Aggregate "_tags_quarter" to "Tags.properties";
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
				"Aggregated.properties")) {
			aggregatedProp.store(out, null);
		} catch (IOException e) {
			Log.e(e);
		}
		try (FileOutputStream out = new FileOutputStream(dataDir +
				"Tags.properties")) {
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

		dataDir = Conf.getAbsolutePath("data");
		new File(dataDir).mkdirs();
		dataDir += '/';
		Properties p = Conf.load("Collector");
		int port = Numbers.parseInt(p.getProperty("port"), 5514);
		String host = p.getProperty("host");
		host = host == null || host.isEmpty() ? "0.0.0.0" : host;
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
		Runnable minutely = null;
		try (
			DatagramSocket socket = new DatagramSocket(new
					InetSocketAddress(host, port));
			ManagementMonitor monitor = new ManagementMonitor("metric.server");
		) {
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

				Map<FileName, List<MetricRow>> rowsMap = new HashMap<>();
				Map<String, Integer> countMap = new HashMap<>();
				for (String line : baq.toString().split("\n")) {
					// Truncate tailing '\r'
					int length = line.length();
					if (length > 0 && line.charAt(length - 1) == '\r') {
						line = line.substring(0, length - 1);
					}
					// Parse name, aggregation, value and tags
					// <name>/<aggregation>/<value>[?<tag>=<value>[&...]]
					String[] paths;
					Map<String, String> tagMap = new HashMap<>();
					int index = line.indexOf('?');
					if (index < 0) {
						paths = line.split("/");
					} else {
						paths = line.substring(0, index).split("/");
						String query = line.substring(index + 1);
						for (String tag : query.split("&")) {
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
					}
					if (paths.length > 6) {
						// For aggregation-before-collection metric, insert immediately
						put(rowsMap, name, Numbers.parseInt(paths[1], currentMinute.get()),
								tagMap, Numbers.parseLong(paths[2]), __(paths[3]),
								__(paths[4]), __(paths[5]), __(paths[6]));
					} else {
						// For aggregation-during-collection metric, aggregate first
						Metric.put(name, __(paths[1]), tagMap);
					}
					Integer count = countMap.get(name);
					countMap.put(name, Integer.valueOf(count == null ?
							1 : count.intValue() + 1));
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
				if (!rowsMap.isEmpty()) {
					executor.execute(Runnables.wrap(() -> insert(rowsMap)));
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