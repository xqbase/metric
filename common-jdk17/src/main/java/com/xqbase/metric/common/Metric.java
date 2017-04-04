package com.xqbase.metric.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Metric {
	private static ConcurrentMap<MetricKey, MetricValue>
			map = new ConcurrentHashMap<>();

	private static void put(MetricKey key, double value) {
		MetricValue current;
		do {
			current = map.get(key);
		} while (current == null ? map.putIfAbsent(key, new MetricValue(value)) != null :
				!map.replace(key, current, new MetricValue(current, value)));
	}

	public static void put(String name, double value, Map<String, String> tagMap) {
		put(new MetricKey(name, tagMap), value);
	}

	public static void put(String name, double value, String... tagPairs) {
		put(new MetricKey(name, tagPairs), value);
	}

	public static List<MetricEntry> removeAll() {
		List<MetricEntry> metrics = new ArrayList<>();
		List<MetricKey> keys = new ArrayList<>(map.keySet());
		for (MetricKey key : keys) {
			MetricValue value = map.remove(key);
			if (value != null) {
				metrics.add(new MetricEntry(key, value));
			}
		}
		return metrics;
	}
}