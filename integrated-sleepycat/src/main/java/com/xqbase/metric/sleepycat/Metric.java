package com.xqbase.metric.sleepycat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.xqbase.metric.common.MetricEntry;
import com.xqbase.metric.common.MetricKey;
import com.xqbase.metric.common.MetricValue;

public class Metric {
	private static ConcurrentHashMap<MetricKey, MetricValue>
			map = new ConcurrentHashMap<>();

	private static void put(MetricKey key, double value) {
		MetricValue current;
		do {
			current = map.get(key);
		} while (current == null ? map.putIfAbsent(key, new MetricValue(value)) != null :
				!map.replace(key, current, new MetricValue(current, value)));
	}

	public static void put(String name, double value, HashMap<String, String> tagMap) {
		put(new MetricKey(name, tagMap), value);
	}

	public static void put(String name, double value, String... tagPairs) {
		put(new MetricKey(name, tagPairs), value);
	}

	public static ArrayList<MetricEntry> removeAll() {
		ArrayList<MetricEntry> metrics = new ArrayList<>();
		ArrayList<MetricKey> keys = new ArrayList<>(map.keySet());
		for (MetricKey key : keys) {
			MetricValue value = map.remove(key);
			if (value != null) {
				metrics.add(new MetricEntry(key, value));
			}
		}
		return metrics;
	}
}