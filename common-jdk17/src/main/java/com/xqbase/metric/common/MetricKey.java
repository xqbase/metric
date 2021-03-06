package com.xqbase.metric.common;

import java.util.HashMap;
import java.util.Map;

public class MetricKey {
	public static void putTagMap(Map<String, String> tagMap,
			String... tagPairs) {
		for (int i = 0; i < tagPairs.length - 1; i += 2) {
			String key = tagPairs[i];
			String value = tagPairs[i + 1];
			if (key != null && value != null) {
				tagMap.put(key, value);
			}
		}
	}

	private String name;
	private Map<String, String> tagMap;

	public MetricKey(String name, Map<String, String> tagMap) {
		this.name = name;
		this.tagMap = tagMap;
	}

	public MetricKey(String name, String... tagPairs) {
		this.name = name;
		tagMap = new HashMap<>();
		putTagMap(tagMap, tagPairs);
	}

	public String getName() {
		return name;
	}

	public Map<String, String> getTagMap() {
		return tagMap;
	}

	@Override
	public boolean equals(Object obj) {
		MetricKey metricKey = (MetricKey) obj;
		return metricKey.name.equals(name) && metricKey.tagMap.equals(tagMap);
	}

	@Override
	public int hashCode() {
		return name.hashCode() + tagMap.hashCode();
	}
}