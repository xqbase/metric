package com.xqbase.metric.aggregator;

import java.util.HashMap;

public class MetricEntry {
	private MetricKey key;
	private MetricValue value;

	public MetricEntry(MetricKey key, MetricValue value) {
		this.key = key;
		this.value = value;
	}

	public String getName() {
		return key.getName();
	}

	public HashMap<String, String> getTagMap() {
		return key.getTagMap();
	}

	public int getCount() {
		return value.getCount();
	}

	public double getSum() {
		return value.getSum();
	}

	public double getMax() {
		return value.getMax();
	}

	public double getMin() {
		return value.getMin();
	}
}