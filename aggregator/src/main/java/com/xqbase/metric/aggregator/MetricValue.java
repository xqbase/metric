package com.xqbase.metric.aggregator;

public class MetricValue implements Cloneable {
	private int count;
	private double sum, max, min;

	private MetricValue(int count, double sum, double max, double min) {
		this.count = count;
		this.sum = sum;
		this.max = max;
		this.min = min;
	}

	public MetricValue(double value) {
		this(1, value, value, value);
	}

	public MetricValue(MetricValue old, double value) {
		this(old.count + 1, old.sum + value,
				Math.max(old.max, value), Math.min(old.min, value));
	}

	@Override
	public MetricValue clone() {
		return new MetricValue(count, sum, max, min);
	}

	public int getCount() {
		return count;
	}

	public double getSum() {
		return sum;
	}

	public double getMax() {
		return max;
	}

	public double getMin() {
		return min;
	}
}