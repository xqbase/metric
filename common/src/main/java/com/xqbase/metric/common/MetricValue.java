package com.xqbase.metric.common;

public class MetricValue implements Cloneable {
	private int count;
	private double sum, max, min, sqr;

	public MetricValue(int count, double sum, double max, double min, double sqr) {
		this.count = count;
		this.sum = sum;
		this.max = max;
		this.min = min;
		this.sqr = sqr;
	}

	public MetricValue(double value) {
		this(1, value, value, value, value * value);
	}

	public MetricValue(MetricValue old, double value) {
		this(old.count + 1, old.sum + value, Math.max(old.max, value),
				Math.min(old.min, value), old.sqr + value * value);
	}

	@Override
	public MetricValue clone() {
		return new MetricValue(count, sum, max, min, sqr);
	}

	public void add(MetricValue value) {
		count += value.count;
		sum += value.sum;
		max = Math.max(max, value.max);
		min = Math.min(min, value.min);
		sqr += value.sqr;
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

	public double getSqr() {
		return sqr;
	}

	public double getAvg() {
		return sum / count;
	}

	public double getStd() {
		if (count == 0) {
			return 0;
		}
		double base = sqr * count - sum * sum;
		return base < 0 ? 0 : Math.sqrt(base) / count;
	}
}