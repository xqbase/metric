package com.xqbase.metric.common;

public class MetricValue implements Cloneable {
	private static double __(double d) {
		return Double.isNaN(d) ? 0 : d;
	}

	private long count;
	private double sum, max, min, sqr;

	private void set(long count, double sum, double max, double min, double sqr) {
		this.count = count;
		this.sum = sum;
		this.max = max;
		this.min = min;
		this.sqr = sqr;
	}

	MetricValue() { /* for Kryo Deserialization */ }

	public MetricValue(long count, double sum, double max, double min, double sqr) {
		set(count, __(sum), __(max), __(min), __(sqr));
	}

	public MetricValue(double value) {
		double d = __(value);
		set(1, d, d, d, d * d);
	}

	public MetricValue(MetricValue old, double value) {
		double d = __(value);
		set(old.count + 1, old.sum + d, Math.max(old.max, d),
				Math.min(old.min, d), old.sqr + d * d);
	}

	@Override
	public MetricValue clone() {
		MetricValue another = new MetricValue();
		another.set(count, sum, max, min, sqr);
		return another;
	}

	public void add(MetricValue value) {
		count += value.count;
		sum += value.sum;
		max = Math.max(max, value.max);
		min = Math.min(min, value.min);
		sqr += value.sqr;
	}

	public long getCount() {
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
		return count == 0 ? 0 : sum / count;
	}

	public double getStd() {
		if (count == 0) {
			return 0;
		}
		double base = sqr * count - sum * sum;
		return base < 0 ? 0 : Math.sqrt(base) / count;
	}
}