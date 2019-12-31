package com.xqbase.metric.client;

import java.net.InetSocketAddress;
import java.util.Random;

import com.xqbase.metric.common.Metric;

public class TestMetric {
	public static void main(String[] args) {
		MetricClient.startup(new InetSocketAddress("localhost", 5514));
		Random random = new Random();
		while (true) {
			Metric.put("test", random.nextInt(100), "r", "" + random.nextInt(100));
			Metric.put("test", Double.POSITIVE_INFINITY, "r", "+Infinity");
			Metric.put("test", Double.NEGATIVE_INFINITY, "r", "-Infinity");
			Metric.put("test", Double.NaN, "r", "NaN");
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		// Metric.shutdown();
	}
}