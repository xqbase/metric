package com.xqbase.metric.agent;

import java.net.InetSocketAddress;
import java.util.Random;

import com.xqbase.metric.aggregator.Metric;

public class TestMetric {
	public static void main(String[] args) {
		Metric.startup(new InetSocketAddress("localhost", 5514));
		Random random = new Random();
		while (true) {
			Metric.put("test", random.nextInt(100), "r", "" + random.nextInt(100));
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		// Metric.shutdown();
	}
}