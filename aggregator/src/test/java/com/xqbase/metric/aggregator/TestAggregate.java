package com.xqbase.metric.aggregator;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.xqbase.metric.aggregator.Metric;
import com.xqbase.metric.aggregator.MetricEntry;

public class TestAggregate {
	private static final int THREADS = 16;

	public static void main(String[] args) {
		ExecutorService executor = Executors.newCachedThreadPool();
		long t = System.currentTimeMillis();
		final AtomicInteger latch = new AtomicInteger(THREADS);
		final Random random = new Random();
		for (int i = 0; i < THREADS; i ++) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					for (int j = 0; j < 1048576; j ++) {
						Metric.put("" + random.nextInt(1024), 1);
					}
					latch.decrementAndGet();
				}
			});
		}
		int count = 0;
		while (latch.get() > 0) {
			for (MetricEntry entry : Metric.removeAll()) {
				count += entry.getCount();
			}
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {/**/}
		}
		for (MetricEntry entry : Metric.removeAll()) {
			count += entry.getCount();
		}
		// Should be 16777216
		System.out.println("Get " + count +
				" in " + (System.currentTimeMillis() - t) + " ms");
		executor.shutdown();
	}
}