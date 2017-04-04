package com.xqbase.metric.client;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

import com.sun.management.OperatingSystemMXBean;
import com.xqbase.metric.common.Metric;
import com.xqbase.metric.common.MetricKey;

public class ManagementMonitor implements Runnable {
	private static double MB(long value) {
		return (double) value / 1048576;
	}

	private static double PERCENT(long dividend, long divisor) {
		return divisor == 0 ? 0 : (double) dividend * 100 / divisor;
	}

	private static Map<String, String> getTagMap(String... tagPairs) {
		Map<String, String> tagMap = new HashMap<>();
		MetricKey.putTagMap(tagMap, tagPairs);
		return tagMap;
	}

	private String cpu, threads, memoryMB, memoryPercent;
	private Map<String, String> tagMap; 
	private ThreadMXBean thread = ManagementFactory.getThreadMXBean();
	private MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
	private OperatingSystemMXBean os = null;

	private void put(String name, double value, String... tagPairs) {
		Map<String, String> tagMap_ = new HashMap<>(tagMap);
		MetricKey.putTagMap(tagMap_, tagPairs);
		Metric.put(name, value, tagMap_);
	}

	public ManagementMonitor(String prefix, String... tagPairs) {
		this(prefix, getTagMap(tagPairs));
	}

	public ManagementMonitor(String prefix, Map<String, String> tagMap) {
		cpu = prefix + ".cpu";
		threads = prefix + ".threads";
		memoryMB = prefix + ".memory.mb";
		memoryPercent = prefix + ".memory.percent";
		java.lang.management.OperatingSystemMXBean os_ =
				ManagementFactory.getOperatingSystemMXBean();
		if (os_ instanceof OperatingSystemMXBean) {
			os = (OperatingSystemMXBean) os_;
		}
		this.tagMap = tagMap;
	}

	@Override
	public void run() {
		put(threads, thread.getThreadCount(), "type", "total");
		put(threads, thread.getDaemonThreadCount(), "type", "daemon");

		// Runtime rt = Runtime.getRuntime();
		// add(memory, MB(rt.totalMemory() - rt.freeMemory()), "type", "heap_used");
		MemoryUsage heap = memory.getHeapMemoryUsage();
		put(memoryMB, MB(heap.getCommitted()), "type", "heap_committed");
		put(memoryMB, MB(heap.getUsed()), "type", "heap_used");
		put(memoryPercent, PERCENT(heap.getUsed(), heap.getMax()), "type", "heap");
		MemoryUsage nonHeap = memory.getNonHeapMemoryUsage();
		put(memoryMB, MB(nonHeap.getCommitted()), "type", "non_heap_committed");
		put(memoryMB, MB(nonHeap.getUsed()), "type", "non_heap_used");
		// nonHeap.getMax() always returns -1 in Java 1.8
		// put(memoryPercent, PERCENT(nonHeap.getUsed(), nonHeap.getMax()), "type", "non_heap");

		if (os == null) {
			return;
		}
		long totalPhysical = os.getTotalPhysicalMemorySize();
		long usedPhysical = totalPhysical - os.getFreePhysicalMemorySize();
		put(memoryMB, MB(usedPhysical), "type", "physical");
		put(memoryPercent, PERCENT(usedPhysical, totalPhysical),
				"type", "physical_memory");
		long totalSwap = os.getTotalSwapSpaceSize();
		long usedSwap = totalSwap - os.getFreeSwapSpaceSize();
		put(memoryMB, MB(usedSwap), "type", "swap");
		put(memoryMB, MB(os.getCommittedVirtualMemorySize()),
				"type", "process_committed");
		put(memoryPercent, PERCENT(usedSwap, totalSwap), "type", "swap_space");

		put(cpu, Math.max(os.getSystemCpuLoad() * 100, 0), "type", "system");
		put(cpu, Math.max(os.getProcessCpuLoad() * 100, 0), "type", "process");
	}
}