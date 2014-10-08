package com.xqbase.metric.client;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;

import com.sun.management.OperatingSystemMXBean;
import com.xqbase.metric.common.Metric;

public class ManagementMonitor implements Runnable {
	private static double MB(long value) {
		return (double) value / 1048576;
	}

	private static double PERCENT(long dividend, long divisor) {
		return divisor == 0 ? 0 : (double) dividend * 100 / divisor; 
	}

	private String cpu, threads, memoryMB, memoryPercent;

	public ManagementMonitor(String prefix) {
		cpu = prefix + ".cpu";
		threads = prefix + ".threads";
		memoryMB = prefix + ".memory.mb";
		memoryPercent = prefix + ".memory.percent";
	}

	@Override
	public void run() {
		ThreadMXBean thread = ManagementFactory.getThreadMXBean();
		Metric.put(threads, thread.getThreadCount(), "type", "total");
		Metric.put(threads, thread.getDaemonThreadCount(), "type", "daemon");

		// Runtime rt = Runtime.getRuntime();
		// Metric.add(memory, inMB(rt.totalMemory() - rt.freeMemory()), "type", "heap_used");
		MemoryMXBean memory_ = ManagementFactory.getMemoryMXBean();
		MemoryUsage heap = memory_.getHeapMemoryUsage();
		Metric.put(memoryMB, MB(heap.getCommitted()), "type", "heap_committed");
		Metric.put(memoryMB, MB(heap.getUsed()), "type", "heap_used");
		Metric.put(memoryPercent, PERCENT(heap.getUsed(), heap.getMax()), "type", "heap");
		MemoryUsage nonHeap = memory_.getNonHeapMemoryUsage();
		Metric.put(memoryMB, MB(nonHeap.getCommitted()), "type", "non_heap_committed");
		Metric.put(memoryMB, MB(nonHeap.getUsed()), "type", "non_heap_used");
		Metric.put(memoryPercent, PERCENT(nonHeap.getUsed(), nonHeap.getMax()), "type", "non_heap");

		java.lang.management.OperatingSystemMXBean os_ =
				ManagementFactory.getOperatingSystemMXBean();
		if (!(os_ instanceof OperatingSystemMXBean)) {
			return;
		}
		OperatingSystemMXBean os = (OperatingSystemMXBean) os_;
		long totalPhysical = os.getTotalPhysicalMemorySize();
		long usedPhysical = totalPhysical - os.getFreePhysicalMemorySize();
		Metric.put(memoryMB, MB(usedPhysical), "type", "physical");
		Metric.put(memoryPercent, PERCENT(usedPhysical, totalPhysical),
				"type", "physical_memory");
		long totalSwap = os.getTotalSwapSpaceSize();
		long usedSwap = totalSwap - os.getFreeSwapSpaceSize();
		Metric.put(memoryMB, MB(usedSwap), "type", "swap");
		Metric.put(memoryMB, MB(os.getCommittedVirtualMemorySize()),
				"type", "process_committed");
		Metric.put(memoryPercent, PERCENT(usedSwap, totalSwap),
				"type", "swap_space");

		double system = os.getSystemCpuLoad();
		if (system >= 0) {
			Metric.put(cpu, system * 100, "type", "system");
		}
		double process = os.getProcessCpuLoad();
		if (process >= 0) {
			Metric.put(cpu, process * 100, "type", "process");
		}
	}
}