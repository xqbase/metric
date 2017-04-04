package com.xqbase.metric.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DeflaterOutputStream;

import com.xqbase.metric.common.Metric;
import com.xqbase.metric.common.MetricEntry;

public class MetricClient {
	private static final int MINUTE = 60000;
	private static final int MAX_PACKET_SIZE = 64000;

	private static String encode(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void send(DatagramSocket socket,
			InetSocketAddress[] addrs, String data) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DeflaterOutputStream dos = new
				DeflaterOutputStream(baos)) {
			dos.write(data.getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		byte[] b = baos.toByteArray();
		for (InetSocketAddress addr : addrs) {
			// Resolve "addr" every time
			DatagramPacket packet = new DatagramPacket(b, b.length,
					new InetSocketAddress(addr.getHostString(), addr.getPort()));
			socket.send(packet);
		}
	}

	static void send(InetSocketAddress[] addrs, int minute,
			List<MetricEntry> metrics) {
		StringBuilder packet = new StringBuilder();
		try (DatagramSocket socket = new DatagramSocket()) {
			for (MetricEntry metric : metrics) {
				StringBuilder row = new StringBuilder();
				row.append(encode(metric.getName())).append('/').
						append(minute).append('/').
						append(metric.getCount()).append('/').
						append(metric.getSum()).append('/').
						append(metric.getMax()).append('/').
						append(metric.getMin()).append('/').
						append(metric.getSqr());
				Map<String, String> tagMap = metric.getTagMap();
				if (!tagMap.isEmpty()) {
					int question = row.length();
					for (Map.Entry<String, String> tag : tagMap.entrySet()) {
						row.append('&').append(encode(tag.getKey())).
								append('=').append(encode(tag.getValue()));
					}
					row.setCharAt(question, '?');
				}
				if (packet.length() + row.length() >= MAX_PACKET_SIZE) {
					send(socket, addrs, packet.toString());
					packet.setLength(0);
				}
				packet.append(row).append('\n');
			}
			send(socket, addrs, packet.toString());
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

	static volatile ScheduledThreadPoolExecutor timer = null;
	static volatile Runnable scheduled = null;

	private static Runnable command;

	public static synchronized void startup(final InetSocketAddress... addrs) {
		if (timer != null) {
			return;
		}
		long start = System.currentTimeMillis();
		final AtomicInteger now = new AtomicInteger((int) (start / MINUTE));
		final Random random = new Random();
		command = new Runnable() {
			@Override
			public void run() {
				try {
					final int minute = now.incrementAndGet();
					final List<MetricEntry> metrics = Metric.removeAll();
					if (metrics.isEmpty()) {
						return;
					}
					if (timer == null) {
						send(addrs, minute, metrics);
						return;
					}
					Runnable scheduled_ = new Runnable() {
						@Override
						public void run() {
							scheduled = null;
							try {
								send(addrs, minute, metrics);
							} catch (Error | RuntimeException e) {
								e.printStackTrace();
							}
						}
					};
					scheduled = scheduled_;
					timer.schedule(scheduled_,
							random.nextInt(MINUTE), TimeUnit.MILLISECONDS);
				} catch (Error | RuntimeException e) {
					e.printStackTrace();
				}
			}
		};
		timer = new ScheduledThreadPoolExecutor(1);
		timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		timer.scheduleAtFixedRate(command,
				MINUTE - start % MINUTE, MINUTE, TimeUnit.MILLISECONDS);
	}

	public static synchronized void shutdown() {
		if (timer == null) {
			return;
		}
		timer.shutdown();
		try {
			while (!timer.awaitTermination(1, TimeUnit.SECONDS)) {/**/}
		} catch (InterruptedException e) {/**/}
		timer = null;
		if (scheduled != null) {
			scheduled.run();
			scheduled = null;
		}
		command.run();
	}
}