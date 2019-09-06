package com.xqbase.metric.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
	public static final int MAX_PACKET_SIZE_FRAG = 65535 - 28;
	public static final int MAX_PACKET_SIZE = 1500 - 28;

	private static final int MINUTE = 60000;

	private static int maxPacketSize = MAX_PACKET_SIZE;

	public static void setMaxPacketSize(int maxPacketSize) {
		MetricClient.maxPacketSize = maxPacketSize;
	}

	private static String encode(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void send(DatagramSocket socket,
			InetSocketAddress[] addrs, byte[] b) throws IOException {
		for (InetSocketAddress addr : addrs) {
			// Resolve "addr" every time
			DatagramPacket packet = new DatagramPacket(b, b.length,
					InetAddress.getByName(addr.getHostString()), addr.getPort());
			socket.send(packet);
		}
	}

	private static void send(InetSocketAddress[] addrs, int minute,
			List<MetricEntry> metrics) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DeflaterOutputStream dos = new DeflaterOutputStream(baos, true);
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
					tagMap.forEach((key, value) -> {
						row.append('&').append(encode(key)).
								append('=').append(encode(value));
					});
					row.setCharAt(question, '?');
				}
				if (baos.size() + row.length() + 10 > maxPacketSize) {
					dos.close();
					send(socket, addrs, baos.toByteArray());
					baos.reset();
					dos = new DeflaterOutputStream(baos, true);
				}
				dos.write(row.toString().getBytes());
				dos.write((byte) '\n');
				dos.flush();
			}
			dos.close();
			send(socket, addrs, baos.toByteArray());
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

	private static volatile ScheduledThreadPoolExecutor timer = null;
	private static volatile Runnable scheduled = null;
	private static Runnable command;

	public static synchronized void startup(InetSocketAddress... addrs) {
		if (timer != null) {
			return;
		}
		long start = System.currentTimeMillis();
		AtomicInteger now = new AtomicInteger((int) (start / MINUTE));
		Random random = new Random();
		command = () -> {
			try {
				int minute = now.incrementAndGet();
				List<MetricEntry> metrics = Metric.removeAll();
				if (metrics.isEmpty()) {
					return;
				}
				if (timer == null) {
					send(addrs, minute, metrics);
					return;
				}
				Runnable scheduled_ = () -> {
					scheduled = null;
					try {
						send(addrs, minute, metrics);
					} catch (Error | RuntimeException e) {
						e.printStackTrace();
					}
				};
				scheduled = scheduled_;
				timer.schedule(scheduled_,
						random.nextInt(MINUTE), TimeUnit.MILLISECONDS);
			} catch (Error | RuntimeException e) {
				e.printStackTrace();
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