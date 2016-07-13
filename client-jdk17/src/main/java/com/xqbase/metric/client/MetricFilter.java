package com.xqbase.metric.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.xqbase.metric.common.Metric;
import com.xqbase.metric.common.MetricKey;

public class MetricFilter implements Filter {
	private static AtomicInteger count = new AtomicInteger(0);

	private FilterConfig conf;
	private String requestTime;
	private ScheduledThreadPoolExecutor timer;

	HashMap<String, String> tagMap;
	AtomicInteger connections = new AtomicInteger(0);

	protected String getAddresses() {
		return conf.getInitParameter("addresses");
	}

	protected String getPrefix() {
		return conf.getInitParameter("prefix");
	}

	protected String getTags() {
		return conf.getInitParameter("tags");
	}

	@Override
	public void init(FilterConfig conf_) {
		if (count.getAndIncrement() > 0) {
			return;
		}

		conf = conf_;

		ArrayList<InetSocketAddress> addrs = new ArrayList<>();
		String addresses = getAddresses();
		if (addresses != null) {
			for (String s : addresses.split("[,;]")) {
				String[] ss = s.split("[:/]");
				if (ss.length > 1) {
					try {
						addrs.add(new InetSocketAddress(ss[0],
								Integer.parseInt(ss[1])));
					} catch (NumberFormatException e) {
						// Ignored
					}
				}
			}
		}
		MetricClient.startup(addrs.toArray(new InetSocketAddress[0]));

		String prefix = getPrefix();
		final String connections_ = prefix + ".webapp.connections";
		requestTime = prefix + ".webapp.request_time";

		tagMap = new HashMap<>();
		String tags = getTags();
		if (tags != null) {
			for (String s : tags.split("[,;]")) {
				String[] ss = s.split("[:=]");
				if (ss.length > 1) {
					tagMap.put(ss[0], ss[1]);
				}
			}
		}

		timer = new ScheduledThreadPoolExecutor(1);
		timer.scheduleAtFixedRate(new ManagementMonitor(prefix + ".server", tagMap),
				0, 5, TimeUnit.SECONDS);
		timer.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Metric.put(connections_, connections.get(), tagMap);
			}
		}, 1, 1, TimeUnit.SECONDS);
	}

	@Override
	public void destroy() {
		if (count.decrementAndGet() > 0) {
			return;
		}
		timer.shutdown();
		MetricClient.shutdown();
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		long t = System.currentTimeMillis();
		if (!(request instanceof HttpServletRequest) ||
				!(response instanceof HttpServletResponse)) {
			chain.doFilter(request, response);
			return;
		}
		HttpServletRequest req = (HttpServletRequest) request;
		HttpServletResponse resp = (HttpServletResponse) response;

		String path = req.getServletPath();
		path = path == null || path.isEmpty() ? "/" : path;
		int slash = path.indexOf('/', 1);
		path = slash < 0 ? path : path.substring(0, slash);

		connections.incrementAndGet();
		try {
			chain.doFilter(request, response);
		} finally {
			connections.decrementAndGet();

			int status = resp.getStatus();
			path = status == HttpServletResponse.SC_NOT_FOUND ? "__404__" : path;
			String type = "" + resp.getContentType();
			int colon = type.indexOf(';', 1);
			type = colon < 0 ? type : type.substring(0, colon);
			HashMap<String, String> tagMap_ = new HashMap<>(tagMap);
			MetricKey.putTagMap(tagMap_, "path", path,
					"status", "" + status, "content_type", type,
					"charset", "" + resp.getCharacterEncoding());
			Metric.put(requestTime, System.currentTimeMillis() - t, tagMap_);
		}
	}
}