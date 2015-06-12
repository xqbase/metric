package com.xqbase.metric.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
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

public class MetricFilter implements Filter {
	private FilterConfig conf;
	private String requestTime;
	private ScheduledThreadPoolExecutor timer;
	private AtomicInteger connections = new AtomicInteger(0);

	protected String getAddresses() {
		return conf.getInitParameter("addresses");
	}

	protected String getPrefix() {
		return conf.getInitParameter("prefix");
	}

	@Override
	public void init(FilterConfig conf_) {
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
		String connections_ = prefix + ".webapp.connections";
		requestTime = prefix + ".webapp.request_time";

		timer = new ScheduledThreadPoolExecutor(1);
		timer.scheduleAtFixedRate(new ManagementMonitor(prefix + ".server"),
				0, 5, TimeUnit.SECONDS);
		timer.scheduleAtFixedRate(() -> Metric.put(connections_, connections.get()),
				1, 1, TimeUnit.SECONDS);
	}

	@Override
	public void destroy() {
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
			Metric.put(requestTime, System.currentTimeMillis() - t,
					"path", path, "status", "" + status, "content_type", type,
					"charset", "" + resp.getCharacterEncoding());
		}
	}
}