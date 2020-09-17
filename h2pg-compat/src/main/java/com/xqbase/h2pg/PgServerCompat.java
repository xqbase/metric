package com.xqbase.h2pg;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.h2.server.Service;
import org.h2.server.pg.PgServer;
import org.h2.server.pg.PgServerThread;
import org.h2.util.NetUtils2;

public class PgServerCompat implements Service {
	private static Field stop, serverSocket, running, pid;
	private static Method allow, trace, traceError;

	private static Field getField(String name) throws ReflectiveOperationException {
		Field field = PgServer.class.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}

	private static Method getMethod(String name, Class<?>... paramTypes)
			throws ReflectiveOperationException {
		Method method = PgServer.class.getDeclaredMethod(name, paramTypes);
		method.setAccessible(true);
		return method;
	}

	static {
		try {
			stop = getField("stop");
			serverSocket = getField("serverSocket");
			running = getField("running");
			pid = getField("pid");
			allow = getMethod("allow", Socket.class);
			trace = getMethod("trace", String.class);
			traceError = getMethod("traceError", Exception.class);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	PgServer server = new PgServer();

	@Override
	public void init(String... args) throws Exception {
		server.init(args);
	}

	@Override
	public String getURL() {
		return server.getURL();
	}

	@Override
	public void start() throws SQLException {
		server.start();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void listen() {
		String threadName = Thread.currentThread().getName();
		try {
			while (!stop.getBoolean(server)) {
				Socket s = ((ServerSocket) serverSocket.get(server)).accept();
				if (!((Boolean) allow.invoke(server, s)).booleanValue()) {
					trace("Connection not allowed");
					s.close();
				} else {
					NetUtils2.setTcpQuickack(s, true);
					PgServerThreadCompat c = new PgServerThreadCompat(s, this);
					((Set<PgServerThread>) running.get(server)).add(c.thread);
					int id = ((AtomicInteger) pid.get(server)).incrementAndGet();
					c.setProcessId(id);
					Thread thread = new Thread(c, threadName + " thread-" + id);
					thread.setDaemon(isDaemon());
					c.setThread(thread);
					thread.start();
				}
			}
		} catch (Exception e) {
			try {
				if (!stop.getBoolean(server)) {
					e.printStackTrace();
				}
			} catch (ReflectiveOperationException ee) {
				throw new RuntimeException(ee);
			}
		}
	}

	@Override
	public void stop() {
		server.stop();
	}

	@Override
	public boolean isRunning(boolean traceError_) {
		return server.isRunning(traceError_);
	}

	@Override
	public boolean getAllowOthers() {
		return server.getAllowOthers();
	}

	@Override
	public String getName() {
		return server.getName();
	}

	@Override
	public String getType() {
		return server.getType();
	}

	@Override
	public int getPort() {
		return server.getPort();
	}

	@Override
	public boolean isDaemon() {
		return server.isDaemon();
	}

	void trace(String s) {
		try {
			trace.invoke(server, s);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	void traceError(Exception e) {
		try {
			traceError.invoke(server, e);
		} catch (ReflectiveOperationException ee) {
			throw new RuntimeException(ee);
		}
	}
}