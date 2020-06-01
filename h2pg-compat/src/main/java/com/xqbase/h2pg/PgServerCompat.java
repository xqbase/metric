package com.xqbase.h2pg;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.h2.server.pg.PgServer;
import org.h2.server.pg.PgServerEx;
import org.h2.server.pg.PgServerThread;
import org.h2.util.NetUtils2;

public class PgServerCompat extends PgServerEx {
	private static Field stop, serverSocket, running, pid, isDaemon;
	private static Method allow;

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
			isDaemon = getField("isDaemon");
			allow = getMethod("allow", Socket.class);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void listen() {
		String threadName = Thread.currentThread().getName();
		try {
			while (!stop.getBoolean(this)) {
				Socket s = ((ServerSocket) serverSocket.get(this)).accept();
				if (!((Boolean) allow.invoke(this, s)).booleanValue()) {
					trace("Connection not allowed");
					s.close();
				} else {
					NetUtils2.setTcpQuickack(s, true);
					PgServerThreadCompat c = new PgServerThreadCompat(s, this);
					((Set<PgServerThread>) running.get(this)).add(c);
					int id = ((AtomicInteger) pid.get(this)).incrementAndGet();
					c.setProcessId(id);
					Thread thread = new Thread(c, threadName + " thread-" + id);
					thread.setDaemon(isDaemon.getBoolean(this));
					c.setThread(thread);
					thread.start();
				}
			}
		} catch (Exception e) {
			try {
				if (!stop.getBoolean(this)) {
					e.printStackTrace();
				}
			} catch (ReflectiveOperationException ee) {
				throw new RuntimeException(ee);
			}
		}
	}
}