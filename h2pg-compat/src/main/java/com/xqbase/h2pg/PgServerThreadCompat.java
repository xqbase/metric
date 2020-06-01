package com.xqbase.h2pg;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;

import org.h2.server.pg.PgServer;
import org.h2.server.pg.PgServerThread;

public class PgServerThreadCompat implements Runnable {
	private static Field out, dataInRaw, stop;
	private static Constructor<?> constructor;
	private static Method setProcessId, setThread;

	private static Field getField(String name) throws ReflectiveOperationException {
		Field field = PgServerThread.class.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}

	private static Method getMethod(String name, Class<?>... paramTypes)
			throws ReflectiveOperationException {
		Method method = PgServerThread.class.getDeclaredMethod(name, paramTypes);
		method.setAccessible(true);
		return method;
	}

	static {
		try {
			constructor = PgServerThread.class.getConstructor(Socket.class, PgServer.class);
			constructor.setAccessible(true);
			setProcessId = getMethod("setProcessId", int.class);
			setThread = getMethod("setThread", Thread.class);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	private Socket socket;
	private PgServer server;
	private PgServerThread thread;

	public PgServerThreadCompat(Socket socket, PgServer server) {
		this.socket = socket;
		this.server = server;
		try {
			thread = (PgServerThread) constructor.newInstance(socket, server);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {
		try {
			PgServerCompat.trace.invoke(server, "Connect");
			InputStream ins = socket.getInputStream();
			out = socket.getOutputStream();
			dataInRaw = new DataInputStream(ins);
			while (!stop) {
				process();
				out.flush();
			}
		} catch (EOFException e) {
			// more or less normal disconnect
		} catch (Exception e) {
			server.traceError(e);
		} finally {
			server.trace("Disconnect");
			close();
		}
	}

	void setProcessId(int id) {
		try {
			setProcessId.invoke(thread, Integer.valueOf(id));
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	void setThread(Thread thread) {
		try {
			setThread.invoke(this.thread, thread);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}
}