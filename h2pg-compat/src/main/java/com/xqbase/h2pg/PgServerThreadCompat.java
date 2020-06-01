package com.xqbase.h2pg;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;

import org.h2.server.pg.PgServerThread;
import org.h2.server.pg.PgServerThreadEx;

public class PgServerThreadCompat extends PgServerThreadEx {
	private static Field out, dataInRaw, stop;
	private static Method process;

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
			out = getField("out");
			dataInRaw = getField("dataInRaw");
			stop = getField("stop");
			process = getMethod("process");
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	private Socket socket;
	private PgServerCompat server;

	public PgServerThreadCompat(Socket socket, PgServerCompat server) {
		super(socket, server);
		this.socket = socket;
		this.server = server;
	}

	@Override
	public void run() {
		try {
			server.trace("Connect");
			InputStream ins = socket.getInputStream();
			out.set(this, socket.getOutputStream());
			dataInRaw.set(this, new DataInputStream(ins));
			while (!stop.getBoolean(this)) {
				process.invoke(this);
				((OutputStream) out.get(this)).flush();
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
}