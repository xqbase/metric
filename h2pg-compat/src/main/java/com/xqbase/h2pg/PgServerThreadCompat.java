package com.xqbase.h2pg;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.charset.Charset;

import org.h2.server.pg.PgServerThread;
import org.h2.server.pg.PgServerThreadEx;
import org.h2.util.Bits;
import org.h2.util.ScriptReader;
import org.h2.util.Utils;

public class PgServerThreadCompat extends PgServerThreadEx {
	private static Field initDone, out, dataInRaw, stop;
	private static Method process, getEncoding;

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
			initDone = getField("initDone");
			out = getField("out");
			dataInRaw = getField("dataInRaw");
			stop = getField("stop");
			process = getMethod("process");
			getEncoding = getMethod("getEncoding");
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	private static final String[] REPLACE_FROM = {
		" \"a\".\"attnum\"=ANY(\"c\".\"conkey\") ",
		" n.nspname = ANY(current_schemas(true)), ",
	};
	private static final String[] REPLACE_TO = {
		" ARRAY_CONTAINS(\"c\".\"conkey\", \"a\".\"attnum\") ",
		" true, ",
	};

	private static String getSQL(String s) {
		String sql = s;
		boolean replaced = false;
		for (int i = 0; i < REPLACE_FROM.length; i ++) {
			int index = s.indexOf(REPLACE_FROM[i]);
			if (index < 0) {
				continue;
			}
			sql = sql.substring(0, index) + REPLACE_TO[i] +
					s.substring(index + REPLACE_FROM[i].length());
			replaced = true;
		}
		return replaced ? sql : null;
	}

	private static int findZero(byte[] b, int left, int right) throws EOFException {
		for (int i = left; i < right; i ++) {
			if (b[i] == 0) {
				return i;
			}
		}
		throw new EOFException();
	}

	private Socket socket;
	private PgServerCompat server;
	private InputStream ins;

	public PgServerThreadCompat(Socket socket, PgServerCompat server) {
		super(socket, server);
		this.socket = socket;
		this.server = server;
	}

	private void read(byte[] b, int off, int len) throws IOException {
		int off_ = off;
		int len_ = len;
		while (len_ > 0) {
			int l = ins.read(b, off_, len_);
			if (l < 0) {
				throw new EOFException();
			}
			off_ += l;
			len_ -= l;
		}
	}

	private void read() throws IOException, ReflectiveOperationException {
		int x = 0;
		int initLen = 0;
		if (initDone.getBoolean(this)) {
			x = ins.read();
			if (x < 0) {
				throw new EOFException();
			}
			initLen = 1;
		}
		byte[] head = new byte[4];
		read(head, 0, 4);
		int dataLen = Bits.readInt(head, 0);
		if (dataLen < 4) {
			throw new EOFException();
		}
		byte[] data = Utils.newBytes(initLen + dataLen);
		if (initLen == 1) {
			data[0] = (byte) x;
		}
		System.arraycopy(head, 0, data, initLen, 4);
		read(data, initLen + 4, dataLen - 4);
		switch (x) {
		case 'P':
			int z1 = findZero(data, 5, data.length) + 1;
			int z2 = findZero(data, z1, data.length);
			Charset charset = (Charset) getEncoding.invoke(this);
			String sql = getSQL(new String(data, z1, z2 - z1, charset));
			if (sql != null) {
				byte[] sqlb = sql.getBytes(charset);
				byte[] data_ = new byte[data.length - z2 + z1 + sqlb.length];
				data_[0] = 'P';
				Bits.writeInt(data_, 1, data_.length - 1);
				System.arraycopy(data, 5, data_, 5, z1 - 5);
				System.arraycopy(sqlb, 0, data_, z1, sqlb.length);
				System.arraycopy(data, z2, data_, z1 + sqlb.length, data.length - z2);
				data = data_;
			}
			break;
		case 'Q':
			z1 = findZero(data, 5, data.length);
			charset = (Charset) getEncoding.invoke(this);
			StringBuilder sb = new StringBuilder();
			boolean replaced = false;
			try (ScriptReader reader = new ScriptReader(new
					InputStreamReader(new ByteArrayInputStream(data, 5, z1 - 5), charset))) {
				String line;
				while ((line = reader.readStatement()) != null) {
					sql = getSQL(line);
					if (sql == null) {
						sb.append(line).append(';');
					} else {
						sb.append(sql).append(';');
						replaced = true;
					}
				}
			}
			if (replaced) {
				byte[] sqlb = sb.substring(0, sb.length() - 1).getBytes(charset);
				byte[] data_ = new byte[data.length - z1 + 5 + sqlb.length];
				data_[0] = 'Q';
				Bits.writeInt(data_, 1, data_.length - 1);
				System.arraycopy(sqlb, 0, data_, 5, sqlb.length);
				System.arraycopy(data, z1, data_, 5 + sqlb.length, data.length - z1);
				data = data_;
			}
			break;
		default:
		}
		dataInRaw.set(this, new DataInputStream(new ByteArrayInputStream(data)));
	}

	@Override
	public void run() {
		try {
			server.trace("Connect");
			ins = socket.getInputStream();
			out.set(this, socket.getOutputStream());
			// dataInRaw.set(this, new DataInputStream(ins));
			while (!stop.getBoolean(this)) {
				read();
				process.invoke(this);
				((OutputStream) out.get(this)).flush();
			}
		} catch (EOFException e) {
			// more or less normal disconnect
		} catch (IOException | ReflectiveOperationException e) {
			server.traceError(e);
		} finally {
			server.trace("Disconnect");
			close();
		}
	}
}