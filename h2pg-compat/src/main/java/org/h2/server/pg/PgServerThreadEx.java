package org.h2.server.pg;

import java.net.Socket;

import org.h2.server.pg.PgServer;
import org.h2.server.pg.PgServerThread;

public class PgServerThreadEx extends PgServerThread {
	public PgServerThreadEx(Socket socket, PgServer server) {
		super(socket, server);
	}

	@Override
	public void setProcessId(int id) {
		super.setProcessId(id);
	}

	@Override
	public void setThread(Thread thread) {
		super.setThread(thread);
	}

	@Override
	protected void close() {
		super.close();
	}
}