package org.h2.server.pg;

public class PgServerEx extends PgServer {
	@Override
	public void trace(String s) {
		super.trace(s);
	}

	@Override
	public void traceError(Exception e) {
		super.traceError(e);
	}
}