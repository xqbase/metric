package com.xqbase.p6spy2;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.Properties;
import java.util.logging.Logger;

interface Wrapped {/**/}

public class Driver implements java.sql.Driver {
	private static final String PREFIX = "jdbc:p6spy2:";

	static {
		try {
			DriverManager.registerDriver(new Driver());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static Object invoke(Object delegate,
			Method method, Object[] args) throws Throwable {
		if (args != null && args.length == 1 && args[0] instanceof Class) {
			switch (method.getName()) {
			case "unwrap":
				return delegate;
			case "isWrapperFor":
				return Boolean.valueOf(((Class<?>) args[0]).
						isAssignableFrom(delegate.getClass()));
			default:
			}
		}
		Object value;
		try {
			value = method.invoke(delegate, args);
		} catch (InvocationTargetException e) {
			throw e.getTargetException();
		}
		if (!(value instanceof Wrapper) || value instanceof Wrapped) {
			return value;
		}
		Class<?>[] fromIfaces = value.getClass().getInterfaces();
		Class<?>[] toIfaces = new Class[fromIfaces.length + 1];
		System.arraycopy(fromIfaces, 0, toIfaces, 0, fromIfaces.length);
		toIfaces[fromIfaces.length] = Wrapped.class;
		return Proxy.newProxyInstance(Driver.class.getClassLoader(),
				toIfaces, (InvocationHandler) (proxy, method_, args_) ->
				invoke(value, method_, args_));
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}
		Connection delegate = DriverManager.getConnection("jdbc:" +
				url.substring(PREFIX.length()), info);
		return (Connection) Proxy.
				newProxyInstance(Driver.class.getClassLoader(),
				new Class[] {Connection.class, Wrapper.class},
				(InvocationHandler) (proxy, method, args) -> {
			return invoke(delegate, method, args);
		});
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url != null && url.startsWith(PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
}