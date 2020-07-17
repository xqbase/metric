package com.xqbase.p6spy2;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

interface Wrapped {/**/}

public class Driver implements java.sql.Driver {
	private static final String PREFIX = "jdbc:p6spy2:";
	private static Method unwrap, isWrapperFor;
	private static Logger logger;
	private static FileHandler handler;

	static {
		try {
			DriverManager.registerDriver(new Driver());
			unwrap = Wrapper.class.getMethod("unwrap", Class.class);
			isWrapperFor = Wrapper.class.getMethod("isWrapperFor", Class.class);

			System.setProperty("java.util.logging.SimpleFormatter.format",
					"%1$tY-%1$tm-%1$td %1$tk:%1$tM:%1$tS.%1$tL %2$s%n%4$s: %5$s%6$s%n");
			new File("/var/log/p6spy2").mkdirs();
			handler = new FileHandler("/var/log/p6spy2/%g.log", 16777216, 10, true);
			handler.setFormatter(new SimpleFormatter());
			handler.setLevel(Level.ALL);
			logger = Logger.getAnonymousLogger();
			logger.setLevel(Level.ALL);
			logger.setUseParentHandlers(false);
			logger.addHandler(handler);
		} catch (SQLException | ReflectiveOperationException | IOException e) {
			throw new RuntimeException(e);
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.removeHandler(handler);
			handler.close();
		}));
	}

	private static Set<String> excludedClasses = new HashSet<>(Arrays.
			asList("com.xqbase.p6spy2.Driver", "com.xqbase.p6spy2.", "com.sun.proxy."));

	private static void log(Level l, CharSequence s, Throwable t) {
		String sourceClass = "", sourceMethod = "";
		for (StackTraceElement ste : new Throwable().getStackTrace()) {
			String cls = ste.getClassName();
			int dollar = cls.indexOf('$');
			if (!excludedClasses.contains(dollar < 0 ? cls : cls.substring(0, dollar))) {
				sourceClass = cls;
				sourceMethod = ste.getMethodName();
				break;
			}
		}
		logger.logp(l, sourceClass, sourceMethod, s.toString(), t);
	}

	private static Object invoke(Object delegate,
			Method method, Object[] args) throws Throwable {
		if (args != null && args.length == 1 && args[0] instanceof Class) {
			if (method.equals(unwrap)) {
				return delegate;
			}
			if (method.equals(isWrapperFor)) {
				return Boolean.valueOf(((Class<?>) args[0]).
						isAssignableFrom(delegate.getClass()));
			}
		}
		StringBuilder sb = new StringBuilder(delegate.getClass().getName()).
				append('.').append(method.getName()).append('(');
		if (args != null && args.length > 0) {
			for (Object arg : args) {
				sb.append(arg instanceof String[] ?
						Arrays.asList((String[]) arg) : arg).append(", ");
			}
			sb.setLength(sb.length() - 2);
		}
		sb.append(')');
		Object value;
		try {
			value = method.invoke(delegate, args);
		} catch (InvocationTargetException e) {
			Throwable t = e.getTargetException();
			log(Level.SEVERE, sb, t);
			throw t;
		}
		if (method.getReturnType() != void.class) {
			sb.append(" -> ").append(value);
		}
		log(Level.INFO, sb, null);
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
		String url_ = "jdbc:" + url.substring(PREFIX.length());
		StringBuilder sb = new StringBuilder("java.sql.DriverManager.getConnection(").
				append(url_).append(", ").append(info).append(')');
		Connection delegate;
		try {
			delegate = DriverManager.getConnection(url_, info);
		} catch (SQLException | RuntimeException e) {
			log(Level.SEVERE, sb, e);
			throw e;
		}
		log(Level.INFO, sb.append(" -> ").append(delegate), null);
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
	public DriverPropertyInfo[] getPropertyInfo(String url,
			Properties info) throws SQLException {
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
	public Logger getParentLogger() {
		return logger;
	}
}