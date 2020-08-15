package com.xqbase.metric;

import java.io.File;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Migrate {
	private static final int BATCH = 100;
	private static final String[] DRIVERS = {
		"org.h2.Driver",
		"org.apache.derby.jdbc.EmbeddedDriver",
		"org.sqlite.JDBC",
		"org.postgresql.Driver",
		"com.mysql.jdbc.Driver",
	};

	private static SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

	private static void log(String msg) {
		System.out.println(format.format(new Date()) + " " + msg);
	}

	public static void main(String[] args) throws SQLException {
		if (args == null || args.length < 2) {
			System.out.println("Migrate Usage:");
			System.out.println("\tjava -jar migrate.jar \"<src.jdbc.url>\" \"<dst.jdbc.url>\"");
			System.out.println("or");
			System.out.println(("\tjava -cp migrate.jar:<src.jdbc.driver.jar>:" +
					"<dst.jdbc.driver.jar> com.xqbase.metric.Migrate " +
					"\"<src.jdbc.url>\" \"<dst.jdbc.url>\"").replace(':', File.pathSeparatorChar));
			return;
		}
		for (String driver : DRIVERS) {
			try {
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
				System.err.println("" + e);
			}
		}
		boolean pgSrc = args[0].startsWith("jdbc:postgresql://");
		boolean sqliteDst = args[1].startsWith("jdbc:sqlite:");
		try (
			Connection src = DriverManager.getConnection(args[0]);
			Statement stSrc = src.createStatement();
			Connection dst = DriverManager.getConnection(args[1]);
			Statement stDst = dst.createStatement();
		) {
			src.setReadOnly(true);
			List<String> tables = new ArrayList<>();
			try (ResultSet rs = src.getMetaData().
					getTables(src.getCatalog(), src.getSchema(), null,
							pgSrc ? new String[] {"TABLE"} : null)) {
				while (rs.next()) {
					tables.add(rs.getString("TABLE_NAME").toLowerCase());
				}
			}
			for (String table : tables) {
				log("Begin migrating table " + table + " ...");
				stDst.execute((sqliteDst ? "DELETE FROM " : "TRUNCATE TABLE ") + table);
				if (pgSrc) {
					src.setAutoCommit(false);
				}
				try (ResultSet rs = stSrc.executeQuery("SELECT * FROM " + table)) {
					int columns = rs.getMetaData().getColumnCount();
					try (PreparedStatement ps = dst.prepareStatement("INSERT INTO "  + table +
							" VALUES (" + String.join(", ", Collections.nCopies(columns, "?")) + ")")) {
						int rows = 0;
						int batch = 0;
						while (rs.next()) {
							for (int i = 1; i <= columns; i ++) {
								Object x = rs.getObject(i);
								if (x instanceof Blob) {
									x = ((Blob) x).getBytes(1, (int) ((Blob) x).length());
								} else if (x instanceof Clob) {
									x = ((Clob) x).getSubString(1, (int) ((Clob) x).length());
								}
								ps.setObject(i, x);
							}
							ps.addBatch();
							batch ++;
							if (batch == BATCH) {
								ps.executeBatch();
								rows += batch;
								log(rows + " rows inserted into table " + table);
								batch = 0;
							}
						}
						if (batch > 0) {
							ps.executeBatch();
							rows += batch;
							log(rows + " rows inserted into table " + table);
						}
					}
				}
				if (pgSrc) {
					src.setAutoCommit(true);
				}
				log("End migrating table " + table);
			}
		}
	}
}