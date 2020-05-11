package com.xqbase.metric;

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

	private static SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

	private static void log(String msg) {
		System.out.println(format.format(new Date()) + " " + msg);
	}

	public static void main(String[] args) throws SQLException {
		if (args == null || args.length < 2) {
			System.out.println("Migrate Usage: java -jar metric-migrate.jar <src.jdbc.url> <dst.jdbc.url>");
			return;
		}
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println("" + e);
		}
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println("" + e);
		}
		try (
			Connection src = DriverManager.getConnection(args[0]);
			Statement stSrc = src.createStatement();
			Connection dst = DriverManager.getConnection(args[1]);
		) {
			List<String> tables = new ArrayList<>();
			try (ResultSet rs = src.getMetaData().
					getTables(src.getCatalog(), src.getSchema(), null, null)) {
				while (rs.next()) {
					tables.add(rs.getString("TABLE_NAME").toLowerCase());
				}
			}
			for (String table : tables) {
				log("Begin migrating table " + table + " ...");
				try (ResultSet rs = stSrc.executeQuery("SELECT * FROM " + table)) {
					int columns = rs.getMetaData().getColumnCount();
					try (PreparedStatement ps = dst.prepareStatement("INSERT INTO "  + table +
							" VALUES (" + String.join(", ", Collections.nCopies(columns, "?")) + ")")) {
						int rows = 0;
						int batch = 0;
						while (rs.next()) {
							for (int i = 1; i <= columns; i ++) {
								ps.setObject(i, rs.getObject(i));
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
				log("End migrating table " + table);
			}
		}
	}
}