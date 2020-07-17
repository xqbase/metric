package com.xqbase.p6spy2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.jdbc.JdbcDatabaseMetaData;

public class Test {
	public static void main(String[] args) throws SQLException {
		try (Connection conn = DriverManager.getConnection("jdbc:p6spy2:h1:mem:test")) {
			//
		} catch (SQLException e) {
			assert (e.getSQLState().equals("08001"));
		}
		try (
			Connection conn = DriverManager.getConnection("jdbc:p6spy2:h2:mem:test");
			Statement st = conn.createStatement();
		) {
			st.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, x1 VARCHAR)");
			try (PreparedStatement ps = conn.
					prepareStatement("INSERT INTO test (id, x1) VALUES (?, ?)")) {
				ps.setInt(1, 1);
				ps.setString(2, "test");
				assert ps.executeUpdate() == 1;
			}
			try (PreparedStatement ps = conn.
					prepareStatement("SELECT * FROM test WHERE id = ?")) {
				ResultSetMetaData rsmd = ps.getMetaData();
				assert rsmd.getColumnCount() == 2;
				assert rsmd.getColumnName(1).equals("ID");
				assert rsmd.getColumnType(1) == Types.INTEGER;
				assert rsmd.getColumnName(2).equals("X1");
				assert rsmd.getColumnType(2) == Types.VARCHAR;
				ps.setInt(1, 1);
				try (ResultSet rs = ps.executeQuery()) {
					assert rs.next();
					assert rs.getInt("id") == 1;
					assert rs.getString("x1").equals("test");
					assert !rs.next();
				}
			}
			try {
				st.execute("INSERT INTO test (id, x1) VALUES (1, 2)");
				assert false;
			} catch (SQLException e) {
				assert e.getSQLState().equals("23505");
			}
			DatabaseMetaData dmd = conn.getMetaData();
			try (ResultSet rs = dmd.getPrimaryKeys(null, null, "TEST")) {
				assert rs.next();
				assert rs.getString("TABLE_NAME").equals("TEST");
				assert rs.getString("COLUMN_NAME").equals("ID");
				assert !rs.next();
			}
			assert dmd.unwrap(DatabaseMetaData.class).
					getClass() == JdbcDatabaseMetaData.class;
			assert dmd.isWrapperFor(JdbcDatabaseMetaData.class);
		}
	}
}