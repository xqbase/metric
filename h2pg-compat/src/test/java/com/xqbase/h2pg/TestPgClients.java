package com.xqbase.h2pg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPgClients {
	private Server server;
	private Connection conn;
	private Statement stat;

	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Before
	public void before() throws SQLException {
		server = new Server(new PgServerCompat(), "-ifNotExists",
				"-pgPort", "5535", "-key", "pgserver", "mem:pgserver");
		server.start();
		conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
		stat = conn.createStatement();
		stat.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, x1 INTEGER)");
	}

	@Test
	public void testPgJdbc() throws SQLException {
		stat.execute("CREATE TABLE test2 (x1 INT, x2 INT, x3 INT, PRIMARY KEY (x1, x2), UNIQUE (x3, x2))");
		stat.execute("CREATE INDEX ON test2 (x3, x1)");
		DatabaseMetaData dmd = conn.getMetaData();
		try (ResultSet rs = dmd.getIndexInfo(null, null, "test2", false, true)) {
			Map<String, List<String>> expected = new HashMap<>();
			expected.put("p", Arrays.asList("x1", "x2"));
			expected.put("u", Arrays.asList("x3", "x2"));
			expected.put("i", Arrays.asList("x3", "x1"));
			Map<String, List<String>> actual = new HashMap<>();
			while (rs.next()) {
				String type = rs.getBoolean("NON_UNIQUE") ? "i" : rs.getInt("TYPE") == 1 ? "p" : "u";
				actual.computeIfAbsent(type, k -> Arrays.asList(null, null)).
						set(rs.getInt("ORDINAL_POSITION") - 1, rs.getString("COLUMN_NAME"));
			}
			assertEquals(expected, actual);
		}
		ConsumerEx<ResultSet, SQLException> rsConsumer = rs -> {
			assertTrue(rs.next());
			assertEquals("x1", rs.getString("COLUMN_NAME"));
			assertEquals(1, rs.getInt("KEY_SEQ"));
			assertTrue(rs.next());
			assertEquals("x2", rs.getString("COLUMN_NAME"));
			assertEquals(2, rs.getInt("KEY_SEQ"));
			assertFalse(rs.next());
		};
		// for PgJDBC 42.2.9
		try (ResultSet rs = stat.executeQuery("SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, " +
				"ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, (i.keys).n AS KEY_SEQ, " +
				"ci.relname AS PK_NAME FROM pg_catalog.pg_class ct " +
				"JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) " +
				"JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) " +
				"JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary, " +
				"information_schema._pg_expandarray(i.indkey) AS keys FROM pg_catalog.pg_index i) i " +
				"ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) " +
				"JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) " +
				"WHERE true AND ct.relname = E'test2' AND i.indisprimary " +
				"ORDER BY table_name, pk_name, key_seq")) {
			rsConsumer.accept(rs);
		}
		// for PgJDBC 42.2.10
		try (ResultSet rs = stat.executeQuery("SELECT result.TABLE_CAT, result.TABLE_SCHEM, " +
				"result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME FROM " +
				"(SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME, " +
				"a.attname AS COLUMN_NAME, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, " +
				"ci.relname AS PK_NAME, information_schema._pg_expandarray(i.indkey) AS KEYS, " +
				"a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct " +
				"JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) " +
				"JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) " +
				"JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) " +
				"JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) " +
				"WHERE true  AND ct.relname = E'test2' AND i.indisprimary  ) result " +
				"where result.A_ATTNUM = (result.KEYS).x " +
				"ORDER BY result.table_name, result.pk_name, result.key_seq")) {
			rsConsumer.accept(rs);
		}
		try (ResultSet rs = dmd.getPrimaryKeys(null, null, "test2")) {
			rsConsumer.accept(rs);
		}
	}

	@Test
	public void testPgAdmin() throws SQLException {
		stat.execute("SET client_min_messages=notice");
		try (ResultSet rs = stat.executeQuery("SELECT set_config('bytea_output','escape',false) " +
				"FROM pg_settings WHERE name = 'bytea_output'")) {
			assertFalse(rs.next());
		}
		stat.execute("SET client_encoding='UNICODE'");
		try (ResultSet rs = stat.executeQuery("SELECT version()")) {
			assertTrue(rs.next());
			assertNotNull(rs.getString("version"));
		}
		try (ResultSet rs = stat.executeQuery("SELECT " +
				"db.oid as did, db.datname, db.datallowconn, " +
				"pg_encoding_to_char(db.encoding) AS serverencoding, " +
				"has_database_privilege(db.oid, 'CREATE') as cancreate, datlastsysoid " +
				"FROM pg_database db WHERE db.datname = current_database()")) {
			assertTrue(rs.next());
			assertEquals("pgserver", rs.getString("datname"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT " +
				"oid as id, rolname as name, rolsuper as is_superuser, " +
				"CASE WHEN rolsuper THEN true ELSE rolcreaterole END as can_create_role, " +
				"CASE WHEN rolsuper THEN true ELSE rolcreatedb END as can_create_db " +
				"FROM pg_catalog.pg_roles WHERE rolname = current_user")) {
			assertTrue(rs.next());
			assertEquals("sa", rs.getString("name"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT " +
				"db.oid as did, db.datname as name, ta.spcname as spcname, db.datallowconn, " +
				"has_database_privilege(db.oid, 'CREATE') as cancreate, datdba as owner " +
				"FROM pg_database db LEFT OUTER JOIN pg_tablespace ta ON db.dattablespace = ta.oid " +
				"WHERE db.oid > 100000::OID")) {
			assertTrue(rs.next());
			assertEquals("pgserver", rs.getString("name"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT nsp.oid, nsp.nspname as name, " +
				"has_schema_privilege(nsp.oid, 'CREATE') as can_create, " +
				"has_schema_privilege(nsp.oid, 'USAGE') as has_usage " +
				"FROM pg_namespace nsp WHERE nspname NOT LIKE 'pg\\_%' AND NOT (" +
				"(nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_class " +
				"WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR " +
				"(nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_class " +
				"WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR " +
				"(nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_class " +
				"WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1))" +
				") ORDER BY nspname")) {
			assertTrue(rs.next());
			assertEquals("public", rs.getString("name"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT format_type(23, NULL)")) {
			assertTrue(rs.next());
			assertEquals("INTEGER", rs.getString(1));
			assertFalse(rs.next());
		}
		stat.execute("SET LOCAL join_collapse_limit=8");
		try (ResultSet rs = stat.executeQuery("SELECT 'session_stats' AS chart_name, " +
				"row_to_json(t) AS chart_data FROM (SELECT " +
				"(SELECT count(*) FROM pg_stat_activity) AS \"Total\", " +
				"(SELECT count(*) FROM pg_stat_activity WHERE state = 'active')  AS \"Active\", " +
				"(SELECT count(*) FROM pg_stat_activity WHERE state = 'idle')  AS \"Idle\"" +
				") t UNION ALL " +
				"SELECT 'tps_stats' AS chart_name, row_to_json(t) AS chart_data " +
				"FROM (SELECT " +
				"(SELECT sum(xact_commit) + sum(xact_rollback) FROM pg_stat_database) AS \"Transactions\", " +
				"(SELECT sum(xact_commit) FROM pg_stat_database) AS \"Commits\", " +
				"(SELECT sum(xact_rollback) FROM pg_stat_database) AS \"Rollbacks\"" +
				") t UNION ALL " +
				"SELECT 'ti_stats' AS chart_name, row_to_json(t) AS chart_data FROM (SELECT " +
				"(SELECT sum(tup_inserted) FROM pg_stat_database) AS \"Inserts\", " +
				"(SELECT sum(tup_updated) FROM pg_stat_database) AS \"Updates\", " +
				"(SELECT sum(tup_deleted) FROM pg_stat_database) AS \"Deletes\"" +
				") t UNION ALL " +
				"SELECT 'to_stats' AS chart_name, row_to_json(t) AS chart_data FROM (SELECT " +
				"(SELECT sum(tup_fetched) FROM pg_stat_database) AS \"Fetched\", " +
				"(SELECT sum(tup_returned) FROM pg_stat_database) AS \"Returned\"" +
				") t UNION ALL " +
				"SELECT 'bio_stats' AS chart_name, row_to_json(t) AS chart_data " +
				"FROM (SELECT " +
				"(SELECT sum(blks_read) FROM pg_stat_database) AS \"Reads\", " +
				"(SELECT sum(blks_hit) FROM pg_stat_database) AS \"Hits\"" +
				") t")) {
			assertTrue(rs.next());
			assertEquals("{}", rs.getString("chart_data"));
		}
		try (ResultSet rs = stat.executeQuery("SELECT nsp.nspname as schema_name, " +
				"(nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_class " +
				"WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR " +
				"(nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_class " +
				"WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR " +
				"(nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_class " +
				"WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1)) AS is_catalog, " +
				"CASE WHEN nsp.nspname = ANY('{information_schema}') THEN false ELSE true END " +
				"AS db_support FROM pg_catalog.pg_namespace nsp WHERE nsp.oid = 0::OID")) {
			assertTrue(rs.next());
			assertEquals("public", rs.getString("schema_name"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT DISTINCT dep.deptype, " +
				"dep.refclassid, cl.relkind, ad.adbin, ad.adsrc, CASE " +
				"WHEN cl.relkind IS NOT NULL " +
				"THEN cl.relkind || COALESCE(dep.refobjsubid::character varying, '') " +
				"WHEN tg.oid IS NOT NULL THEN 'T'::text " +
				"WHEN ty.oid IS NOT NULL AND ty.typbasetype = 0 THEN 'y'::text " +
				"WHEN ty.oid IS NOT NULL AND ty.typbasetype != 0 THEN 'd'::text " +
				"WHEN ns.oid IS NOT NULL THEN 'n'::text " +
				"WHEN pr.oid IS NOT NULL AND prtyp.typname = 'trigger' THEN 't'::text " +
				"WHEN pr.oid IS NOT NULL THEN 'P'::text " +
				"WHEN la.oid IS NOT NULL THEN 'l'::text " +
				"WHEN rw.oid IS NOT NULL THEN 'R'::text " +
				"WHEN co.oid IS NOT NULL THEN 'C'::text || contype " +
				"WHEN ad.oid IS NOT NULL THEN 'A'::text " +
				"ELSE '' END AS type, COALESCE(coc.relname, clrw.relname) AS ownertable, " +
				"CASE WHEN cl.relname IS NOT NULL OR att.attname IS NOT NULL " +
				"THEN cl.relname || COALESCE('.' || att.attname, '') " +
				"ELSE COALESCE(cl.relname, co.conname, pr.proname, tg.tgname, " +
				"ty.typname, la.lanname, rw.rulename, ns.nspname) END AS refname, " +
				"COALESCE(nsc.nspname, nso.nspname, nsp.nspname, nst.nspname, nsrw.nspname) AS nspname, " +
				"CASE WHEN inhits.inhparent IS NOT NULL THEN '1' ELSE '0' END AS is_inherits, " +
				"CASE WHEN inhed.inhparent IS NOT NULL THEN '1' ELSE '0' END AS is_inherited " +
				"FROM pg_depend dep " +
				"LEFT JOIN pg_class cl ON dep.refobjid=cl.oid " +
				"LEFT JOIN pg_attribute att ON dep.refobjid=att.attrelid AND dep.refobjsubid=att.attnum " +
				"LEFT JOIN pg_namespace nsc ON cl.relnamespace=nsc.oid " +
				"LEFT JOIN pg_proc pr ON dep.refobjid=pr.oid " +
				"LEFT JOIN pg_namespace nsp ON pr.pronamespace=nsp.oid " +
				"LEFT JOIN pg_trigger tg ON dep.refobjid=tg.oid " +
				"LEFT JOIN pg_type ty ON dep.refobjid=ty.oid " +
				"LEFT JOIN pg_namespace nst ON ty.typnamespace=nst.oid " +
				"LEFT JOIN pg_constraint co ON dep.refobjid=co.oid " +
				"LEFT JOIN pg_class coc ON co.conrelid=coc.oid " +
				"LEFT JOIN pg_namespace nso ON co.connamespace=nso.oid " +
				"LEFT JOIN pg_rewrite rw ON dep.refobjid=rw.oid " +
				"LEFT JOIN pg_class clrw ON clrw.oid=rw.ev_class " +
				"LEFT JOIN pg_namespace nsrw ON clrw.relnamespace=nsrw.oid " +
				"LEFT JOIN pg_language la ON dep.refobjid=la.oid " +
				"LEFT JOIN pg_namespace ns ON dep.refobjid=ns.oid " +
				"LEFT JOIN pg_attrdef ad ON ad.adrelid=att.attrelid AND ad.adnum=att.attnum " +
				"LEFT JOIN pg_type prtyp ON prtyp.oid = pr.prorettype " +
				"LEFT JOIN pg_inherits inhits ON (inhits.inhrelid=dep.refobjid) " +
				"LEFT JOIN pg_inherits inhed ON (inhed.inhparent=dep.refobjid) " +
				"WHERE dep.objid=0::oid AND refclassid IN ( SELECT oid FROM pg_class WHERE relname IN " +
				"('pg_class', 'pg_constraint', 'pg_conversion', 'pg_language', 'pg_proc', " +
				"'pg_rewrite', 'pg_namespace', 'pg_trigger', 'pg_type', 'pg_attrdef', " +
				"'pg_event_trigger', 'pg_foreign_server', 'pg_foreign_data_wrapper')) " +
				"ORDER BY refclassid, cl.relkind")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT rolname AS refname, " +
				"refclassid, deptype FROM pg_shdepend dep " +
				"LEFT JOIN pg_roles r ON refclassid=1260 AND refobjid=r.oid " +
				"WHERE dep.objid=0::oid ORDER BY 1")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT rel.oid, rel.relname AS name, " +
				"(SELECT count(*) FROM pg_trigger WHERE tgrelid=rel.oid) AS triggercount, " +
				"(SELECT count(*) FROM pg_trigger WHERE tgrelid=rel.oid AND tgenabled = 'O') " +
				"AS has_enable_triggers, " +
				"(SELECT count(1) FROM pg_inherits WHERE inhrelid=rel.oid LIMIT 1) as is_inherits, " +
				"(SELECT count(1) FROM pg_inherits WHERE inhparent=rel.oid LIMIT 1) as is_inherited " +
				"FROM pg_class rel WHERE rel.relkind IN ('r','s','t') AND rel.relnamespace = 0::oid " +
				"ORDER BY rel.relname")) {
			assertTrue(rs.next());
			assertEquals("test", rs.getString("name"));
			assertFalse(rs.next());
		}
		stat.execute("CREATE TABLE test2 (x1 INT, x2 INT, PRIMARY KEY (x1, x2))");
		int oid, oid2;
		try (ResultSet rs = stat.executeQuery("SELECT oid FROM pg_class " +
				"WHERE relname IN ('test', 'test2') ORDER BY relname")) {
			rs.next();
			oid = rs.getInt("oid");
			rs.next();
			oid2 = rs.getInt("oid");
		}
		try (ResultSet rs = stat.executeQuery("SELECT nsp.nspname AS schema ," +
				"rel.relname AS table FROM pg_class rel " +
				"JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid::oid " +
				"WHERE rel.oid = " + oid + "::oid")) {
			assertTrue(rs.next());
			assertEquals("public", rs.getString("schema"));
			assertEquals("test", rs.getString("table"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT att.attname as name, att.attnum as OID, " +
				"format_type(ty.oid,NULL) AS datatype, att.attnotnull as not_null, " +
				"att.atthasdef as has_default_val FROM pg_attribute att " +
				"JOIN pg_type ty ON ty.oid=atttypid " +
				"JOIN pg_namespace tn ON tn.oid=ty.typnamespace " +
				"JOIN pg_class cl ON cl.oid=att.attrelid " +
				"JOIN pg_namespace na ON na.oid=cl.relnamespace " +
				"LEFT OUTER JOIN pg_type et ON et.oid=ty.typelem " +
				"LEFT OUTER JOIN pg_attrdef def ON adrelid=att.attrelid AND adnum=att.attnum " +
				"LEFT OUTER JOIN (pg_depend JOIN pg_class cs ON classid='pg_class'::regclass " +
				"AND objid=cs.oid AND cs.relkind='S') ON refobjid=att.attrelid AND refobjsubid=att.attnum " +
				"LEFT OUTER JOIN pg_namespace ns ON ns.oid=cs.relnamespace " +
				"LEFT OUTER JOIN pg_index pi ON pi.indrelid=att.attrelid AND indisprimary " +
				"WHERE att.attrelid = " + oid + "::oid AND att.attnum > 0 AND att.attisdropped IS FALSE " +
				"ORDER BY att.attnum")) {
			assertTrue(rs.next());
			assertEquals("id", rs.getString("name"));
			assertTrue(rs.next());
			assertEquals("x1", rs.getString("name"));
			assertFalse(rs.next());
		}
		try (PreparedStatement ps = conn.prepareStatement("SELECT at.attname, at.attnum, " +
				"ty.typname FROM pg_attribute at LEFT JOIN pg_type ty ON (ty.oid = at.atttypid) " +
				"WHERE attrelid=?::oid AND attnum = ANY ((SELECT con.conkey FROM pg_class rel " +
				"LEFT OUTER JOIN pg_constraint con ON con.conrelid=rel.oid AND con.contype='p' " +
				"WHERE rel.relkind IN ('r','s','t') AND rel.oid = ?::oid)::oid[])\r\n")) {
			ps.setInt(1, oid);
			ps.setInt(2, oid);
			try (ResultSet rs = ps.executeQuery()) {
				assertTrue(rs.next());
				assertEquals("id", rs.getString("attname"));
				assertEquals(1, rs.getInt("attnum"));
				assertFalse(rs.next());
			}
			ps.setInt(1, oid2);
			ps.setInt(2, oid2);
			try (ResultSet rs = ps.executeQuery()) {
				assertTrue(rs.next());
				assertEquals("x1", rs.getString("attname"));
				assertEquals(1, rs.getInt("attnum"));
				assertTrue(rs.next());
				assertEquals("x2", rs.getString("attname"));
				assertEquals(2, rs.getInt("attnum"));
				assertFalse(rs.next());
			}
		}
	}

	@Test
	public void testHeidiSQL() throws SQLException {
		try (ResultSet rs = stat.executeQuery("SHOW ssl")) {
			assertTrue(rs.next());
			assertEquals("off", rs.getString(1));
		}
		stat.execute("SET search_path TO 'public', '$user'");
		try (ResultSet rs = stat.executeQuery("SELECT *, NULL AS data_length, " +
				"pg_relation_size(QUOTE_IDENT(t.TABLE_SCHEMA) || '.' || QUOTE_IDENT(t.TABLE_NAME))::bigint " +
				"AS index_length, " +
				"c.reltuples, obj_description(c.oid) AS comment " +
				"FROM \"information_schema\".\"tables\" AS t " +
				"LEFT JOIN \"pg_namespace\" n ON t.table_schema = n.nspname " +
				"LEFT JOIN \"pg_class\" c ON n.oid = c.relnamespace AND c.relname=t.table_name " +
				"WHERE t.\"table_schema\"='public'")) {
			assertTrue(rs.next());
			assertEquals("test", rs.getString("table_name"));
			assertTrue(rs.getLong("index_length") >= 0L); // test pg_relation_size()
			assertNull(rs.getString("comment")); // test obj_description()
		}
		try (ResultSet rs = stat.executeQuery("SELECT \"p\".\"proname\", \"p\".\"proargtypes\" " +
				"FROM \"pg_catalog\".\"pg_namespace\" AS \"n\" " +
				"JOIN \"pg_catalog\".\"pg_proc\" AS \"p\" ON \"p\".\"pronamespace\" = \"n\".\"oid\" " +
				"WHERE \"n\".\"nspname\"='public'")) {
			assertFalse(rs.next()); // "pg_proc" always empty
		}
		try (ResultSet rs = stat.executeQuery("SELECT DISTINCT a.attname AS column_name, " +
				"a.attnum, a.atttypid, FORMAT_TYPE(a.atttypid, a.atttypmod) AS data_type, " +
				"CASE a.attnotnull WHEN false THEN 'YES' ELSE 'NO' END AS IS_NULLABLE, " +
				"com.description AS column_comment, pg_get_expr(def.adbin, def.adrelid) AS column_default, " +
				"NULL AS character_maximum_length FROM pg_attribute AS a " +
				"JOIN pg_class AS pgc ON pgc.oid = a.attrelid " +
				"LEFT JOIN pg_description AS com ON (pgc.oid = com.objoid AND a.attnum = com.objsubid) " +
				"LEFT JOIN pg_attrdef AS def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum) " +
				"WHERE a.attnum > 0 AND pgc.oid = a.attrelid AND pg_table_is_visible(pgc.oid) " +
				"AND NOT a.attisdropped AND pgc.relname = 'test' ORDER BY a.attnum")) {
			assertTrue(rs.next());
			assertEquals("id", rs.getString("column_name"));
			assertTrue(rs.next());
			assertEquals("x1", rs.getString("column_name"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SHOW ALL")) {
			ResultSetMetaData rsMeta = rs.getMetaData();
			assertEquals("name", rsMeta.getColumnName(1));
			assertEquals("setting", rsMeta.getColumnName(2));
		}
		try (PreparedStatement ps = conn.prepareStatement(
				"SELECT \"c\".\"conname\" AS \"CONSTRAINT_NAME\", " +
				"CASE \"c\".\"contype\" WHEN 'c' THEN 'CHECK' WHEN 'f' THEN 'FOREIGN KEY' " +
				"WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' END AS \"CONSTRAINT_TYPE\", " +
				"\"a\".\"attname\" AS \"COLUMN_NAME\" FROM \"pg_constraint\" AS \"c\" " +
				"LEFT JOIN \"pg_class\" \"t\" ON \"c\".\"conrelid\"=\"t\".\"oid\" " +
				"LEFT JOIN \"pg_attribute\" \"a\" ON \"t\".\"oid\"=\"a\".\"attrelid\" " +
				"LEFT JOIN \"pg_namespace\" \"n\" ON \"t\".\"relnamespace\"=\"n\".\"oid\" " +
				"WHERE c.contype IN ('p', 'u') AND \"a\".\"attnum\"=ANY(\"c\".\"conkey\") " +
				"AND \"n\".\"nspname\"='public' AND \"t\".\"relname\"=? " +
				"ORDER BY \"a\".\"attnum\"")) {
			ps.setString(1, "test");
			try (ResultSet rs = ps.executeQuery()) {
				assertTrue(rs.next());
				assertEquals("PRIMARY KEY", rs.getString("constraint_type"));
				assertEquals("id", rs.getString("column_name"));
				assertFalse(rs.next());
			}

			stat.execute("CREATE TABLE test2 (x1 INT PRIMARY KEY, x2 INT, UNIQUE (x2))");
			ps.setString(1, "test2");
			try (ResultSet rs = ps.executeQuery()) {
				assertTrue(rs.next());
				assertEquals("PRIMARY KEY", rs.getString("constraint_type"));
				assertEquals("x1", rs.getString("column_name"));
				assertTrue(rs.next());
				assertEquals("UNIQUE", rs.getString("constraint_type"));
				assertEquals("x2", rs.getString("column_name"));
				assertFalse(rs.next());
			}

			stat.execute("CREATE TABLE test3 (x1 INT, x2 INT, PRIMARY KEY (x1, x2))");
			ps.setString(1, "test3");
			try (ResultSet rs = ps.executeQuery()) {
				assertTrue(rs.next());
				assertEquals("PRIMARY KEY", rs.getString("constraint_type"));
				assertEquals("x1", rs.getString("column_name"));
				assertTrue(rs.next());
				assertEquals("PRIMARY KEY", rs.getString("constraint_type"));
				assertEquals("x2", rs.getString("column_name"));
				assertFalse(rs.next());
			}
		}
	}

	@Test
	public void testDBeaver() throws SQLException {
		try (ResultSet rs = stat.executeQuery("SELECT t.oid,t.*,c.relkind FROM pg_catalog.pg_type t " +
				"LEFT OUTER JOIN pg_class c ON c.oid=t.typrelid WHERE typnamespace=-1000")) {
			// just no exception
		}
		stat.execute("SET search_path TO 'ab', 'c\"d', 'e''f'");
		try (ResultSet rs = stat.executeQuery("SHOW search_path")) {
			assertTrue(rs.next());
			assertEquals("pg_catalog, ab, \"c\"\"d\", \"e'f\"", rs.getString("search_path"));
		}
		stat.execute("SET search_path TO ab, \"c\"\"d\", \"e'f\"");
		try (ResultSet rs = stat.executeQuery("SHOW search_path")) {
			assertTrue(rs.next());
			assertEquals("pg_catalog, ab, \"c\"\"d\", \"e'f\"", rs.getString("search_path"));
		}
		int oid;
		try (ResultSet rs = stat.executeQuery("SELECT oid FROM pg_class WHERE relname = 'test'")) {
			rs.next();
			oid = rs.getInt("oid");
		}
		try (ResultSet rs = stat.executeQuery("SELECT i.*,i.indkey as keys," +
				"c.relname,c.relnamespace,c.relam,c.reltablespace," +
				"tc.relname as tabrelname,dsc.description," +
				"pg_catalog.pg_get_expr(i.indpred, i.indrelid) as pred_expr," +
				"pg_catalog.pg_get_expr(i.indexprs, i.indrelid, true) as expr," +
				"pg_catalog.pg_relation_size(i.indexrelid) as index_rel_size," +
				"pg_catalog.pg_stat_get_numscans(i.indexrelid) as index_num_scans " +
				"FROM pg_catalog.pg_index i " +
				"INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid " +
				"INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid " +
				"LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid " +
				"WHERE i.indrelid=" + oid + " ORDER BY c.relname")) {
			assertTrue(rs.next());
			assertEquals("test", rs.getString("tabrelname"));
			assertEquals("{1}", rs.getString("keys"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT c.oid,c.*," +
				"t.relname as tabrelname,rt.relnamespace as refnamespace,d.description " +
				"FROM pg_catalog.pg_constraint c " +
				"INNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid " +
				"LEFT OUTER JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid " +
				"LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid " +
				"AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass WHERE c.conrelid=" + oid)) {
			assertTrue(rs.next());
			assertEquals("test", rs.getString("tabrelname"));
			assertEquals("p", rs.getString("contype"));
			assertEquals(Short.valueOf((short) 1), ((Object[]) rs.getArray("conkey").getArray())[0]);
		}
	}

	@Test
	public void testAdminer() throws SQLException {
		try (ResultSet rs = stat.executeQuery("SHOW LC_COLLATE")) {
			assertTrue(rs.next());
			assertEquals("C", rs.getString(1));
		}
		try (ResultSet rs = stat.executeQuery("SELECT specific_name AS \"SPECIFIC_NAME\", " +
				"routine_type AS \"ROUTINE_TYPE\", routine_name AS \"ROUTINE_NAME\", " +
				"type_udt_name AS \"DTD_IDENTIFIER\" FROM information_schema.routines " +
				"WHERE routine_schema = current_schema() ORDER BY SPECIFIC_NAME")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT c.relname AS \"Name\", " +
				"CASE c.relkind WHEN 'r' THEN 'table' " +
				"WHEN 'm' THEN 'materialized view' ELSE 'view' END AS \"Engine\", " +
				"pg_relation_size(c.oid) AS \"Data_length\", " +
				"pg_total_relation_size(c.oid) - pg_relation_size(c.oid) AS \"Index_length\", " +
				"obj_description(c.oid, 'pg_class') AS \"Comment\", " +
				"CASE WHEN c.relhasoids THEN 'oid' ELSE '' END AS \"Oid\", " +
				"c.reltuples as \"Rows\", n.nspname FROM pg_class c " +
				"JOIN pg_namespace n ON(n.nspname = current_schema() AND n.oid = c.relnamespace) " +
				"WHERE relkind IN ('r', 'm', 'v', 'f') ORDER BY relname")) {
			assertTrue(rs.next());
			assertEquals("test", rs.getString("Name"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT a.attname AS field, " +
				"format_type(a.atttypid, a.atttypmod) AS full_type, " +
				"pg_get_expr(d.adbin, d.adrelid) AS default, a.attnotnull::int, " +
				"col_description(c.oid, a.attnum) AS comment, 0 AS identity FROM pg_class c " +
				"JOIN pg_namespace n ON c.relnamespace = n.oid " +
				"JOIN pg_attribute a ON c.oid = a.attrelid " +
				"LEFT JOIN pg_attrdef d ON c.oid = d.adrelid AND a.attnum = d.adnum " +
				"WHERE c.relname = 'test' AND n.nspname = current_schema() AND " +
				"NOT a.attisdropped AND a.attnum > 0 ORDER BY a.attnum")) {
			assertTrue(rs.next());
			assertEquals("id", rs.getString("field"));
			assertEquals("INTEGER", rs.getString("full_type"));
			assertTrue(rs.next());
			assertEquals("x1", rs.getString("field"));
			assertEquals("INTEGER", rs.getString("full_type"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT relname, indisunique::int, " +
				"indisprimary::int, indkey, indoption , (indpred IS NOT NULL)::int as indispartial " +
				"FROM pg_index i, pg_class ci WHERE i.indrelid = 11 AND ci.oid = i.indexrelid")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT conname, condeferrable::int AS deferrable, " +
				"pg_get_constraintdef(oid) AS definition FROM pg_constraint " +
				"WHERE conrelid = (SELECT pc.oid FROM pg_class AS pc " +
				"INNER JOIN pg_namespace AS pn ON (pn.oid = pc.relnamespace) " +
				"WHERE pc.relname = 'test' AND pn.nspname = current_schema()) " +
				"AND contype = 'f'::char ORDER BY conkey, conname")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT * FROM information_schema.triggers " +
				"WHERE event_object_table = 'test'")) {
			assertFalse(rs.next());
		}
		stat.execute("RESET statement_timeout");
	}

	@Test
	public void testValentina() throws SQLException {
		try (ResultSet rs = stat.executeQuery("SELECT oid, datname, datconnlimit, encoding, " +
				"shobj_description( oid, 'pg_database' ) as comment, " +
				"pg_get_userbyid( datdba ) AS owner FROM pg_database " +
				"WHERE NOT datistemplate AND datname<>'postgres'")) {
			assertTrue(rs.next());
			assertEquals("pgserver", rs.getString("datname"));
			assertEquals("sa", rs.getString("owner"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT word from pg_get_keywords()")) {
			assertTrue(rs.next());
			assertNotNull(rs.getString("word"));
		}
		try (ResultSet rs = stat.executeQuery("SELECT p.proname AS fld_procedure" +
				", pg_catalog.format_type(p.prorettype, NULL) AS fld_return_type, " +
				"CASE WHEN p.pronargs = 0 AND p.proname = 'count' " +
				"THEN CAST('*' AS pg_catalog.text) ELSE pg_catalog.array_to_string(ARRAY" +
				"( SELECT pg_catalog.format_type(p.proargtypes[s.i], NULL) " +
				"FROM pg_catalog.generate_series(0, pg_catalog.array_upper(p.proargtypes, 1)) " +
				"AS s(i) ), ', ') END AS fld_arguments, " +
				"pg_catalog.obj_description(p.oid, 'pg_proc') as fld_description " +
				"FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n " +
				"ON n.oid = p.pronamespace AND pg_catalog.pg_function_is_visible(p.oid) " +
				"ORDER BY 1, 2, 4")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT oid, nspname, " +
				"obj_description( oid ) AS comment, pg_get_userbyid( nspowner ) AS owner " +
				"FROM pg_namespace WHERE nspname <> 'information_schema' " +
				"AND substr( nspname, 0, 4 ) <> 'pg_'")) {
			assertTrue(rs.next());
			assertEquals("public", rs.getString("nspname"));
			assertEquals("sa", rs.getString("owner"));
			assertTrue(rs.next());
			assertEquals("pg_catalog", rs.getString("nspname"));
			assertEquals("sa", rs.getString("owner"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM pg_event_trigger")) {
			assertTrue(rs.next());
			assertEquals(0, rs.getLong(1));
		}
		try (ResultSet rs = stat.executeQuery(
				"( SELECT lanname as fld_ident, 21 AS fld_kind FROM pg_language ) " +
				"UNION ( SELECT nspname as fld_ident, 2 AS fld_kind FROM pg_namespace ) " +
				"UNION ( SELECT schemaname || '.' || tablename AS fld_ident, 1 AS fld_kind FROM pg_tables ) " +
				"UNION ( SELECT tablename AS fld_ident, 1 AS fld_kind FROM pg_tables ) " +
				"UNION ( SELECT table_schema || '.' || table_name || '.' || column_name AS fld_ident, " +
				"3 AS fld_kind FROM information_schema.COLUMNS ) " +
				"UNION ( SELECT table_name || '.' || column_name AS fld_ident, " +
				"3 AS fld_kind FROM information_schema.COLUMNS ) " +
				"UNION ( SELECT column_name AS fld_ident, 3 AS fld_kind FROM information_schema.COLUMNS ) " +
				"UNION ( SELECT ns.nspname || '.' || proname AS fld_ident, 9 AS fld_kind " +
				"FROM pg_proc JOIN pg_namespace ns ON pronamespace = ns.oid ) " +
				"UNION ( SELECT proname AS fld_ident, 9 AS fld_kind FROM pg_proc ) " +
				"UNION ( SELECT schemaname || '.' || viewname AS fld_ident, 15 AS fld_kind FROM pg_views ) " +
				"UNION ( SELECT viewname AS fld_ident, 15 AS fld_kind FROM pg_views ) " +
				"UNION ( SELECT tgname AS fld_ident, 14 AS fld_kind FROM pg_trigger ) " +
				"UNION ( SELECT constraint_name AS fld_ident, 4 AS fld_kind " +
				"FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' ) " +
				"UNION ( SELECT cl.relname AS fld_ident, 16 AS fld_kind " +
				"FROM pg_index i JOIN pg_class cl ON cl.oid = i.indexrelid ) " +
				"UNION ( SELECT c.conname AS fld_ident, 17 AS fld_kind FROM pg_constraint c " +
				"JOIN pg_class cl ON c.conrelid = cl.oid AND c.contype = 'u' ) " +
				"UNION (  SELECT conname AS fld_ident, 18 AS fld_kind FROM pg_constraint " +
				"JOIN pg_class cl ON pg_constraint.conrelid = cl.oid AND contype='c' ) " +
				"UNION ( SELECT ns.nspname || '.' || cl.relname AS fld_ident, 19 AS fld_kind " +
				"FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace AND cl.relkind = 'S' )" +
				"UNION ( SELECT cl.relname AS fld_ident, 19 AS fld_kind " +
				"FROM pg_class cl WHERE cl.relkind = 'S' )")) {
			// just no exception
		}
		try (ResultSet rs = stat.executeQuery("SELECT  pg_class.oid, relname AS tablename, " +
				"nsp.nspname AS schema, pg_get_userbyid( relowner ) AS owner,conname, " +
				"relhasoids, obj_description( pg_class.oid ) AS comment, " +
				"( SELECT COUNT(*) FROM pg_attribute att WHERE att.attrelid = pg_class.oid " +
				"AND att.attnum > 0 AND att.attisdropped IS FALSE ) AS field_count " +
				"FROM pg_class JOIN pg_namespace nsp ON relnamespace = nsp.oid " +
				"AND relkind = 'r' AND nsp.nspname = 'public' " +
				"LEFT OUTER JOIN pg_constraint " +
				"ON pg_constraint.conrelid = pg_class.oid AND contype='p'")) {
			assertTrue(rs.next());
			assertEquals("test", rs.getString("tablename"));
			assertEquals("public", rs.getString("schema"));
			assertEquals("sa", rs.getString("owner"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery(
				"SELECT COUNT(*) FROM pg_catalog.pg_namespace ns " +
				"JOIN pg_catalog.pg_proc p ON p.pronamespace = ns.oid " +
				"AND ns.nspname = 'public' AND p.proisagg = FALSE")) {
			assertTrue(rs.next());
			assertEquals(0, rs.getLong(1));
		}
		try (ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM pg_type T " +
				"JOIN pg_namespace nsp ON nsp.oid = T.typnamespace " +
				"LEFT JOIN pg_class ct ON ct.oid = T.typrelid AND ct.relkind <> 'c' " +
				"WHERE ( T.typtype != 'd' AND T.typtype != 'p' AND T.typcategory != 'A' ) " +
				"AND (ct.oid IS NULL OR ct.oid = 0) -- filter for tables\r\n" +
				"AND nsp.nspname =  'public'")) {
			// just no exception
		}
		try (ResultSet rs = stat.executeQuery("SELECT att.attname AS column_name, " +
				"format_type(ty.oid, NULL) AS data_type, " +
				"ty.oid AS type_id, tn.nspname AS type_schema, " +
				"pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS column_default, " +
				"NOT att.attnotnull AS is_nullable, att.attnum AS ordinal_position, " +
				"att.attndims AS dimensions, att.atttypmod AS modifiers, '' AS comment, " +
				"'' AS collation FROM pg_attribute att " +
				"JOIN pg_type ty ON ty.oid = atttypid " +
				"JOIN pg_namespace tn ON tn.oid = ty.typnamespace " +
				"JOIN pg_class cl ON cl.oid = att.attrelid " +
				"JOIN pg_namespace na ON na.oid = cl.relnamespace " +
				"LEFT OUTER JOIN pg_attrdef def ON adrelid = att.attrelid " +
				"AND adnum = att.attnum WHERE na.nspname = 'public' AND cl.relname = 'test' " +
				"AND att.attnum > 0 AND att.attisdropped IS FALSE")) {
			assertTrue(rs.next());
			assertEquals("id", rs.getString("column_name"));
			assertEquals("INTEGER", rs.getString("data_type"));
			assertEquals(1, rs.getInt("ordinal_position"));
			assertTrue(rs.next());
			assertEquals("x1", rs.getString("column_name"));
			assertEquals("INTEGER", rs.getString("data_type"));
			assertEquals(2, rs.getInt("ordinal_position"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM pg_index i " +
				"LEFT JOIN pg_class ct ON ct.oid = i.indrelid " +
				"LEFT JOIN pg_class ci ON ci.oid = i.indexrelid " +
				"LEFT JOIN pg_namespace tns ON tns.oid = ct.relnamespace " +
				"LEFT JOIN pg_depend dep ON dep.classid = ci.tableoid " +
				"AND dep.objid = ci.oid AND dep.refobjsubid = '0' " +
				"LEFT JOIN pg_constraint con ON con.tableoid = dep.refclassid AND con.oid = dep.refobjid " +
				"WHERE conname IS NULL AND tns.nspname = 'public' AND ct.relname = 'test'")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getLong(1));
		}
		try (ResultSet rs = stat.executeQuery("SELECT ( SELECT CASE " +
				"WHEN ( reltuples IS NOT NULL AND reltuples > 1000 ) " +
				"THEN CONCAT( '~',  reltuples::BIGINT::TEXT ) " +
				"ELSE ( SELECT COUNT(*) FROM \"public\".\"test\" )::TEXT  END ) AS r " +
				"FROM pg_class JOIN pg_namespace nsp ON relnamespace = nsp.oid " +
				"AND relname= 'test' and nspname = 'public'")) {
			assertTrue(rs.next());
			assertEquals("0", rs.getObject("r"));
		}
		try (ResultSet rs = stat.executeQuery("SELECT ( SELECT CASE " +
				"WHEN ( reltuples IS NOT NULL AND reltuples > 1000 ) " +
				"THEN CONCAT( '~',  reltuples::BIGINT::TEXT ) " +
				"ELSE ( SELECT COUNT(*) FROM \"public\".\"test\" )::TEXT  END ) AS r " +
				"FROM pg_class JOIN pg_namespace nsp ON relnamespace = nsp.oid " +
				"AND relname= 'test' and nspname = 'public'")) {
			assertTrue(rs.next());
			assertEquals("0", rs.getObject("r"));
		}
		stat.execute("INSERT INTO test (x1) VALUES (2), (3), (4), (5)");
		try (ResultSet rs = stat.executeQuery("SELECT x1 FROM test ORDER BY id OFFSET 1 LIMIT 2")) {
			assertTrue(rs.next());
			assertEquals(3, rs.getInt("x1"));
			assertTrue(rs.next());
			assertEquals(4, rs.getInt("x1"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT DISTINCT i.indisunique FROM pg_index i " +
				"LEFT JOIN pg_class ct ON ct.oid = i.indrelid " +
				"LEFT JOIN pg_namespace tns ON tns.oid = ct.relnamespace " +
				"WHERE tns.nspname  = 'public' AND ct.relname = 'test' " +
				"AND array_length( i.indkey, 1 ) = 1 " +
				"AND quote_ident( 'name' ) = pg_get_indexdef( i.indexrelid, 1, TRUE )")) {
			assertFalse(rs.next());
		}
		stat.execute("CREATE TABLE test2 (x1 INT PRIMARY KEY, x2 INT, x3 INT, UNIQUE (x2, x3))");
		try (ResultSet rs = stat.executeQuery("SELECT c.oid, c.conname, " +
				"( SELECT obj_description( c.oid ) ) AS comment, " +
				"array_to_string( array(   " +
				"SELECT a.attname FROM pg_attribute a WHERE a.attnum = ANY( c.conkey ) " +
				"AND a.attrelid = c.conrelid ORDER BY (    SELECT i FROM ( " +
				"SELECT generate_series( array_lower( c.conkey, 1 ), array_upper( c.conkey, 1 ) ) ) g( i) " +
				"WHERE c.conkey[i] = a.attnum LIMIT 1 ) ), '\r\n' ) AS unique_fields FROM pg_constraint c " +
				"JOIN pg_class ON c.conrelid = pg_class.oid " +
				"JOIN pg_namespace n ON n.oid = relnamespace " +
				"WHERE c.contype = 'u' AND nspname ='public' AND relname = 'test2'")) {
			assertTrue(rs.next());
			assertEquals("x2\r\nx3", rs.getString("unique_fields"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("select pg_listening_channels()")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("EXPLAIN VERBOSE SELECT * FROM test")) {
			assertTrue(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT t.oid AS id, nsp.nspname AS schema, " +
				"t.typname AS name, t.typtype AS kind, pg_get_userbyid( t.typowner ) AS owner, " +
				"t.typlen AS size, obj_description( t.oid ) as comment, " +
				"NOT t.typnotnull AS is_nullable, t.typdefault, t_base.typname AS base_type, " +
				"t.typndims, CONCAT( '\"', cn.nspname, '\".\"', c.collname, '\"' ) AS collation, " +
				"information_schema._pg_char_max_length( t.typbasetype, t.typtypmod ) AS length, " +
				"information_schema._pg_numeric_precision( t.typbasetype, t.typtypmod ) AS precision, " +
				"information_schema._pg_numeric_scale( t.typbasetype, t.typtypmod ) AS scale, " +
				"information_schema._pg_datetime_precision( t.typbasetype, t.typtypmod ) AS datetime_precision " +
				"FROM pg_type T JOIN pg_namespace nsp ON nsp.oid = t.typnamespace " +
				"JOIN pg_type t_base ON t.typbasetype = t_base.oid " +
				"LEFT JOIN pg_collation c ON c.oid = t.typcollation " +
				"LEFT JOIN pg_namespace cn ON c.collnamespace = cn.oid " +
				"WHERE t.typtype = 'd' AND nsp.nspname = 'public'")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery(
				"SELECT proname || '$$' || array_to_string(p.proargtypes, '_') AS function_name, " +
				"n.nspname AS schema, proname, typname, lanname, p.oid, " +
				"pg_get_functiondef(p.oid) AS text, obj_description(p.oid) AS comment, " +
				"(SELECT CASE WHEN p.proallargtypes IS NULL " +
				"THEN array_to_string(array(SELECT t.typname FROM pg_type t JOIN (SELECT i FROM (SELECT " +
				"generate_series(array_lower(p.proargtypes, 1), array_upper(p.proargtypes, 1))) g(i)) sub " +
				"ON p.proargtypes[sub.i] = t.oid ORDER BY sub.i), '\r\n ') " +
				"ELSE array_to_string(array(SELECT t.typname FROM pg_type t JOIN (SELECT i FROM (SELECT " +
				"generate_series(array_lower(p.proallargtypes, 1), array_upper(p.proallargtypes, 1))) g(i)) sub " +
				"ON p.proallargtypes[sub.i] = t.oid ORDER BY sub.i), '\r\n') END) AS argtypenames, " +
				"array_to_string(array(SELECT t.typname FROM pg_type t JOIN (SELECT i FROM (SELECT " +
				"generate_series(array_lower(p.proargtypes, 1), array_upper(p.proargtypes, 1))) g(i)) sub " +
				"ON p.proargtypes[sub.i] = t.oid ORDER BY sub.i), '\r\n ') AS argsignature, " +
				"array_to_string(p.proargmodes, '\r\n') AS argmodes FROM pg_catalog.pg_namespace n " +
				"JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid " +
				"JOIN (SELECT 0 AS oid, '' AS lanname WHERE FALSE) l ON p.prolang = l.oid " +
				"JOIN pg_catalog.pg_type t ON p.prorettype = t.oid " +
				"WHERE proisagg = false AND n.nspname = 'public'")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT c.oid, c.conname AS constraint_name, " +
				"ns.nspname AS schema, ns_c.nspname As \"Schema Child\", " +
				"ns_p.nspname As \"Schema Parent\", obj_description( c.oid ) AS comment, " +
				"confdeltype, confupdtype, confmatchtype, cls_c.relname AS table_name, " +
				"cls_p.relname AS foreign_table_name FROM pg_constraint c " +
				"JOIN pg_namespace ns ON ns.oid = c.connamespace " +
				"JOIN pg_class cls_c ON cls_c.oid = c.conrelid " +
				"JOIN pg_namespace ns_c ON ns_c.oid = cls_c.relnamespace " +
				"JOIN pg_class cls_p ON cls_p.oid = c.confrelid " +
				"JOIN pg_namespace ns_p ON ns_p.oid = cls_p.relnamespace " +
				"WHERE c.contype = 'f' AND ( ns_c.nspname = 'public' OR ns_p.nspname = 'public' )")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT T.oid AS id, nsp.nspname AS schema, " +
				"T.typname AS name, T.typtype AS kind, pg_get_userbyid(t.typowner) AS owner, " +
				"typlen AS len, obj_description(t.oid) AS comment FROM pg_type T " +
				"JOIN pg_namespace nsp ON nsp.oid = T.typnamespace " +
				"LEFT JOIN pg_class ct ON ct.oid = T.typrelid AND ct.relkind <> 'c' " +
				"WHERE (T.typtype != 'd' AND T.typtype != 'p' AND TRUE) " +
				"AND (ct.oid IS NULL OR ct.oid = 0) AND nsp.nspname = 'public'")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT cl.oid, cl.relname AS viewname, " +
				"ns.nspname AS schema,ltrim( pg_get_viewdef( cl.oid ) ) AS definition, " +
				"obj_description( cl.oid ) AS comment FROM pg_class cl " +
				"JOIN pg_namespace ns ON ns.oid=relnamespace " +
				"AND cl.relkind = 'v' AND ns.nspname = 'public'")) {
			assertFalse(rs.next());
		}
		try (PreparedStatement ps = conn.prepareStatement("INSERT INTO \"public\".\"test\" " +
				"( \"id\", \"x1\" ) VALUES(?, ? ) RETURNING \"id\", \"x1\"")) {
			ps.setInt(1, 5);
			ps.setInt(2, 10);
			assertEquals(1, ps.executeUpdate());
		}
	}

	@Test
	public void testPhpPgAdmin() throws SQLException {
		try (ResultSet rs = stat.executeQuery("SELECT pdb.datname AS datname, " +
				"pr.rolname AS datowner, pg_encoding_to_char(encoding) AS datencoding, " +
				"(SELECT description FROM pg_catalog.pg_shdescription pd " +
				"WHERE pdb.oid=pd.objoid AND pd.classoid='pg_database'::regclass) AS datcomment, " +
				"(SELECT spcname FROM pg_catalog.pg_tablespace pt " +
				"WHERE pt.oid=pdb.dattablespace) AS tablespace, " +
				"pg_catalog.pg_database_size(pdb.oid) as dbsize " +
				"FROM pg_catalog.pg_database pdb " +
				"LEFT JOIN pg_catalog.pg_roles pr ON (pdb.datdba = pr.oid) " +
				"WHERE true AND NOT pdb.datistemplate ORDER BY pdb.datname")) {
			assertTrue(rs.next());
			assertEquals("pgserver", rs.getString("datname"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT current_schemas(false) AS search_path")) {
			assertTrue(rs.next());
			assertEquals("{public}", rs.getString("search_path"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT c.relname, " +
				"pg_catalog.pg_get_userbyid(c.relowner) AS relowner, " +
				"pg_catalog.obj_description(c.oid, 'pg_class') AS relcomment, reltuples::bigint, " +
				"(SELECT spcname FROM pg_catalog.pg_tablespace pt WHERE pt.oid=c.reltablespace) " +
				"AS tablespace FROM pg_catalog.pg_class c " +
				"LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
				"WHERE c.relkind = 'r' AND nspname='public' ORDER BY c.relname")) {
			assertTrue(rs.next());
			assertEquals("test", rs.getString("relname"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT a.attname, a.attnum, " +
				"pg_catalog.format_type(a.atttypid, a.atttypmod) as type, a.atttypmod, a.attnotnull, " +
				"a.atthasdef, pg_catalog.pg_get_expr(adef.adbin, adef.adrelid, true) as adsrc, " +
				"a.attstattarget, a.attstorage, t.typstorage, " +
				"(SELECT 1 FROM pg_catalog.pg_depend pd, pg_catalog.pg_class pc " +
				"WHERE pd.objid=pc.oid AND pd.classid=pc.tableoid AND pd.refclassid=pc.tableoid " +
				"AND pd.refobjid=a.attrelid AND pd.refobjsubid=a.attnum AND pd.deptype='i' " +
				"AND pc.relkind='S') IS NOT NULL AS attisserial, " +
				"pg_catalog.col_description(a.attrelid, a.attnum) AS comment " +
				"FROM pg_catalog.pg_attribute a " +
				"LEFT JOIN pg_catalog.pg_attrdef adef ON a.attrelid=adef.adrelid AND a.attnum=adef.adnum " +
				"LEFT JOIN pg_catalog.pg_type t ON a.atttypid=t.oid " +
				"WHERE a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='test' " +
				"AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')) " +
				"AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum")) {
			assertTrue(rs.next());
			assertEquals("id", rs.getString("attname"));
			assertTrue(rs.next());
			assertEquals("x1", rs.getString("attname"));
			assertFalse(rs.next());
		}
		stat.execute("SET TRANSACTION READ ONLY");
		stat.execute("CREATE TABLE test2 (x1 INT, x2 INT, PRIMARY KEY (x1, x2))");
		try (ResultSet rs = stat.executeQuery("SELECT DISTINCT " +
				"max(SUBSTRING(array_dims(c.conkey) FROM  $pattern$^\\[.*:(.*)\\]$$pattern$)) as nb " +
				"FROM pg_catalog.pg_constraint AS c " +
				"JOIN pg_catalog.pg_class AS r ON (c.conrelid=r.oid) " +
				"JOIN pg_catalog.pg_namespace AS ns ON (r.relnamespace=ns.oid) " +
				"WHERE r.relname = 'test2' AND ns.nspname='public'")) {
			assertTrue(rs.next());
			assertEquals(2, rs.getInt("nb"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT c.relname, " +
				"pg_catalog.pg_get_userbyid(c.relowner)  AS relowner, " +
				"pg_catalog.obj_description(c.oid, 'pg_class') AS relcomment\r\n" +
				"FROM pg_catalog.pg_class c " +
				"LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace) " +
				"WHERE (n.nspname='public') AND (c.relkind = 'v'::\"char\") " +
				"ORDER BY relname\r\n")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT c.oid AS conid, c.contype, " +
				"c.conname, pg_catalog.pg_get_constraintdef(c.oid, true) AS consrc, " +
				"ns1.nspname as p_schema, r1.relname as p_table, ns2.nspname as f_schema, " +
				"r2.relname as f_table, f1.attname as p_field, f1.attnum AS p_attnum, " +
				"f2.attname as f_field, f2.attnum AS f_attnum, " +
				"pg_catalog.obj_description(c.oid, 'pg_constraint') AS constcomment, " +
				"c.conrelid, c.confrelid FROM pg_catalog.pg_constraint AS c " +
				"JOIN pg_catalog.pg_class AS r1 ON (c.conrelid=r1.oid) " +
				"JOIN pg_catalog.pg_attribute AS f1 ON (f1.attrelid=r1.oid AND (f1.attnum=c.conkey[1])) " +
				"JOIN pg_catalog.pg_namespace AS ns1 ON r1.relnamespace=ns1.oid " +
				"LEFT JOIN (pg_catalog.pg_class AS r2 JOIN pg_catalog.pg_namespace AS ns2 " +
				"ON (r2.relnamespace=ns2.oid)) ON (c.confrelid=r2.oid) " +
				"LEFT JOIN pg_catalog.pg_attribute AS f2 ON (f2.attrelid=r2.oid AND " +
				"((c.confkey[1]=f2.attnum AND c.conkey[1]=f1.attnum))) " +
				"WHERE r1.relname = 'test' AND ns1.nspname='public' ORDER BY 1")) {
			assertTrue(rs.next());
			assertEquals("p", rs.getString("contype"));
			assertEquals("test", rs.getString("p_table"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT indrelid, indkey " +
				"FROM pg_catalog.pg_index WHERE indisunique AND indrelid=" +
				"(SELECT oid FROM pg_catalog.pg_class WHERE relname='test2' AND relnamespace=(" +
				"SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='public')) " +
				"AND indpred IS NULL AND indexprs IS NULL ORDER BY indisprimary DESC LIMIT 1")) {
			assertTrue(rs.next());
			assertTrue(Arrays.asList("{1,2}", "{2,1}").contains(rs.getString("indkey")));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT attnum, attname " +
				"FROM pg_catalog.pg_attribute WHERE attrelid=(" +
				"SELECT oid FROM pg_catalog.pg_class WHERE relname='test2' AND " +
				"relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='public')) " +
				"AND attnum IN ('')")) {
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT attnum, attname " +
				"FROM pg_catalog.pg_attribute WHERE attrelid=(" +
				"SELECT oid FROM pg_catalog.pg_class WHERE relname='test2' AND " +
				"relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='public')) " +
				"AND attnum IN ('{1,2}')")) {
			assertTrue(rs.next());
			assertEquals("x1", rs.getString("attname"));
			assertTrue(rs.next());
			assertEquals("x2", rs.getString("attname"));
			assertFalse(rs.next());
		}
	}

	@Test
	public void testSquirrelSQL() throws SQLException {
		try (ResultSet rs = stat.executeQuery("SELECT nspname AS TABLE_SCHEM, " +
				"NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace WHERE nspname <> 'pg_toast' " +
				"AND (nspname !~ '^pg_temp_' OR nspname = (pg_catalog.current_schemas(true))[1]) " +
				"AND (nspname !~ '^pg_toast_temp_' OR nspname = " +
				"replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_')) " +
				"ORDER BY TABLE_SCHEM")) {
			assertTrue(rs.next());
			assertEquals("information_schema", rs.getString("TABLE_SCHEM"));
			assertTrue(rs.next());
			assertEquals("pg_catalog", rs.getString("TABLE_SCHEM"));
			assertTrue(rs.next());
			assertEquals("public", rs.getString("TABLE_SCHEM"));
			assertFalse(rs.next());
		}
	}

	@Test
	public void testJSqlParser() throws SQLException {
		stat.execute("INSERT INTO test (x1) VALUES (2), (3), (4)");
		try (ResultSet rs = stat.executeQuery("SELECT id, x1 FROM test " +
				"WHERE id = ANY(SELECT x1 FROM test WHERE row_to_json() = '{}') ORDER BY id")) {
			assertTrue(rs.next());
			assertEquals(2, rs.getInt("id"));
			assertEquals(3, rs.getInt("x1"));
			assertTrue(rs.next());
			assertEquals(3, rs.getInt("id"));
			assertEquals(4, rs.getInt("x1"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT x1 " +
				"FROM test WHERE id::varchar = ANY('{3,4,5}')")) {
			assertTrue(rs.next());
			assertEquals(4, rs.getInt("x1"));
			assertFalse(rs.next());
		}
		// See https://github.com/JSQLParser/JSqlParser/issues/720
		/*
		try (ResultSet rs = stat.executeQuery("SELECT 0 IS NULL, 1 IS NOT NULL, 0 IN (2, 1)")) {
			assertTrue(rs.next());
			assertFalse(rs.getBoolean(1));
			assertTrue(rs.getBoolean(2));
			assertFalse(rs.getBoolean(3));
		}
		*/
		stat.execute("CREATE TABLE test2 (id INT, indpred INT)");
		stat.execute("INSERT INTO test2 (id, indpred) VALUES (1, 0), (2, NULL)");
		try (ResultSet rs = stat.executeQuery(
				"SELECT (indpred IS NOT NULL)::int AS i FROM test2 ORDER BY id")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt("i"));
			assertTrue(rs.next());
			assertEquals(0, rs.getInt("i"));
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT pg_total_relation_size(0)::text::int::text")) {
			assertTrue(rs.next());
			assertEquals("0", rs.getObject(1));
		}
		try (ResultSet rs = stat.executeQuery("SELECT * FROM generate_series(5, 10)")) {
			for (int i = 5; i <= 10; i ++) {
				assertTrue(rs.next());
				assertEquals(i, rs.getLong(1));
			}
			assertFalse(rs.next());
		}
		try (ResultSet rs = stat.executeQuery("SELECT generate_series(5, 10) gs")) {
			for (int i = 5; i <= 10; i ++) {
				assertTrue(rs.next());
				assertEquals(i, rs.getLong("gs"));
			}
			assertFalse(rs.next());
		}
		/*
		try (ResultSet rs = stat.executeQuery("SELECT array_upper(ARRAY[5, 6, 7, 8, 9, 10], 1)")) {
			assertEquals(6, rs.getLong(1));
		}
		*/
		try (ResultSet rs = stat.executeQuery("SELECT array_upper()")) {
			assertFalse(true);
		} catch (SQLException e) {
			assertEquals("42001", e.getSQLState());
		}
		int oidPgClass, oidTest;
		try (ResultSet rs = stat.executeQuery("SELECT oid FROM pg_class " +
				"WHERE relname IN ('pg_class', 'test') ORDER BY oid")) {
			rs.next();
			oidPgClass = rs.getInt("oid");
			rs.next();
			oidTest = rs.getInt("oid");
		}
		try (ResultSet rs = stat.executeQuery("SELECT " + (oidPgClass & 0xFFFFFFFFL) +
				"::regclass, " + oidTest + "::regclass")) {
			rs.next();
			assertEquals("pg_class", rs.getString(1));
			assertEquals("test", rs.getString(2));
		}
		try (ResultSet rs = stat.executeQuery("SELECT 10000001::regclass")) {
			rs.next();
			assertEquals("10000001", rs.getString(1));
		}
		try (ResultSet rs = stat.executeQuery("SELECT x1 FROM test")) {
			ResultSetMetaData rsmd = rs.getMetaData();
			assertEquals("test", rsmd.getTableName(1));
		}
	}

	@After
	public void after() throws SQLException {
		stat.close();
		conn.close();
		server.stop();
	}
}