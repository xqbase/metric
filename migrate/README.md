## Table Migration Tool

Migrate huge data between different databases, with fast speed, little memory usage, and without dumping into temporary files.

### Usage

	java -jar migrate.jar "<src.jdbc.url>" "<dst.jdbc.url>"

or

	java -cp migrate.jar:<src.jdbc.driver.jar>:<dst.jdbc.driver.jar> com.xqbase.metric.Migrate "<src.jdbc.url>" "<dst.jdbc.url>"

### JDBC Drivers

JDBC driver jars can be packaged into `migrate.jar`, or loaded with `-cp`.

#### In one Package

If the migration may be frequently done from a source to a target, and JDBC drivers don't need to upgrade, it is worth to do some work to build one stand-alone package.

Add the dependency in `pom.xml` to package a JDBC driver, e.g.

	<dependencies>
		<dependency>
			<groupId>org.postgresql</groupId>
			<artifactId>postgresql</artifactId>
			<version>42.2.9</version>
			<scope>runtime</scope>
		</dependency>
		<dependency>
			<groupId>mysql</groupId>
			<artifactId>mysql-connector-java</artifactId>
			<version>8.0.19</version>
			<scope>runtime</scope>
		</dependency>
	</dependencies>

In this case, `META-INF/service/java.sql.Driver` does not work, so JDBC driver must be programmatically loaded by `Migrate.java`, e.g.

	String[] DRIVERS = {
		"org.postgresql.Driver",
		"com.mysql.jdbc.Driver",
	};
	...
	for (String driver : DRIVERS) {
		Class.forName(driver);
	}

Then package with `mvn package`, and run with `java -jar migrate.jar "jdbc:postgresql://..." "jdbc:mysql://..."`.

#### In ClassPaths

No need to rebuild package, but command line is verbose:

	java -cp migrate.jar:postgresql-42.2.9.jar:mysql-connector-java-8.0.19.jar com.xqbase.metric.Migrate "jdbc:postgresql://..." "jdbc:mysql://..."

(In Windows, path separator `:` should be replaced with `;`)

### Table Schema

Migration Tool cannot understand `CREATE TABLE` syntaxes from different databases, so users should create table on target database manually.

For example, a table in PostgreSQL has schema:

	CREATE TABLE metric_name (
		id SERIAL PRIMARY KEY,
		name VARCHAR(64) NOT NULL,
		minute_size BIGINT NOT NULL DEFAULT 0,
		quarter_size BIGINT NOT NULL DEFAULT 0,
		aggregated_time INTEGER NOT NULL DEFAULT 0,
		tags TEXT DEFAULT NULL, -- tags may be longer than 64K
		UNIQUE (name));

Before migrating into MySQL, the following SQL must be run:

	CREATE TABLE metric_name (
		id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(64) NOT NULL,
		minute_size BIGINT NOT NULL DEFAULT 0,
		quarter_size BIGINT NOT NULL DEFAULT 0,
		aggregated_time INTEGER NOT NULL DEFAULT 0,
		tags LONGTEXT DEFAULT NULL,
		UNIQUE (name));

Migration Tool uses `INSERT INTO <table> VALUES (<row>)` without giving column names, so column number and order in target table must be consistent with source table.

### Source JDBC URL

Migration Tool uses `SELECT * FROM <table>` without condition or pagination. To prevent all data loaded into memory, cursor fetch should be enabled in JDBC URL.

#### MySQL

Just set `defaultFetchSize` to `Integer.MIN_VALUE` in URL (see https://stackoverflow.com/a/12814037/4260959 ). Other options need to write additional codes (see https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html ). A typical MySQL JDBC URL with cursor-fetch feature is:

	jdbc:mysql://localhost/metric?user=root&password=****&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&defaultFetchSize=-2147483648

#### PostgreSQL

Set a reasonable `defaultRowFetchSize` (e.g. `100`). Migration Tool will automatically turn off `autoCommit` for PostgreSQL (see https://jdbc.postgresql.org/documentation/head/query.html ). A typical PostgreSQL JDBC URL with cursor-fetch feature is:

	jdbc:postgresql://localhost/postgres?user=postgres&password=****&currentSchema=metric&defaultRowFetchSize=100

#### H2 Database

Set `lazy_query_execution=1` in URL, e.g.

	jdbc:h2:file:/var/lib/metric/data/metric;lazy_query_execution=1

#### Other Embedded Databases

Apache Derby and SQLite-JDBC will automatically use cursor fetch without additional options in URL.

### Destination JDBC URL

Migration Tool uses `PreparedStatement.executeBatch()` to increase performance of insertion. It works well on all embedded databases and PostgreSQL.

Increase `BATCH` value in `Migrate.java` may speed up insertion, but it uses more memory.

#### MySQL

Set `rewriteBatchedStatements=true` in URL to make `executeBatch()` work (see https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html ), e.g.

	jdbc:mysql://localhost/metric?user=root&password=****&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true