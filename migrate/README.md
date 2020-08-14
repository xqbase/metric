## Data Migration Tool

Migrate huge data between different databases, with fast speed, little memory usage, and without dumping into temporary files.

### Usage

	java [-cp <jdbc-driver>[:<jdbc-driver>]] -jar migrate.jar <src.jdbc.url> <dst.jdbc.url>

### JDBC Drivers

JDBC driver jars can be packaged into `migrate.jar`, or loaded with `-cp`.

#### In Package

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

#### Stand-alone Jar File (-cp)

Just load JDBC driver with `-cp`:

	java -cp postgresql.jar:mysql-connector-java.jar -jar migrate.jar "jdbc:postgresql://..." "jdbc:mysql://..."

(In Windows, path separator `:` should be replaced with `;`)

### Table Schema

### Source JDBC URL

### Destination JDBC URL