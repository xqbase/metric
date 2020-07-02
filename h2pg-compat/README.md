### PgJDBC

PgJDBC 42.2.9 and 42.2.14 tested.

New supported APIs:

- `DatabaseMetaData.getIndexInfo()`
- `DatabaseMetaData.getPrimaryKeys()`

### PgAdmin

PgAdmin 4.16 tested.

Supported operations:

- List Databases, Schemas, Tables, Columns
- Query
- View/Edit Data
- Update/Delete on Data Output

Unsupported operations:

- Insert on Data Output, due to uncommitted `INSERT INTO ... RETURNING ...`

### HeidiSQL

HeidiSQL 11.0.0 tested.

### DBeaver

DBeaver 7.0.5 tested.

### Adminer

Adminer 4.7.7 tested.

### Valentina Studio

Valentina Studio 10.0 tested.

- Insert on Data Editor: success but no response, perhaps due to committed `INSERT INTO ... RETURNING ...`

### phpPgAdmin

phpPgAdmin 7.0-dev (docker.io/dockage/phppgadmin) and 7.12.1 tested.

Unsupported operations:

- Rules, Admin, Info

Unsupported operations with 7.12.1:

- Columns, Browse: for tables with composite primary key

### SQuirreL SQL

SQuirreL SQL 4.1.0 tested.

### Sqlectron

Sqlectron 1.30.0 tested.