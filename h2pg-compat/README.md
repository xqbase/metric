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

### pgcli

pgcli 3.0.0 tested.

### HeidiSQL

HeidiSQL 11.0.0.5919 tested.

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

### Postico

Postico 1.5.13 tested.

### DbVisualizer

DbVisualizer Free 11.0.3 tested.

### LibreOffice Base

LibreOffice Base 6.3.6.2 tested.

Unsupported operations:

- Edit in Table Data View

### Navicat

Navicat 15.0.17 tested.

Should check `View / Show Hidden Items` to show all tables.

### DataGrip

DataGrip 2020.1.5 tested.

### Database Navigator plugin for IntelliJ IDEs

Database Navigator plugin 3.2.0627 tested.

### TablePlus

TablePlus 3.6.3 tested.

### dbForge Studio

dbForge Studio 2.2.207 tested.

Unsupported operations:

- Data editing
- Select `TEXT` columns in non-paginal mode
- Select `REAL` or `ARRAY` columns in paginal or non-paginal mode

### Tableau

Tableau 2020.2.2 tested via JDBC and ODBC.

Connection via PostgreSQL directly is not supported.

### ~Toad Edge~

Toad Edge not supported.

### ~OmniDB~

OmniDB not supported.