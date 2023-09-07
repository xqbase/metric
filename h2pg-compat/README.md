### PgJDBC

PgJDBC 42.2.9 and 42.2.14 tested.

New supported APIs:

- `DatabaseMetaData.getIndexInfo()`
- `DatabaseMetaData.getPrimaryKeys()`
- `DatabaseMetaData.getImportedKeys()`
- `DatabaseMetaData.getVersionColumns()`

### PgAdmin

PgAdmin 4.16 tested.

Known issue:

- Unable to insert in DataGrid, due to uncommitted `INSERT INTO ... RETURNING ...`

### Npgsql

Npgsql 4.0.11 and 4.1.5 tested.

Known issues:

- Unable to select `ARRAY` columns (should convert with `::TEXT`)
- Should use `PgServerCompat.setNpgsql41(true)` to support Npgsql 4.1.x (date/time format not supported, should convert with `::TEXT`)

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

Known issue:

- Insertion in DataGrid succeeds but has no response, due to committed `INSERT INTO ... RETURNING ...`

### phpPgAdmin

phpPgAdmin 7.x-dev (docker.io/dockage/phppgadmin) and 7.12.1 tested.

Known issue:

- Unable to open DataGrid (Columns and Browse) for tables with composite primary key

### SQuirreL SQL

SQuirreL SQL 4.1.0 tested.

### Sqlectron

Sqlectron 1.30.0 tested.

### Postico

Postico 1.5.13 tested.

### DbVisualizer

DbVisualizer Free 11.0.3 tested.

### LibreOffice / OpenOffice Base

LibreOffice Base 6.3.6.2 and OpenOffice Base 4.1.7 tested.

Known issue:

- Unable to edit in DataGrid via native PostgreSQL or ODBC (JDBC works)

### Navicat

Navicat 15.0.17 tested.

Known issue:

- Should check `View / Show Hidden Items` to show all tables

### DataGrip

DataGrip 2020.1.5 tested.

### Database Navigator plugin for IntelliJ IDEs

Database Navigator plugin 3.2.0627 tested.

### TablePlus

TablePlus 3.6.3 and TablePlus iOS 3.8.0 tested.

### dbForge Studio

dbForge Studio 2.3.237 tested.

Known issues:

- Unable to edit in DataGrid
- Unable to select `ARRAY` columns (should convert with `::TEXT`)

### Tableau

Tableau 2020.2.2 tested.

Known Issue:

- Native PostgreSQL doesn't work (JDBC and ODBC work)

### Power BI

Power BI 2.83.5894.721 tested.

### Pentaho

Pentaho 9.0.0.0 tested.

### ~Toad Edge~

Toad Edge not supported.

### ~OmniDB~

OmniDB not supported.