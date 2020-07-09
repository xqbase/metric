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

Unsupported operations:

- Design Table

### ~Toad Edge~

Toad Edge not supported.

### ~OmniDB~

OmniDB not supported.