#### PgAdmin

PgAdmin 4.16 tested.

Supported Operations:

- List Databases, Schemas, Tables, Columns
- Query
- View/Edit Data
- Update/Delete on Data Output

Unsupported Operations:

- Insert on Data Output, due to uncommitted `INSERT INTO ... RETURNING ...`

#### HeidiSQL

HeidiSQL 11.0.0 tested.

#### DBeaver

DBeaver 7.0.5 tested.

#### Adminer

Adminer 4.7.7 tested.

#### Valentina Studio

Valentina Studio 10.0 tested.

- Insert on Data Editor: success but no response, perhaps due to committed `INSERT INTO ... RETURNING ...`

#### phpPgAdmin

phpPgAdmin 7.12.1 tested.

Unsupported Operations:

- Columns, Browse: for tables with composite primary key
- Indexes (due to incomplete implementation of `pg_index`)
- Rules, Admin, Info