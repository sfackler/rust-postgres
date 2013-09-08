Rust-Postgres
=============
A native PostgreSQL driver for Rust.

Overview
========
Rust-Postgres is a pure-Rust frontend for the popular PostgreSQL database. It
exposes a high level interface in the vein of JDBC or Go's `database/sql`
package.
```rust
extern mod postgres;

use postgres::PostgresConnection;
use postgres::types::ToSql;

#[deriving(ToStr)]
struct Person {
    id: i32,
    name: ~str,
    awesome: bool,
    data: Option<~[u8]>
}

fn main() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost");

    conn.update("CREATE TABLE person (
                    id      SERIAL PRIMARY KEY,
                    name    VARCHAR NOT NULL,
                    awesome BOOL NOT NULL,
                    data    BYTEA
                 )", []);
    let me = Person {
        id: 0,
        name: ~"Steven",
        awesome: true,
        data: None
    };
    conn.update("INSERT INTO person (name, awesome, data)
                    VALUES ($1, $2, $3)",
                 [&me.name as &ToSql, &me.awesome as &ToSql,
                  &me.data as &ToSql]);

    let stmt = conn.prepare("SELECT id, name, awesome, data FROM person");
    for row in stmt.query([]) {
        let person = Person {
            id: row[0],
            name: row[1],
            awesome: row[2],
            data: row[3]
        };
        println!("Found person {}", person.to_str());
    }
}
```

Requirements
============

* **Rust** - Rust-Postgres is developed against the *master* branch of the Rust
    repository. It will most likely not build against the releases on
    http://www.rust-lang.org.

* **PostgreSQL 7.4 or later** - Rust-Postgres speaks version 3 of the
    PostgreSQL protocol, which corresponds to versions 7.4 and later. If your
    version of Postgres was compiled in the last decade, you should be okay.

Usage
=====

Connecting
----------
Connect to a Postgres server using the standard URI format:
```rust
let conn = PostgresConnection::connect("postgres://user:pass@host:port/database?arg1=val1&arg2=val2");
```
`pass` may be omitted if not needed. `port` defaults to `5432` and `database`
defaults to the value of `user` if not specified. The driver supports `trust`,
`password` and `md5` authentication.

Statement Preparation
---------------------
Prepared statements can have parameters, represented as `$n` where `n` is an
index into the parameter array starting from 1:
```rust
let stmt = conn.prepare("SELECT * FROM foo WHERE bar = $1 AND baz = $2");
```

Querying
--------
A prepared statement can be executed with the `query` and `update` methods.
Both methods take an array of parameters to bind to the query represented as
`&ToSql` trait objects. `update` returns the number of rows affected by the
query (or 0 if not applicable):
```rust
let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2");
let updates = stmt.update([&1i32 as &ToSql, & &"biz" as &ToSql]);
println!("{} rows were updated", updates);
```
`query` returns a result iterator. Fields of each row in the result can be
accessed either by their indicies or their column names:
```rust
let stmt = conn.prepare("SELECT bar, baz FROM foo");
for row in stmt.query([]) {
    let bar: i32 = row[0];
    let baz: ~str = row["baz"];
    println!("bar: {}, baz: {}", bar, baz);
}
```
In addition, `PostgresConnection` has a utility `update` method which is useful
if a statement is only going to be executed once:
```rust
let updates = conn.update("UPDATE foo SET bar = $1 WHERE baz = $2",
                          [&1i32 as &ToSql, & &"biz" as &ToSql]);
println!("{} rows were updated", updates);
```

Transactions
------------
Transactions are encapsulated by the `in_transaction` method. `in_transaction`
takes a closure which is passed a `PostgresTransaction` object which has the
functionality of a `PostgresConnection` as well as methods to control the
result of the transaction:
```rust
do conn.in_transaction |trans| {
    trans.update(...);
    let stmt = trans.prepare(...);

    if a_bad_thing_happened {
        trans.set_rollback();
    }

    if the_coast_is_clear {
        trans.set_commit();
    }
}
```
A transaction will commit by default. Nested transactions are supported via
savepoints.

Lazy Queries
------------
Some queries may return a large amount of data. Inside of a transaction,
prepared statements have an additional method, `lazy_query`. The rows returned
from a call to `lazy_query` are pulled from the database lazily as needed:
```rust
do conn.in_transaction |trans| {
    let stmt = trans.prepare(query)

    // No more than 100 rows will be stored in memory at any time
    for row in stmt.lazy_query(100, params) {
        // do things
    }
}
```

Error Handling
--------------
The methods described above will fail if there is an error. For each of these
methods, there is a second variant prefixed with `try_` which returns a
`Result`:
```rust
match conn.try_update(query, params) {
    Ok(updates) => println!("{} rows were updated", updates),
    Err(err) => println!("An error occurred: {}", err.to_str())
}
```

Type Correspondence
-------------------
Rust-Postgres enforces a strict correspondence between Rust types and Postgres
types. The driver currently natively supports the following conversions:

<table>
    <thead>
        <tr>
            <td>Rust Type</td>
            <td>Postgres Type</td>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>bool</td>
            <td>BOOL</td>
        </tr>
        <tr>
            <td>i8</td>
            <td>"char"</td>
        </tr>
        <tr>
            <td>i16</td>
            <td>SMALLINT</td>
        </tr>
        <tr>
            <td>i32</td>
            <td>INT</td>
        </tr>
        <tr>
            <td>i64</td>
            <td>BIGINT</td>
        </tr>
        <tr>
            <td>f32</td>
            <td>FLOAT4</td>
        </tr>
        <tr>
            <td>f64</td>
            <td>FLOAT8</td>
        </tr>
        <tr>
            <td>str</td>
            <td>VARCHAR, CHAR(n), TEXT</td>
        </tr>
        <tr>
            <td>[u8]</td>
            <td>BYTEA</td>
        </tr>
        <tr>
            <td>extra::json::Json</td>
            <td>JSON</td>
        </tr>
        <tr>
            <td>extra::uuid::Uuid</td>
            <td>UUID</td>
        </tr>
        <tr>
            <td>extra::time::Timespec</td>
            <td>TIMESTAMP</td>
        </tr>
    </tbody>
</table>

More conversions can be defined by implementing the `ToSql` and `FromSql`
traits.

Development
===========
Rust-Postgres is still under active development, so don't be surprised if APIs
change and things break. If something's not working properly, file an issue or
submit a pull request!
