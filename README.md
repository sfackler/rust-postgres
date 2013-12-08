Rust-Postgres
=============
A native PostgreSQL driver for Rust.

Documentation is available at http://sfackler.com/doc/rust-postgres/.

[![Build Status](https://travis-ci.org/sfackler/rust-postgres.png?branch=master)](https://travis-ci.org/sfackler/rust-postgres)

Overview
========
Rust-Postgres is a pure-Rust frontend for the popular PostgreSQL database. It
exposes a high level interface in the vein of JDBC or Go's `database/sql`
package.
```rust
extern mod postgres = "github.com/sfackler/rust-postgres";
extern mod extra;

use extra::time;
use extra::time::Timespec;

use postgres::{PostgresConnection, PostgresStatement, NoSsl};
use postgres::types::ToSql;

struct Person {
    id: i32,
    name: ~str,
    time_created: Timespec,
    data: Option<~[u8]>
}

fn main() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost",
                                           &NoSsl);

    conn.update("CREATE TABLE person (
                    id              SERIAL PRIMARY KEY,
                    name            VARCHAR NOT NULL,
                    time_created    TIMESTAMP NOT NULL,
                    data            BYTEA
                 )", []);
    let me = Person {
        id: 0,
        name: ~"Steven",
        time_created: time::get_time(),
        data: None
    };
    conn.update("INSERT INTO person (name, time_created, data)
                    VALUES ($1, $2, $3)",
                 [&me.name as &ToSql, &me.time_created as &ToSql,
                  &me.data as &ToSql]);

    let stmt = conn.prepare("SELECT id, name, time_created, data FROM person");
    for row in stmt.query([]) {
        let person = Person {
            id: row[1],
            name: row[2],
            time_created: row[3],
            data: row[4]
        };
        println!("Found person {}", person.name);
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
let conn = PostgresConnection::connect("postgres://user:pass@host:port/database?arg1=val1&arg2=val2",
                                       &NoSsl);
```
`pass` may be omitted if not needed. `port` defaults to `5432` and `database`
defaults to the value of `user` if not specified. The driver supports `trust`,
`password`, and `md5` authentication.

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
`query` returns an iterator over the rows returned from the database. The
fields in a row can be accessed either by their indices or their column names,
though access by index is more efficient. Like statement parameters, result
columns are one-indexed.
```rust
let stmt = conn.prepare("SELECT bar, baz FROM foo");
for row in stmt.query([]) {
    let bar: i32 = row[1];
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
The `transaction` method will start a new transaction. It returns a
`PostgresTransaction` object which has the functionality of a
`PostgresConnection` as well as methods to control the result of the
transaction:
```rust
let trans = conn.transaction();
trans.update(...);
let stmt = trans.prepare(...);

if a_bad_thing_happened {
    trans.set_rollback();
}

if the_coast_is_clear {
    trans.set_commit();
}

drop(trans);
```
The transaction will be active until the `PostgresTransaction` object falls out
of scope. A transaction will commit by default. Nested transactions are
supported via savepoints.

Error Handling
--------------
The methods described above will fail if there is an error. For each of these
methods, there is a second variant prefixed with `try_` which returns a
`Result`:
```rust
match conn.try_update(query, params) {
    Ok(updates) => println!("{} rows were updated", updates),
    Err(err) => match err.code {
        NotNullViolation => println!("Something was NULL that shouldn't be"),
        SyntaxError => println!("Invalid query syntax"),
        _ => println!("A bad thing happened: {}", err.message),
    }
}
```

Connection Pooling
------------------
A very basic fixed-size connection pool is provided in the `pool` module. A
single pool can be shared across tasks and `get_connection` will block until a
connection is available.
```rust
let pool = PostgresConnectionPool::new("postgres://postgres@localhost",
                                       &NoSsl, 5);

for _ in range(0, 10) {
    let pool = pool.clone();
    do task::spawn {
        let conn = pool.get_connection();
        conn.query(...);
    }
}
```

Type Correspondence
-------------------
Rust-Postgres enforces a strict correspondence between Rust types and Postgres
types. The driver currently supports the following conversions:

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
            <td>SMALLINT, SMALLSERIAL</td>
        </tr>
        <tr>
            <td>i32</td>
            <td>INT, SERIAL</td>
        </tr>
        <tr>
            <td>i64</td>
            <td>BIGINT, BIGSERIAL</td>
        </tr>
        <tr>
            <td>f32</td>
            <td>REAL</td>
        </tr>
        <tr>
            <td>f64</td>
            <td>DOUBLE PRECISION</td>
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
            <td>TIMESTAMP, TIMESTAMP WITH TIME ZONE</td>
        </tr>
        <tr>
            <td>types::range::Range&lt;i32&gt;</td>
            <td>INT4RANGE</td>
        </tr>
        <tr>
            <td>types::range::Range&lt;i64&gt;</td>
            <td>INT8RANGE</td>
        </tr>
        <tr>
            <td>types::range::Range&lt;Timespec&gt;</td>
            <td>TSRANGE, TSTZRANGE</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;bool&gt;&gt;</td>
            <td>BOOL[], BOOL[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;~[u8]&gt;&gt;</td>
            <td>BYTEA[], BYTEA[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;i8&gt;&gt;</td>
            <td>"char"[], "char"[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;i16&gt;&gt;</td>
            <td>INT2[], INT2[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;i32&gt;&gt;</td>
            <td>INT4[], INT4[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;~str&gt;&gt;</td>
            <td>TEXT[], CHAR(n)[], VARCHAR[], TEXT[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;i64&gt;&gt;</td>
            <td>INT8[], INT8[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;Timespec&gt;&gt;</td>
            <td>TIMESTAMP[], TIMESTAMPTZ[], TIMESTAMP[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;f32&gt;&gt;</td>
            <td>FLOAT4[], FLOAT4[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;f64&gt;&gt;</td>
            <td>FLOAT8[], FLOAT8[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;Uuid&gt;&gt;</td>
            <td>UUID[], UUID[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;Range&lt;i32&gt;&gt;&gt;</td>
            <td>INT4RANGE[], INT4RANGE[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;Range&lt;Timespec&gt;&gt;&gt;</td>
            <td>TSRANGE[], TSTZRANGE[], TSRANGE[][], ...</td>
        </tr>
        <tr>
            <td>types::array::ArrayBase&lt;Option&lt;Range&lt;i64&gt;&gt;&gt;</td>
            <td>INT8RANGE[], INT8RANGE[][], ...</td>
        </tr>
        <tr>
            <td>std::hashmap::HashMap&lt;~str, Option&lt;~str&gt;&gt;</td>
            <td>HSTORE</td>
        </tr>
    </tbody>
</table>

More conversions can be defined by implementing the `ToSql` and `FromSql`
traits.

Development
===========
Like Rust itself, Rust-Postgres is still in the early stages of development, so
don't be surprised if APIs change and things break. If something's not working
properly, file an issue or submit a pull request!
