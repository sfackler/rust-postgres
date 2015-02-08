# Rust-Postgres
A native PostgreSQL driver for Rust.

Documentation is available at https://sfackler.github.io/rust-postgres/doc/postgres

[![Build Status](https://travis-ci.org/sfackler/rust-postgres.png?branch=master)](https://travis-ci.org/sfackler/rust-postgres)

You can integrate Rust-Postgres into your project through the [releases on crates.io](https://crates.io/crates/postgres):
```toml
# Cargo.toml
[dependencies]
postgres = "0.6"
```

## Overview
Rust-Postgres is a pure-Rust frontend for the popular PostgreSQL database. It
exposes a high level interface in the vein of JDBC or Go's `database/sql`
package.
```rust
extern crate postgres;

use postgres::{Connection, SslMode};

struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>
}

fn main() {
    let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None)
            .unwrap();

    conn.execute("CREATE TABLE person (
                    id              SERIAL PRIMARY KEY,
                    name            VARCHAR NOT NULL,
                    data            BYTEA
                  )", &[]).unwrap();
    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None
    };
    conn.execute("INSERT INTO person (name, data) VALUES ($1, $2)",
                 &[&me.name, &me.data]).unwrap();

    let stmt = conn.prepare("SELECT id, name, data FROM person").unwrap();
    for row in stmt.query(&[]).unwrap() {
        let person = Person {
            id: row.get(0),
            name: row.get(1),
            data: row.get(2)
        };
        println!("Found person {}", person.name);
    }
}
```

## Requirements
* **Rust** - Rust-Postgres is developed against the nightly releases on
    http://www.rust-lang.org.

* **PostgreSQL 7.4 or later** - Rust-Postgres speaks version 3 of the
    PostgreSQL protocol, which corresponds to versions 7.4 and later. If your
    version of Postgres was compiled in the last decade, you should be okay.

## Usage

### Connecting
Connect to a Postgres server using the standard URI format:
```rust
let conn = try!(Connection::connect("postgres://user:pass@host:port/database?arg1=val1&arg2=val2",
                                    &SslMode::None));
```
`pass` may be omitted if not needed. `port` defaults to `5432` and `database`
defaults to the value of `user` if not specified. The driver supports `trust`,
`password`, and `md5` authentication.

Unix domain sockets can be used as well. The `host` portion of the URI should be
set to the absolute path to the directory containing the socket file. Since `/`
is a reserved character in URLs, the path should be URL encoded.
```rust
let conn = try!(Connection::connect("postgres://postgres@%2Frun%2Fpostgres", &SslMode::None));
```
Paths which contain non-UTF8 characters can be handled in a different manner;
see the documentation for details.

### Statement Preparation
Prepared statements can have parameters, represented as `$n` where `n` is an
index into the parameter array starting from 1:
```rust
let stmt = try!(conn.prepare("SELECT * FROM foo WHERE bar = $1 AND baz = $2"));
```

### Querying
A prepared statement can be executed with the `query` and `execute` methods.
Both methods take an array of parameters to bind to the query represented as
`&ToSql` trait objects. `execute` returns the number of rows affected by the
query (or 0 if not applicable):
```rust
let stmt = try!(conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2"));
let updates = try!(stmt.execute(&[&1i32, &"biz"]));
println!("{} rows were updated", updates);
```
`query` returns an iterator over the rows returned from the database. The
fields in a row can be accessed either by their indices or their column names,
though access by index is more efficient. Unlike statement parameters, result
columns are zero-indexed.
```rust
let stmt = try!(conn.prepare("SELECT bar, baz FROM foo"));
for row in try!(stmt.query(&[])) {
    let bar: i32 = row.get(0);
    let baz: String = row.get("baz");
    println!("bar: {}, baz: {}", bar, baz);
}
```
In addition, `Connection` has a utility `execute` method which is useful if a
statement is only going to be executed once:
```rust
let updates = try!(conn.execute("UPDATE foo SET bar = $1 WHERE baz = $2",
                                &[&1i32, &"biz"]));
println!("{} rows were updated", updates);
```

### Transactions
The `transaction` method will start a new transaction. It returns a
`Transaction` object which has the functionality of a
`Connection` as well as methods to control the result of the
transaction:
```rust
let trans = try!(conn.transaction());
try!(trans.execute(...));
let stmt = try!(trans.prepare(...));

if the_coast_is_clear {
    trans.set_commit();
}

try!(trans.finish());
```
The transaction will be active until the `Transaction` object falls out of
scope. A transaction will roll back by default. Nested transactions are
supported via savepoints.

### Type Correspondence
Rust-Postgres enforces a strict correspondence between Rust types and Postgres
types. The driver currently supports the following conversions:

<table>
    <thead>
        <tr>
            <th>Rust Type</th>
            <th>Postgres Type</th>
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
            <td>u32</td>
            <td>OID</td>
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
            <td>str/String</td>
            <td>VARCHAR, CHAR(n), TEXT, CITEXT</td>
        </tr>
        <tr>
            <td>[u8]/Vec&lt;u8&gt;</td>
            <td>BYTEA</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/rust-lang/rustc-serialize">serialize::json::Json</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>JSON/JSONB</td>
        </tr>
        <tr>
            <td>IpAddr</td>
            <td>INET/CIDR</td>
        </tr>
        <tr>
            <td>time::Timespec</td>
            <td>TIMESTAMP, TIMESTAMP WITH TIME ZONE</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/rust-lang/uuid">uuid::Uuid</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>UUID</td>
        </tr>
        <tr>
            <td>std::collections::HashMap&lt;String, Option&lt;String&gt;&gt;</td>
            <td>HSTORE</td>
        </tr>
    </tbody>
</table>

More conversions can be defined by implementing the `ToSql` and `FromSql`
traits.

Support for Postgres arrays is located in the
[postgres-array](https://github.com/sfackler/rust-postgres-array) crate.

Support for Postgres ranges is located in the
[postgres-range](https://github.com/sfackler/rust-postgres-range) crate.

## Optional features

### UUID type

[UUID](http://www.postgresql.org/docs/9.4/static/datatype-uuid.html) support is
provided optionally by the `uuid` feature.

### JSON/JSONB types

[JSON and JSONB](http://www.postgresql.org/docs/9.4/static/datatype-json.html)
support is provided optionally by the `rustc-serialize` feature.

## Development
Like Rust itself, Rust-Postgres is still in the early stages of development, so
don't be surprised if APIs change and things break. If something's not working
properly, file an issue or submit a pull request!
