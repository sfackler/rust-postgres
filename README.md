# Rust-Postgres
A native PostgreSQL driver for Rust.

[Documentation](https://sfackler.github.io/rust-postgres/doc/v0.13.1/postgres)

[![Build Status](https://travis-ci.org/sfackler/rust-postgres.png?branch=master)](https://travis-ci.org/sfackler/rust-postgres) [![Latest Version](https://img.shields.io/crates/v/postgres.svg)](https://crates.io/crates/postgres)

You can integrate Rust-Postgres into your project through the [releases on crates.io](https://crates.io/crates/postgres):
```toml
[dependencies]
postgres = "0.13"
```

## Overview
Rust-Postgres is a pure-Rust frontend for the popular PostgreSQL database.
```rust
extern crate postgres;

use postgres::{Connection, TlsMode};

struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

fn main() {
    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
    conn.execute("CREATE TABLE person (
                    id              SERIAL PRIMARY KEY,
                    name            VARCHAR NOT NULL,
                    data            BYTEA
                  )", &[]).unwrap();
    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute("INSERT INTO person (name, data) VALUES ($1, $2)",
                 &[&me.name, &me.data]).unwrap();
    for row in &conn.query("SELECT id, name, data FROM person", &[]).unwrap() {
        let person = Person {
            id: row.get(0),
            name: row.get(1),
            data: row.get(2),
        };
        println!("Found person {}", person.name);
    }
}
```

## Requirements
* **Rust** - Rust-Postgres is developed against the 1.10 release of Rust
    available on http://www.rust-lang.org. It should also compile against more
    recent releases.

* **PostgreSQL 7.4 or later** - Rust-Postgres speaks version 3 of the
    PostgreSQL protocol, which corresponds to versions 7.4 and later. If your
    version of Postgres was compiled in the last decade, you should be okay.

## Usage

### Connecting
Connect to a Postgres server using the standard URI format:
```rust
let conn = try!(Connection::connect("postgres://user:pass@host:port/database?arg1=val1&arg2=val2",
                                    TlsMode::None));
```
`pass` may be omitted if not needed. `port` defaults to `5432` and `database`
defaults to the value of `user` if not specified. The driver supports `trust`,
`password`, and `md5` authentication.

Unix domain sockets can be used as well. The `host` portion of the URI should
be set to the absolute path to the directory containing the socket file. Since
`/` is a reserved character in URLs, the path should be URL encoded. If Postgres
stored its socket files in `/run/postgres`, the connection would then look like:
```rust
let conn = try!(Connection::connect("postgres://postgres@%2Frun%2Fpostgres", TlsMode::None));
```
Paths which contain non-UTF8 characters can be handled in a different manner;
see the documentation for details.

### Querying
SQL statements can be executed with the `query` and `execute` methods. Both
methods take a query string as well as a slice of parameters to bind to the
query. The `i`th query parameter is specified in the query string by `$i`. Note
that query parameters are 1-indexed rather than the more common 0-indexing.

`execute` returns the number of rows affected by the query (or 0 if not
applicable):
```rust
let updates = try!(conn.execute("UPDATE foo SET bar = $1 WHERE baz = $2", &[&1i32, &"biz"]));
println!("{} rows were updated", updates);
```

`query` returns an iterable object holding the rows returned from the database.
The fields in a row can be accessed either by their indices or their column
names, though access by index is more efficient. Unlike statement parameters,
result columns are zero-indexed.
```rust
for row in &try!(conn.query("SELECT bar, baz FROM foo WHERE buz = $1", &[&1i32])) {
    let bar: i32 = row.get(0);
    let baz: String = row.get("baz");
    println!("bar: {}, baz: {}", bar, baz);
}
```

### Statement Preparation
If the same statement will be executed repeatedly (possibly with different
parameters), explicitly preparing it can improve performance:

```rust
let stmt = try!(conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2"));
for (bar, baz) in updates {
    try!(stmt.execute(&[bar, baz]));
}
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
// ...

try!(trans.commit());
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
            <td>VARCHAR, CHAR(n), TEXT, CITEXT, NAME</td>
        </tr>
        <tr>
            <td>[u8]/Vec&lt;u8&gt;</td>
            <td>BYTEA</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/rust-lang-nursery/rustc-serialize">serialize::json::Json</a>
                and
                <a href="https://github.com/serde-rs/json">serde_json::Value</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>JSON, JSONB</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/rust-lang-deprecated/time">time::Timespec</a>
                and
                <a href="https://github.com/lifthrasiir/rust-chrono">chrono::NaiveDateTime</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>TIMESTAMP</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/rust-lang-deprecated/time">time::Timespec</a>,
                <a href="https://github.com/lifthrasiir/rust-chrono">chrono::DateTime&lt;UTC&gt;</a>,
                <a href="https://github.com/lifthrasiir/rust-chrono">chrono::DateTime&lt;Local&gt;</a>,
                and
                <a href="https://github.com/lifthrasiir/rust-chrono">chrono::DateTime&lt;FixedOffset&gt;</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>TIMESTAMP WITH TIME ZONE</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/lifthrasiir/rust-chrono">chrono::NaiveDate</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>DATE</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/lifthrasiir/rust-chrono">chrono::NaiveTime</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>TIME</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/rust-lang-nursery/uuid">uuid::Uuid</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>UUID</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/contain-rs/bit-vec">bit_vec::BitVec</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>BIT, VARBIT</td>
        </tr>
        <tr>
            <td>HashMap&lt;String, Option&lt;String&gt;&gt;</td>
            <td>HSTORE</td>
        </tr>
        <tr>
            <td>
                <a href="https://github.com/abaumhauer/eui48">eui48::MacAddress</a>
                (<a href="#optional-features">optional</a>)
            </td>
            <td>MACADDR</td>
        </tr>
    </tbody>
</table>

`Option<T>` implements `FromSql` where `T: FromSql` and `ToSql` where `T:
ToSql`, and represents nullable Postgres values.

`&[T]` and `Vec<T>` implement `ToSql` where `T: ToSql`, and  `Vec<T>`
additionally implements `FromSql` where `T: FromSql`, which represent
one-dimensional Postgres arrays.

More conversions can be defined by implementing the `ToSql` and `FromSql`
traits.

The [postgres-derive](https://github.com/sfackler/rust-postgres-derive)
crate will synthesize `ToSql` and `FromSql` implementations for enum, domain,
and composite Postgres types.

Full support for array types is located in the
[postgres-array](https://github.com/sfackler/rust-postgres-array) crate.

Support for range types is located in the
[postgres-range](https://github.com/sfackler/rust-postgres-range) crate.

Support for the large object API is located in the
[postgres-large-object](https://github.com/sfackler/rust-postgres-large-object)
crate.

## Optional features

### UUID type

[UUID](http://www.postgresql.org/docs/9.4/static/datatype-uuid.html) support is
provided optionally by the `with-uuid` feature, which adds `ToSql` and `FromSql`
implementations for `uuid`'s `Uuid` type.

### JSON/JSONB types

[JSON and JSONB](http://www.postgresql.org/docs/9.4/static/datatype-json.html)
support is provided optionally by the `with-rustc-serialize` feature, which adds
`ToSql` and `FromSql` implementations for `rustc-serialize`'s `Json` type, and
the `with-serde_json` feature, which adds implementations for `serde_json`'s
`Value` type.

### TIMESTAMP/TIMESTAMPTZ/DATE/TIME types

[Date and Time](http://www.postgresql.org/docs/9.1/static/datatype-datetime.html)
support is provided optionally by the `with-time` feature, which adds `ToSql`
and `FromSql` implementations for `time`'s `Timespec` type, or the `with-chrono`
feature, which adds `ToSql` and `FromSql` implementations for `chrono`'s
`DateTime`, `NaiveDateTime`, `NaiveDate` and `NaiveTime` types.

### BIT/VARBIT types

[BIT and VARBIT](http://www.postgresql.org/docs/9.4/static/datatype-bit.html)
support is provided optionally by the `with-bit-vec` feature, which adds `ToSql`
and `FromSql` implementations for `bit-vec`'s `BitVec` type.

### MACADDR type

[MACADDR](http://www.postgresql.org/docs/9.4/static/datatype-net-types.html#DATATYPE-MACADDR)
support is provided optionally by the `with-eui48` feature, which adds `ToSql`
and `FromSql` implementations for `eui48`'s `MacAddress` type.
