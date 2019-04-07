//! A synchronous client for the PostgreSQL database.
//!
//! # Example
//!
//! ```no_run
//! use postgres::{Client, NoTls};
//!
//! # fn main() -> Result<(), postgres::Error> {
//! let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
//!
//! client.simple_query("
//!     CREATE TABLE person (
//!         id      SERIAL PRIMARY KEY,
//!         name    TEXT NOT NULL,
//!         data    BYTEA
//!     )
//! ")?;
//!
//! let name = "Ferris";
//! let data = None::<&[u8]>;
//! client.execute(
//!     "INSERT INTO person (name, data) VALUES ($1, $2)",
//!     &[&name, &data],
//! )?;
//!
//! for row in client.query("SELECT id, name, data FROM person", &[])? {
//!     let id: i32 = row.get(0);
//!     let name: &str = row.get(1);
//!     let data: Option<&[u8]> = row.get(2);
//!
//!     println!("found person: {} {} {:?}", id, name, data);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Implementation
//!
//! This crate is a lightweight wrapper over tokio-postgres. The `tokio_postgres::Connection` is spawned onto an
//! executor, and the `tokio_postgres::Client` is wrapped in the `postgres::Client`, which simply waits on the futures
//! the nonblocking client creates.
//!
//! # Runtime
//!
//! A client can be constructed directly from a `tokio-postgres` client via a `From` implementation, but the `runtime`
//! Cargo feature (enabled by default) provides a more convenient interface. By default, connections will be spawned
//! onto a static tokio `Runtime`, but a custom `Executor` can also be used instead.
//!
//! # SSL/TLS support
//!
//! TLS support is implemented via external libraries. `Client::connect` and `Config::connect` take a TLS implementation
//! as an argument. The `NoTls` type in this crate can be used when TLS is not required. Otherwise, the
//! `tokio-postgres-openssl` and `tokio-postgres-native-tls` crates provide implementations backed by the `openssl` and
//! `native-tls` crates, respectively.
#![doc(html_root_url = "https://docs.rs/postgres/0.16.0-rc.1")]
#![warn(clippy::all, rust_2018_idioms, missing_docs)]

#[cfg(feature = "runtime")]
use lazy_static::lazy_static;
#[cfg(feature = "runtime")]
use tokio::runtime::{self, Runtime};

#[cfg(feature = "runtime")]
pub use tokio_postgres::Socket;
pub use tokio_postgres::{error, row, tls, types, Column, Portal, SimpleQueryMessage, Statement};

pub use crate::client::*;
#[cfg(feature = "runtime")]
pub use crate::config::Config;
pub use crate::copy_out_reader::*;
#[doc(no_inline)]
pub use crate::error::Error;
pub use crate::query_iter::*;
pub use crate::query_portal_iter::*;
#[doc(no_inline)]
pub use crate::row::{Row, SimpleQueryRow};
pub use crate::simple_query_iter::*;
#[doc(no_inline)]
pub use crate::tls::NoTls;
pub use crate::to_statement::*;
pub use crate::transaction::*;

mod client;
#[cfg(feature = "runtime")]
pub mod config;
mod copy_out_reader;
mod query_iter;
mod query_portal_iter;
mod simple_query_iter;
mod to_statement;
mod transaction;

#[cfg(feature = "runtime")]
#[cfg(test)]
mod test;

#[cfg(feature = "runtime")]
lazy_static! {
    static ref RUNTIME: Runtime = runtime::Builder::new()
        .name_prefix("postgres-")
        .build()
        .unwrap();
}
