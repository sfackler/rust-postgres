//! An asynchronous, pipelined, PostgreSQL client.
//!
//! # Example
//!
//! ```no_run
//! use futures::{Future, Stream};
//! use tokio_postgres::NoTls;
//!
//! # #[cfg(not(feature = "runtime"))]
//! # let fut = futures::future::ok(());
//! # #[cfg(feature = "runtime")]
//! let fut =
//!     // Connect to the database
//!     tokio_postgres::connect("host=localhost user=postgres", NoTls)
//!
//!     .map(|(client, connection)| {
//!         // The connection object performs the actual communication with the database,
//!         // so spawn it off to run on its own.
//!         let connection = connection.map_err(|e| eprintln!("connection error: {}", e));
//!         tokio::spawn(connection);
//!
//!         // The client is what you use to make requests.
//!         client
//!     })
//!
//!     .and_then(|mut client| {
//!         // Now we can prepare a simple statement that just returns its parameter.
//!         client.prepare("SELECT $1::TEXT")
//!             .map(|statement| (client, statement))
//!     })
//!
//!     .and_then(|(mut client, statement)| {
//!         // And then execute it, returning a Stream of Rows which we collect into a Vec
//!         client.query(&statement, &[&"hello world"]).collect()
//!     })
//!
//!     // Now we can check that we got back the same string we sent over.
//!     .map(|rows| {
//!         let value: &str = rows[0].get(0);
//!         assert_eq!(value, "hello world");
//!     })
//!
//!     // And report any errors that happened.
//!     .map_err(|e| {
//!         eprintln!("error: {}", e);
//!     });
//!
//! // By default, tokio_postgres uses the tokio crate as its runtime.
//! tokio::run(fut);
//! ```
//!
//! # Behavior
//!
//! Calling a method like `Client::query` on its own does nothing. The associated request is not sent to the database
//! until the future returned by the method is first polled. Requests are executed in the order that they are first
//! polled, not in the order that their futures are created.
//!
//! # Pipelining
//!
//! The client supports *pipelined* requests. Pipelining can improve performance in use cases in which multiple,
//! independent queries need to be executed. In a traditional workflow, each query is sent to the server after the
//! previous query completes. In contrast, pipelining allows the client to send all of the queries to the server up
//! front, minimizing time spent by one side waiting for the other to finish sending data:
//!
//! ```not_rust
//!             Sequential                              Pipelined
//! | Client         | Server          |    | Client         | Server          |
//! |----------------|-----------------|    |----------------|-----------------|
//! | send query 1   |                 |    | send query 1   |                 |
//! |                | process query 1 |    | send query 2   | process query 1 |
//! | receive rows 1 |                 |    | send query 3   | process query 2 |
//! | send query 2   |                 |    | receive rows 1 | process query 3 |
//! |                | process query 2 |    | receive rows 2 |                 |
//! | receive rows 2 |                 |    | receive rows 3 |                 |
//! | send query 3   |                 |
//! |                | process query 3 |
//! | receive rows 3 |                 |
//! ```
//!
//! In both cases, the PostgreSQL server is executing the queries sequentially - pipelining just allows both sides of
//! the connection to work concurrently when possible.
//!
//! Pipelining happens automatically when futures are polled concurrently (for example, by using the futures `join`
//! combinator):
//!
//! ```rust
//! use futures::Future;
//! use tokio_postgres::{Client, Error, Statement};
//!
//! fn pipelined_prepare(
//!     client: &mut Client,
//! ) -> impl Future<Item = (Statement, Statement), Error = Error>
//! {
//!     client.prepare("SELECT * FROM foo")
//!         .join(client.prepare("INSERT INTO bar (id, name) VALUES ($1, $2)"))
//! }
//! ```
//!
//! # Runtime
//!
//! The client works with arbitrary `AsyncRead + AsyncWrite` streams. Convenience APIs are provided to handle the
//! connection process, but these are gated by the `runtime` Cargo feature, which is enabled by default. If disabled,
//! all dependence on the tokio runtime is removed.
//!
//! # SSL/TLS support
//!
//! TLS support is implemented via external libraries. `Client::connect` and `Config::connect` take a TLS implementation
//! as an argument. The `NoTls` type in this crate can be used when TLS is not required. Otherwise, the
//! `postgres-openssl` and `postgres-native-tls` crates provide implementations backed by the `openssl` and `native-tls`
//! crates, respectively.
#![doc(html_root_url = "https://docs.rs/tokio-postgres/0.4.0-rc.3")]
#![warn(rust_2018_idioms, clippy::all, missing_docs)]
#![feature(async_await)]

pub use crate::client::Client;
pub use crate::config::Config;
pub use crate::connection::Connection;
use crate::error::DbError;
pub use crate::error::Error;
pub use crate::row::{Row, SimpleQueryRow};
#[cfg(feature = "runtime")]
pub use crate::socket::Socket;
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
pub use crate::tls::NoTls;
pub use statement::{Column, Statement};

mod client;
mod codec;
pub mod config;
#[cfg(feature = "runtime")]
mod connect;
mod connect_raw;
#[cfg(feature = "runtime")]
mod connect_socket;
mod connect_tls;
mod connection;
pub mod error;
mod maybe_tls_stream;
mod prepare;
mod query;
pub mod row;
#[cfg(feature = "runtime")]
mod socket;
mod statement;
pub mod tls;
pub mod types;

/// A convenience function which parses a connection string and connects to the database.
///
/// See the documentation for [`Config`] for details on the connection string format.
///
/// Requires the `runtime` Cargo feature (enabled by default).
///
/// [`Config`]: ./Config.t.html
#[cfg(feature = "runtime")]
pub async fn connect<T>(
    config: &str,
    tls: T,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: MakeTlsConnect<Socket>,
{
    let config = config.parse::<Config>()?;
    config.connect(tls).await
}

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    process_id: i32,
    channel: String,
    payload: String,
}

/// An asynchronous message from the server.
#[allow(clippy::large_enum_variant)]
pub enum AsyncMessage {
    /// A notice.
    ///
    /// Notices use the same format as errors, but aren't "errors" per-se.
    Notice(DbError),
    /// A notification.
    ///
    /// Connections can subscribe to notifications with the `LISTEN` command.
    Notification(Notification),
    #[doc(hidden)]
    __NonExhaustive,
}

impl Notification {
    /// The process ID of the notifying backend process.
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// The name of the channel that the notify has been raised on.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The "payload" string passed from the notifying process.
    pub fn payload(&self) -> &str {
        &self.payload
    }
}
