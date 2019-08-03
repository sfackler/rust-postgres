//! TLS support for `tokio-postgres` and `postgres` via `native-tls.
//!
//! # Examples
//!
//! ```no_run
//! use native_tls::{Certificate, TlsConnector};
//! use postgres_native_tls::MakeTlsConnector;
//! use std::fs;
//!
//! # fn main() -> Result<(), Box<std::error::Error>> {
//! let cert = fs::read("database_cert.pem")?;
//! let cert = Certificate::from_pem(&cert)?;
//! let connector = TlsConnector::builder()
//!     .add_root_certificate(cert)
//!     .build()?;
//! let connector = MakeTlsConnector::new(connector);
//!
//! let connect_future = tokio_postgres::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! );
//!
//! // ...
//! # Ok(())
//! # }
//! ```
//!
//! ```no_run
//! use native_tls::{Certificate, TlsConnector};
//! use postgres_native_tls::MakeTlsConnector;
//! use std::fs;
//!
//! # fn main() -> Result<(), Box<std::error::Error>> {
//! let cert = fs::read("database_cert.pem")?;
//! let cert = Certificate::from_pem(&cert)?;
//! let connector = TlsConnector::builder()
//!     .add_root_certificate(cert)
//!     .build()?;
//! let connector = MakeTlsConnector::new(connector);
//!
//! let mut client = postgres::Client::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! )?;
//! # Ok(())
//! # }
//! ```
#![doc(html_root_url = "https://docs.rs/postgres-native-tls/0.2.0-rc.1")]
#![warn(rust_2018_idioms, clippy::all, missing_docs)]
#![feature(async_await)]

use tokio_io::{AsyncRead, AsyncWrite};
#[cfg(feature = "runtime")]
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};
use tokio_tls::TlsStream;
use std::pin::Pin;
use std::future::Future;

#[cfg(test)]
mod test;

/// A `MakeTlsConnect` implementation using the `native-tls` crate.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct MakeTlsConnector(native_tls::TlsConnector);

#[cfg(feature = "runtime")]
impl MakeTlsConnector {
    /// Creates a new connector.
    pub fn new(connector: native_tls::TlsConnector) -> MakeTlsConnector {
        MakeTlsConnector(connector)
    }
}

#[cfg(feature = "runtime")]
impl<S> MakeTlsConnect<S> for MakeTlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + 'static + Send,
{
    type Stream = TlsStream<S>;
    type TlsConnect = TlsConnector;
    type Error = native_tls::Error;

    fn make_tls_connect(&mut self, domain: &str) -> Result<TlsConnector, native_tls::Error> {
        Ok(TlsConnector::new(self.0.clone(), domain))
    }
}

/// A `TlsConnect` implementation using the `native-tls` crate.
pub struct TlsConnector {
    connector: tokio_tls::TlsConnector,
    domain: String,
}

impl TlsConnector {
    /// Creates a new connector configured to connect to the specified domain.
    pub fn new(connector: native_tls::TlsConnector, domain: &str) -> TlsConnector {
        TlsConnector {
            connector: tokio_tls::TlsConnector::from(connector),
            domain: domain.to_string(),
        }
    }
}

impl<S> TlsConnect<S> for TlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + 'static + Send,
{
    type Stream = TlsStream<S>;
    type Error = native_tls::Error;
    type Future = Pin<Box<dyn Future<Output = Result<(TlsStream<S>, ChannelBinding), native_tls::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let future = async move {
            let stream = self.connector.connect(&self.domain, stream).await?;

            // FIXME https://github.com/tokio-rs/tokio/issues/1383
            let channel_binding = ChannelBinding::none();

            Ok((stream, channel_binding))
        };

        Box::pin(future)
    }
}
