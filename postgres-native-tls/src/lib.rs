//! TLS support for `tokio-postgres` and `postgres` via `native-tls.
//!
//! # Examples
//!
//! ```no_run
//! use native_tls::{Certificate, TlsConnector};
//! use postgres_native_tls::MakeTlsConnector;
//! use std::fs;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
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
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let cert = fs::read("database_cert.pem")?;
//! let cert = Certificate::from_pem(&cert)?;
//! let connector = TlsConnector::builder()
//!     .add_root_certificate(cert)
//!     .build()?;
//! let connector = MakeTlsConnector::new(connector);
//!
//! let client = postgres::Client::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! )?;
//! # Ok(())
//! # }
//! ```
#![doc(html_root_url = "https://docs.rs/postgres-native-tls/0.3")]
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

use std::task::{Context, Poll};
use std::future::Future;
use std::io;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use bytes::{Buf, BufMut};
use tokio_postgres::tls;
#[cfg(feature = "runtime")]
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};
use std::mem::MaybeUninit;

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
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<TlsStream<S>, native_tls::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let future = async move {
            let stream = self.connector.connect(&self.domain, stream).await?;

            Ok(TlsStream(stream))
        };

        Box::pin(future)
    }
}

/// The stream returned by `TlsConnector`.
pub struct TlsStream<S>(tokio_tls::TlsStream<S>);

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        self.0.prepare_uninitialized_buffer(buf)
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }

    fn poll_read_buf<B: BufMut>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut B,
    ) -> Poll<io::Result<usize>>
    where
        Self: Sized,
    {
        Pin::new(&mut self.0).poll_read_buf(cx, buf)
    }
}

impl<S> AsyncWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }

    fn poll_write_buf<B: Buf>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut B,
    ) -> Poll<io::Result<usize>>
    where
        Self: Sized,
    {
        Pin::new(&mut self.0).poll_write_buf(cx, buf)
    }
}

impl<S> tls::TlsStream for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        // FIXME https://github.com/tokio-rs/tokio/issues/1383
        ChannelBinding::none()
    }
}
