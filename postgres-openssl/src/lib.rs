//! TLS support for `tokio-postgres` and `postgres` via `openssl`.
//!
//! # Examples
//!
//! ```no_run
//! use openssl::ssl::{SslConnector, SslMethod};
//! use postgres_openssl::MakeTlsConnector;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = SslConnector::builder(SslMethod::tls())?;
//! builder.set_ca_file("database_cert.pem")?;
//! let connector = MakeTlsConnector::new(builder.build());
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
//! use openssl::ssl::{SslConnector, SslMethod};
//! use postgres_openssl::MakeTlsConnector;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = SslConnector::builder(SslMethod::tls())?;
//! builder.set_ca_file("database_cert.pem")?;
//! let connector = MakeTlsConnector::new(builder.build());
//!
//! let client = postgres::Client::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! )?;
//!
//! // ...
//! # Ok(())
//! # }
//! ```
#![doc(html_root_url = "https://docs.rs/postgres-openssl/0.3")]
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

use std::task::{Poll, Context};
#[cfg(feature = "runtime")]
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
#[cfg(feature = "runtime")]
use openssl::ssl::SslConnector;
use openssl::ssl::{ConnectConfiguration, SslRef};
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::pin::Pin;
#[cfg(feature = "runtime")]
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use bytes::{Buf, BufMut};
use tokio_openssl::{HandshakeError, SslStream};
use tokio_postgres::tls;
#[cfg(feature = "runtime")]
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};
use std::mem::MaybeUninit;

#[cfg(test)]
mod test;

/// A `MakeTlsConnect` implementation using the `openssl` crate.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct MakeTlsConnector {
    connector: SslConnector,
    config: Arc<dyn Fn(&mut ConnectConfiguration, &str) -> Result<(), ErrorStack> + Sync + Send>,
}

#[cfg(feature = "runtime")]
impl MakeTlsConnector {
    /// Creates a new connector.
    pub fn new(connector: SslConnector) -> MakeTlsConnector {
        MakeTlsConnector {
            connector,
            config: Arc::new(|_, _| Ok(())),
        }
    }

    /// Sets a callback used to apply per-connection configuration.
    ///
    /// The the callback is provided the domain name along with the `ConnectConfiguration`.
    pub fn set_callback<F>(&mut self, f: F)
    where
        F: Fn(&mut ConnectConfiguration, &str) -> Result<(), ErrorStack> + 'static + Sync + Send,
    {
        self.config = Arc::new(f);
    }
}

#[cfg(feature = "runtime")]
impl<S> MakeTlsConnect<S> for MakeTlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Debug + 'static + Sync + Send,
{
    type Stream = TlsStream<S>;
    type TlsConnect = TlsConnector;
    type Error = ErrorStack;

    fn make_tls_connect(&mut self, domain: &str) -> Result<TlsConnector, ErrorStack> {
        let mut ssl = self.connector.configure()?;
        (self.config)(&mut ssl, domain)?;
        Ok(TlsConnector::new(ssl, domain))
    }
}

/// A `TlsConnect` implementation using the `openssl` crate.
pub struct TlsConnector {
    ssl: ConnectConfiguration,
    domain: String,
}

impl TlsConnector {
    /// Creates a new connector configured to connect to the specified domain.
    pub fn new(ssl: ConnectConfiguration, domain: &str) -> TlsConnector {
        TlsConnector {
            ssl,
            domain: domain.to_string(),
        }
    }
}

impl<S> TlsConnect<S> for TlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Debug + 'static + Sync + Send,
{
    type Stream = TlsStream<S>;
    type Error = HandshakeError<S>;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<TlsStream<S>, HandshakeError<S>>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let future = async move {
            let stream = tokio_openssl::connect(self.ssl, &self.domain, stream).await?;
            Ok(TlsStream(stream))
        };

        Box::pin(future)
    }
}

/// The stream returned by `TlsConnector`.
pub struct TlsStream<S>(SslStream<S>);

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
        match tls_server_end_point(self.0.ssl()) {
            Some(buf) => ChannelBinding::tls_server_end_point(buf),
            None => ChannelBinding::none(),
        }
    }
}

fn tls_server_end_point(ssl: &SslRef) -> Option<Vec<u8>> {
    let cert = ssl.peer_certificate()?;
    let algo_nid = cert.signature_algorithm().object().nid();
    let signature_algorithms = algo_nid.signature_algorithms()?;
    let md = match signature_algorithms.digest {
        Nid::MD5 | Nid::SHA1 => MessageDigest::sha256(),
        nid => MessageDigest::from_nid(nid)?,
    };
    cert.digest(md).ok().map(|b| b.to_vec())
}
