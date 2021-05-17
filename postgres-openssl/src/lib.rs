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
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

#[cfg(feature = "runtime")]
use openssl::error::ErrorStack;
#[cfg(feature = "runtime")]
use openssl::ssl::SslConnector;
use openssl::ssl::{self, ConnectConfiguration, SslFiletype, SslRef};
use openssl::x509::X509VerifyResult;
use openssl::{hash::MessageDigest, ssl::SslMethod};
use openssl::{nid::Nid, ssl::SslVerifyMode};
use std::fmt::{self, Debug};
use std::future::Future;
use std::io;
use std::pin::Pin;
#[cfg(feature = "runtime")]
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{error::Error, path::PathBuf};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_openssl::SslStream;
#[cfg(feature = "runtime")]
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};
use tokio_postgres::{config::SslMode, tls};

#[cfg(test)]
mod test;

/// TLS configuration.
#[cfg(feature = "runtime")]
pub struct TlsConfig {
    /// SSL mode (`sslmode`).
    pub mode: SslMode,
    /// Location of the client cert and key (`sslcert`, `sslkey`).
    pub client_cert: Option<(PathBuf, PathBuf)>,
    /// Location of the root certificate (`sslrootcert`).
    pub root_cert: Option<PathBuf>,
}

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

    /// Creates a new connector from the provided [`TlsConfig`].
    ///
    /// The returned [`MakeTlsConnector`] will be configured to mimick libpq-ssl behavior.
    pub fn from_tls_config(tls_config: TlsConfig) -> Result<MakeTlsConnector, ErrorStack> {
        let mut builder = SslConnector::builder(SslMethod::tls_client())?;
        // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
        // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
        //
        // For more details, check out Table 33.1. SSL Mode Descriptions in
        // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
        let (verify_mode, verify_hostname) = match tls_config.mode {
            SslMode::Disable | SslMode::Prefer => (SslVerifyMode::NONE, false),
            SslMode::Require => match tls_config.root_cert {
                // If a root CA file exists, the behavior of sslmode=require will be the same as
                // that of verify-ca, meaning the server certificate is validated against the CA.
                //
                // For more details, check out the note about backwards compatibility in
                // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
                Some(_) => (SslVerifyMode::PEER, false),
                None => (SslVerifyMode::NONE, false),
            },
            SslMode::VerifyCa => (SslVerifyMode::PEER, false),
            SslMode::VerifyFull => (SslVerifyMode::PEER, true),
            _ => panic!("unexpected sslmode {:?}", tls_config.mode),
        };

        // Configure peer verification
        builder.set_verify(verify_mode);

        // Configure certificates
        if tls_config.client_cert.is_some() {
            let (cert, key) = tls_config.client_cert.unwrap();
            builder.set_certificate_file(cert, SslFiletype::PEM)?;
            builder.set_private_key_file(key, SslFiletype::PEM)?;
        }
        if tls_config.root_cert.is_some() {
            builder.set_ca_file(tls_config.root_cert.unwrap())?;
        }

        let mut tls_connector = MakeTlsConnector::new(builder.build());

        // Configure hostname verification
        match (verify_mode, verify_hostname) {
            (SslVerifyMode::PEER, false) => tls_connector.set_callback(|connect, _| {
                connect.set_verify_hostname(false);
                Ok(())
            }),
            _ => {}
        }

        Ok(tls_connector)
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
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = TlsStream<S>;
    type Error = Box<dyn Error + Send + Sync>;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<TlsStream<S>, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let future = async move {
            let ssl = self.ssl.into_ssl(&self.domain)?;
            let mut stream = SslStream::new(ssl, stream)?;
            match Pin::new(&mut stream).connect().await {
                Ok(()) => Ok(TlsStream(stream)),
                Err(error) => Err(Box::new(ConnectError {
                    error,
                    verify_result: stream.ssl().verify_result(),
                }) as _),
            }
        };

        Box::pin(future)
    }
}

#[derive(Debug)]
struct ConnectError {
    error: ssl::Error,
    verify_result: X509VerifyResult,
}

impl fmt::Display for ConnectError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, fmt)?;

        if self.verify_result != X509VerifyResult::OK {
            fmt.write_str(": ")?;
            fmt::Display::fmt(&self.verify_result, fmt)?;
        }

        Ok(())
    }
}

impl Error for ConnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// The stream returned by `TlsConnector`.
pub struct TlsStream<S>(SslStream<S>);

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
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
