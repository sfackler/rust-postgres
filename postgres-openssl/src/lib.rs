//! TLS support for `tokio-postgres` and `postgres` via `openssl`.
//!
//! # Examples
//!
//! ```no_run
//! use openssl::ssl::{SslConnector, SslMethod};
//! use postgres_openssl::MakeTlsConnector;
//!
//! # fn main() -> Result<(), Box<std::error::Error>> {
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
//! # fn main() -> Result<(), Box<std::error::Error>> {
//! let mut builder = SslConnector::builder(SslMethod::tls())?;
//! builder.set_ca_file("database_cert.pem")?;
//! let connector = MakeTlsConnector::new(builder.build());
//!
//! let mut client = postgres::Client::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! )?;
//!
//! // ...
//! # Ok(())
//! # }
//! ```
#![doc(html_root_url = "https://docs.rs/postgres-openssl/0.2.0-rc.1")]
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

#[cfg(feature = "runtime")]
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
#[cfg(feature = "runtime")]
use openssl::ssl::SslConnector;
use openssl::ssl::{ConnectConfiguration, SslRef};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
#[cfg(feature = "runtime")]
use std::sync::Arc;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_openssl::{HandshakeError, SslStream};
#[cfg(feature = "runtime")]
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};

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
    type Stream = SslStream<S>;
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
    type Stream = SslStream<S>;
    type Error = HandshakeError<S>;
    type Future = Pin<
        Box<dyn Future<Output = Result<(SslStream<S>, ChannelBinding), HandshakeError<S>>> + Send>,
    >;

    fn connect(self, stream: S) -> Self::Future {
        let future = async move {
            let stream = tokio_openssl::connect(self.ssl, &self.domain, stream).await?;

            let channel_binding = match tls_server_end_point(stream.ssl()) {
                Some(buf) => ChannelBinding::tls_server_end_point(buf),
                None => ChannelBinding::none(),
            };

            Ok((stream, channel_binding))
        };

        Box::pin(future)
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
