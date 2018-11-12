//! OpenSSL support for the `postgres` crate.
pub extern crate openssl;
extern crate postgres;

use std::error::Error;
use std::io::{self, Read, Write};
use std::fmt;
use openssl::error::ErrorStack;
use openssl::ssl::{SslMethod, SslConnector, SslStream};
use postgres::tls::{TlsStream, Stream, TlsHandshake};

#[cfg(test)]
mod test;

/// A `TlsHandshake` implementation that uses OpenSSL.
///
/// Requires the `with-openssl` feature.
pub struct OpenSsl {
    connector: SslConnector,
    disable_verification: bool,
}

impl fmt::Debug for OpenSsl {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("OpenSsl").finish()
    }
}

impl OpenSsl {
    /// Creates a `OpenSsl` with `SslConnector`'s default configuration.
    pub fn new() -> Result<OpenSsl, ErrorStack> {
        let connector = SslConnector::builder(SslMethod::tls())?.build();
        Ok(OpenSsl::from(connector))
    }

    /// Returns a reference to the inner `SslConnector`.
    pub fn connector(&self) -> &SslConnector {
        &self.connector
    }

    /// Returns a mutable reference to the inner `SslConnector`.
    pub fn connector_mut(&mut self) -> &mut SslConnector {
        &mut self.connector
    }

    /// If set, the
    /// `SslConnector::danger_connect_without_providing_domain_for_certificate_verification_and_server_name_indication`
    /// method will be used to connect.
    ///
    /// If certificate verification has been disabled in the `SslConnector`, verification must be
    /// additionally disabled here for that setting to take effect.
    pub fn danger_disable_hostname_verification(&mut self, disable_verification: bool) {
        self.disable_verification = disable_verification;
    }
}

impl From<SslConnector> for OpenSsl {
    fn from(connector: SslConnector) -> OpenSsl {
        OpenSsl {
            connector: connector,
            disable_verification: false,
        }
    }
}

impl TlsHandshake for OpenSsl {
    fn tls_handshake(
        &self,
        domain: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Send + Sync>> {
        let mut ssl = self.connector.configure()?;
        if self.disable_verification {
            ssl.set_use_server_name_indication(false);
            ssl.set_verify_hostname(false);
        }
        let stream = ssl.connect(domain, stream)?;
        Ok(Box::new(OpenSslStream(stream)))
    }
}

#[derive(Debug)]
struct OpenSslStream(SslStream<Stream>);

impl Read for OpenSslStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for OpenSslStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl TlsStream for OpenSslStream {
    fn get_ref(&self) -> &Stream {
        self.0.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.0.get_mut()
    }
}
