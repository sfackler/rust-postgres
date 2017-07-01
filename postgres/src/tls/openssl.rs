//! OpenSSL support.
pub extern crate openssl;

use std::error::Error;
use std::fmt;

use self::openssl::error::ErrorStack;
use self::openssl::ssl::{SslMethod, SslConnector, SslConnectorBuilder, SslStream};
use tls::{TlsStream, Stream, TlsHandshake};

impl TlsStream for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

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
        let connector = SslConnectorBuilder::new(SslMethod::tls())?.build();
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
        let stream = if self.disable_verification {
            self.connector.danger_connect_without_providing_domain_for_certificate_verification_and_server_name_indication(stream)?
        } else {
            self.connector.connect(domain, stream)?
        };
        Ok(Box::new(stream))
    }
}
