//! OpenSSL support.
extern crate openssl;

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
pub struct OpenSsl(SslConnector);

impl fmt::Debug for OpenSsl {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("OpenSsl").finish()
    }
}

impl OpenSsl {
    /// Creates a `OpenSsl` with `SslConnector`'s default configuration.
    pub fn new() -> Result<OpenSsl, ErrorStack> {
        let connector = try!(SslConnectorBuilder::new(SslMethod::tls())).build();
        Ok(OpenSsl(connector))
    }

    /// Returns a reference to the inner `SslConnector`.
    pub fn connector(&self) -> &SslConnector {
        &self.0
    }

    /// Returns a mutable reference to the inner `SslConnector`.
    pub fn connector_mut(&mut self) -> &mut SslConnector {
        &mut self.0
    }
}

impl From<SslConnector> for OpenSsl {
    fn from(connector: SslConnector) -> OpenSsl {
        OpenSsl(connector)
    }
}

impl TlsHandshake for OpenSsl {
    fn tls_handshake(&self,
                     domain: &str,
                     stream: Stream)
                     -> Result<Box<TlsStream>, Box<Error + Send + Sync>> {
        let stream = try!(self.0.connect(domain, stream));
        Ok(Box::new(stream))
    }
}
