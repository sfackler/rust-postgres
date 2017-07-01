//! Native TLS support.
pub extern crate native_tls;

use std::error::Error;
use std::fmt;

use self::native_tls::TlsConnector;
use tls::{TlsStream, Stream, TlsHandshake};

impl TlsStream for native_tls::TlsStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

/// A `TlsHandshake` implementation that uses the native-tls crate.
///
/// Requires the `with-native-tls` feature.
pub struct NativeTls(TlsConnector);

impl fmt::Debug for NativeTls {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("NativeTls").finish()
    }
}

impl NativeTls {
    /// Creates a new `NativeTls` with its default configuration.
    pub fn new() -> Result<NativeTls, native_tls::Error> {
        let connector = TlsConnector::builder()?;
        let connector = connector.build()?;
        Ok(NativeTls(connector))
    }

    /// Returns a reference to the inner `TlsConnector`.
    pub fn connector(&self) -> &TlsConnector {
        &self.0
    }

    /// Returns a mutable reference to the inner `TlsConnector`.
    pub fn connector_mut(&mut self) -> &mut TlsConnector {
        &mut self.0
    }
}

impl From<TlsConnector> for NativeTls {
    fn from(connector: TlsConnector) -> NativeTls {
        NativeTls(connector)
    }
}

impl TlsHandshake for NativeTls {
    fn tls_handshake(
        &self,
        domain: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Send + Sync>> {
        let stream = self.0.connect(domain, stream)?;
        Ok(Box::new(stream))
    }
}
