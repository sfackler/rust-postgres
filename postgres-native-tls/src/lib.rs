//! Native TLS support for the `postgres` crate.
pub extern crate native_tls;
extern crate postgres;

use native_tls::TlsConnector;
use postgres::tls::{Stream, TlsHandshake, TlsStream};
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};

#[cfg(test)]
mod test;

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
        let connector = TlsConnector::builder().build()?;
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
        Ok(Box::new(NativeTlsStream(stream)))
    }
}

#[derive(Debug)]
struct NativeTlsStream(native_tls::TlsStream<Stream>);

impl Read for NativeTlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for NativeTlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl TlsStream for NativeTlsStream {
    fn get_ref(&self) -> &Stream {
        self.0.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.0.get_mut()
    }
}
