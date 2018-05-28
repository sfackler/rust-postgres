pub extern crate native_tls;
extern crate postgres;

use native_tls::TlsConnector;
use postgres::tls::{Stream, TlsHandshake, TlsStream};
use std::error::Error;
use std::fmt::{self, Debug};
use std::io::{self, Read, Write};

#[cfg(test)]
mod test;

pub struct NativeTls {
    connector: TlsConnector,
}

impl Debug for NativeTls {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("NativeTls").finish()
    }
}

impl NativeTls {
    pub fn new() -> Result<NativeTls, native_tls::Error> {
        let connector = TlsConnector::builder()?.build()?;
        Ok(NativeTls::with_connector(connector))
    }

    pub fn with_connector(connector: TlsConnector) -> NativeTls {
        NativeTls { connector }
    }
}

impl TlsHandshake for NativeTls {
    fn tls_handshake(
        &self,
        domain: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Sync + Send>> {
        let stream = self.connector.connect(domain, stream)?;
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
