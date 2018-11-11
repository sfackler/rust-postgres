pub extern crate openssl;
extern crate postgres;

use openssl::error::ErrorStack;
use openssl::ssl::{ConnectConfiguration, SslConnector, SslMethod, SslStream};
use postgres::tls::{Stream, TlsHandshake, TlsStream};
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};

#[cfg(test)]
mod test;

pub struct OpenSsl {
    connector: SslConnector,
    config: Box<Fn(&mut ConnectConfiguration) -> Result<(), ErrorStack> + Sync + Send>,
}

impl fmt::Debug for OpenSsl {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("OpenSsl").finish()
    }
}

impl OpenSsl {
    pub fn new() -> Result<OpenSsl, ErrorStack> {
        let connector = SslConnector::builder(SslMethod::tls())?.build();
        Ok(OpenSsl::with_connector(connector))
    }

    pub fn with_connector(connector: SslConnector) -> OpenSsl {
        OpenSsl {
            connector,
            config: Box::new(|_| Ok(())),
        }
    }

    pub fn callback<F>(&mut self, f: F)
    where
        F: Fn(&mut ConnectConfiguration) -> Result<(), ErrorStack> + 'static + Sync + Send,
    {
        self.config = Box::new(f);
    }
}

impl TlsHandshake for OpenSsl {
    fn tls_handshake(
        &self,
        domain: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Sync + Send>> {
        let mut ssl = self.connector.configure()?;
        (self.config)(&mut ssl)?;
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
