//! OpenSSL support.
extern crate tokio_openssl;
pub extern crate openssl;

use futures::Future;
use self::openssl::ssl::{SslMethod, SslConnector, SslConnectorBuilder};
use self::openssl::error::ErrorStack;
use std::error::Error;
use self::tokio_openssl::{SslConnectorExt, SslStream};

use BoxedFuture;
use tls::{Stream, TlsStream, Handshake};

impl TlsStream for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref().get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut().get_mut()
    }
}

/// A `Handshake` implementation using OpenSSL.
pub struct OpenSsl(SslConnector);

impl OpenSsl {
    /// Creates a new `OpenSsl` with default settings.
    pub fn new() -> Result<OpenSsl, ErrorStack> {
        let connector = SslConnectorBuilder::new(SslMethod::tls())?.build();
        Ok(OpenSsl(connector))
    }
}

impl From<SslConnector> for OpenSsl {
    fn from(connector: SslConnector) -> OpenSsl {
        OpenSsl(connector)
    }
}

impl Handshake for OpenSsl {
    fn handshake(
        &self,
        host: &str,
        stream: Stream,
    ) -> Box<Future<Item = Box<TlsStream>, Error = Box<Error + Sync + Send>> + Send> {
        self.0
            .connect_async(host, stream)
            .map(|s| {
                let s: Box<TlsStream> = Box::new(s);
                s
            })
            .map_err(|e| {
                let e: Box<Error + Sync + Send> = Box::new(e);
                e
            })
            .boxed2()
    }
}
