use futures::{Future, BoxFuture};
use openssl::ssl::{SslMethod, SslConnector, SslConnectorBuilder};
use openssl::error::ErrorStack;
use std::error::Error;
use tokio_openssl::{SslConnectorExt, SslStream};

use tls::{Stream, TlsStream, Handshake};

impl TlsStream for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref().get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut().get_mut()
    }
}

pub struct OpenSsl(SslConnector);

impl OpenSsl {
    pub fn new() -> Result<OpenSsl, ErrorStack> {
        let connector = try!(SslConnectorBuilder::new(SslMethod::tls())).build();
        Ok(OpenSsl(connector))
    }
}

impl From<SslConnector> for OpenSsl {
    fn from(connector: SslConnector) -> OpenSsl {
        OpenSsl(connector)
    }
}

impl Handshake for OpenSsl {
    fn handshake(self: Box<Self>,
                 host: &str,
                 stream: Stream)
                 -> BoxFuture<Box<TlsStream>, Box<Error + Sync + Send>> {
        self.0.connect_async(host, stream)
            .map(|s| {
                let s: Box<TlsStream> = Box::new(s);
                s
            })
            .map_err(|e| {
                let e: Box<Error + Sync + Send> = Box::new(e);
                e
            })
            .boxed()
    }
}