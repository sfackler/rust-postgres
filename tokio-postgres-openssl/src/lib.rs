extern crate bytes;
extern crate futures;
extern crate openssl;
extern crate tokio_io;
extern crate tokio_openssl;
extern crate tokio_postgres;

#[cfg(test)]
extern crate tokio;

use bytes::{Buf, BufMut};
use futures::{Future, IntoFuture, Poll};
use openssl::error::ErrorStack;
use openssl::ssl::{ConnectConfiguration, SslConnector, SslMethod, SslRef};
use std::error::Error;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_openssl::ConnectConfigurationExt;
use tokio_postgres::tls::{Socket, TlsConnect, TlsStream};

#[cfg(test)]
mod test;

pub struct TlsConnector {
    connector: SslConnector,
    callback: Box<Fn(&mut ConnectConfiguration) -> Result<(), ErrorStack> + Sync + Send>,
}

impl TlsConnector {
    pub fn new() -> Result<TlsConnector, ErrorStack> {
        let connector = SslConnector::builder(SslMethod::tls())?.build();
        Ok(TlsConnector::with_connector(connector))
    }

    pub fn with_connector(connector: SslConnector) -> TlsConnector {
        TlsConnector {
            connector,
            callback: Box::new(|_| Ok(())),
        }
    }

    pub fn set_callback<F>(&mut self, f: F)
    where
        F: Fn(&mut ConnectConfiguration) -> Result<(), ErrorStack> + 'static + Sync + Send,
    {
        self.callback = Box::new(f);
    }
}

impl TlsConnect for TlsConnector {
    fn connect(
        &self,
        domain: &str,
        socket: Socket,
    ) -> Box<Future<Item = Box<TlsStream + Send>, Error = Box<Error + Sync + Send>> + Sync + Send> {
        let f = self
            .connector
            .configure()
            .and_then(|mut ssl| (self.callback)(&mut ssl).map(|_| ssl))
            .map_err(|e| {
                let e: Box<Error + Sync + Send> = Box::new(e);
                e
            })
            .into_future()
            .and_then({
                let domain = domain.to_string();
                move |ssl| {
                    ssl.connect_async(&domain, socket)
                        .map(|s| {
                            let s: Box<TlsStream + Send> = Box::new(SslStream(s));
                            s
                        })
                        .map_err(|e| {
                            let e: Box<Error + Sync + Send> = Box::new(e);
                            e
                        })
                }
            });
        Box::new(f)
    }
}

struct SslStream(tokio_openssl::SslStream<Socket>);

impl Read for SslStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl AsyncRead for SslStream {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        self.0.prepare_uninitialized_buffer(buf)
    }

    fn read_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: BufMut,
    {
        self.0.read_buf(buf)
    }
}

impl Write for SslStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl AsyncWrite for SslStream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.0.shutdown()
    }

    fn write_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: Buf,
    {
        self.0.write_buf(buf)
    }
}

impl TlsStream for SslStream {
    fn tls_unique(&self) -> Option<Vec<u8>> {
        let f = if self.0.get_ref().ssl().session_reused() {
            SslRef::peer_finished
        } else {
            SslRef::finished
        };

        let len = f(self.0.get_ref().ssl(), &mut []);
        let mut buf = vec![0; len];
        f(self.0.get_ref().ssl(), &mut buf);

        Some(buf)
    }
}
