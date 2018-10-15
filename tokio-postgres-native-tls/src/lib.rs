extern crate bytes;
extern crate futures;
extern crate native_tls;
extern crate tokio_io;
extern crate tokio_postgres;
extern crate tokio_tls;

#[cfg(test)]
extern crate tokio;

use bytes::{Buf, BufMut};
use futures::{Future, Poll};
use std::error::Error;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_postgres::tls::{Socket, TlsConnect, TlsStream};

#[cfg(test)]
mod test;

pub struct TlsConnector {
    connector: tokio_tls::TlsConnector,
}

impl TlsConnector {
    pub fn new() -> Result<TlsConnector, native_tls::Error> {
        let connector = native_tls::TlsConnector::new()?;
        Ok(TlsConnector::with_connector(connector))
    }

    pub fn with_connector(connector: native_tls::TlsConnector) -> TlsConnector {
        TlsConnector {
            connector: tokio_tls::TlsConnector::from(connector),
        }
    }
}

impl TlsConnect for TlsConnector {
    fn connect(
        &self,
        domain: &str,
        socket: Socket,
    ) -> Box<Future<Item = Box<TlsStream>, Error = Box<Error + Sync + Send>> + Sync + Send> {
        let f = self
            .connector
            .connect(domain, socket)
            .map(|s| {
                let s: Box<TlsStream> = Box::new(SslStream(s));
                s
            }).map_err(|e| {
                let e: Box<Error + Sync + Send> = Box::new(e);
                e
            });
        Box::new(f)
    }
}

struct SslStream(tokio_tls::TlsStream<Socket>);

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
    fn tls_server_end_point(&self) -> Option<Vec<u8>> {
        self.0.get_ref().tls_server_end_point().unwrap_or(None)
    }
}
