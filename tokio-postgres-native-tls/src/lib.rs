extern crate native_tls;
extern crate tokio_io;
extern crate tokio_postgres;
extern crate tokio_tls;

#[macro_use]
extern crate futures;

#[cfg(test)]
extern crate tokio;

use futures::{Async, Future, Poll};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_postgres::{ChannelBinding, TlsConnect};
use tokio_tls::{Connect, TlsStream};

#[cfg(test)]
mod test;

pub struct TlsConnector {
    connector: tokio_tls::TlsConnector,
    domain: String,
}

impl TlsConnector {
    pub fn new(domain: &str) -> Result<TlsConnector, native_tls::Error> {
        let connector = native_tls::TlsConnector::new()?;
        Ok(TlsConnector::with_connector(connector, domain))
    }

    pub fn with_connector(connector: native_tls::TlsConnector, domain: &str) -> TlsConnector {
        TlsConnector {
            connector: tokio_tls::TlsConnector::from(connector),
            domain: domain.to_string(),
        }
    }
}

impl<S> TlsConnect<S> for TlsConnector
where
    S: AsyncRead + AsyncWrite,
{
    type Stream = TlsStream<S>;
    type Error = native_tls::Error;
    type Future = TlsConnectFuture<S>;

    fn connect(self, stream: S) -> TlsConnectFuture<S> {
        TlsConnectFuture(self.connector.connect(&self.domain, stream))
    }
}

pub struct TlsConnectFuture<S>(Connect<S>);

impl<S> Future for TlsConnectFuture<S>
where
    S: AsyncRead + AsyncWrite,
{
    type Item = (TlsStream<S>, ChannelBinding);
    type Error = native_tls::Error;

    fn poll(&mut self) -> Poll<(TlsStream<S>, ChannelBinding), native_tls::Error> {
        let stream = try_ready!(self.0.poll());
        let mut channel_binding = ChannelBinding::new();

        if let Some(buf) = stream.get_ref().tls_server_end_point().unwrap_or(None) {
            channel_binding = channel_binding.tls_server_end_point(buf);
        }

        Ok(Async::Ready((stream, channel_binding)))
    }
}
