extern crate openssl;
extern crate tokio_io;
extern crate tokio_openssl;
extern crate tokio_postgres;

#[macro_use]
extern crate futures;

#[cfg(test)]
extern crate tokio;

use futures::{Async, Future, Poll};
use openssl::ssl::{ConnectConfiguration, HandshakeError, SslRef};
use std::fmt::Debug;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_openssl::{ConnectAsync, ConnectConfigurationExt, SslStream};
use tokio_postgres::{ChannelBinding, TlsConnect};

#[cfg(test)]
mod test;

pub struct TlsConnector {
    ssl: ConnectConfiguration,
    domain: String,
}

impl TlsConnector {
    pub fn new(ssl: ConnectConfiguration, domain: &str) -> TlsConnector {
        TlsConnector {
            ssl,
            domain: domain.to_string(),
        }
    }
}

impl<S> TlsConnect<S> for TlsConnector
where
    S: AsyncRead + AsyncWrite + Debug + 'static + Sync + Send,
{
    type Stream = SslStream<S>;
    type Error = HandshakeError<S>;
    type Future = TlsConnectFuture<S>;

    fn connect(self, stream: S) -> TlsConnectFuture<S> {
        TlsConnectFuture(self.ssl.connect_async(&self.domain, stream))
    }
}

pub struct TlsConnectFuture<S>(ConnectAsync<S>);

impl<S> Future for TlsConnectFuture<S>
where
    S: AsyncRead + AsyncWrite + Debug + 'static + Sync + Send,
{
    type Item = (SslStream<S>, ChannelBinding);
    type Error = HandshakeError<S>;

    fn poll(&mut self) -> Poll<(SslStream<S>, ChannelBinding), HandshakeError<S>> {
        let stream = try_ready!(self.0.poll());

        let f = if stream.get_ref().ssl().session_reused() {
            SslRef::peer_finished
        } else {
            SslRef::finished
        };

        let len = f(stream.get_ref().ssl(), &mut []);
        let mut tls_unique = vec![0; len];
        f(stream.get_ref().ssl(), &mut tls_unique);

        Ok(Async::Ready((
            stream,
            ChannelBinding::new().tls_unique(tls_unique),
        )))
    }
}
