#![warn(rust_2018_idioms)]

use futures::{try_ready, Async, Future, Poll};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
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

        let channel_binding = match tls_server_end_point(stream.get_ref().ssl()) {
            Some(buf) => ChannelBinding::tls_server_end_point(buf),
            None => ChannelBinding::none(),
        };

        Ok(Async::Ready((stream, channel_binding)))
    }
}

fn tls_server_end_point(ssl: &SslRef) -> Option<Vec<u8>> {
    let cert = ssl.peer_certificate()?;
    let algo_nid = cert.signature_algorithm().object().nid();
    let signature_algorithms = algo_nid.signature_algorithms()?;
    let md = match signature_algorithms.digest {
        Nid::MD5 | Nid::SHA1 => MessageDigest::sha256(),
        nid => MessageDigest::from_nid(nid)?,
    };
    cert.digest(md).ok().map(|b| b.to_vec())
}
