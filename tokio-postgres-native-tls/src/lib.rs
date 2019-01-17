#![warn(rust_2018_idioms, clippy::all)]

use futures::{try_ready, Async, Future, Poll};
use tokio_io::{AsyncRead, AsyncWrite};
#[cfg(feature = "runtime")]
use tokio_postgres::MakeTlsConnect;
use tokio_postgres::{ChannelBinding, TlsConnect};
use tokio_tls::{Connect, TlsStream};

#[cfg(test)]
mod test;

#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct MakeTlsConnector(native_tls::TlsConnector);

#[cfg(feature = "runtime")]
impl MakeTlsConnector {
    pub fn new(connector: native_tls::TlsConnector) -> MakeTlsConnector {
        MakeTlsConnector(connector)
    }
}

#[cfg(feature = "runtime")]
impl<S> MakeTlsConnect<S> for MakeTlsConnector
where
    S: AsyncRead + AsyncWrite,
{
    type Stream = TlsStream<S>;
    type TlsConnect = TlsConnector;
    type Error = native_tls::Error;

    fn make_tls_connect(&mut self, domain: &str) -> Result<TlsConnector, native_tls::Error> {
        Ok(TlsConnector::new(self.0.clone(), domain))
    }
}

pub struct TlsConnector {
    connector: tokio_tls::TlsConnector,
    domain: String,
}

impl TlsConnector {
    pub fn new(connector: native_tls::TlsConnector, domain: &str) -> TlsConnector {
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

        let channel_binding = match stream.get_ref().tls_server_end_point().unwrap_or(None) {
            Some(buf) => ChannelBinding::tls_server_end_point(buf),
            None => ChannelBinding::none(),
        };

        Ok(Async::Ready((stream, channel_binding)))
    }
}
