#![feature(type_alias_impl_trait)]

use std::{
    io,
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut};
use futures::future::TryFutureExt;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect};
use tokio_rustls::{client::TlsStream, TlsConnector};
use webpki::{DNSName, DNSNameRef};


pub struct MakeRustlsConnect {
    config: Arc<ClientConfig>,
}

impl MakeRustlsConnect {
    pub fn new(config: ClientConfig) -> Self {
        Self { config: Arc::new(config) }
    }
}

impl<S> MakeTlsConnect<S> for MakeRustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Stream = RustlsStream<S>;
    type TlsConnect = RustlsConnect;
    type Error = std::io::Error;

    fn make_tls_connect(&mut self, hostname: &str) -> std::io::Result<RustlsConnect> {
        DNSNameRef::try_from_ascii_str(hostname)
            .map(|dns_name| RustlsConnect {
                hostname: dns_name.to_owned(),
                connector: Arc::clone(&self.config).into(),
            })
            .map_err(|_| io::ErrorKind::InvalidInput.into())
    }
}

pub struct RustlsConnect {
    hostname: DNSName,
    connector: TlsConnector,
}

impl<S> TlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Stream = RustlsStream<S>;
    type Error = std::io::Error;
    type Future = impl Future<Output = std::io::Result<RustlsStream<S>>>;

    fn connect(self, stream: S) -> Self::Future {
        self.connector.connect(self.hostname.as_ref(), stream)
            .map_ok(|s| RustlsStream(Box::pin(s)))
    }
}

pub struct RustlsStream<S>(Pin<Box<TlsStream<S>>>);

impl<S> tokio_postgres::tls::TlsStream for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none() // TODO
    }
}

impl<S> AsyncRead for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<tokio::io::Result<usize>> {
        self.0.as_mut().poll_read(cx, buf)
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        self.0.prepare_uninitialized_buffer(buf)
    }

    fn poll_read_buf<B: BufMut>(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut B) -> Poll<tokio::io::Result<usize>>
    where
        Self: Sized,
    {
        self.0.as_mut().poll_read_buf(cx, buf)
    }
}

impl<S> AsyncWrite for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<tokio::io::Result<usize>> {
        self.0.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        self.0.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        self.0.as_mut().poll_shutdown(cx)
    }

    fn poll_write_buf<B: Buf>(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut B) -> Poll<tokio::io::Result<usize>>
    where
        Self: Sized,
    {
        self.0.as_mut().poll_write_buf(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    use futures::future::TryFutureExt;

    #[tokio::test]
    async fn it_works() {
        let config = rustls::ClientConfig::new();
        let tls = super::MakeRustlsConnect::new(config);
        let (client, conn) = tokio_postgres::connect("sslmode=require host=localhost user=postgres", tls).await.unwrap();
        tokio::spawn(conn.map_err(|e| panic!("{:?}", e)));
        let stmt = client.prepare("SELECT 1").await.unwrap();
        let _ = client.query(&stmt, &[]).await.unwrap();
    }
}
