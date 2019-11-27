use bytes::{Buf, BufMut};
use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;

#[derive(Debug)]
enum Inner {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

/// The standard stream type used by the crate.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[derive(Debug)]
pub struct Socket(Inner);

impl Socket {
    pub(crate) fn new_tcp(stream: TcpStream) -> Socket {
        Socket(Inner::Tcp(stream))
    }

    #[cfg(unix)]
    pub(crate) fn new_unix(stream: UnixStream) -> Socket {
        Socket(Inner::Unix(stream))
    }
}

impl AsyncRead for Socket {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        match &self.0 {
            Inner::Tcp(s) => s.prepare_uninitialized_buffer(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.prepare_uninitialized_buffer(buf),
        }
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.0 {
            Inner::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            Inner::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }

    fn poll_read_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut B,
    ) -> Poll<io::Result<usize>>
    where
        Self: Sized,
        B: BufMut,
    {
        match &mut self.0 {
            Inner::Tcp(s) => Pin::new(s).poll_read_buf(cx, buf),
            #[cfg(unix)]
            Inner::Unix(s) => Pin::new(s).poll_read_buf(cx, buf),
        }
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.0 {
            Inner::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            Inner::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.0 {
            Inner::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            Inner::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.0 {
            Inner::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            Inner::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }

    fn poll_write_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut B,
    ) -> Poll<io::Result<usize>>
    where
        Self: Sized,
        B: Buf,
    {
        match &mut self.0 {
            Inner::Tcp(s) => Pin::new(s).poll_write_buf(cx, buf),
            #[cfg(unix)]
            Inner::Unix(s) => Pin::new(s).poll_write_buf(cx, buf),
        }
    }
}
