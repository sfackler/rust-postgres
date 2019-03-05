use bytes::{Buf, BufMut};
use futures::Poll;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_tcp::TcpStream;
#[cfg(unix)]
use tokio_uds::UnixStream;

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

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.0 {
            Inner::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.read(buf),
        }
    }
}

impl AsyncRead for Socket {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        match &self.0 {
            Inner::Tcp(s) => s.prepare_uninitialized_buffer(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.prepare_uninitialized_buffer(buf),
        }
    }

    fn read_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: BufMut,
    {
        match &mut self.0 {
            Inner::Tcp(s) => s.read_buf(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.read_buf(buf),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.0 {
            Inner::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.0 {
            Inner::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Inner::Unix(s) => s.flush(),
        }
    }
}

impl AsyncWrite for Socket {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match &mut self.0 {
            Inner::Tcp(s) => s.shutdown(),
            #[cfg(unix)]
            Inner::Unix(s) => s.shutdown(),
        }
    }

    fn write_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: Buf,
    {
        match &mut self.0 {
            Inner::Tcp(s) => s.write_buf(buf),
            #[cfg(unix)]
            Inner::Unix(s) => s.write_buf(buf),
        }
    }
}
