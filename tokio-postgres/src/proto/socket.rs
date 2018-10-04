use bytes::{Buf, BufMut};
use futures::Poll;
use std::io::{self, Read, Write};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

#[cfg(unix)]
use tokio_uds::UnixStream;

pub enum Socket {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Socket::Tcp(stream) => stream.read(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.read(buf),
        }
    }
}

impl AsyncRead for Socket {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        match self {
            Socket::Tcp(stream) => stream.prepare_uninitialized_buffer(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.prepare_uninitialized_buffer(buf),
        }
    }

    fn read_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: BufMut,
    {
        match self {
            Socket::Tcp(stream) => stream.read_buf(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.read_buf(buf),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Socket::Tcp(stream) => stream.write(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Socket::Tcp(stream) => stream.flush(),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.flush(),
        }
    }
}

impl AsyncWrite for Socket {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match self {
            Socket::Tcp(stream) => stream.shutdown(),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.shutdown(),
        }
    }

    fn write_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: Buf,
    {
        match self {
            Socket::Tcp(stream) => stream.write_buf(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.write_buf(buf),
        }
    }
}
