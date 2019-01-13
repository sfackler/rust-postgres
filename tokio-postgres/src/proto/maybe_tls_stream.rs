use bytes::{Buf, BufMut};
use futures::Poll;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};

pub enum MaybeTlsStream<T, U> {
    Raw(T),
    Tls(U),
}

impl<T, U> Read for MaybeTlsStream<T, U>
where
    T: Read,
    U: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Raw(s) => s.read(buf),
            MaybeTlsStream::Tls(s) => s.read(buf),
        }
    }
}

impl<T, U> AsyncRead for MaybeTlsStream<T, U>
where
    T: AsyncRead,
    U: AsyncRead,
{
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        match self {
            MaybeTlsStream::Raw(s) => s.prepare_uninitialized_buffer(buf),
            MaybeTlsStream::Tls(s) => s.prepare_uninitialized_buffer(buf),
        }
    }

    fn read_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: BufMut,
    {
        match self {
            MaybeTlsStream::Raw(s) => s.read_buf(buf),
            MaybeTlsStream::Tls(s) => s.read_buf(buf),
        }
    }
}

impl<T, U> Write for MaybeTlsStream<T, U>
where
    T: Write,
    U: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Raw(s) => s.write(buf),
            MaybeTlsStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            MaybeTlsStream::Raw(s) => s.flush(),
            MaybeTlsStream::Tls(s) => s.flush(),
        }
    }
}

impl<T, U> AsyncWrite for MaybeTlsStream<T, U>
where
    T: AsyncWrite,
    U: AsyncWrite,
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match self {
            MaybeTlsStream::Raw(s) => s.shutdown(),
            MaybeTlsStream::Tls(s) => s.shutdown(),
        }
    }

    fn write_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: Buf,
    {
        match self {
            MaybeTlsStream::Raw(s) => s.write_buf(buf),
            MaybeTlsStream::Tls(s) => s.write_buf(buf),
        }
    }
}
