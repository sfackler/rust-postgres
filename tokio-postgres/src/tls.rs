use bytes::{Buf, BufMut};
use futures::future::{self, FutureResult};
use futures::{try_ready, Async, Future, Poll};
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};
use void::Void;

pub struct ChannelBinding {
    pub(crate) tls_server_end_point: Option<Vec<u8>>,
}

impl ChannelBinding {
    pub fn none() -> ChannelBinding {
        ChannelBinding {
            tls_server_end_point: None,
        }
    }

    pub fn tls_server_end_point(tls_server_end_point: Vec<u8>) -> ChannelBinding {
        ChannelBinding {
            tls_server_end_point: Some(tls_server_end_point),
        }
    }
}

pub trait TlsMode<S> {
    type Stream: AsyncRead + AsyncWrite;
    type Error: Into<Box<dyn Error + Sync + Send>>;
    type Future: Future<Item = (Self::Stream, ChannelBinding), Error = Self::Error>;

    fn request_tls(&self) -> bool;

    fn handle_tls(self, use_tls: bool, stream: S) -> Self::Future;
}

pub trait TlsConnect<S> {
    type Stream: AsyncRead + AsyncWrite;
    type Error: Into<Box<dyn Error + Sync + Send>>;
    type Future: Future<Item = (Self::Stream, ChannelBinding), Error = Self::Error>;

    fn connect(self, stream: S) -> Self::Future;
}

#[derive(Debug, Copy, Clone)]
pub struct NoTls;

impl<S> TlsMode<S> for NoTls
where
    S: AsyncRead + AsyncWrite,
{
    type Stream = S;
    type Error = Void;
    type Future = FutureResult<(S, ChannelBinding), Void>;

    fn request_tls(&self) -> bool {
        false
    }

    fn handle_tls(self, use_tls: bool, stream: S) -> FutureResult<(S, ChannelBinding), Void> {
        debug_assert!(!use_tls);

        future::ok((stream, ChannelBinding::none()))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PreferTls<T>(pub T);

impl<T, S> TlsMode<S> for PreferTls<T>
where
    T: TlsConnect<S>,
    S: AsyncRead + AsyncWrite,
{
    type Stream = MaybeTlsStream<T::Stream, S>;
    type Error = T::Error;
    type Future = PreferTlsFuture<T::Future, S>;

    fn request_tls(&self) -> bool {
        true
    }

    fn handle_tls(self, use_tls: bool, stream: S) -> PreferTlsFuture<T::Future, S> {
        let f = if use_tls {
            PreferTlsFutureInner::Tls(self.0.connect(stream))
        } else {
            PreferTlsFutureInner::Raw(Some(stream))
        };

        PreferTlsFuture(f)
    }
}

enum PreferTlsFutureInner<F, S> {
    Tls(F),
    Raw(Option<S>),
}

pub struct PreferTlsFuture<F, S>(PreferTlsFutureInner<F, S>);

impl<F, S, T> Future for PreferTlsFuture<F, S>
where
    F: Future<Item = (T, ChannelBinding)>,
{
    type Item = (MaybeTlsStream<T, S>, ChannelBinding);
    type Error = F::Error;

    fn poll(&mut self) -> Poll<(MaybeTlsStream<T, S>, ChannelBinding), F::Error> {
        match &mut self.0 {
            PreferTlsFutureInner::Tls(f) => {
                let (stream, channel_binding) = try_ready!(f.poll());
                Ok(Async::Ready((MaybeTlsStream::Tls(stream), channel_binding)))
            }
            PreferTlsFutureInner::Raw(s) => Ok(Async::Ready((
                MaybeTlsStream::Raw(s.take().expect("future polled after completion")),
                ChannelBinding::none(),
            ))),
        }
    }
}

pub enum MaybeTlsStream<T, U> {
    Tls(T),
    Raw(U),
}

impl<T, U> Read for MaybeTlsStream<T, U>
where
    T: Read,
    U: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Tls(s) => s.read(buf),
            MaybeTlsStream::Raw(s) => s.read(buf),
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
            MaybeTlsStream::Tls(s) => s.prepare_uninitialized_buffer(buf),
            MaybeTlsStream::Raw(s) => s.prepare_uninitialized_buffer(buf),
        }
    }

    fn read_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: BufMut,
    {
        match self {
            MaybeTlsStream::Tls(s) => s.read_buf(buf),
            MaybeTlsStream::Raw(s) => s.read_buf(buf),
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
            MaybeTlsStream::Tls(s) => s.write(buf),
            MaybeTlsStream::Raw(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            MaybeTlsStream::Tls(s) => s.flush(),
            MaybeTlsStream::Raw(s) => s.flush(),
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
            MaybeTlsStream::Tls(s) => s.shutdown(),
            MaybeTlsStream::Raw(s) => s.shutdown(),
        }
    }

    fn write_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: Buf,
    {
        match self {
            MaybeTlsStream::Tls(s) => s.write_buf(buf),
            MaybeTlsStream::Raw(s) => s.write_buf(buf),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct RequireTls<T>(pub T);

impl<T, S> TlsMode<S> for RequireTls<T>
where
    T: TlsConnect<S>,
{
    type Stream = T::Stream;
    type Error = Box<dyn Error + Sync + Send>;
    type Future = RequireTlsFuture<T::Future>;

    fn request_tls(&self) -> bool {
        true
    }

    fn handle_tls(self, use_tls: bool, stream: S) -> RequireTlsFuture<T::Future> {
        let f = if use_tls {
            Ok(self.0.connect(stream))
        } else {
            Err(TlsUnsupportedError(()).into())
        };

        RequireTlsFuture { f: Some(f) }
    }
}

#[derive(Debug)]
pub struct TlsUnsupportedError(());

impl fmt::Display for TlsUnsupportedError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("TLS was required but not supported by the server")
    }
}

impl Error for TlsUnsupportedError {}

pub struct RequireTlsFuture<T> {
    f: Option<Result<T, Box<dyn Error + Sync + Send>>>,
}

impl<T> Future for RequireTlsFuture<T>
where
    T: Future,
    T::Error: Into<Box<dyn Error + Sync + Send>>,
{
    type Item = T::Item;
    type Error = Box<dyn Error + Sync + Send>;

    fn poll(&mut self) -> Poll<T::Item, Box<dyn Error + Sync + Send>> {
        match self.f.take().expect("future polled after completion") {
            Ok(mut f) => match f.poll().map_err(Into::into)? {
                Async::Ready(r) => Ok(Async::Ready(r)),
                Async::NotReady => {
                    self.f = Some(Ok(f));
                    Ok(Async::NotReady)
                }
            },
            Err(e) => Err(e),
        }
    }
}
