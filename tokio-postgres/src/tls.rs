use futures::future::{self, FutureResult};
use futures::{Future, Poll};
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};

pub(crate) mod private {
    pub struct ForcePrivateApi;
}

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

#[cfg(feature = "runtime")]
pub trait MakeTlsConnect<S> {
    type Stream: AsyncRead + AsyncWrite;
    type TlsConnect: TlsConnect<S, Stream = Self::Stream>;
    type Error: Into<Box<dyn Error + Sync + Send>>;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error>;
}

pub trait TlsConnect<S> {
    type Stream: AsyncRead + AsyncWrite;
    type Error: Into<Box<dyn Error + Sync + Send>>;
    type Future: Future<Item = (Self::Stream, ChannelBinding), Error = Self::Error>;

    fn connect(self, stream: S) -> Self::Future;

    #[doc(hidden)]
    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        true
    }
}

#[derive(Debug, Copy, Clone)]
pub struct NoTls;

#[cfg(feature = "runtime")]
impl<S> MakeTlsConnect<S> for NoTls where {
    type Stream = NoTlsStream;
    type TlsConnect = NoTls;
    type Error = NoTlsError;

    fn make_tls_connect(&mut self, _: &str) -> Result<NoTls, NoTlsError> {
        Ok(NoTls)
    }
}

impl<S> TlsConnect<S> for NoTls {
    type Stream = NoTlsStream;
    type Error = NoTlsError;
    type Future = FutureResult<(NoTlsStream, ChannelBinding), NoTlsError>;

    fn connect(self, _: S) -> FutureResult<(NoTlsStream, ChannelBinding), NoTlsError> {
        future::err(NoTlsError(()))
    }

    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        false
    }
}

pub enum NoTlsStream {}

impl Read for NoTlsStream {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        match *self {}
    }
}

impl AsyncRead for NoTlsStream {}

impl Write for NoTlsStream {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        match *self {}
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {}
    }
}

impl AsyncWrite for NoTlsStream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match *self {}
    }
}

#[derive(Debug)]
pub struct NoTlsError(());

impl fmt::Display for NoTlsError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("no TLS implementation configured")
    }
}

impl Error for NoTlsError {}
