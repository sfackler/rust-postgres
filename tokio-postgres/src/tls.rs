use futures::future::{self, FutureResult};
use futures::{Future, Poll};
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};

pub(crate) mod private {
    pub struct ForcePrivateApi;
}

/// Channel binding information returned from a TLS handshake.
pub struct ChannelBinding {
    pub(crate) tls_server_end_point: Option<Vec<u8>>,
}

impl ChannelBinding {
    /// Creates a `ChannelBinding` containing no information.
    pub fn none() -> ChannelBinding {
        ChannelBinding {
            tls_server_end_point: None,
        }
    }

    /// Creates a `ChannelBinding` containing `tls-server-end-point` channel binding information.
    pub fn tls_server_end_point(tls_server_end_point: Vec<u8>) -> ChannelBinding {
        ChannelBinding {
            tls_server_end_point: Some(tls_server_end_point),
        }
    }
}

/// A constructor of `TlsConnect`ors.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[cfg(feature = "runtime")]
pub trait MakeTlsConnect<S> {
    /// The stream type created by the `TlsConnect` implementation.
    type Stream: AsyncRead + AsyncWrite;
    /// The `TlsConnect` implementation created by this type.
    type TlsConnect: TlsConnect<S, Stream = Self::Stream>;
    /// The error type retured by the `TlsConnect` implementation.
    type Error: Into<Box<dyn Error + Sync + Send>>;

    /// Creates a new `TlsConnect`or.
    ///
    /// The domain name is provided for certificate verification and SNI.
    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error>;
}

/// An asynchronous function wrapping a stream in a TLS session.
pub trait TlsConnect<S> {
    /// The stream returned by the future.
    type Stream: AsyncRead + AsyncWrite;
    /// The error type returned by the future.
    type Error: Into<Box<dyn Error + Sync + Send>>;
    /// The future returned by the connector.
    type Future: Future<Item = (Self::Stream, ChannelBinding), Error = Self::Error>;

    /// Returns a future performing a TLS handshake over the stream.
    fn connect(self, stream: S) -> Self::Future;

    #[doc(hidden)]
    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        true
    }
}

/// A `MakeTlsConnect` and `TlsConnect` implementation which simply returns an error.
///
/// This can be used when `sslmode` is `none` or `prefer`.
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

/// The TLS "stream" type produced by the `NoTls` connector.
///
/// Since `NoTls` doesn't support TLS, this type is uninhabited.
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

/// The error returned by `NoTls`.
#[derive(Debug)]
pub struct NoTlsError(());

impl fmt::Display for NoTlsError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("no TLS implementation configured")
    }
}

impl Error for NoTlsError {}
