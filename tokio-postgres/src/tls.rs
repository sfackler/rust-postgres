//! TLS support.

use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, io};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
    type Stream: TlsStream + Unpin;
    /// The `TlsConnect` implementation created by this type.
    type TlsConnect: TlsConnect<S, Stream = Self::Stream>;
    /// The error type returned by the `TlsConnect` implementation.
    type Error: Into<Box<dyn Error + Sync + Send>>;

    /// Creates a new `TlsConnect`or.
    ///
    /// The domain name is provided for certificate verification and SNI.
    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error>;
}

/// An asynchronous function wrapping a stream in a TLS session.
pub trait TlsConnect<S> {
    /// The stream returned by the future.
    type Stream: TlsStream + Unpin;
    /// The error returned by the future.
    type Error: Into<Box<dyn Error + Sync + Send>>;
    /// The future returned by the connector.
    type Future: Future<Output = Result<Self::Stream, Self::Error>>;

    /// Returns a future performing a TLS handshake over the stream.
    fn connect(self, stream: S) -> Self::Future;

    #[doc(hidden)]
    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        true
    }
}

/// A TLS-wrapped connection to a PostgreSQL database.
pub trait TlsStream: AsyncRead + AsyncWrite {
    /// Returns channel binding information for the session.
    fn channel_binding(&self) -> ChannelBinding;
}

/// A `MakeTlsConnect` and `TlsConnect` implementation which simply returns an error.
///
/// This can be used when `sslmode` is `none` or `prefer`.
#[derive(Debug, Copy, Clone)]
pub struct NoTls;

#[cfg(feature = "runtime")]
impl<S> MakeTlsConnect<S> for NoTls {
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
    type Future = NoTlsFuture;

    fn connect(self, _: S) -> NoTlsFuture {
        NoTlsFuture(())
    }

    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        false
    }
}

/// The future returned by `NoTls`.
pub struct NoTlsFuture(());

impl Future for NoTlsFuture {
    type Output = Result<NoTlsStream, NoTlsError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(NoTlsError(())))
    }
}

/// The TLS "stream" type produced by the `NoTls` connector.
///
/// Since `NoTls` doesn't support TLS, this type is uninhabited.
pub enum NoTlsStream {}

impl AsyncRead for NoTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match *self {}
    }
}

impl AsyncWrite for NoTlsStream {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        match *self {}
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        match *self {}
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        match *self {}
    }
}

impl TlsStream for NoTlsStream {
    fn channel_binding(&self) -> ChannelBinding {
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

/// Represents a stream which can start tls itself.
pub trait StartTlsStream: TlsStream {
    /// Switch from plaintext to TLS
    fn start_tls(self) -> Self;
}
/// Use TLS when socket already originates TLS connection
pub struct PassthroughTls;

#[derive(Debug)]
/// Error type for PassthroughTls.
/// Should never be returned.
pub struct PassthroughTlsError;

impl Error for PassthroughTlsError {}

impl fmt::Display for PassthroughTlsError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("PassthroughTlsError: future already polled")
    }
}

impl<S: StartTlsStream + Unpin> TlsConnect<S> for PassthroughTls {
    type Stream = S;
    type Error = PassthroughTlsError;
    type Future = futures_util::future::Ready<Result<S, PassthroughTlsError>>;

    fn connect(self, s: Self::Stream) -> Self::Future {
        let tls = s.start_tls();
        futures_util::future::ready(Ok(tls))
    }

    fn can_connect(&self, _: private::ForcePrivateApi) -> bool {
        true
    }
}
