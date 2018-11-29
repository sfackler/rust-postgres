//! Types and traits for TLS support.
pub use priv_io::Stream;

use std::error::Error;
use std::fmt;
use std::io::prelude::*;

/// A trait implemented by TLS streams.
pub trait TlsStream: fmt::Debug + Read + Write + Send {
    /// Returns a reference to the underlying `Stream`.
    fn get_ref(&self) -> &Stream;

    /// Returns a mutable reference to the underlying `Stream`.
    fn get_mut(&mut self) -> &mut Stream;

    /// Returns the data associated with the `tls-server-end-point` channel binding type as
    /// described in [RFC 5929], if supported.
    ///
    /// An implementation only needs to support one of this or `tls_unique`.
    ///
    /// [RFC 5929]: https://tools.ietf.org/html/rfc5929
    fn tls_server_end_point(&self) -> Option<Vec<u8>> {
        None
    }
}

/// A trait implemented by types that can initiate a TLS session over a Postgres
/// stream.
pub trait TlsHandshake: fmt::Debug {
    /// Performs a client-side TLS handshake, returning a wrapper around the
    /// provided stream.
    ///
    /// The host portion of the connection parameters is provided for hostname
    /// verification.
    fn tls_handshake(
        &self,
        host: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Sync + Send>>;
}

impl<T: TlsHandshake + ?Sized> TlsHandshake for Box<T> {
    fn tls_handshake(
        &self,
        host: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Sync + Send>> {
        (**self).tls_handshake(host, stream)
    }
}
