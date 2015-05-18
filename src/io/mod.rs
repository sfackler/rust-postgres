//! Types and traits for SSL adaptors.
pub use priv_io::Stream;

use std::error::Error;
use std::io::prelude::*;

#[cfg(feature = "openssl")]
mod openssl;

/// A trait implemented by SSL adaptors.
pub trait StreamWrapper: Read+Write+Send {
    /// Returns a reference to the underlying `Stream`.
    fn get_ref(&self) -> &Stream;

    /// Returns a mutable reference to the underlying `Stream`.
    fn get_mut(&mut self) -> &mut Stream;
}

/// A trait implemented by types that can negotiate SSL over a Postgres stream.
///
/// If the `openssl` Cargo feature is enabled, this trait will be implemented
/// for `openssl::ssl::SslContext`.
pub trait NegotiateSsl {
    /// Negotiates an SSL session, returning a wrapper around the provided
    /// stream.
    ///
    /// The host portion of the connection parameters is provided for hostname
    /// verification.
    fn negotiate_ssl(&self, host: &str, stream: Stream) -> Result<Box<StreamWrapper>, Box<Error>>;
}
