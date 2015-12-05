//! Types and traits for SSL adaptors.
pub use priv_io::Stream;

use std::error::Error;
use std::io::prelude::*;

#[cfg(feature = "openssl")]
mod openssl;
#[cfg(feature = "security-framework")]
mod security_framework;

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
///
/// If the `security-framework` Cargo feature is enabled, this trait will be
/// implemented for `security_framework::secure_transport::ClientBuilder`.
///
/// It is also implemented for `Fn(&str, Stream) -> Result<Box<StreamWrapper>,
/// Box<Error + Sync + Send>` closures.
pub trait NegotiateSsl {
    /// Negotiates an SSL session, returning a wrapper around the provided
    /// stream.
    ///
    /// The host portion of the connection parameters is provided for hostname
    /// verification.
    fn negotiate_ssl(&self,
                     host: &str,
                     stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error + Sync + Send>>;
}

impl<F> NegotiateSsl for F
    where F: Fn(&str, Stream) -> Result<Box<StreamWrapper>, Box<Error + Sync + Send>>
{
    fn negotiate_ssl(&self,
                     host: &str,
                     stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error + Sync + Send>> {
        (*self)(host, stream)
    }
}
