//! Types and traits for SSL adaptors.
pub use priv_io::Stream;

use std::error::Error;
use std::io::prelude::*;
use std::fmt;

#[cfg(feature = "with-openssl")]
pub mod openssl;
#[cfg(feature = "security-framework")]
pub mod security_framework;

#[cfg(all(feature = "openssl", not(feature = "with-openssl")))]
const _CHECK: OpensslFeatureRenamedSeeDocs = "";

/// A trait implemented by SSL adaptors.
pub trait StreamWrapper: fmt::Debug + Read + Write + Send {
    /// Returns a reference to the underlying `Stream`.
    fn get_ref(&self) -> &Stream;

    /// Returns a mutable reference to the underlying `Stream`.
    fn get_mut(&mut self) -> &mut Stream;
}

/// A trait implemented by types that can negotiate SSL over a Postgres stream.
pub trait NegotiateSsl: fmt::Debug {
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
