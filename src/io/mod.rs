//! Types and traits for TLS adaptors.
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

/// A trait implemented by TLS adaptors.
pub trait TlsStream: fmt::Debug + Read + Write + Send {
    /// Returns a reference to the underlying `Stream`.
    fn get_ref(&self) -> &Stream;

    /// Returns a mutable reference to the underlying `Stream`.
    fn get_mut(&mut self) -> &mut Stream;
}

/// A trait implemented by types that can negotiate TLS over a Postgres stream.
pub trait TlsHandshake: fmt::Debug {
    /// Performs a client-side TLS handshake, returning a wrapper around the
    /// provided stream.
    ///
    /// The host portion of the connection parameters is provided for hostname
    /// verification.
    fn tls_handshake(&self,
                     host: &str,
                     stream: Stream)
                     -> Result<Box<TlsStream>, Box<Error + Sync + Send>>;
}
