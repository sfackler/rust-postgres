//! TLS support.

use futures::BoxFuture;
use std::error::Error;
use tokio_core::io::Io;

pub use stream::Stream;

#[cfg(feature = "with-openssl")]
pub mod openssl;

/// A trait implemented by streams returned from `Handshake` implementations.
pub trait TlsStream: Io + Send {
    /// Returns a shared reference to the inner stream.
    fn get_ref(&self) -> &Stream;

    /// Returns a mutable reference to the inner stream.
    fn get_mut(&mut self) -> &mut Stream;
}

impl Io for Box<TlsStream> {}

impl TlsStream for Stream {
    fn get_ref(&self) -> &Stream {
        self
    }

    fn get_mut(&mut self) -> &mut Stream {
        self
    }
}

/// A trait implemented by types that can manage TLS encryption for a stream.
pub trait Handshake: 'static + Sync + Send {
    /// Performs a TLS handshake, returning a wrapped stream.
    fn handshake(self: Box<Self>,
                 host: &str,
                 stream: Stream)
                 -> BoxFuture<Box<TlsStream>, Box<Error + Sync + Send>>;
}
