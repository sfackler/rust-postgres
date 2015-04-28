//! Types and traits for SSL adaptors.

pub use priv_io::Stream;

use openssl::ssl::{SslStream, SslContext};
use std::error::Error;
use std::io::prelude::*;

/// A trait implemented by SSL adaptors.
pub trait StreamWrapper: Read+Write+Send {
    /// Returns a reference to the underlying `Stream`.
    fn get_ref(&self) -> &Stream;

    /// Returns a mutable reference to the underlying `Stream`.
    fn get_mut(&mut self) -> &mut Stream;
}

impl StreamWrapper for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

/// A trait implemented by types that can negotiate SSL over a Postgres stream.
pub trait NegotiateSsl {
    /// Negotiates an SSL session, returning a wrapper around the provided
    /// stream.
    ///
    /// The host portion of the connection parameters is provided for hostname
    /// verification.
    fn negotiate_ssl(&mut self, host: &str, stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error>>;
}

impl NegotiateSsl for SslContext {
    fn negotiate_ssl(&mut self, _: &str, stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error>> {
        let stream = try!(SslStream::new(self, stream));
        Ok(Box::new(stream))
    }
}

/// An uninhabited type implementing `NegotiateSsl`.
///
/// `NoSsl` cannot be instantiated, so the only `SslMode<NoSslMode>` value that
/// can exist is `SslMode::None`. `NoSsl` is the default value of `SslMode`'s
/// parameter so `&mut SslMode::None` can always be passed into
/// `Connection::connect` even if no SSL implementation is available.
pub enum NoSsl {}

impl NegotiateSsl for NoSsl {
    fn negotiate_ssl(&mut self, _: &str, _: Stream) -> Result<Box<StreamWrapper>, Box<Error>> {
        match *self {}
    }
}
