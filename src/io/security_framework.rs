//! Security Framework support.
extern crate security_framework;

use self::security_framework::secure_transport::{SslStream, ClientBuilder};
use io::{Stream, StreamWrapper, NegotiateSsl};
use std::error::Error;

impl StreamWrapper for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

/// A `NegotiateSsl` implementation that uses Security Framework.
///
/// Requires the `security-framework` feature.
#[derive(Debug)]
pub struct Negotiator(ClientBuilder);

impl Negotiator {
    /// Returns a new `Negotiator` with default settings.
    pub fn new() -> Negotiator {
        ClientBuilder::new().into()
    }

    /// Returns a reference to the associated `ClientBuilder`.
    pub fn builder(&self) -> &ClientBuilder {
        &self.0
    }

    /// Returns a mutable reference to the associated `ClientBuilder`.
    pub fn builder_mut(&mut self) -> &mut ClientBuilder {
        &mut self.0
    }
}

impl From<ClientBuilder> for Negotiator {
    fn from(b: ClientBuilder) -> Negotiator {
        Negotiator(b)
    }
}

impl NegotiateSsl for Negotiator {
    fn negotiate_ssl(&self,
                     domain: &str,
                     stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error + Send + Sync>> {
        let stream = try!(self.0.handshake(domain, stream));
        Ok(Box::new(stream))
    }
}
