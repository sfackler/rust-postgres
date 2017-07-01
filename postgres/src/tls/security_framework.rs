//! Security Framework support.
pub extern crate security_framework;

use self::security_framework::secure_transport::{SslStream, ClientBuilder};
use tls::{Stream, TlsStream, TlsHandshake};
use std::error::Error;

impl TlsStream for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

/// A `TlsHandshake` implementation that uses the Security Framework.
///
/// Requires the `with-security-framework` feature.
#[derive(Debug)]
pub struct SecurityFramework(ClientBuilder);

impl SecurityFramework {
    /// Returns a new `SecurityFramework` with default settings.
    pub fn new() -> SecurityFramework {
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

impl From<ClientBuilder> for SecurityFramework {
    fn from(b: ClientBuilder) -> SecurityFramework {
        SecurityFramework(b)
    }
}

impl TlsHandshake for SecurityFramework {
    fn tls_handshake(
        &self,
        domain: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Send + Sync>> {
        let stream = self.0.handshake(domain, stream)?;
        Ok(Box::new(stream))
    }
}
