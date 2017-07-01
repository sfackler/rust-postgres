//! SChannel support.

pub extern crate schannel;

use std::error::Error;
use std::fmt;

use self::schannel::schannel_cred::{SchannelCred, Direction};
use self::schannel::tls_stream;
use tls::{TlsStream, Stream, TlsHandshake};

impl TlsStream for tls_stream::TlsStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

/// A `TlsHandshake` implementation that uses the `schannel` crate.
///
/// Requires the `with-schannel` feature.
pub struct Schannel(());

impl fmt::Debug for Schannel {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Schannel").finish()
    }
}

impl Schannel {
    /// Constructs a new `SChannel` with a default configuration.
    pub fn new() -> Schannel {
        Schannel(())
    }
}

impl TlsHandshake for Schannel {
    fn tls_handshake(
        &self,
        host: &str,
        stream: Stream,
    ) -> Result<Box<TlsStream>, Box<Error + Sync + Send>> {
        let creds = SchannelCred::builder().acquire(Direction::Outbound)?;
        let stream = tls_stream::Builder::new().domain(host).connect(
            creds,
            stream,
        )?;
        Ok(Box::new(stream))
    }
}
