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

impl NegotiateSsl for ClientBuilder {
    fn negotiate_ssl(&self,
                     domain: &str,
                     stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error + Send + Sync>> {
        let stream = try!(self.handshake(domain, stream));
        Ok(Box::new(stream))
    }
}
