extern crate openssl;
extern crate openssl_verify;

use std::error::Error;

use self::openssl::ssl::{IntoSsl, SslContext, SslStream, SSL_VERIFY_PEER};
use self::openssl_verify::verify_callback;
use io::{StreamWrapper, Stream, NegotiateSsl};

impl StreamWrapper for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

impl NegotiateSsl for SslContext {
    fn negotiate_ssl(&self,
                     domain: &str,
                     stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error + Send + Sync>> {
        let domain = domain.to_owned();
        let mut ssl = try!(self.into_ssl());
        ssl.set_verify_callback(SSL_VERIFY_PEER, move |p, x| verify_callback(&domain, p, x));
        let stream = try!(SslStream::connect(ssl, stream));
        Ok(Box::new(stream))
    }
}
