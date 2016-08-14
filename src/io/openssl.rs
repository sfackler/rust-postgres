//! OpenSSL support.
extern crate openssl;
extern crate openssl_verify;

use std::error::Error;

use self::openssl::error::ErrorStack;
use self::openssl::ssl::{IntoSsl, SslContext, SslStream, SslMethod, SSL_VERIFY_PEER,
                         SSL_OP_NO_SSLV2, SSL_OP_NO_SSLV3, SSL_OP_NO_COMPRESSION};
use self::openssl_verify::verify_callback;
use io::{TlsStream, Stream, TlsHandshake};

impl TlsStream for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

/// A `TlsHandshake` implementation that uses OpenSSL.
///
/// Requires the `with-openssl` feature.
#[derive(Debug)]
pub struct OpenSsl(SslContext);

impl OpenSsl {
    /// Creates a `OpenSsl` with a reasonable default configuration.
    ///
    /// The configuration is modeled after libcurl's and is subject to change.
    pub fn new() -> Result<OpenSsl, ErrorStack> {
        let mut ctx = try!(SslContext::new(SslMethod::Sslv23));
        try!(ctx.set_default_verify_paths());
        ctx.set_options(SSL_OP_NO_SSLV2 | SSL_OP_NO_SSLV3 | SSL_OP_NO_COMPRESSION);
        try!(ctx.set_cipher_list("ALL!EXPORT!EXPORT40!EXPORT56!aNULL!LOW!RC4@STRENGTH"));
        Ok(ctx.into())
    }

    /// Returns a reference to the associated `SslContext`.
    pub fn context(&self) -> &SslContext {
        &self.0
    }

    /// Returns a mutable reference to the associated `SslContext`.
    pub fn context_mut(&mut self) -> &mut SslContext {
        &mut self.0
    }
}

impl From<SslContext> for OpenSsl {
    fn from(ctx: SslContext) -> OpenSsl {
        OpenSsl(ctx)
    }
}

impl TlsHandshake for OpenSsl {
    fn tls_handshake(&self,
                     domain: &str,
                     stream: Stream)
                     -> Result<Box<TlsStream>, Box<Error + Send + Sync>> {
        let domain = domain.to_owned();
        let mut ssl = try!(self.0.into_ssl());
        ssl.set_verify_callback(SSL_VERIFY_PEER, move |p, x| verify_callback(&domain, p, x));
        let stream = try!(SslStream::connect(ssl, stream));
        Ok(Box::new(stream))
    }
}
