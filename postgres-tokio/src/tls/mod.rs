use futures::BoxFuture;
use std::error::Error;
use tokio_core::io::Io;

pub use stream::Stream;

#[cfg(feature = "with-openssl")]
pub mod openssl;

pub trait TlsStream: Io + Send {
    fn get_ref(&self) -> &Stream;

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

pub trait Handshake: 'static + Sync + Send {
    fn handshake(&mut self,
                 host: &str,
                 stream: Stream)
                 -> BoxFuture<Box<TlsStream>, Box<Error + Sync + Send>>;
}
