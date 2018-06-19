macro_rules! try_receive {
    ($e:expr) => {
        match $e {
            Ok(::futures::Async::Ready(v)) => v,
            Ok(::futures::Async::NotReady) => return Ok(::futures::Async::NotReady),
            Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
        }
    };
}

mod client;
mod codec;
mod connection;
mod handshake;
mod prepare;
mod socket;
mod statement;

pub use proto::client::Client;
pub use proto::codec::PostgresCodec;
pub use proto::connection::Connection;
pub use proto::handshake::HandshakeFuture;
pub use proto::prepare::PrepareFuture;
pub use proto::socket::Socket;
pub use proto::statement::Statement;
