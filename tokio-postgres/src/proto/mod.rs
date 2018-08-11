macro_rules! try_receive {
    ($e:expr) => {
        match $e {
            Ok(::futures::Async::Ready(v)) => v,
            Ok(::futures::Async::NotReady) => return Ok(::futures::Async::NotReady),
            Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
        }
    };
}

mod cancel;
mod client;
mod codec;
mod connect;
mod connection;
mod copy_in;
mod copy_out;
mod execute;
mod handshake;
mod prepare;
mod query;
mod row;
mod simple_query;
mod socket;
mod statement;
mod transaction;
mod typeinfo;
mod typeinfo_composite;
mod typeinfo_enum;

pub use proto::cancel::CancelFuture;
pub use proto::client::Client;
pub use proto::codec::PostgresCodec;
pub use proto::connection::Connection;
pub use proto::copy_in::CopyInFuture;
pub use proto::copy_out::CopyOutStream;
pub use proto::execute::ExecuteFuture;
pub use proto::handshake::HandshakeFuture;
pub use proto::prepare::PrepareFuture;
pub use proto::query::QueryStream;
pub use proto::row::Row;
pub use proto::simple_query::SimpleQueryFuture;
pub use proto::socket::Socket;
pub use proto::statement::Statement;
pub use proto::transaction::TransactionFuture;
