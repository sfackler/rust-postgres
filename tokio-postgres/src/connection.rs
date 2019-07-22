use crate::codec::{BackendMessages, FrontendMessage, PostgresCodec};
use crate::maybe_tls_stream::MaybeTlsStream;
use futures::channel::mpsc;
use std::collections::HashMap;
use tokio::codec::Framed;

pub enum RequestMessages {
    Single(FrontendMessage),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Connection<S, T> {
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
}

impl<S, T> Connection<S, T> {
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            receiver,
        }
    }
}
