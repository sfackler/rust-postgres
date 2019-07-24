use crate::codec::BackendMessages;
use crate::connection::{Request, RequestMessages};
use crate::prepare::prepare;
use crate::types::Type;
use crate::{Error, Statement};
use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use postgres_protocol::message::backend::Message;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct Responses {
    receiver: mpsc::Receiver<BackendMessages>,
    cur: BackendMessages,
}

impl Responses {
    pub async fn next(&mut self) -> Result<Message, Error> {
        loop {
            match self.cur.next().map_err(Error::parse)? {
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(message) => return Ok(message),
                None => {}
            }

            match self.receiver.next().await {
                Some(messages) => self.cur = messages,
                None => return Err(Error::closed()),
            }
        }
    }
}

pub struct InnerClient {
    sender: mpsc::UnboundedSender<Request>,
}

impl InnerClient {
    pub fn send(&self, messages: RequestMessages) -> Result<Responses, Error> {
        let (sender, receiver) = mpsc::channel(1);
        let request = Request { messages, sender };
        self.sender
            .unbounded_send(request)
            .map_err(|_| Error::closed())?;

        Ok(Responses {
            receiver,
            cur: BackendMessages::empty(),
        })
    }
}

pub struct Client {
    inner: Arc<InnerClient>,
    process_id: i32,
    secret_key: i32,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Request>,
        process_id: i32,
        secret_key: i32,
    ) -> Client {
        Client {
            inner: Arc::new(InnerClient { sender }),
            process_id,
            secret_key,
        }
    }

    pub(crate) fn inner(&self) -> Arc<InnerClient> {
        self.inner.clone()
    }

    pub fn prepare<'a>(
        &mut self,
        query: &'a str,
    ) -> impl Future<Output = Result<Statement, Error>> + 'a {
        self.prepare_typed(query, &[])
    }

    pub fn prepare_typed<'a>(
        &mut self,
        query: &'a str,
        parameter_types: &'a [Type],
    ) -> impl Future<Output = Result<Statement, Error>> + 'a {
        prepare(self.inner(), query, parameter_types)
    }
}
