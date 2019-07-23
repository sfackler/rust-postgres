use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::{ready, task::Context, Poll, Stream, StreamExt};
use postgres_protocol::message::backend;
use std::pin::Pin;

use crate::codec::BackendMessages;
use crate::Error;

pub fn channel() -> (mpsc::Sender<BackendMessages>, Responses) {
    let (sender, receiver) = mpsc::channel(1);

    (
        sender,
        Responses {
            receiver,
            cur: BackendMessages::empty(),
        },
    )
}

pub struct Responses {
    receiver: mpsc::Receiver<BackendMessages>,
    cur: BackendMessages,
}

impl Stream for Responses {
    type Item = Result<backend::Message, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(message) = self.cur.next().map_err(Error::parse)? {
                return Poll::Ready(Some(Ok(message)));
            }

            match ready!(self.receiver.poll_next_unpin(cx)) {
                Some(messages) => self.cur = messages,
                None => return Poll::Ready(None),
            }
        }
    }
}
