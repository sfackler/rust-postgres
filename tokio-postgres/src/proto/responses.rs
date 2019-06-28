use fallible_iterator::FallibleIterator;
use futures::sync::mpsc;
use futures::{try_ready, Async, Poll, Stream};
use postgres_protocol::message::backend;

use crate::proto::codec::BackendMessages;
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
    type Item = backend::Message;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<backend::Message>, Error> {
        loop {
            if let Some(message) = self.cur.next().map_err(Error::parse)? {
                return Ok(Async::Ready(Some(message)));
            }

            match try_ready!(self.receiver.poll().map_err(|()| Error::closed())) {
                Some(messages) => self.cur = messages,
                None => return Ok(Async::Ready(None)),
            }
        }
    }
}
