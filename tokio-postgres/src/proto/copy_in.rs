use bytes::{Buf, IntoBuf};
use futures::sink;
use futures::sync::mpsc;
use futures::{try_ready, Async, AsyncSink, Future, Poll, Sink, Stream};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::error::Error as StdError;
use std::mem;

use crate::proto::client::{Client, PendingRequest};
use crate::proto::statement::Statement;
use crate::Error;

pub struct CopyMessage {
    pub data: Vec<u8>,
    pub done: bool,
}

pub struct CopyInReceiver {
    receiver: mpsc::Receiver<CopyMessage>,
    done: bool,
}

impl CopyInReceiver {
    pub fn new(receiver: mpsc::Receiver<CopyMessage>) -> CopyInReceiver {
        CopyInReceiver {
            receiver,
            done: false,
        }
    }
}

impl Stream for CopyInReceiver {
    type Item = Vec<u8>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Vec<u8>>, ()> {
        if self.done {
            return Ok(Async::Ready(None));
        }

        match self.receiver.poll()? {
            Async::Ready(Some(mut data)) => {
                if data.done {
                    self.done = true;
                    frontend::copy_done(&mut data.data);
                    frontend::sync(&mut data.data);
                }

                Ok(Async::Ready(Some(data.data)))
            }
            Async::Ready(None) => {
                self.done = true;
                let mut buf = vec![];
                frontend::copy_fail("", &mut buf).unwrap();
                frontend::sync(&mut buf);
                Ok(Async::Ready(Some(buf)))
            }
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

#[derive(StateMachineFuture)]
pub enum CopyIn<S>
where
    S: Stream,
    S::Item: IntoBuf,
    <S::Item as IntoBuf>::Buf: Send,
    S::Error: Into<Box<dyn StdError + Sync + Send>>,
{
    #[state_machine_future(start, transitions(ReadCopyInResponse))]
    Start {
        client: Client,
        request: PendingRequest,
        statement: Statement,
        stream: S,
        sender: mpsc::Sender<CopyMessage>,
    },
    #[state_machine_future(transitions(WriteCopyData))]
    ReadCopyInResponse {
        stream: S,
        sender: mpsc::Sender<CopyMessage>,
        receiver: mpsc::Receiver<Message>,
    },
    #[state_machine_future(transitions(WriteCopyDone))]
    WriteCopyData {
        stream: S,
        buf: Vec<u8>,
        pending_message: Option<CopyMessage>,
        sender: mpsc::Sender<CopyMessage>,
        receiver: mpsc::Receiver<Message>,
    },
    #[state_machine_future(transitions(ReadCommandComplete))]
    WriteCopyDone {
        future: sink::Send<mpsc::Sender<CopyMessage>>,
        receiver: mpsc::Receiver<Message>,
    },
    #[state_machine_future(transitions(Finished))]
    ReadCommandComplete { receiver: mpsc::Receiver<Message> },
    #[state_machine_future(ready)]
    Finished(u64),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<S> PollCopyIn<S> for CopyIn<S>
where
    S: Stream,
    S::Item: IntoBuf,
    <S::Item as IntoBuf>::Buf: Send,
    S::Error: Into<Box<dyn StdError + Sync + Send>>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<S>>) -> Poll<AfterStart<S>, Error> {
        let state = state.take();
        let receiver = state.client.send(state.request)?;

        // the statement can drop after this point, since its close will queue up after the copy
        transition!(ReadCopyInResponse {
            stream: state.stream,
            sender: state.sender,
            receiver
        })
    }

    fn poll_read_copy_in_response<'a>(
        state: &'a mut RentToOwn<'a, ReadCopyInResponse<S>>,
    ) -> Poll<AfterReadCopyInResponse<S>, Error> {
        loop {
            let message = try_ready_receive!(state.receiver.poll());

            match message {
                Some(Message::BindComplete) => {}
                Some(Message::CopyInResponse(_)) => {
                    let state = state.take();
                    transition!(WriteCopyData {
                        stream: state.stream,
                        buf: vec![],
                        pending_message: None,
                        sender: state.sender,
                        receiver: state.receiver
                    })
                }
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(_) => return Err(Error::unexpected_message()),
                None => return Err(Error::closed()),
            }
        }
    }

    fn poll_write_copy_data<'a>(
        state: &'a mut RentToOwn<'a, WriteCopyData<S>>,
    ) -> Poll<AfterWriteCopyData, Error> {
        if let Some(message) = state.pending_message.take() {
            match state
                .sender
                .start_send(message)
                .map_err(|_| Error::closed())?
            {
                AsyncSink::Ready => {}
                AsyncSink::NotReady(message) => {
                    state.pending_message = Some(message);
                    return Ok(Async::NotReady);
                }
            }
        }

        loop {
            let done = loop {
                match try_ready!(state.stream.poll().map_err(Error::copy_in_stream)) {
                    Some(data) => {
                        // FIXME avoid collect
                        frontend::copy_data(&data.into_buf().collect::<Vec<_>>(), &mut state.buf)
                            .map_err(Error::encode)?;
                        if state.buf.len() > 4096 {
                            break false;
                        }
                    }
                    None => break true,
                }
            };

            let message = CopyMessage {
                data: mem::replace(&mut state.buf, vec![]),
                done,
            };

            if done {
                let state = state.take();
                transition!(WriteCopyDone {
                    future: state.sender.send(message),
                    receiver: state.receiver,
                });
            }

            match state
                .sender
                .start_send(message)
                .map_err(|_| Error::closed())?
            {
                AsyncSink::Ready => {}
                AsyncSink::NotReady(message) => {
                    state.pending_message = Some(message);
                    return Ok(Async::NotReady);
                }
            }
        }
    }

    fn poll_write_copy_done<'a>(
        state: &'a mut RentToOwn<'a, WriteCopyDone>,
    ) -> Poll<AfterWriteCopyDone, Error> {
        try_ready!(state.future.poll().map_err(|_| Error::closed()));
        let state = state.take();

        transition!(ReadCommandComplete {
            receiver: state.receiver
        })
    }

    fn poll_read_command_complete<'a>(
        state: &'a mut RentToOwn<'a, ReadCommandComplete>,
    ) -> Poll<AfterReadCommandComplete, Error> {
        let message = try_ready_receive!(state.receiver.poll());

        match message {
            Some(Message::CommandComplete(body)) => {
                let rows = body
                    .tag()
                    .map_err(Error::parse)?
                    .rsplit(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap_or(0);
                transition!(Finished(rows))
            }
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }
}

impl<S> CopyInFuture<S>
where
    S: Stream,
    S::Item: IntoBuf,
    <S::Item as IntoBuf>::Buf: Send,
    S::Error: Into<Box<dyn StdError + Sync + Send>>,
{
    pub fn new(
        client: Client,
        request: PendingRequest,
        statement: Statement,
        stream: S,
        sender: mpsc::Sender<CopyMessage>,
    ) -> CopyInFuture<S> {
        CopyIn::start(client, request, statement, stream, sender)
    }
}
