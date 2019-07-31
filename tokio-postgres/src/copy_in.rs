use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::Error;
use bytes::{Buf, BufMut, BytesMut, IntoBuf};
use futures::channel::mpsc;
use futures::ready;
use futures::{SinkExt, Stream, StreamExt, TryStream, TryStreamExt};
use pin_utils::pin_mut;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use postgres_protocol::message::frontend::CopyData;

enum CopyInMessage {
    Message(FrontendMessage),
    Done,
}

pub struct CopyInReceiver {
    receiver: mpsc::Receiver<CopyInMessage>,
    done: bool,
}

impl CopyInReceiver {
    fn new(receiver: mpsc::Receiver<CopyInMessage>) -> CopyInReceiver {
        CopyInReceiver {
            receiver,
            done: false,
        }
    }
}

impl Stream for CopyInReceiver {
    type Item = FrontendMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<FrontendMessage>> {
        if self.done {
            return Poll::Ready(None);
        }

        match ready!(self.receiver.poll_next_unpin(cx)) {
            Some(CopyInMessage::Message(message)) => Poll::Ready(Some(message)),
            Some(CopyInMessage::Done) => {
                self.done = true;
                let mut buf = vec![];
                frontend::copy_done(&mut buf);
                frontend::sync(&mut buf);
                Poll::Ready(Some(FrontendMessage::Raw(buf)))
            }
            None => {
                self.done = true;
                let mut buf = vec![];
                frontend::copy_fail("", &mut buf).unwrap();
                frontend::sync(&mut buf);
                Poll::Ready(Some(FrontendMessage::Raw(buf)))
            }
        }
    }
}

pub async fn copy_in<S>(
    client: Arc<InnerClient>,
    buf: Result<Vec<u8>, Error>,
    stream: S,
) -> Result<u64, Error>
where
    S: TryStream,
    S::Ok: IntoBuf,
    <S::Ok as IntoBuf>::Buf: 'static + Send,
    S::Error: Into<Box<dyn error::Error + Sync + Send>>,
{
    let buf = buf?;

    let (mut sender, receiver) = mpsc::channel(1);
    let receiver = CopyInReceiver::new(receiver);
    let mut responses = client.send(RequestMessages::CopyIn(receiver))?;

    sender
        .send(CopyInMessage::Message(FrontendMessage::Raw(buf)))
        .await
        .map_err(|_| Error::closed())?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    match responses.next().await? {
        Message::CopyInResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    let mut bytes = BytesMut::new();
    let stream = stream.into_stream();
    pin_mut!(stream);

    while let Some(buf) = stream.try_next().await.map_err(Error::copy_in_stream)? {
        let buf = buf.into_buf();

        let data: Box<dyn Buf + Send> = if buf.remaining() > 4096 {
            if bytes.is_empty() {
                Box::new(buf)
            } else {
                Box::new(bytes.take().freeze().into_buf().chain(buf))
            }
        } else {
            bytes.reserve(buf.remaining());
            bytes.put(buf);
            if bytes.len() > 4096 {
                Box::new(bytes.take().freeze().into_buf())
            } else {
                continue;
            }
        };

        let data = CopyData::new(data).map_err(Error::encode)?;
        sender
            .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
            .await
            .map_err(|_| Error::closed())?;
    }

    if !bytes.is_empty() {
        let data: Box<dyn Buf + Send> = Box::new(bytes.freeze().into_buf());
        let data = CopyData::new(data).map_err(Error::encode)?;
        sender
            .send(CopyInMessage::Message(FrontendMessage::CopyData(data)))
            .await
            .map_err(|_| Error::closed())?;
    }

    sender
        .send(CopyInMessage::Done)
        .await
        .map_err(|_| Error::closed())?;

    match responses.next().await? {
        Message::CommandComplete(body) => {
            let rows = body
                .tag()
                .map_err(Error::parse)?
                .rsplit(' ')
                .next()
                .unwrap()
                .parse()
                .unwrap_or(0);
            Ok(rows)
        }
        _ => Err(Error::unexpected_message()),
    }
}
