use bytes::{Buf, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io;
use tokio_util::codec::{Decoder, Encoder};
use tracing::Span;

pub enum FrontendMessage {
    Raw(Bytes),
    CopyData(CopyData<Box<dyn Buf + Send>>),
}

pub enum BackendMessage {
    Normal {
        messages: BackendMessages,
        request_complete: bool,
    },
    Async(backend::Message),
}

#[derive(Default)]
pub struct BackendMessages(BytesMut);

impl BackendMessages {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn extend_from_slice(&mut self, buf: &[u8]) {
        self.0.extend_from_slice(buf);
    }

    pub fn extend(&mut self, rhs: Self) {
        self.0.extend_from_slice(&rhs.0);
    }
}

impl FallibleIterator for BackendMessages {
    type Item = backend::Message;
    type Error = io::Error;

    fn next(&mut self) -> io::Result<Option<backend::Message>> {
        backend::Message::parse(&mut self.0)
    }
}

pub struct PostgresCodec;

impl Encoder<(FrontendMessage, Span)> for PostgresCodec {
    type Error = io::Error;

    fn encode(&mut self, (item, span): (FrontendMessage, Span), dst: &mut BytesMut) -> io::Result<()> {
        match item {
            FrontendMessage::Raw(buf) => {
                span.in_scope(|| {
                    log::debug!("sending raw message: {}", buf.len());
                });
                dst.extend_from_slice(&buf)
            },
            FrontendMessage::CopyData(data) => {
                span.in_scope(|| {
                    log::debug!("sending copy data message: {}", data.len);
                });
                data.write(dst)
            },
        }

        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
        let mut idx = 0;
        let mut request_complete = false;

        while let Some(header) = backend::Header::parse(&src[idx..])? {
            let len = header.len() as usize + 1;
            if src[idx..].len() < len {
                break;
            }

            match header.tag() {
                backend::NOTICE_RESPONSE_TAG
                | backend::NOTIFICATION_RESPONSE_TAG
                | backend::PARAMETER_STATUS_TAG => {
                    if idx == 0 {
                        let message = backend::Message::parse(src)?.unwrap();
                        return Ok(Some(BackendMessage::Async(message)));
                    } else {
                        break;
                    }
                }
                _ => {}
            }

            idx += len;

            if header.tag() == backend::READY_FOR_QUERY_TAG {
                request_complete = true;
                break;
            }
        }

        if idx == 0 {
            Ok(None)
        } else {
            Ok(Some(BackendMessage::Normal {
                messages: BackendMessages(src.split_to(idx)),
                request_complete,
            }))
        }
    }
}
