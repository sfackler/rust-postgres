use bytes::{Buf, BytesMut};
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io;
use tokio_codec::{Decoder, Encoder};

pub enum FrontendMessage {
    Raw(Vec<u8>),
    CopyData(CopyData<Box<dyn Buf + Send>>),
}

pub struct PostgresCodec;

impl Encoder for PostgresCodec {
    type Item = FrontendMessage;
    type Error = io::Error;

    fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> Result<(), io::Error> {
        match item {
            FrontendMessage::Raw(buf) => dst.extend_from_slice(&buf),
            FrontendMessage::CopyData(data) => data.write(dst),
        }

        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = backend::Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<backend::Message>, io::Error> {
        backend::Message::parse(src)
    }
}
