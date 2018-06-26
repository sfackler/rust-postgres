use bytes::BytesMut;
use postgres_protocol::message::backend;
use std::io;
use tokio_codec::{Decoder, Encoder};

pub struct PostgresCodec;

impl Encoder for PostgresCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), io::Error> {
        dst.extend_from_slice(&item);
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
