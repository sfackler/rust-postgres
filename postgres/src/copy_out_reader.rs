use crate::lazy_pin::LazyPin;
use crate::Rt;
use bytes::{Buf, Bytes};
use futures::StreamExt;
use std::io::{self, BufRead, Read};
use tokio_postgres::CopyOutStream;

/// The reader returned by the `copy_out` method.
pub struct CopyOutReader<'a> {
    pub(crate) runtime: Rt<'a>,
    pub(crate) stream: LazyPin<CopyOutStream>,
    cur: Bytes,
}

impl<'a> CopyOutReader<'a> {
    pub(crate) fn new(runtime: Rt<'a>, stream: CopyOutStream) -> CopyOutReader<'a> {
        CopyOutReader {
            runtime,
            stream: LazyPin::new(stream),
            cur: Bytes::new(),
        }
    }
}

impl Read for CopyOutReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let b = self.fill_buf()?;
        let len = usize::min(buf.len(), b.len());
        buf[..len].copy_from_slice(&b[..len]);
        self.consume(len);
        Ok(len)
    }
}

impl BufRead for CopyOutReader<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if !self.cur.has_remaining() {
            match self.runtime.block_on(self.stream.pinned().next()) {
                Some(Ok(cur)) => self.cur = cur,
                Some(Err(e)) => return Err(io::Error::new(io::ErrorKind::Other, e)),
                None => {}
            };
        }

        Ok(self.cur.bytes())
    }

    fn consume(&mut self, amt: usize) {
        self.cur.advance(amt);
    }
}
