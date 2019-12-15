use crate::Rt;
use bytes::{Buf, Bytes};
use futures::StreamExt;
use std::io::{self, BufRead, Cursor, Read};
use std::pin::Pin;
use tokio_postgres::{CopyOutStream, Error};

/// The reader returned by the `copy_out` method.
pub struct CopyOutReader<'a> {
    runtime: Rt<'a>,
    stream: Pin<Box<CopyOutStream>>,
    cur: Cursor<Bytes>,
}

impl<'a> CopyOutReader<'a> {
    pub(crate) fn new(
        mut runtime: Rt<'a>,
        stream: CopyOutStream,
    ) -> Result<CopyOutReader<'a>, Error> {
        let mut stream = Box::pin(stream);
        let cur = match runtime.block_on(stream.next()) {
            Some(Ok(cur)) => cur,
            Some(Err(e)) => return Err(e),
            None => Bytes::new(),
        };

        Ok(CopyOutReader {
            runtime,
            stream,
            cur: Cursor::new(cur),
        })
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
        if self.cur.remaining() == 0 {
            match self.runtime.block_on(self.stream.next()) {
                Some(Ok(cur)) => self.cur = Cursor::new(cur),
                Some(Err(e)) => return Err(io::Error::new(io::ErrorKind::Other, e)),
                None => {}
            };
        }

        Ok(Buf::bytes(&self.cur))
    }

    fn consume(&mut self, amt: usize) {
        self.cur.advance(amt);
    }
}
