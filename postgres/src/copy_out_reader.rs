use bytes::{Buf, Bytes};
use futures::stream::{self, Stream};
use std::io::{self, BufRead, Cursor, Read};
use std::marker::PhantomData;
use tokio_postgres::impls;
use tokio_postgres::Error;

/// The reader returned by the `copy_out` method.
pub struct CopyOutReader<'a> {
    it: stream::Wait<impls::CopyOut>,
    cur: Cursor<Bytes>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend borrow until drop
impl<'a> Drop for CopyOutReader<'a> {
    fn drop(&mut self) {}
}

impl<'a> CopyOutReader<'a> {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(stream: impls::CopyOut) -> Result<CopyOutReader<'a>, Error> {
        let mut it = stream.wait();
        let cur = match it.next() {
            Some(Ok(cur)) => cur,
            Some(Err(e)) => return Err(e),
            None => Bytes::new(),
        };

        Ok(CopyOutReader {
            it,
            cur: Cursor::new(cur),
            _p: PhantomData,
        })
    }
}

impl<'a> Read for CopyOutReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let b = self.fill_buf()?;
        let len = usize::min(buf.len(), b.len());
        buf[..len].copy_from_slice(&b[..len]);
        self.consume(len);
        Ok(len)
    }
}

impl<'a> BufRead for CopyOutReader<'a> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.cur.remaining() == 0 {
            match self.it.next() {
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
