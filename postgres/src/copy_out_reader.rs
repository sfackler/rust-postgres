use bytes::{Buf, Bytes};
use futures::{executor, Stream};
use std::io::{self, BufRead, Cursor, Read};
use std::marker::PhantomData;
use std::pin::Pin;
use tokio_postgres::Error;

/// The reader returned by the `copy_out` method.
pub struct CopyOutReader<'a, S>
where
    S: Stream,
{
    it: executor::BlockingStream<Pin<Box<S>>>,
    cur: Cursor<Bytes>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend borrow until drop
impl<'a, S> Drop for CopyOutReader<'a, S>
where
    S: Stream,
{
    fn drop(&mut self) {}
}

impl<'a, S> CopyOutReader<'a, S>
where
    S: Stream<Item = Result<Bytes, Error>>,
{
    pub(crate) fn new(stream: S) -> Result<CopyOutReader<'a, S>, Error> {
        let mut it = executor::block_on_stream(Box::pin(stream));
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

impl<'a, S> Read for CopyOutReader<'a, S>
where
    S: Stream<Item = Result<Bytes, Error>>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let b = self.fill_buf()?;
        let len = usize::min(buf.len(), b.len());
        buf[..len].copy_from_slice(&b[..len]);
        self.consume(len);
        Ok(len)
    }
}

impl<'a, S> BufRead for CopyOutReader<'a, S>
where
    S: Stream<Item = Result<Bytes, Error>>,
{
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
