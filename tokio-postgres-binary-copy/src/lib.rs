use bytes::{BigEndian, BufMut, ByteOrder, Bytes, BytesMut};
use futures::{future, Stream};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use std::convert::TryFrom;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_postgres::types::{IsNull, ToSql, Type};

#[cfg(test)]
mod test;

const BLOCK_SIZE: usize = 4096;

pin_project! {
    pub struct BinaryCopyStream<F> {
        #[pin]
        future: F,
        buf: Arc<Mutex<BytesMut>>,
        done: bool,
    }
}

impl<F> BinaryCopyStream<F>
where
    F: Future<Output = Result<(), Box<dyn Error + Sync + Send>>>,
{
    pub fn new<M>(types: &[Type], write_values: M) -> BinaryCopyStream<F>
    where
        M: FnOnce(BinaryCopyWriter) -> F,
    {
        let mut buf = BytesMut::new();
        buf.reserve(11 + 4 + 4);
        buf.put_slice(b"PGCOPY\n\xff\r\n\0"); // magic
        buf.put_i32_be(0); // flags
        buf.put_i32_be(0); // header extension

        let buf = Arc::new(Mutex::new(buf));
        let writer = BinaryCopyWriter {
            buf: buf.clone(),
            types: types.to_vec(),
            idx: 0,
        };

        BinaryCopyStream {
            future: write_values(writer),
            buf,
            done: false,
        }
    }
}

impl<F> Stream for BinaryCopyStream<F>
where
    F: Future<Output = Result<(), Box<dyn Error + Sync + Send>>>,
{
    type Item = Result<Bytes, Box<dyn Error + Sync + Send>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        *this.done = this.future.poll(cx)?.is_ready();

        let mut buf = this.buf.lock();
        if *this.done {
            buf.reserve(2);
            buf.put_i16_be(-1);
            Poll::Ready(Some(Ok(buf.take().freeze())))
        } else if buf.len() > BLOCK_SIZE {
            Poll::Ready(Some(Ok(buf.take().freeze())))
        } else {
            Poll::Pending
        }
    }
}

// FIXME this should really just take a reference to the buffer, but that requires HKT :(
pub struct BinaryCopyWriter {
    buf: Arc<Mutex<BytesMut>>,
    types: Vec<Type>,
    idx: usize,
}

impl BinaryCopyWriter {
    pub async fn write(
        &mut self,
        value: &(dyn ToSql + Send),
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        future::poll_fn(|_| {
            if self.buf.lock().len() > BLOCK_SIZE {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;

        let mut buf = self.buf.lock();
        if self.idx == 0 {
            buf.reserve(2);
            buf.put_i16_be(self.types.len() as i16);
        }
        let idx = buf.len();
        buf.reserve(4);
        buf.put_i32_be(0);
        let len = match value.to_sql_checked(&self.types[self.idx], &mut buf)? {
            IsNull::Yes => -1,
            IsNull::No => i32::try_from(buf.len() - idx - 4)?,
        };
        BigEndian::write_i32(&mut buf[idx..], len);

        self.idx = (self.idx + 1) % self.types.len();

        Ok(())
    }
}
