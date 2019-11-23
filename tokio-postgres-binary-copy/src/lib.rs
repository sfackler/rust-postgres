use bytes::{BigEndian, BufMut, ByteOrder, Bytes, BytesMut, Buf};
use futures::{future, ready, Stream};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use std::convert::TryFrom;
use std::error::Error;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_postgres::types::{IsNull, ToSql, Type, FromSql, WrongType};
use tokio_postgres::CopyStream;
use std::io::Cursor;

#[cfg(test)]
mod test;

const BLOCK_SIZE: usize = 4096;
const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
const HEADER_LEN: usize = MAGIC.len() + 4 + 4;

pin_project! {
    pub struct BinaryCopyInStream<F> {
        #[pin]
        future: F,
        buf: Arc<Mutex<BytesMut>>,
        done: bool,
    }
}

impl<F> BinaryCopyInStream<F>
where
    F: Future<Output = Result<(), Box<dyn Error + Sync + Send>>>,
{
    pub fn new<M>(types: &[Type], write_values: M) -> BinaryCopyInStream<F>
    where
        M: FnOnce(BinaryCopyInWriter) -> F,
    {
        let mut buf = BytesMut::new();
        buf.reserve(HEADER_LEN);
        buf.put_slice(MAGIC); // magic
        buf.put_i32_be(0); // flags
        buf.put_i32_be(0); // header extension

        let buf = Arc::new(Mutex::new(buf));
        let writer = BinaryCopyInWriter {
            buf: buf.clone(),
            types: types.to_vec(),
        };

        BinaryCopyInStream {
            future: write_values(writer),
            buf,
            done: false,
        }
    }
}

impl<F> Stream for BinaryCopyInStream<F>
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
pub struct BinaryCopyInWriter {
    buf: Arc<Mutex<BytesMut>>,
    types: Vec<Type>,
}

impl BinaryCopyInWriter {
    pub async fn write(
        &mut self,
        values: &[&(dyn ToSql + Send)],
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        self.write_raw(values.iter().cloned()).await
    }

    pub async fn write_raw<'a, I>(&mut self, values: I) -> Result<(), Box<dyn Error + Sync + Send>>
    where
        I: IntoIterator<Item = &'a (dyn ToSql + Send)>,
        I::IntoIter: ExactSizeIterator,
    {
        let values = values.into_iter();
        assert!(
            values.len() == self.types.len(),
            "expected {} values but got {}",
            self.types.len(),
            values.len(),
        );

        future::poll_fn(|_| {
            if self.buf.lock().len() > BLOCK_SIZE {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;

        let mut buf = self.buf.lock();

        buf.reserve(2);
        buf.put_u16_be(self.types.len() as u16);

        for (value, type_) in values.zip(&self.types) {
            let idx = buf.len();
            buf.reserve(4);
            buf.put_i32_be(0);
            let len = match value.to_sql_checked(type_, &mut buf)? {
                IsNull::Yes => -1,
                IsNull::No => i32::try_from(buf.len() - idx - 4)?,
            };
            BigEndian::write_i32(&mut buf[idx..], len);
        }

        Ok(())
    }
}

struct Header {
    has_oids: bool,
}

pin_project! {
    pub struct BinaryCopyOutStream {
        #[pin]
        stream: CopyStream,
        types: Arc<Vec<Type>>,
        header: Option<Header>,
    }
}

impl BinaryCopyOutStream {
    pub fn new(types: &[Type], stream: CopyStream) -> BinaryCopyOutStream {
        BinaryCopyOutStream {
            stream,
            types: Arc::new(types.to_vec()),
            header: None,
        }
    }
}

impl Stream for BinaryCopyOutStream {
    type Item = Result<BinaryCopyOutRow, Box<dyn Error + Sync + Send>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let chunk = match ready!(this.stream.poll_next(cx)) {
            Some(Ok(chunk)) => chunk,
            Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
            None => return Poll::Ready(Some(Err("unexpected EOF".into()))),
        };
        let mut chunk= Cursor::new(chunk);

        let has_oids = match &this.header {
            Some(header) => header.has_oids,
            None => {
                check_remaining(&chunk, HEADER_LEN)?;
                if &chunk.bytes()[..MAGIC.len()] != MAGIC {
                    return Poll::Ready(Some(Err("invalid magic value".into())));
                }
                chunk.advance(MAGIC.len());

                let flags = chunk.get_i32_be();
                let has_oids = (flags & (1 << 16)) != 0;

                let header_extension = chunk.get_u32_be() as usize;
                check_remaining(&chunk, header_extension)?;
                chunk.advance(header_extension);

                *this.header = Some(Header { has_oids });
                has_oids
            }
        };

        check_remaining(&chunk, 2)?;
        let mut len = chunk.get_i16_be();
        if len == -1 {
            return Poll::Ready(None);
        }

        if has_oids {
            len += 1;
        }
        if len as usize != this.types.len() {
            return Poll::Ready(Some(Err("unexpected tuple size".into())));
        }

        let mut ranges = vec![];
        for _ in 0..len {
            check_remaining(&chunk, 4)?;
            let len = chunk.get_i32_be();
            if len == -1 {
                ranges.push(None);
            } else {
                let len = len as usize;
                check_remaining(&chunk, len)?;
                let start = chunk.position() as usize;
                ranges.push(Some(start..start + len));
                chunk.advance(len);
            }
        }

        Poll::Ready(Some(Ok(BinaryCopyOutRow {
            buf: chunk.into_inner(),
            ranges,
            types: this.types.clone(),
        })))
    }
}

fn check_remaining(buf: &impl Buf, len: usize) -> Result<(), Box<dyn Error + Sync + Send>> {
    if buf.remaining() < len {
        Err("unexpected EOF".into())
    } else {
        Ok(())
    }
}

pub struct BinaryCopyOutRow {
    buf: Bytes,
    ranges: Vec<Option<Range<usize>>>,
    types: Arc<Vec<Type>>,
}

impl BinaryCopyOutRow {
    pub fn try_get<'a, T>(&'a self, idx: usize) -> Result<T, Box<dyn Error + Sync + Send>> where T: FromSql<'a> {
        let type_ = &self.types[idx];
        if !T::accepts(type_) {
            return Err(WrongType::new::<T>(type_.clone()).into());
        }

        match &self.ranges[idx] {
            Some(range) => T::from_sql(type_, &self.buf[range.clone()]).map_err(Into::into),
            None => T::from_sql_null(type_).map_err(Into::into)
        }
    }

    pub fn get<'a, T>(&'a self, idx: usize) -> T where T: FromSql<'a> {
        match self.try_get(idx) {
            Ok(value) => value,
            Err(e) => panic!("error retrieving column {}: {}", idx, e),
        }
    }
}
