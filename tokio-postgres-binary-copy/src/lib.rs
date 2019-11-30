use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{ready, SinkExt, Stream};
use pin_project_lite::pin_project;
use std::convert::TryFrom;
use std::error::Error;
use std::io::Cursor;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type, WrongType};
use tokio_postgres::{CopyInSink, CopyOutStream};

#[cfg(test)]
mod test;

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
const HEADER_LEN: usize = MAGIC.len() + 4 + 4;

pin_project! {
    pub struct BinaryCopyInWriter {
        #[pin]
        sink: CopyInSink<Bytes>,
        types: Vec<Type>,
        buf: BytesMut,
    }
}

impl BinaryCopyInWriter {
    pub fn new(sink: CopyInSink<Bytes>, types: &[Type]) -> BinaryCopyInWriter {
        let mut buf = BytesMut::new();
        buf.reserve(HEADER_LEN);
        buf.put_slice(MAGIC); // magic
        buf.put_i32(0); // flags
        buf.put_i32(0); // header extension

        BinaryCopyInWriter {
            sink,
            types: types.to_vec(),
            buf,
        }
    }

    pub async fn write(
        self: Pin<&mut Self>,
        values: &[&(dyn ToSql + Send)],
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        self.write_raw(values.iter().cloned()).await
    }

    pub async fn write_raw<'a, I>(
        self: Pin<&mut Self>,
        values: I,
    ) -> Result<(), Box<dyn Error + Sync + Send>>
    where
        I: IntoIterator<Item = &'a (dyn ToSql + Send)>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut this = self.project();

        let values = values.into_iter();
        assert!(
            values.len() == this.types.len(),
            "expected {} values but got {}",
            this.types.len(),
            values.len(),
        );

        this.buf.put_i16(this.types.len() as i16);

        for (value, type_) in values.zip(this.types) {
            let idx = this.buf.len();
            this.buf.put_i32(0);
            let len = match value.to_sql_checked(type_, this.buf)? {
                IsNull::Yes => -1,
                IsNull::No => i32::try_from(this.buf.len() - idx - 4)?,
            };
            BigEndian::write_i32(&mut this.buf[idx..], len);
        }

        if this.buf.len() > 4096 {
            this.sink.send(this.buf.split().freeze()).await?;
        }

        Ok(())
    }

    pub async fn finish(self: Pin<&mut Self>) -> Result<u64, tokio_postgres::Error> {
        let mut this = self.project();

        this.buf.put_i16(-1);
        this.sink.send(this.buf.split().freeze()).await?;
        this.sink.finish().await
    }
}

struct Header {
    has_oids: bool,
}

pin_project! {
    pub struct BinaryCopyOutStream {
        #[pin]
        stream: CopyOutStream,
        types: Arc<Vec<Type>>,
        header: Option<Header>,
    }
}

impl BinaryCopyOutStream {
    pub fn new(types: &[Type], stream: CopyOutStream) -> BinaryCopyOutStream {
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
        let mut chunk = Cursor::new(chunk);

        let has_oids = match &this.header {
            Some(header) => header.has_oids,
            None => {
                check_remaining(&chunk, HEADER_LEN)?;
                if &chunk.bytes()[..MAGIC.len()] != MAGIC {
                    return Poll::Ready(Some(Err("invalid magic value".into())));
                }
                chunk.advance(MAGIC.len());

                let flags = chunk.get_i32();
                let has_oids = (flags & (1 << 16)) != 0;

                let header_extension = chunk.get_u32() as usize;
                check_remaining(&chunk, header_extension)?;
                chunk.advance(header_extension);

                *this.header = Some(Header { has_oids });
                has_oids
            }
        };

        check_remaining(&chunk, 2)?;
        let mut len = chunk.get_i16();
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
            let len = chunk.get_i32();
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
    pub fn try_get<'a, T>(&'a self, idx: usize) -> Result<T, Box<dyn Error + Sync + Send>>
    where
        T: FromSql<'a>,
    {
        let type_ = &self.types[idx];
        if !T::accepts(type_) {
            return Err(WrongType::new::<T>(type_.clone()).into());
        }

        match &self.ranges[idx] {
            Some(range) => T::from_sql(type_, &self.buf[range.clone()]).map_err(Into::into),
            None => T::from_sql_null(type_).map_err(Into::into),
        }
    }

    pub fn get<'a, T>(&'a self, idx: usize) -> T
    where
        T: FromSql<'a>,
    {
        match self.try_get(idx) {
            Ok(value) => value,
            Err(e) => panic!("error retrieving column {}: {}", idx, e),
        }
    }
}
