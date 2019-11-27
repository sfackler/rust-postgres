use futures::Stream;
use std::io::{self, Cursor, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct CopyInStream<R>(pub R);

impl<R> Stream for CopyInStream<R>
where
    R: Read + Unpin,
{
    type Item = io::Result<Cursor<Vec<u8>>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Cursor<Vec<u8>>>>> {
        let mut buf = vec![];
        match self.0.by_ref().take(4096).read_to_end(&mut buf)? {
            0 => Poll::Ready(None),
            _ => Poll::Ready(Some(Ok(Cursor::new(buf)))),
        }
    }
}
