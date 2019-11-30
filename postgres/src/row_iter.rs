use fallible_iterator::FallibleIterator;
use futures::executor::{self, BlockingStream};
use std::marker::PhantomData;
use std::pin::Pin;
use tokio_postgres::{Error, Row, RowStream};

/// The iterator returned by `query_raw`.
pub struct RowIter<'a> {
    it: BlockingStream<Pin<Box<RowStream>>>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend the borrow until drop
impl Drop for RowIter<'_> {
    fn drop(&mut self) {}
}

impl<'a> RowIter<'a> {
    pub(crate) fn new(stream: RowStream) -> RowIter<'a> {
        RowIter {
            it: executor::block_on_stream(Box::pin(stream)),
            _p: PhantomData,
        }
    }
}

impl FallibleIterator for RowIter<'_> {
    type Item = Row;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Row>, Error> {
        self.it.next().transpose()
    }
}
