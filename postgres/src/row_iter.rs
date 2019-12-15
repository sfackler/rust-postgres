use crate::Rt;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use std::pin::Pin;
use tokio_postgres::{Error, Row, RowStream};

/// The iterator returned by `query_raw`.
pub struct RowIter<'a> {
    runtime: Rt<'a>,
    it: Pin<Box<RowStream>>,
}

// no-op impl to extend the borrow until drop
impl Drop for RowIter<'_> {
    fn drop(&mut self) {}
}

impl<'a> RowIter<'a> {
    pub(crate) fn new(runtime: Rt<'a>, stream: RowStream) -> RowIter<'a> {
        RowIter {
            runtime,
            it: Box::pin(stream),
        }
    }
}

impl FallibleIterator for RowIter<'_> {
    type Item = Row;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Row>, Error> {
        self.runtime.block_on(self.it.next()).transpose()
    }
}
