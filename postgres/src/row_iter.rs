use fallible_iterator::FallibleIterator;
use std::pin::Pin;
use tokio::runtime::Runtime;
use tokio_postgres::{Error, Row, RowStream};
use futures::StreamExt;

/// The iterator returned by `query_raw`.
pub struct RowIter<'a> {
    runtime: &'a mut Runtime,
    it: Pin<Box<RowStream>>,
}

// no-op impl to extend the borrow until drop
impl Drop for RowIter<'_> {
    fn drop(&mut self) {}
}

impl<'a> RowIter<'a> {
    pub(crate) fn new(runtime: &'a mut Runtime, stream: RowStream) -> RowIter<'a> {
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
