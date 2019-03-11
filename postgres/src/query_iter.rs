use fallible_iterator::FallibleIterator;
use futures::stream::{self, Stream};
use std::marker::PhantomData;
use tokio_postgres::impls;
use tokio_postgres::{Error, Row};

pub struct QueryIter<'a> {
    it: stream::Wait<impls::Query>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend the borrow until drop
impl<'a> Drop for QueryIter<'a> {
    fn drop(&mut self) {}
}

impl<'a> QueryIter<'a> {
    pub(crate) fn new(stream: impls::Query) -> QueryIter<'a> {
        QueryIter {
            it: stream.wait(),
            _p: PhantomData,
        }
    }
}

impl<'a> FallibleIterator for QueryIter<'a> {
    type Item = Row;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Row>, Error> {
        match self.it.next() {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}
