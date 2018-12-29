use fallible_iterator::FallibleIterator;
use futures::stream::{self, Stream};
use std::marker::PhantomData;
use tokio_postgres::{Error, Row};

pub struct Query<'a> {
    it: stream::Wait<tokio_postgres::Query>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend the borrow until drop
impl<'a> Drop for Query<'a> {
    fn drop(&mut self) {}
}

impl<'a> Query<'a> {
    pub(crate) fn new(stream: tokio_postgres::Query) -> Query<'a> {
        Query {
            it: stream.wait(),
            _p: PhantomData,
        }
    }
}

impl<'a> FallibleIterator for Query<'a> {
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
