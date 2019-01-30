use fallible_iterator::FallibleIterator;
use futures::stream::{self, Stream};
use std::marker::PhantomData;
use tokio_postgres::impls;
use tokio_postgres::{Error, Row};

pub struct Query<'a> {
    it: stream::Wait<impls::Query>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend the borrow until drop
impl<'a> Drop for Query<'a> {
    fn drop(&mut self) {}
}

impl<'a> Query<'a> {
    pub(crate) fn new(stream: impls::Query) -> Query<'a> {
        Query {
            it: stream.wait(),
            _p: PhantomData,
        }
    }

    /// A convenience API which collects the resulting rows into a `Vec` and returns them.
    pub fn into_vec(self) -> Result<Vec<Row>, Error> {
        self.collect()
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
