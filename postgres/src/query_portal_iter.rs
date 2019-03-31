use fallible_iterator::FallibleIterator;
use futures::stream::{self, Stream};
use std::marker::PhantomData;
use tokio_postgres::impls;
use tokio_postgres::{Error, Row};

/// The iterator returned by the `query_portal_iter` method.
pub struct QueryPortalIter<'a> {
    it: stream::Wait<impls::QueryPortal>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend the borrow until drop
impl<'a> Drop for QueryPortalIter<'a> {
    fn drop(&mut self) {}
}

impl<'a> QueryPortalIter<'a> {
    pub(crate) fn new(stream: impls::QueryPortal) -> QueryPortalIter<'a> {
        QueryPortalIter {
            it: stream.wait(),
            _p: PhantomData,
        }
    }
}

impl<'a> FallibleIterator for QueryPortalIter<'a> {
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
