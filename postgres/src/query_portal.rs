use fallible_iterator::FallibleIterator;
use futures::stream::{self, Stream};
use std::marker::PhantomData;
use tokio_postgres::impls;
use tokio_postgres::{Error, Row};

pub struct QueryPortal<'a> {
    it: stream::Wait<impls::QueryPortal>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend the borrow until drop
impl<'a> Drop for QueryPortal<'a> {
    fn drop(&mut self) {}
}

impl<'a> QueryPortal<'a> {
    pub(crate) fn new(stream: impls::QueryPortal) -> QueryPortal<'a> {
        QueryPortal {
            it: stream.wait(),
            _p: PhantomData,
        }
    }
}

impl<'a> FallibleIterator for QueryPortal<'a> {
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
