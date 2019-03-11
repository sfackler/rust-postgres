use fallible_iterator::FallibleIterator;
use futures::stream::{self, Stream};
use std::marker::PhantomData;
use tokio_postgres::impls;
use tokio_postgres::{Error, SimpleQueryMessage};

pub struct SimpleQueryIter<'a> {
    it: stream::Wait<impls::SimpleQuery>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend borrow until drop
impl<'a> Drop for SimpleQueryIter<'a> {
    fn drop(&mut self) {}
}

impl<'a> SimpleQueryIter<'a> {
    pub(crate) fn new(stream: impls::SimpleQuery) -> SimpleQueryIter<'a> {
        SimpleQueryIter {
            it: stream.wait(),
            _p: PhantomData,
        }
    }
}

impl<'a> FallibleIterator for SimpleQueryIter<'a> {
    type Item = SimpleQueryMessage;
    type Error = Error;

    fn next(&mut self) -> Result<Option<SimpleQueryMessage>, Error> {
        match self.it.next() {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}
