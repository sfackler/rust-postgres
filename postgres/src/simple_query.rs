use fallible_iterator::FallibleIterator;
use futures::stream::{self, Stream};
use std::marker::PhantomData;
use tokio_postgres::impls;
use tokio_postgres::{Error, SimpleQueryMessage};

pub struct SimpleQuery<'a> {
    it: stream::Wait<impls::SimpleQuery>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend borrow until drop
impl<'a> Drop for SimpleQuery<'a> {
    fn drop(&mut self) {}
}

impl<'a> SimpleQuery<'a> {
    pub(crate) fn new(stream: impls::SimpleQuery) -> SimpleQuery<'a> {
        SimpleQuery {
            it: stream.wait(),
            _p: PhantomData,
        }
    }

    /// A convenience API which collects the resulting messages into a `Vec` and returns them.
    pub fn into_vec(self) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.collect()
    }
}

impl<'a> FallibleIterator for SimpleQuery<'a> {
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
