use fallible_iterator::FallibleIterator;
use futures::executor::{self, BlockingStream};
use futures::Stream;
use std::marker::PhantomData;
use std::pin::Pin;

pub struct Iter<'a, S>
where
    S: Stream,
{
    it: BlockingStream<Pin<Box<S>>>,
    _p: PhantomData<&'a mut ()>,
}

// no-op impl to extend the borrow until drop
impl<'a, S> Drop for Iter<'a, S>
where
    S: Stream,
{
    fn drop(&mut self) {}
}

impl<'a, S> Iter<'a, S>
where
    S: Stream,
{
    pub fn new(stream: S) -> Iter<'a, S> {
        Iter {
            it: executor::block_on_stream(Box::pin(stream)),
            _p: PhantomData,
        }
    }
}

impl<'a, S, T, E> FallibleIterator for Iter<'a, S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = T;
    type Error = E;

    fn next(&mut self) -> Result<Option<T>, E> {
        self.it.next().transpose()
    }
}
