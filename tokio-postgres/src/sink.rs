use futures::{Sink, Future, Poll, AsyncSink, Async, Stream};
use futures::stream::Fuse;

pub trait SinkExt: Sink {
    fn send2(self, item: Self::SinkItem) -> Send2<Self>
    where
        Self: Sized;

    // unlike send_all, this doesn't close the stream.
    fn send_all2<S>(self, stream: S) -> SendAll2<Self, S>
    where
        S: Stream<Item = Self::SinkItem>,
        Self::SinkError: From<S::Error>,
        Self: Sized;
}

impl<T> SinkExt for T
where
    T: Sink,
{
    fn send2(self, item: Self::SinkItem) -> Send2<Self>
    where
        Self: Sized,
    {
        Send2 {
            sink: Some(self),
            item: Some(item),
        }
    }

    fn send_all2<S>(self, stream: S) -> SendAll2<Self, S>
    where
        S: Stream<Item = Self::SinkItem>,
        Self::SinkError: From<S::Error>,
        Self: Sized,
    {
        SendAll2 {
            sink: Some(self),
            stream: Some(stream.fuse()),
            buffered: None,
        }
    }
}

pub struct Send2<T>
where
    T: Sink,
{
    sink: Option<T>,
    item: Option<T::SinkItem>,
}

impl<T> Future for Send2<T>
where
    T: Sink,
{
    type Item = T;
    type Error = (T::SinkError, T);

    fn poll(&mut self) -> Poll<T, (T::SinkError, T)> {
        let mut sink = self.sink.take().expect("poll called after completion");

        if let Some(item) = self.item.take() {
            match sink.start_send(item) {
                Ok(AsyncSink::NotReady(item)) => {
                    self.sink = Some(sink);
                    self.item = Some(item);
                    return Ok(Async::NotReady);
                }
                Ok(AsyncSink::Ready) => {}
                Err(e) => return Err((e, sink)),
            }
        }

        match sink.poll_complete() {
            Ok(Async::Ready(())) => {}
            Ok(Async::NotReady) => {
                self.sink = Some(sink);
                return Ok(Async::NotReady);
            }
            Err(e) => return Err((e, sink)),
        }

        Ok(Async::Ready(sink))
    }
}

pub struct SendAll2<T, U>
where
    U: Stream,
{
    sink: Option<T>,
    stream: Option<Fuse<U>>,
    buffered: Option<U::Item>,
}

impl<T, U> Future for SendAll2<T, U>
where
    T: Sink,
    U: Stream<Item = T::SinkItem>,
    T::SinkError: From<U::Error>,
{
    type Item = (T, U);
    type Error = (T::SinkError, T, U);

    fn poll(&mut self) -> Poll<(T, U), (T::SinkError, T, U)> {
        let mut stream = self.stream.take().expect("poll called after completion");
        let mut sink = self.sink.take().expect("poll called after completion");

        if let Some(item) = self.buffered.take() {
            match sink.start_send(item) {
                Ok(AsyncSink::Ready) => {}
                Ok(AsyncSink::NotReady(item)) => {
                    self.sink = Some(sink);
                    self.buffered = Some(item);
                    self.stream = Some(stream);
                    return Ok(Async::NotReady);
                }
                Err(e) => return Err((e, sink, stream.into_inner())),
            }
        }

        loop {
            match stream.poll() {
                Ok(Async::Ready(Some(item))) => {
                    match sink.start_send(item) {
                        Ok(AsyncSink::Ready) => {}
                        Ok(AsyncSink::NotReady(item)) => {
                            self.sink = Some(sink);
                            self.buffered = Some(item);
                            self.stream = Some(stream);
                            return Ok(Async::NotReady);
                        }
                        Err(e) => return Err((e, sink, stream.into_inner())),
                    }
                }
                Ok(Async::Ready(None)) => {
                    match sink.poll_complete() {
                        Ok(Async::Ready(())) => return Ok(Async::Ready((sink, stream.into_inner()))),
                        Ok(Async::NotReady) => {
                            self.sink = Some(sink);
                            self.stream = Some(stream);
                            return Ok(Async::NotReady);
                        }
                        Err(e) => return Err((e, sink, stream.into_inner())),
                    }
                }
                Ok(Async::NotReady) => {
                    match sink.poll_complete() {
                        Ok(Async::Ready(())) => return Ok(Async::Ready((sink, stream.into_inner()))),
                        Ok(Async::NotReady) => {
                            self.sink = Some(sink);
                            self.stream = Some(stream);
                            return Ok(Async::NotReady);
                        }
                        Err(e) => return Err((e, sink, stream.into_inner())),
                    }
                    return Ok(Async::NotReady);
                }
                Err(e) => return Err((e.into(), sink, stream.into_inner())),
            }
        }
    }
}
