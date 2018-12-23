use futures::sync::mpsc;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use std::io::{self, Read};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
#[cfg(feature = "runtime")]
use tokio_postgres::{MakeTlsMode, Socket, TlsMode};

#[cfg(feature = "runtime")]
use crate::Builder;
use crate::{Query, Statement, Transaction};

pub struct Client(tokio_postgres::Client);

impl Client {
    #[cfg(feature = "runtime")]
    pub fn connect<T>(params: &str, tls_mode: T) -> Result<Client, Error>
    where
        T: MakeTlsMode<Socket> + 'static + Send,
        T::TlsMode: Send,
        T::Stream: Send,
        T::Future: Send,
        <T::TlsMode as TlsMode<Socket>>::Future: Send,
    {
        params.parse::<Builder>()?.connect(tls_mode)
    }

    #[cfg(feature = "runtime")]
    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.0.prepare(query).wait().map(Statement)
    }

    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.0.prepare_typed(query, types).wait().map(Statement)
    }

    pub fn execute<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<u64, Error>
    where
        T: ?Sized + Query,
    {
        let statement = query.__statement(self)?;
        self.0.execute(&statement.0, params).wait()
    }

    pub fn query<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + Query,
    {
        let statement = query.__statement(self)?;
        self.0.query(&statement.0, params).collect().wait()
    }

    pub fn copy_in<T, R>(
        &mut self,
        query: &T,
        params: &[&dyn ToSql],
        reader: R,
    ) -> Result<u64, Error>
    where
        T: ?Sized + Query,
        R: Read,
    {
        let statement = query.__statement(self)?;
        let (sender, receiver) = mpsc::channel(1);
        let future = self.0.copy_in(&statement.0, params, CopyInStream(receiver));

        CopyInFuture {
            future,
            sender,
            reader,
            pending: None,
            done: false,
        }
        .wait()
    }

    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.0.batch_execute(query).wait()
    }

    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.batch_execute("BEGIN")?;
        Ok(Transaction::new(self))
    }

    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

impl From<tokio_postgres::Client> for Client {
    fn from(c: tokio_postgres::Client) -> Client {
        Client(c)
    }
}

enum CopyData {
    Data(Vec<u8>),
    Error(io::Error),
    Done,
}

struct CopyInStream(mpsc::Receiver<CopyData>);

impl Stream for CopyInStream {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Vec<u8>>, io::Error> {
        match self.0.poll().expect("mpsc::Receiver can't error") {
            Async::Ready(Some(CopyData::Data(buf))) => Ok(Async::Ready(Some(buf))),
            Async::Ready(Some(CopyData::Error(e))) => Err(e),
            Async::Ready(Some(CopyData::Done)) => Ok(Async::Ready(None)),
            Async::Ready(None) => Err(io::Error::new(io::ErrorKind::Other, "writer disconnected")),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

struct CopyInFuture<R> {
    future: tokio_postgres::CopyIn<CopyInStream>,
    sender: mpsc::Sender<CopyData>,
    reader: R,
    pending: Option<CopyData>,
    done: bool,
}

impl<R> Future for CopyInFuture<R>
where
    R: Read,
{
    type Item = u64;
    type Error = Error;

    fn poll(&mut self) -> Poll<u64, Error> {
        loop {
            if let Async::Ready(n) = self.future.poll()? {
                return Ok(Async::Ready(n));
            }

            let data = match self.pending.take() {
                Some(pending) => pending,
                None => {
                    if self.done {
                        continue;
                    }

                    let mut buf = vec![];
                    match self.reader.by_ref().take(4096).read_to_end(&mut buf) {
                        Ok(0) => {
                            self.done = true;
                            CopyData::Done
                        }
                        Ok(_) => CopyData::Data(buf),
                        Err(e) => {
                            self.done = true;
                            CopyData::Error(e)
                        }
                    }
                }
            };

            match self.sender.start_send(data) {
                Ok(AsyncSink::Ready) => {}
                Ok(AsyncSink::NotReady(pending)) => {
                    self.pending = Some(pending);
                    return Ok(Async::NotReady);
                }
                // the future's hung up on its end of the channel, so we'll wait for it to error
                Err(_) => {
                    self.done = true;
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}
