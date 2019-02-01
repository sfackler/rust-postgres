use crate::proto::client::Client;
use crate::proto::simple_query::SimpleQueryStream;
use futures::{try_ready, Async, Future, Poll, Stream};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::Error;

#[derive(StateMachineFuture)]
pub enum Transaction<F, T, E>
where
    F: Future<Item = T, Error = E>,
    E: From<Error>,
{
    #[state_machine_future(start, transitions(Beginning))]
    Start { client: Client, future: F },
    #[state_machine_future(transitions(Running))]
    Beginning {
        client: Client,
        begin: SimpleQueryStream,
        future: F,
    },
    #[state_machine_future(transitions(Finishing))]
    Running { client: Client, future: F },
    #[state_machine_future(transitions(Finished))]
    Finishing {
        future: SimpleQueryStream,
        result: Result<T, E>,
    },
    #[state_machine_future(ready)]
    Finished(T),
    #[state_machine_future(error)]
    Failed(E),
}

impl<F, T, E> PollTransaction<F, T, E> for Transaction<F, T, E>
where
    F: Future<Item = T, Error = E>,
    E: From<Error>,
{
    fn poll_start<'a>(
        state: &'a mut RentToOwn<'a, Start<F, T, E>>,
    ) -> Poll<AfterStart<F, T, E>, E> {
        let state = state.take();
        transition!(Beginning {
            begin: state.client.simple_query("BEGIN"),
            client: state.client,
            future: state.future,
        })
    }

    fn poll_beginning<'a>(
        state: &'a mut RentToOwn<'a, Beginning<F, T, E>>,
    ) -> Poll<AfterBeginning<F, T, E>, E> {
        while let Some(_) = try_ready!(state.begin.poll()) {}

        let state = state.take();
        transition!(Running {
            client: state.client,
            future: state.future,
        })
    }

    fn poll_running<'a>(
        state: &'a mut RentToOwn<'a, Running<F, T, E>>,
    ) -> Poll<AfterRunning<T, E>, E> {
        match state.future.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(t)) => transition!(Finishing {
                future: state.client.simple_query("COMMIT"),
                result: Ok(t),
            }),
            Err(e) => transition!(Finishing {
                future: state.client.simple_query("ROLLBACK"),
                result: Err(e),
            }),
        }
    }

    fn poll_finishing<'a>(
        state: &'a mut RentToOwn<'a, Finishing<T, E>>,
    ) -> Poll<AfterFinishing<T>, E> {
        loop {
            match state.future.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(Some(_))) => {}
                Ok(Async::Ready(None)) => {
                    let t = state.take().result?;
                    transition!(Finished(t))
                }
                Err(e) => match state.take().result {
                    Ok(_) => return Err(e.into()),
                    // prioritize the future's error over the rollback error
                    Err(e) => return Err(e),
                },
            }
        }
    }
}

impl<F, T, E> TransactionFuture<F, T, E>
where
    F: Future<Item = T, Error = E>,
    E: From<Error>,
{
    pub fn new(client: Client, future: F) -> TransactionFuture<F, T, E> {
        Transaction::start(client, future)
    }
}
