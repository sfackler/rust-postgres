use crate::connection::{Request, RequestMessages};
use crate::error::Error;
use crate::responses::{self, Responses};
use crate::statement::Statement;
use crate::types::Type;

use futures::channel::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn next_statement() -> String {
    static ID: AtomicUsize = AtomicUsize::new(0);
    format!("s{}", ID.fetch_add(1, Ordering::SeqCst))
}

pub struct PendingRequest(Result<RequestMessages, Error>);

pub struct Client {
    sender: mpsc::UnboundedSender<Request>,
    process_id: i32,
    secret_key: i32,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Request>,
        process_id: i32,
        secret_key: i32,
    ) -> Client {
        Client {
            sender,
            process_id,
            secret_key,
        }
    }

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare_typed(query, &[]).await
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    pub async fn prepare_typed(
        &self,
        query: &str,
        param_types: &[Type],
    ) -> Result<Statement, Error> {
        unimplemented!();
        //self.sender.unbounded_send(PendingRequest)
    }

    pub(crate) fn send(&self, request: PendingRequest) -> Result<Responses, Error> {
        let messages = request.0?;

        let (sender, receiver) = responses::channel();
        self.sender
            .unbounded_send(Request { messages, sender })
            .map(|_| receiver)
            .map_err(|_| Error::closed())
    }
}
