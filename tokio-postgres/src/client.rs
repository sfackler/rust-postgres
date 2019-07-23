use crate::codec::FrontendMessage;
use crate::connection::{Request, RequestMessages};
use crate::error::Error;
use crate::responses::{self, Responses};
use crate::statement::Statement;
use crate::types::Type;
use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::StreamExt;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
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
        let name = next_statement();
        let pending = self.pending(|buf| {
            frontend::parse(&name, query, param_types.iter().map(Type::oid), buf)
                .map_err(Error::parse)?;
            frontend::describe(b'S', &name, buf).map_err(Error::parse)?;
            frontend::sync(buf);
            Ok(())
        });

        let mut response = self.send(pending)?;

        let message = response.next().await;

        match message {
            Some(Ok(Message::ParseComplete)) => (),
            Some(Ok(Message::ErrorResponse(body))) => return Err(Error::db(body)),
            Some(Err(e)) => return Err(e),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        };

        let message = response.next().await;

        let parameters: Vec<_> = match message {
            Some(Ok(Message::ParameterDescription(body))) => {
                body.parameters().collect().map_err(Error::parse)?
            }
            Some(Err(e)) => return Err(e),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        };

        let message = response.next().await;

        let columns = match message {
            Some(Ok(Message::RowDescription(body))) => body
                .fields()
                .map(|f| Ok((f.name().to_string(), f.type_oid())))
                .collect()
                .map_err(Error::parse)?,
            Some(Ok(Message::NoData)) => vec![],
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        };

        let mut parameters = parameters.into_iter();
        if let Some(oid) = parameters.next() {
            // fetch params
            unimplemented!("not yet finished");
        }

        let mut columns = columns.into_iter();
        if let Some((name, oid)) = columns.next() {
            // fetch columns
            unimplemented!("not yet finished");
        }

        unimplemented!("not yet finished");

        /*Ok(Statement::new(
            name,
            parameters,
            columns,
        ))*/
    }

    pub(crate) fn pending<F>(&self, messages: F) -> PendingRequest
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), Error>,
    {
        let mut buf = vec![];
        PendingRequest(
            messages(&mut buf).map(|()| RequestMessages::Single(FrontendMessage::Raw(buf))),
        )
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
