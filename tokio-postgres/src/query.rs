use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{BorrowToSql, IsNull};
use crate::{Error, Portal, Row, Statement};
use bytes::{Bytes, BytesMut};
use futures_util::{ready, Stream};
use log::{debug, log_enabled, Level};
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::{CommandCompleteBody, Message};
use postgres_protocol::message::frontend;
use std::fmt;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};

struct BorrowToSqlParamsDebug<'a, T>(&'a [T]);

impl<'a, T> fmt::Debug for BorrowToSqlParamsDebug<'a, T>
where
    T: BorrowToSql,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|x| x.borrow_to_sql()))
            .finish()
    }
}

pub async fn query<P, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = if log_enabled!(Level::Debug) {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        encode(client, &statement, params)?
    } else {
        encode(client, &statement, params)?
    };
    let responses = start(client, buf).await?;
    Ok(RowStream {
        statement,
        responses,
        rows_affected: None,
        _p: PhantomPinned,
    })
}

pub async fn query_portal(
    client: &InnerClient,
    portal: &Portal,
    max_rows: i32,
) -> Result<RowStream, Error> {
    let buf = client.with_buf(|buf| {
        frontend::execute(portal.name(), max_rows, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(RowStream {
        statement: portal.statement().clone(),
        responses,
        rows_affected: None,
        _p: PhantomPinned,
    })
}

/// Extract the number of rows affected from [`CommandCompleteBody`].
pub fn extract_row_affected(body: &CommandCompleteBody) -> Result<u64, Error> {
    let rows = body
        .tag()
        .map_err(Error::parse)?
        .rsplit(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap_or(0);
    Ok(rows)
}

/// A future that completes with the result of an `execute_prepared` function call.
// Once https://github.com/rust-lang/rust/issues/63063 is stable, we might want to replace this by
//     type Execute = impl Future<Output=Result<u64, Error>> + Sync + Send + Unpin + 'static
// and restore the simpler procedural logic instead of writing the state machine ourselves.
pub struct Execute {
    responses: Result<Responses, Option<Error>>,
    bound: bool,
    rows: u64,
}

impl Future for Execute {
    type Output = Result<u64, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use Poll::*;
        let slf = self.get_mut();
        let responses = match &mut slf.responses {
            Ok(r) => r,
            Err(e) => match e.take() {
                Some(e) => return Ready(Err(e)),
                _ => panic!("Execute future polled after it has already completed"),
            },
        };
        loop {
            let message = match responses.poll_next(cx) {
                Ready(Ok(msg)) => msg,
                Ready(Err(e)) => {
                    slf.responses = Err(None);
                    return Ready(Err(e));
                }
                Pending => return Pending,
            };
            if !slf.bound {
                match message {
                    Message::BindComplete => slf.bound = true,
                    _ => {
                        slf.responses = Err(None);
                        return Ready(Err(Error::unexpected_message()));
                    }
                }
            } else {
                match message {
                    Message::DataRow(_) => {}
                    Message::CommandComplete(body) => {
                        slf.rows = extract_row_affected(&body)?;
                    }
                    Message::EmptyQueryResponse => slf.rows = 0,
                    Message::ReadyForQuery(_) => {
                        slf.responses = Err(None);
                        return Ready(Ok(slf.rows));
                    }
                    _ => {
                        slf.responses = Err(None);
                        return Ready(Err(Error::unexpected_message()));
                    }
                }
            }
        }
    }
}

pub fn execute<P, I>(client: &InnerClient, statement: Statement, params: I) -> Execute
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = if log_enabled!(Level::Debug) {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        encode(client, &statement, params)
    } else {
        encode(client, &statement, params)
    };

    let responses = buf
        .and_then(|buf| client.send(RequestMessages::Single(FrontendMessage::Raw(buf))))
        .map_err(Some);

    Execute {
        responses,
        bound: false,
        rows: 0,
    }
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pub fn encode<P, I>(client: &InnerClient, statement: &Statement, params: I) -> Result<Bytes, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    client.with_buf(|buf| {
        encode_bind(statement, params, "", buf)?;
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}

pub fn encode_bind<P, I>(
    statement: &Statement,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let param_types = statement.params();
    let params = params.into_iter();

    if param_types.len() != params.len() {
        return Err(Error::parameters(params.len(), param_types.len()));
    }

    let (param_formats, params): (Vec<_>, Vec<_>) = params
        .zip(param_types.iter())
        .map(|(p, ty)| (p.borrow_to_sql().encode_format(ty) as i16, p))
        .unzip();

    let params = params.into_iter();

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement.name(),
        param_formats,
        params.zip(param_types).enumerate(),
        |(idx, (param, ty)), buf| match param.borrow_to_sql().to_sql_checked(ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        buf,
    );
    match r {
        Ok(()) => Ok(()),
        Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, error_idx)),
        Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
    }
}

pin_project! {
    /// A stream of table rows.
    pub struct RowStream {
        statement: Statement,
        responses: Responses,
        rows_affected: Option<u64>,
        #[pin]
        _p: PhantomPinned,
    }
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            match ready!(this.responses.poll_next(cx)?) {
                Message::DataRow(body) => {
                    return Poll::Ready(Some(Ok(Row::new(this.statement.clone(), body)?)))
                }
                Message::CommandComplete(body) => {
                    *this.rows_affected = Some(extract_row_affected(&body)?);
                }
                Message::EmptyQueryResponse | Message::PortalSuspended => {}
                Message::ReadyForQuery(_) => return Poll::Ready(None),
                _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
            }
        }
    }
}

impl RowStream {
    /// Returns the number of rows affected by the query.
    ///
    /// This function will return `None` until the stream has been exhausted.
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }
}
