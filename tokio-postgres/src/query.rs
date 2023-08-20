use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::trace::{make_span_for_client, record_error, SpanOperation, record_ok};
use crate::types::{BorrowToSql, IsNull};
use crate::{Error, Portal, Row, Statement};
use bytes::{Bytes, BytesMut};
use futures_util::{ready, Stream};
use log::{debug, log_enabled, Level};
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::{CommandCompleteBody, Message};
use postgres_protocol::message::frontend;
use std::fmt;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::{Instrument, Span};

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

fn encode_with_logs<P, I>(
    client: &InnerClient,
    span: &Span,
    statement: &Statement,
    params: I,
) -> Result<Bytes, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    if log_enabled!(Level::Debug) || !span.is_disabled() {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        if !span.is_disabled() {
            span.record("db.statement", statement.query());

            // while OTEL supports arrays, we don't want to add that as a dependency, so debug view it is.
            let raw_params = BorrowToSqlParamsDebug(params.as_slice());
            span.record("db.statement.params", format!("{raw_params:?}"));
        }
        encode(client, statement, params)
    } else {
        encode(client, statement, params)
    }
}

async fn start_traced(client: &InnerClient, span: &Span, buf: Bytes) -> Result<Responses, Error> {
    match start(client, buf).instrument(span.clone()).await {
        Ok(response) => {
            record_ok(span);
            Ok(response)
        }
        Err(e) => {
            record_error(span, &e);
            Err(e)
        }
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
    let span = make_span_for_client(client, SpanOperation::Query);

    let buf = encode_with_logs(client, &span, &statement, params)?;

    let responses = start_traced(client, &span, buf).await?;

    span.in_scope(|| {
        tracing::trace!("response ready");
    });
    Ok(RowStream {
        statement,
        span,
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
    let span = make_span_for_client(client, SpanOperation::Portal);
    span.record("db.statement", portal.statement().query());

    let buf = client.with_buf(|buf| {
        frontend::execute(portal.name(), max_rows, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(RowStream {
        statement: portal.statement().clone(),
        span,
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

pub async fn execute<P, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<u64, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let span = make_span_for_client(client, SpanOperation::Execute);

    let buf = encode_with_logs(client, &span, &statement, params)?;

    let mut responses = start_traced(client, &span, buf).await?;

    span.in_scope(|| {
        tracing::trace!("response ready");
    });

    let mut rows = 0;
    loop {
        match responses.next().instrument(span.clone()).await? {
            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                rows = extract_row_affected(&body)?;
                span.record("db.sql.rows_affected", rows);
                tracing::trace!("execute complete");
            }
            Message::EmptyQueryResponse => rows = 0,
            Message::ReadyForQuery(_) => return Ok(rows),
            _ => return Err(Error::unexpected_message()),
        }
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
        span: Span,
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
        let _span = this.span.enter();
        loop {
            match ready!(this.responses.poll_next(cx)?) {
                Message::DataRow(body) => {
                    return Poll::Ready(Some(Ok(Row::new(this.statement.clone(), body)?)))
                }
                Message::CommandComplete(body) => {
                    let rows_affected = extract_row_affected(&body)?;

                    this.span.record("db.sql.rows_affected", rows_affected);
                    tracing::trace!("query complete");

                    *this.rows_affected = Some(rows_affected);
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
