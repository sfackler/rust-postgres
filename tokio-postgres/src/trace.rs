use tracing::{field::Empty, Level, Span};

use crate::client::{Addr, InnerClient};

pub(crate) enum SpanOperation {
    Query,
    Portal,
    Execute,
    Prepare,
}

impl SpanOperation {
    const fn name(&self) -> &'static str {
        match self {
            SpanOperation::Query => "query",
            SpanOperation::Portal => "portal",
            SpanOperation::Execute => "execute",
            SpanOperation::Prepare => "prepare",
        }
    }

    const fn level(&self) -> Level {
        match self {
            SpanOperation::Query => Level::DEBUG,
            SpanOperation::Portal => Level::DEBUG,
            SpanOperation::Execute => Level::DEBUG,
            SpanOperation::Prepare => Level::TRACE,
        }
    }
}

macro_rules! span_dynamic_lvl {
    ( $(target: $target:expr,)? $(parent: $parent:expr,)? $lvl:expr, $($tt:tt)* ) => {
        match $lvl {
            tracing::Level::ERROR => {
                tracing::span!(
                    $(target: $target,)?
                    $(parent: $parent,)?
                    tracing::Level::ERROR,
                    $($tt)*
                )
            }
            tracing::Level::WARN => {
                tracing::span!(
                    $(target: $target,)?
                    $(parent: $parent,)?
                    tracing::Level::WARN,
                    $($tt)*
                )
            }
            tracing::Level::INFO => {
                tracing::span!(
                    $(target: $target,)?
                    $(parent: $parent,)?
                    tracing::Level::INFO,
                    $($tt)*
                )
            }
            tracing::Level::DEBUG => {
                tracing::span!(
                    $(target: $target,)?
                    $(parent: $parent,)?
                    tracing::Level::DEBUG,
                    $($tt)*
                )
            }
            tracing::Level::TRACE => {
                tracing::span!(
                    $(target: $target,)?
                    $(parent: $parent,)?
                    tracing::Level::TRACE,
                    $($tt)*
                )
            }
        }
    };
}

pub(crate) fn make_span(client: &InnerClient, operation: SpanOperation) -> Span {
    let span = span_dynamic_lvl!(operation.level(), "query",
        db.system = "postgresql",
        server.address = Empty,
        server.socket.address = Empty,
        server.port = Empty,
        "network.type" = Empty,
        network.transport = Empty,
        db.name = %client.db_name(),
        db.user = %client.db_user(),
        otel.name = %format!("PSQL {} {}", operation.name(), client.db_name()),
        otel.kind = "Client",

        // to set when output
        otel.status_code = Empty,
        exception.message = Empty,
        db.operation = operation.name(),

        // for queries
        db.statement = Empty,
        db.statement.params = Empty,
        db.sql.rows_affected = Empty,
    );
    // only executes if span passed the filter
    span.in_scope(|| {
        if let Some(socket_config) = client.socket_config() {
            if let Some(hostname) = socket_config.hostname.as_deref() {
                span.record("server.address", hostname);
            }
            match &socket_config.addr {
                Addr::Tcp(addr) => {
                    span.record("server.socket.address", addr.to_string());
                    let network_type = if addr.is_ipv4() { "ipv4" } else { "ipv6" };
                    span.record("network.type", network_type);
                    span.record("network.transport", "tcp");
                }
                #[cfg(unix)]
                Addr::Unix(path) => {
                    span.record("server.socket.address", path.to_string_lossy().into_owned());
                    span.record("network.type", "unix");
                }
            }
            span.record("server.port", socket_config.port);
        }
    });
    span
}
