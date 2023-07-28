use tracing::{field::Empty, Level, Span};

use crate::{
    client::{Addr, InnerClient, SocketConfig},
    Error,
};

pub(crate) enum SpanOperation {
    Connect,
    Query,
    Portal,
    Execute,
    Prepare,
}

impl SpanOperation {
    const fn name(&self) -> &'static str {
        match self {
            SpanOperation::Connect => "connect",
            SpanOperation::Query => "query",
            SpanOperation::Portal => "portal",
            SpanOperation::Execute => "execute",
            SpanOperation::Prepare => "prepare",
        }
    }

    const fn level(&self) -> Level {
        match self {
            SpanOperation::Connect => Level::DEBUG,
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

pub(crate) fn make_span_for_client(client: &InnerClient, operation: SpanOperation) -> Span {
    let span = make_span(operation, client.db_user(), client.db_name());
    record_socket_config(&span, client.socket_config());
    span
}

pub(crate) fn make_span(operation: SpanOperation, db_user: &str, db_name: &str) -> Span {
    span_dynamic_lvl!(
        operation.level(),
        "query",
        db.system = "postgresql",
        server.address = Empty,
        server.socket.address = Empty,
        server.port = Empty,
        "network.type" = Empty,
        network.transport = Empty,
        db.name = db_name,
        db.user = db_user,
        otel.name = format!("PSQL {} {}", operation.name(), db_name),
        otel.kind = "Client",
        // to set when output
        otel.status_code = Empty,
        exception.message = Empty,
        db.operation = operation.name(),
        // for queries
        db.statement = Empty,
        db.statement.params = Empty,
        db.sql.rows_affected = Empty,
        // for connections
        db.connect.attempt = Empty,
        db.connect.timing.dns_lookup_ns = Empty,
        db.connect.timing.tcp_handshake_ns = Empty,
        db.connect.timing.tls_handshake_ns = Empty,
        db.connect.timing.auth_ns = Empty,
    )
}

pub(crate) fn record_socket_config(span: &Span, socket_config: Option<&SocketConfig>) {
    if let Some(s) = socket_config {
        record_connect_info(span, &s.addr, s.hostname.as_deref(), s.port);
    }
}

pub(crate) fn record_connect_info(span: &Span, addr: &Addr, hostname: Option<&str>, port: u16) {
    // only executes if span passed the filter
    span.in_scope(|| {
        if let Some(hostname) = hostname {
            span.record("server.address", hostname);
        }
        match addr {
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
        span.record("server.port", port);
    });
}

pub(crate) fn record_error(span: &Span, e: &Error) {
    span.record("otel.status_code", "ERROR")
        .record("exception.message", tracing::field::display(e));
}

pub(crate) fn record_ok(span: &Span) {
    span.record("otel.status_code", "OK");
}
