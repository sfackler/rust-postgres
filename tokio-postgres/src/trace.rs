use tracing::{field::Empty, Span};

use crate::client::{Addr, InnerClient};

pub(crate) fn make_span(client: &InnerClient) -> Span {
    let span = tracing::debug_span!("query",
        db.system = "postgresql",
        server.address = Empty,
        server.socket.address = Empty,
        server.port = Empty,
        "network.type" = Empty,
        network.transport = Empty,
        db.name = %client.db_name(),
        db.user = %client.db_user(),
        otel.name = %format!("psql {}", client.db_name()),
        otel.kind = "Client",

        // to set when output
        otel.status_code = Empty,
        exception.message = Empty,
        db.operation = Empty,

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
