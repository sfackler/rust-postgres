use crate::client::SocketConfig;
use crate::config::{Host, SslMode};
use crate::tls::MakeTlsConnect;
use crate::{cancel_query_raw, connect_socket, Error, Socket};
use std::io;

pub(crate) async fn cancel_query<T>(
    config: Option<SocketConfig>,
    ssl_mode: SslMode,
    mut tls: T,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    T: MakeTlsConnect<Socket>,
{
    let config = match config {
        Some(config) => config,
        None => {
            return Err(Error::connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown host",
            )))
        }
    };

    let hostname = match &config.host {
        Host::Tcp(host) => &**host,
        // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
        #[cfg(unix)]
        Host::Unix(_) => "",
    };
    let tls = tls
        .make_tls_connect(hostname)
        .map_err(|e| Error::tls(e.into()))?;

    let socket = connect_socket::connect_socket(
        &config.host,
        config.port,
        config.connect_timeout,
        config.user_timeout,
        config.keepalive.as_ref(),
    )
    .await?;

    cancel_query_raw::cancel_query_raw(socket, ssl_mode, tls, process_id, secret_key).await
}
