use crate::client::SocketConfig;
use crate::config::{Host, TargetSessionAttrs};
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Client, Config, Connection, Error, SimpleQueryMessage, Socket};
use futures_util::{future, pin_mut, Future, FutureExt, Stream};
use std::io;
use std::task::Poll;

pub async fn connect<T>(
    mut tls: T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: MakeTlsConnect<Socket>,
{
    if config.host.is_empty() {
        return Err(Error::config("host missing".into()));
    }

    if config.port.len() > 1 && config.port.len() != config.host.len() {
        return Err(Error::config("invalid number of ports".into()));
    }

    if !config.hostaddr.is_empty() && config.hostaddr.len() != config.host.len() {
        let msg = format!(
            "invalid number of hostaddrs ({}). Possible values: 0 or number of hosts ({})",
            config.hostaddr.len(),
            config.host.len(),
        );
        return Err(Error::config(msg.into()));
    }

    let mut error = None;
    for (i, host) in config.host.iter().enumerate() {
        let port = config
            .port
            .get(i)
            .or_else(|| config.port.first())
            .copied()
            .unwrap_or(5432);

        // The value of host is always used as the hostname for TLS validation.
        let hostname = match host {
            Host::Tcp(host) => host.as_str(),
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Host::Unix(_) => "",
        };
        let tls = tls
            .make_tls_connect(hostname)
            .map_err(|e| Error::tls(e.into()))?;

        // If both host and hostaddr are specified, the value of hostaddr is used to to establish the TCP connection.
        let hostaddr = match host {
            Host::Tcp(_hostname) => match config.hostaddr.get(i) {
                Some(hostaddr) if hostaddr.is_empty() => Host::Tcp(hostaddr.clone()),
                _ => host.clone(),
            },
            #[cfg(unix)]
            Host::Unix(_v) => host.clone(),
        };

        match connect_once(&hostaddr, port, tls, config).await {
            Ok((client, connection)) => return Ok((client, connection)),
            Err(e) => error = Some(e),
        }
    }

    Err(error.unwrap())
}

async fn connect_once<T>(
    host: &Host,
    port: u16,
    tls: T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: TlsConnect<Socket>,
{
    let socket = connect_socket(
        host,
        port,
        config.connect_timeout,
        if config.keepalives {
            Some(&config.keepalive_config)
        } else {
            None
        },
    )
    .await?;
    let (mut client, mut connection) = connect_raw(socket, tls, config).await?;

    if let TargetSessionAttrs::ReadWrite = config.target_session_attrs {
        let rows = client.simple_query_raw("SHOW transaction_read_only");
        pin_mut!(rows);

        let rows = future::poll_fn(|cx| {
            if connection.poll_unpin(cx)?.is_ready() {
                return Poll::Ready(Err(Error::closed()));
            }

            rows.as_mut().poll(cx)
        })
        .await?;
        pin_mut!(rows);

        loop {
            let next = future::poll_fn(|cx| {
                if connection.poll_unpin(cx)?.is_ready() {
                    return Poll::Ready(Some(Err(Error::closed())));
                }

                rows.as_mut().poll_next(cx)
            });

            match next.await.transpose()? {
                Some(SimpleQueryMessage::Row(row)) => {
                    if row.try_get(0)? == Some("on") {
                        return Err(Error::connect(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "database does not allow writes",
                        )));
                    } else {
                        break;
                    }
                }
                Some(_) => {}
                None => return Err(Error::unexpected_message()),
            }
        }
    }

    client.set_socket_config(SocketConfig {
        host: host.clone(),
        port,
        connect_timeout: config.connect_timeout,
        keepalive: if config.keepalives {
            Some(config.keepalive_config.clone())
        } else {
            None
        },
    });

    Ok((client, connection))
}
