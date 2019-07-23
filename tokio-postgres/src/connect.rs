use crate::config::{Host, TargetSessionAttrs};
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Client, Config, Connection, Error, Socket};

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

    let mut error = None;
    for (i, host) in config.host.iter().enumerate() {
        let hostname = match host {
            Host::Tcp(host) => &**host,
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Host::Unix(_) => "",
        };

        let tls = tls
            .make_tls_connect(hostname)
            .map_err(|e| Error::tls(e.into()))?;

        match connect_once(i, tls, config).await {
            Ok((client, connection)) => return Ok((client, connection)),
            Err(e) => error = Some(e),
        }
    }

    return Err(error.unwrap());
}

async fn connect_once<T>(
    idx: usize,
    tls: T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: TlsConnect<Socket>,
{
    let socket = connect_socket(idx, config).await?;
    let (client, connection) = connect_raw(socket, tls, config, Some(idx)).await?;

    if let TargetSessionAttrs::ReadWrite = config.target_session_attrs {
        unimplemented!()
    }

    Ok((client, connection))
}
