use crate::config::SslMode;
use crate::tls::TlsConnect;
use crate::{connect_tls, Error};
use futures::future;
use postgres_protocol::message::frontend;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub async fn cancel_query_raw<S, T>(
    stream: S,
    mode: SslMode,
    tls: T,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsConnect<S>,
{
    let (mut stream, _) = connect_tls::connect_tls(stream, mode, tls).await?;

    let mut buf = vec![];
    frontend::cancel_request(process_id, secret_key, &mut buf);

    stream.write_all(&buf).await.map_err(Error::io)?;
    stream.flush().await.map_err(Error::io)?;
    future::poll_fn(|cx| Pin::new(&mut stream).poll_shutdown(cx))
        .await
        .map_err(Error::io)?;

    Ok(())
}
