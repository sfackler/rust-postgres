use std::{
    io,
    sync::Arc,
};

use futures::Future;
use rustls::ClientConfig;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect};
use tokio_rustls::{client::TlsStream, TlsConnector};
use webpki::{DNSName, DNSNameRef};


pub struct MakeRustlsConnect {
    config: Arc<ClientConfig>,
}

impl MakeRustlsConnect {
    pub fn new(config: ClientConfig) -> Self {
        Self { config: Arc::new(config) }
    }
}

impl<S> MakeTlsConnect<S> for MakeRustlsConnect
where
    S: AsyncRead + AsyncWrite + Send + 'static
{
    type Stream = TlsStream<S>;
    type TlsConnect = RustlsConnect;
    type Error = io::Error;

    fn make_tls_connect(&mut self, hostname: &str) -> Result<RustlsConnect, Self::Error> {
        DNSNameRef::try_from_ascii_str(hostname)
            .map(|dns_name| RustlsConnect {
                hostname: dns_name.to_owned(),
                connector: Arc::clone(&self.config).into(),
            })
            .map_err(|_| io::ErrorKind::InvalidInput.into())
    }
}

pub struct RustlsConnect {
    hostname: DNSName,
    connector: TlsConnector,
}

impl<S> TlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Send + 'static
{
    type Stream = TlsStream<S>;
    type Error = io::Error;
    type Future = Box<dyn Future<Item=(Self::Stream, ChannelBinding), Error=Self::Error> + Send>;

    fn connect(self, stream: S) -> Self::Future {
        Box::new(
            self.connector.connect(self.hostname.as_ref(), stream)
                .map(|s| (s, ChannelBinding::none()))  // TODO
        )
    }
}

#[cfg(test)]
mod tests {
    use futures::{Future, Stream};
    use tokio::runtime::current_thread;

    #[test]
    fn it_works() {
        let config = rustls::ClientConfig::new();
        let tls = super::MakeRustlsConnect::new(config);
        current_thread::block_on_all(
            tokio_postgres::connect("sslmode=require host=localhost user=postgres", tls)
                .map(|(client, connection)| {
                    tokio::spawn(
                        connection.map_err(|e| panic!("{:?}", e))
                    );
                    client
                })
                .and_then(|mut client| {
                    client.prepare("SELECT 1")
                        .map(|s| (client, s))
                })
                .and_then(|(mut client, statement)| {
                    client.query(&statement, &[]).collect()
                })
        ).unwrap();
    }
}
