use futures::sync::oneshot;
use futures::Future;
use log::error;
use std::str::FromStr;
use tokio_postgres::{Error, MakeTlsMode, Socket, TlsMode};

use crate::{Client, RUNTIME};

pub struct Builder(tokio_postgres::Builder);

impl Default for Builder {
    fn default() -> Builder {
        Builder(tokio_postgres::Builder::default())
    }
}

impl Builder {
    pub fn new() -> Builder {
        Builder(tokio_postgres::Builder::new())
    }

    pub fn param(&mut self, key: &str, value: &str) -> &mut Builder {
        self.0.param(key, value);
        self
    }

    pub fn connect<T>(&self, tls_mode: T) -> Result<Client, Error>
    where
        T: MakeTlsMode<Socket> + 'static + Send,
        T::TlsMode: Send,
        T::Stream: Send,
        T::Future: Send,
        <T::TlsMode as TlsMode<Socket>>::Future: Send,
    {
        let connect = self.0.connect(tls_mode);
        let (client, connection) = oneshot::spawn(connect, &RUNTIME.executor()).wait()?;
        let connection = connection.map_err(|e| error!("postgres connection error: {}", e));
        RUNTIME.executor().spawn(connection);

        Ok(Client::from(client))
    }
}

impl FromStr for Builder {
    type Err = Error;

    fn from_str(s: &str) -> Result<Builder, Error> {
        s.parse().map(Builder)
    }
}
