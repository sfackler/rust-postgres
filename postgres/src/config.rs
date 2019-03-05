use futures::sync::oneshot;
use futures::Future;
use log::error;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use tokio_postgres::config::{SslMode, TargetSessionAttrs};
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Error, Socket};

use crate::{Client, RUNTIME};

#[derive(Debug, Clone, PartialEq)]
pub struct Config(tokio_postgres::Config);

impl Default for Config {
    fn default() -> Config {
        Config(tokio_postgres::Config::default())
    }
}

impl Config {
    pub fn new() -> Config {
        Config(tokio_postgres::Config::new())
    }

    pub fn user(&mut self, user: &str) -> &mut Config {
        self.0.user(user);
        self
    }

    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        self.0.password(password);
        self
    }

    pub fn dbname(&mut self, dbname: &str) -> &mut Config {
        self.0.dbname(dbname);
        self
    }

    pub fn options(&mut self, options: &str) -> &mut Config {
        self.0.options(options);
        self
    }

    pub fn application_name(&mut self, application_name: &str) -> &mut Config {
        self.0.application_name(application_name);
        self
    }

    pub fn ssl_mode(&mut self, ssl_mode: SslMode) -> &mut Config {
        self.0.ssl_mode(ssl_mode);
        self
    }

    pub fn host(&mut self, host: &str) -> &mut Config {
        self.0.host(host);
        self
    }

    #[cfg(unix)]
    pub fn host_path<T>(&mut self, host: T) -> &mut Config
    where
        T: AsRef<Path>,
    {
        self.0.host_path(host);
        self
    }

    pub fn port(&mut self, port: u16) -> &mut Config {
        self.0.port(port);
        self
    }

    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        self.0.connect_timeout(connect_timeout);
        self
    }

    pub fn keepalives(&mut self, keepalives: bool) -> &mut Config {
        self.0.keepalives(keepalives);
        self
    }

    pub fn keepalives_idle(&mut self, keepalives_idle: Duration) -> &mut Config {
        self.0.keepalives_idle(keepalives_idle);
        self
    }

    pub fn target_session_attrs(
        &mut self,
        target_session_attrs: TargetSessionAttrs,
    ) -> &mut Config {
        self.0.target_session_attrs(target_session_attrs);
        self
    }

    pub fn connect<T>(&self, tls_mode: T) -> Result<Client, Error>
    where
        T: MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let connect = self.0.connect(tls_mode);
        let (client, connection) = oneshot::spawn(connect, &RUNTIME.executor()).wait()?;
        let connection = connection.map_err(|e| error!("postgres connection error: {}", e));
        RUNTIME.executor().spawn(connection);

        Ok(Client::from(client))
    }
}

impl FromStr for Config {
    type Err = Error;

    fn from_str(s: &str) -> Result<Config, Error> {
        s.parse().map(Config)
    }
}
