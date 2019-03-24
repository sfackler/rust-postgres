use futures::future::Executor;
use futures::sync::oneshot;
use futures::Future;
use log::error;
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::config::{SslMode, TargetSessionAttrs};
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Error, Socket};

use crate::{Client, RUNTIME};

#[derive(Clone)]
pub struct Config {
    config: tokio_postgres::Config,
    executor: Option<Arc<Executor<Box<Future<Item = (), Error = ()> + Send>>>>,
}

impl fmt::Debug for Config {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Config")
            .field("config", &self.config)
            .finish()
    }
}

impl Default for Config {
    fn default() -> Config {
        Config::new()
    }
}

impl Config {
    pub fn new() -> Config {
        Config {
            config: tokio_postgres::Config::new(),
            executor: None,
        }
    }

    pub fn user(&mut self, user: &str) -> &mut Config {
        self.config.user(user);
        self
    }

    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        self.config.password(password);
        self
    }

    pub fn dbname(&mut self, dbname: &str) -> &mut Config {
        self.config.dbname(dbname);
        self
    }

    pub fn options(&mut self, options: &str) -> &mut Config {
        self.config.options(options);
        self
    }

    pub fn application_name(&mut self, application_name: &str) -> &mut Config {
        self.config.application_name(application_name);
        self
    }

    pub fn ssl_mode(&mut self, ssl_mode: SslMode) -> &mut Config {
        self.config.ssl_mode(ssl_mode);
        self
    }

    pub fn host(&mut self, host: &str) -> &mut Config {
        self.config.host(host);
        self
    }

    #[cfg(unix)]
    pub fn host_path<T>(&mut self, host: T) -> &mut Config
    where
        T: AsRef<Path>,
    {
        self.config.host_path(host);
        self
    }

    pub fn port(&mut self, port: u16) -> &mut Config {
        self.config.port(port);
        self
    }

    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        self.config.connect_timeout(connect_timeout);
        self
    }

    pub fn keepalives(&mut self, keepalives: bool) -> &mut Config {
        self.config.keepalives(keepalives);
        self
    }

    pub fn keepalives_idle(&mut self, keepalives_idle: Duration) -> &mut Config {
        self.config.keepalives_idle(keepalives_idle);
        self
    }

    pub fn target_session_attrs(
        &mut self,
        target_session_attrs: TargetSessionAttrs,
    ) -> &mut Config {
        self.config.target_session_attrs(target_session_attrs);
        self
    }

    pub fn executor<E>(&mut self, executor: E) -> &mut Config
    where
        E: Executor<Box<Future<Item = (), Error = ()> + Send>> + 'static + Sync + Send,
    {
        self.executor = Some(Arc::new(executor));
        self
    }

    pub fn connect<T>(&self, tls_mode: T) -> Result<Client, Error>
    where
        T: MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let (tx, rx) = oneshot::channel();
        let connect = self
            .config
            .connect(tls_mode)
            .then(|r| tx.send(r).map_err(|_| ()));
        self.with_executor(|e| e.execute(Box::new(connect)))
            .unwrap();
        let (client, connection) = rx.wait().unwrap()?;

        let connection = connection.map_err(|e| error!("postgres connection error: {}", e));
        self.with_executor(|e| e.execute(Box::new(connection)))
            .unwrap();

        Ok(Client::from(client))
    }

    fn with_executor<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&Executor<Box<Future<Item = (), Error = ()> + Send>>) -> T,
    {
        match &self.executor {
            Some(e) => f(&**e),
            None => f(&RUNTIME.executor()),
        }
    }
}

impl FromStr for Config {
    type Err = Error;

    fn from_str(s: &str) -> Result<Config, Error> {
        s.parse::<tokio_postgres::Config>().map(Config::from)
    }
}

impl From<tokio_postgres::Config> for Config {
    fn from(config: tokio_postgres::Config) -> Config {
        Config {
            config,
            executor: None,
        }
    }
}
