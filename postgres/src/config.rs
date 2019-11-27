//! Connection configuration.
//!
//! Requires the `runtime` Cargo feature (enabled by default).

use futures::{FutureExt, executor};
use log::error;
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use tokio_executor::Executor;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Error, Socket};

#[doc(inline)]
pub use tokio_postgres::config::{ChannelBinding, SslMode, TargetSessionAttrs};

use crate::{Client, RUNTIME};

/// Connection configuration.
///
/// Configuration can be parsed from libpq-style connection strings. These strings come in two formats:
///
/// # Key-Value
///
/// This format consists of space-separated key-value pairs. Values which are either the empty string or contain
/// whitespace should be wrapped in `'`. `'` and `\` characters should be backslash-escaped.
///
/// ## Keys
///
/// * `user` - The username to authenticate with. Required.
/// * `password` - The password to authenticate with.
/// * `dbname` - The name of the database to connect to. Defaults to the username.
/// * `options` - Command line options used to configure the server.
/// * `application_name` - Sets the `application_name` parameter on the server.
/// * `sslmode` - Controls usage of TLS. If set to `disable`, TLS will not be used. If set to `prefer`, TLS will be used
///     if available, but not used otherwise. If set to `require`, TLS will be forced to be used. Defaults to `prefer`.
/// * `host` - The host to connect to. On Unix platforms, if the host starts with a `/` character it is treated as the
///     path to the directory containing Unix domain sockets. Otherwise, it is treated as a hostname. Multiple hosts
///     can be specified, separated by commas. Each host will be tried in turn when connecting. Required if connecting
///     with the `connect` method.
/// * `port` - The port to connect to. Multiple ports can be specified, separated by commas. The number of ports must be
///     either 1, in which case it will be used for all hosts, or the same as the number of hosts. Defaults to 5432 if
///     omitted or the empty string.
/// * `connect_timeout` - The time limit in seconds applied to each socket-level connection attempt. Note that hostnames
///     can resolve to multiple IP addresses, and this limit is applied to each address. Defaults to no timeout.
/// * `keepalives` - Controls the use of TCP keepalive. A value of 0 disables keepalive and nonzero integers enable it.
///     This option is ignored when connecting with Unix sockets. Defaults to on.
/// * `keepalives_idle` - The number of seconds of inactivity after which a keepalive message is sent to the server.
///     This option is ignored when connecting with Unix sockets. Defaults to 2 hours.
/// * `target_session_attrs` - Specifies requirements of the session. If set to `read-write`, the client will check that
///     the `transaction_read_write` session parameter is set to `on`. This can be used to connect to the primary server
///     in a database cluster as opposed to the secondary read-only mirrors. Defaults to `all`.
///
/// ## Examples
///
/// ```not_rust
/// host=localhost user=postgres connect_timeout=10 keepalives=0
/// ```
///
/// ```not_rust
/// host=/var/lib/postgresql,localhost port=1234 user=postgres password='password with spaces'
/// ```
///
/// ```not_rust
/// host=host1,host2,host3 port=1234,,5678 user=postgres target_session_attrs=read-write
/// ```
///
/// # Url
///
/// This format resembles a URL with a scheme of either `postgres://` or `postgresql://`. All components are optional,
/// and the format accept query parameters for all of the key-value pairs described in the section above. Multiple
/// host/port pairs can be comma-separated. Unix socket paths in the host section of the URL should be percent-encoded,
/// as the path component of the URL specifies the database name.
///
/// ## Examples
///
/// ```not_rust
/// postgresql://user@localhost
/// ```
///
/// ```not_rust
/// postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10
/// ```
///
/// ```not_rust
/// postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write
/// ```
///
/// ```not_rust
/// postgresql:///mydb?user=user&host=/var/lib/postgresql
/// ```
#[derive(Clone)]
pub struct Config {
    config: tokio_postgres::Config,
    // this is an option since we don't want to boot up our default runtime unless we're actually going to use it.
    executor: Option<Arc<Mutex<dyn Executor + Send>>>,
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
    /// Creates a new configuration.
    pub fn new() -> Config {
        Config {
            config: tokio_postgres::Config::new(),
            executor: None,
        }
    }

    /// Sets the user to authenticate with.
    ///
    /// Required.
    pub fn user(&mut self, user: &str) -> &mut Config {
        self.config.user(user);
        self
    }

    /// Sets the password to authenticate with.
    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        self.config.password(password);
        self
    }

    /// Sets the name of the database to connect to.
    ///
    /// Defaults to the user.
    pub fn dbname(&mut self, dbname: &str) -> &mut Config {
        self.config.dbname(dbname);
        self
    }

    /// Sets command line options used to configure the server.
    pub fn options(&mut self, options: &str) -> &mut Config {
        self.config.options(options);
        self
    }

    /// Sets the value of the `application_name` runtime parameter.
    pub fn application_name(&mut self, application_name: &str) -> &mut Config {
        self.config.application_name(application_name);
        self
    }

    /// Sets the SSL configuration.
    ///
    /// Defaults to `prefer`.
    pub fn ssl_mode(&mut self, ssl_mode: SslMode) -> &mut Config {
        self.config.ssl_mode(ssl_mode);
        self
    }

    /// Adds a host to the configuration.
    ///
    /// Multiple hosts can be specified by calling this method multiple times, and each will be tried in order. On Unix
    /// systems, a host starting with a `/` is interpreted as a path to a directory containing Unix domain sockets.
    pub fn host(&mut self, host: &str) -> &mut Config {
        self.config.host(host);
        self
    }

    /// Adds a Unix socket host to the configuration.
    ///
    /// Unlike `host`, this method allows non-UTF8 paths.
    #[cfg(unix)]
    pub fn host_path<T>(&mut self, host: T) -> &mut Config
    where
        T: AsRef<Path>,
    {
        self.config.host_path(host);
        self
    }

    /// Adds a port to the configuration.
    ///
    /// Multiple ports can be specified by calling this method multiple times. There must either be no ports, in which
    /// case the default of 5432 is used, a single port, in which it is used for all hosts, or the same number of ports
    /// as hosts.
    pub fn port(&mut self, port: u16) -> &mut Config {
        self.config.port(port);
        self
    }

    /// Sets the timeout applied to socket-level connection attempts.
    ///
    /// Note that hostnames can resolve to multiple IP addresses, and this timeout will apply to each address of each
    /// host separately. Defaults to no limit.
    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        self.config.connect_timeout(connect_timeout);
        self
    }

    /// Controls the use of TCP keepalive.
    ///
    /// This is ignored for Unix domain socket connections. Defaults to `true`.
    pub fn keepalives(&mut self, keepalives: bool) -> &mut Config {
        self.config.keepalives(keepalives);
        self
    }

    /// Sets the amount of idle time before a keepalive packet is sent on the connection.
    ///
    /// This is ignored for Unix domain sockets, or if the `keepalives` option is disabled. Defaults to 2 hours.
    pub fn keepalives_idle(&mut self, keepalives_idle: Duration) -> &mut Config {
        self.config.keepalives_idle(keepalives_idle);
        self
    }

    /// Sets the requirements of the session.
    ///
    /// This can be used to connect to the primary server in a clustered database rather than one of the read-only
    /// secondary servers. Defaults to `Any`.
    pub fn target_session_attrs(
        &mut self,
        target_session_attrs: TargetSessionAttrs,
    ) -> &mut Config {
        self.config.target_session_attrs(target_session_attrs);
        self
    }

    /// Sets the channel binding behavior.
    ///
    /// Defaults to `prefer`.
    pub fn channel_binding(&mut self, channel_binding: ChannelBinding) -> &mut Config {
        self.config.channel_binding(channel_binding);
        self
    }

    /// Sets the executor used to run the connection futures.
    ///
    /// Defaults to a postgres-specific tokio `Runtime`.
    pub fn executor<E>(&mut self, executor: E) -> &mut Config
    where
        E: Executor + 'static + Send,
    {
        self.executor = Some(Arc::new(Mutex::new(executor)));
        self
    }

    /// Opens a connection to a PostgreSQL database.
    pub fn connect<T>(&self, tls: T) -> Result<Client, Error>
    where
        T: MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let (client, connection) = match &self.executor {
            Some(executor) => {
                let (tx, rx) = mpsc::channel();
                let config = self.config.clone();
                let connect = async move {
                    let r = config.connect(tls).await;
                    let _ = tx.send(r);
                };
                executor.lock().unwrap().spawn(Box::pin(connect)).unwrap();
                rx.recv().unwrap()?
            }
            None => {
                let connect = self.config.connect(tls);
                RUNTIME.handle().enter(|| {
                    executor::block_on(connect)
                })?
            }
        };

        let connection = connection.map(|r| {
            if let Err(e) = r {
                error!("postgres connection error: {}", e)
            }
        });
        match &self.executor {
            Some(executor) => {
                executor
                    .lock()
                    .unwrap()
                    .spawn(Box::pin(connection))
                    .unwrap();
            }
            None => {
                RUNTIME.spawn(connection);
            }
        }

        Ok(Client::from(client))
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
