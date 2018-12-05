//! Connection parameters
use std::error::Error;
use std::mem;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use error;
use params::url::Url;

mod url;

/// The host.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Host {
    /// A TCP hostname.
    Tcp(String),
    /// The path to a directory containing the server's Unix socket.
    Unix(PathBuf),
}

/// Authentication information.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct User {
    name: String,
    password: Option<String>,
}

impl User {
    /// The username.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// An optional password.
    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(|p| &**p)
    }
}

/// Information necessary to open a new connection to a Postgres server.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConnectParams {
    host: Host,
    port: u16,
    user: Option<User>,
    database: Option<String>,
    options: Vec<(String, String)>,
    connect_timeout: Option<Duration>,
    keepalive: Option<Duration>,
}

impl ConnectParams {
    /// Returns a new builder.
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// The target host.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// The target port.
    ///
    /// Defaults to 5432.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The user to log in as.
    ///
    /// A user is required to open a new connection but not to cancel a query.
    pub fn user(&self) -> Option<&User> {
        self.user.as_ref()
    }

    /// The database to connect to.
    pub fn database(&self) -> Option<&str> {
        self.database.as_ref().map(|d| &**d)
    }

    /// Runtime parameters to be passed to the Postgres backend.
    pub fn options(&self) -> &[(String, String)] {
        &self.options
    }

    /// A timeout to apply to each socket-level connection attempt.
    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    /// The interval at which TCP keepalive messages are sent on the socket.
    ///
    /// This is ignored for Unix sockets.
    pub fn keepalive(&self) -> Option<Duration> {
        self.keepalive
    }
}

impl FromStr for ConnectParams {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<ConnectParams, error::Error> {
        s.into_connect_params().map_err(error::connect)
    }
}

/// A builder for `ConnectParams`.
pub struct Builder {
    port: u16,
    user: Option<User>,
    database: Option<String>,
    options: Vec<(String, String)>,
    connect_timeout: Option<Duration>,
    keepalive: Option<Duration>,
}

impl Builder {
    /// Creates a new builder.
    pub fn new() -> Builder {
        Builder {
            port: 5432,
            user: None,
            database: None,
            options: vec![],
            connect_timeout: None,
            keepalive: None,
        }
    }

    /// Sets the port.
    pub fn port(&mut self, port: u16) -> &mut Builder {
        self.port = port;
        self
    }

    /// Sets the user.
    pub fn user(&mut self, name: &str, password: Option<&str>) -> &mut Builder {
        self.user = Some(User {
            name: name.to_string(),
            password: password.map(ToString::to_string),
        });
        self
    }

    /// Sets the database.
    pub fn database(&mut self, database: &str) -> &mut Builder {
        self.database = Some(database.to_string());
        self
    }

    /// Adds a runtime parameter.
    pub fn option(&mut self, name: &str, value: &str) -> &mut Builder {
        self.options.push((name.to_string(), value.to_string()));
        self
    }

    /// Sets the connection timeout.
    pub fn connect_timeout(&mut self, connect_timeout: Option<Duration>) -> &mut Builder {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Sets the keepalive interval.
    pub fn keepalive(&mut self, keepalive: Option<Duration>) -> &mut Builder {
        self.keepalive = keepalive;
        self
    }

    /// Constructs a `ConnectParams` from the builder.
    pub fn build(&mut self, host: Host) -> ConnectParams {
        ConnectParams {
            host: host,
            port: self.port,
            user: self.user.take(),
            database: self.database.take(),
            options: mem::replace(&mut self.options, vec![]),
            connect_timeout: self.connect_timeout,
            keepalive: self.keepalive,
        }
    }
}

/// A trait implemented by types that can be converted into a `ConnectParams`.
pub trait IntoConnectParams {
    /// Converts the value of `self` into a `ConnectParams`.
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>>;
}

impl IntoConnectParams for ConnectParams {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        Ok(self)
    }
}

impl<'a> IntoConnectParams for &'a str {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        match Url::parse(self) {
            Ok(url) => url.into_connect_params(),
            Err(err) => Err(err.into()),
        }
    }
}

impl IntoConnectParams for String {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        self.as_str().into_connect_params()
    }
}

impl IntoConnectParams for Url {
    fn into_connect_params(self) -> Result<ConnectParams, Box<Error + Sync + Send>> {
        let Url {
            host,
            port,
            user,
            path:
                url::Path {
                    path,
                    query: options,
                    ..
                },
            ..
        } = self;

        let mut builder = ConnectParams::builder();

        if let Some(port) = port {
            builder.port(port);
        }

        {
            let mut username: Option<&str> = None;
            let mut password: Option<&str> = None;

            if let Some(info) = &user {
                username = Some(info.user.as_ref());
                password = info.pass.as_ref().map(|p| &**p);
            }

            if let Some((_, value)) = options.iter().find(|(name, _)| &*name == "user") {
                if let Some(_) = username {
                    return Err("user specified twice".into());
                }
                username = Some(value);
            }

            if let Some((_, value)) = options.iter().find(|(name, _)| &*name == "password") {
                if let Some(_) = password {
                    return Err("password specified twice".into());
                }
                password = Some(value);
            }

            if let Some(username) = username {
                builder.user(username, password);
            }
        }

        if !path.is_empty() {
            // path contains the leading /
            builder.database(&path[1..]);
        }

        for (name, value) in options {
            match &*name {
                "connect_timeout" => {
                    let timeout = value.parse().map_err(|_| "invalid connect_timeout")?;
                    let timeout = Duration::from_secs(timeout);
                    builder.connect_timeout(Some(timeout));
                }
                "keepalive" => {
                    let keepalive = value.parse().map_err(|_| "invalid keepalive")?;
                    let keepalive = Duration::from_secs(keepalive);
                    builder.keepalive(Some(keepalive));
                }
                // user/password are handled above
                "user" | "password" => (),
                _ => {
                    builder.option(&name, &value);
                }
            }
        }

        let maybe_path = url::decode_component(&host)?;
        let host = if maybe_path.starts_with('/') {
            Host::Unix(maybe_path.into())
        } else {
            Host::Tcp(maybe_path)
        };

        Ok(builder.build(host))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_url() {
        let params = "postgres://user@host:44/dbname?connect_timeout=10&application_name=foo";
        let params = params.into_connect_params().unwrap();
        assert_eq!(
            params.user(),
            Some(&User {
                name: "user".to_string(),
                password: None,
            })
        );
        assert_eq!(params.host(), &Host::Tcp("host".to_string()));
        assert_eq!(params.port(), 44);
        assert_eq!(params.database(), Some("dbname"));
        assert_eq!(
            params.options(),
            &[("application_name".to_string(), "foo".to_string())][..]
        );
        assert_eq!(params.connect_timeout(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn parse_url_with_query_string() {
        let params = "postgres://host:44/dbname?user=someone&password=supersecret&connect_timeout=10&application_name=foo";
        let params = params.into_connect_params().unwrap();
        assert_eq!(
            params.user(),
            Some(&User {
                name: "someone".to_string(),
                password: Some("supersecret".to_string()),
            })
        );
        assert_eq!(params.host(), &Host::Tcp("host".to_string()));
        assert_eq!(params.port(), 44);
        assert_eq!(params.database(), Some("dbname"));
        assert_eq!(
            params.options(),
            &[("application_name".to_string(), "foo".to_string())][..]
        );
        assert_eq!(params.connect_timeout(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn parse_url_with_mixed_query_string() {
        let params = "postgres://someone@host:44/dbname?password=supersecret&connect_timeout=10&application_name=foo";
        let params = params.into_connect_params().unwrap();
        assert_eq!(
            params.user(),
            Some(&User {
                name: "someone".to_string(),
                password: Some("supersecret".to_string()),
            })
        );
        assert_eq!(params.host(), &Host::Tcp("host".to_string()));
        assert_eq!(params.port(), 44);
        assert_eq!(params.database(), Some("dbname"));
        assert_eq!(
            params.options(),
            &[("application_name".to_string(), "foo".to_string())][..]
        );
        assert_eq!(params.connect_timeout(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn parse_url_with_two_users() {
        let params = "postgres://user@host:44/dbname?user=someone&password=supersecret&connect_timeout=10&application_name=foo";
        match params.into_connect_params() {
            Ok(_) => assert!(false, "These params should not parse succesfully"),
            Err(error) => assert_eq!(format!("{}", error), "user specified twice".to_string()),
        }
    }
}
