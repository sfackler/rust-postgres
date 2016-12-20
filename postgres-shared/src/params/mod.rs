//! Postgres connection parameters.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::mem;

use params::url::Url;

mod url;

/// The host.
#[derive(Clone, Debug)]
pub enum Host {
    /// A TCP hostname.
    Tcp(String),

    /// The path to a directory containing the server's Unix socket.
    Unix(PathBuf),
}

/// Authentication information.
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
pub struct ConnectParams {
    host: Host,
    port: u16,
    user: Option<User>,
    database: Option<String>,
    options: Vec<(String, String)>,
}

impl ConnectParams {
    /// Returns a new builder.
    pub fn builder() -> Builder {
        Builder {
            port: 5432,
            user: None,
            database: None,
            options: vec![],
        }
    }

    /// The target server.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// The target port.
    ///
    /// Defaults to 5432.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The user to login as.
    ///
    /// Connection requires a user but query cancellation does not.
    pub fn user(&self) -> Option<&User> {
        self.user.as_ref()
    }

    /// The database to connect to.
    ///
    /// Defaults to the username.
    pub fn database(&self) -> Option<&str> {
        self.database.as_ref().map(|d| &**d)
    }

    /// Runtime parameters to be passed to the Postgres backend.
    pub fn options(&self) -> &[(String, String)] {
        &self.options
    }
}

/// A builder type for `ConnectParams`.
pub struct Builder {
    port: u16,
    user: Option<User>,
    database: Option<String>,
    options: Vec<(String, String)>,
}

impl Builder {
    pub fn port(&mut self, port: u16) -> &mut Builder {
        self.port = port;
        self
    }

    pub fn user(&mut self, name: &str, password: Option<&str>) -> &mut Builder {
        self.user = Some(User {
            name: name.to_owned(),
            password: password.map(ToOwned::to_owned),
        });
        self
    }

    pub fn database(&mut self, database: &str) -> &mut Builder {
        self.database = Some(database.to_owned());
        self
    }

    pub fn option(&mut self, name: &str, value: &str) -> &mut Builder {
        self.options.push((name.to_owned(), value.to_owned()));
        self
    }

    pub fn build_tcp(&mut self, host: &str) -> ConnectParams {
        self.build(Host::Tcp(host.to_owned()))
    }

    pub fn build_unix<P>(&mut self, host: P) -> ConnectParams
        where P: AsRef<Path>
    {
        self.build(Host::Unix(host.as_ref().to_owned()))
    }

    pub fn build(&mut self, host: Host) -> ConnectParams {
        ConnectParams {
            host: host,
            port: self.port,
            database: self.database.take(),
            user: self.user.take(),
            options: mem::replace(&mut self.options, vec![]),
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
        let Url { host, port, user, path: url::Path { path, query: options, .. }, .. } = self;

        let mut builder = ConnectParams::builder();

        if let Some(port) = port {
            builder.port(port);
        }

        if let Some(info) = user {
            builder.user(&info.user, info.pass.as_ref().map(|p| &**p));
        }

        if !path.is_empty() {
            // path contains the leading /
            builder.database(&path[1..]);
        }

        for (name, value) in options {
            builder.option(&name, &value);
        }

        let maybe_path = try!(url::decode_component(&host));
        if maybe_path.starts_with('/') {
            Ok(builder.build_unix(maybe_path))
        } else {
            Ok(builder.build_tcp(&maybe_path))
        }
    }
}
