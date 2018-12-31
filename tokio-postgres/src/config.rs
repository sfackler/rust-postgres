use std::error;
use std::fmt;
use std::iter;
#[cfg(all(feature = "runtime", unix))]
use std::path::{Path, PathBuf};
use std::str::{self, FromStr};
use std::sync::Arc;
#[cfg(feature = "runtime")]
use std::time::Duration;
use tokio_io::{AsyncRead, AsyncWrite};

#[cfg(feature = "runtime")]
use crate::proto::ConnectFuture;
use crate::proto::HandshakeFuture;
#[cfg(feature = "runtime")]
use crate::{Connect, MakeTlsMode, Socket};
use crate::{Error, Handshake, TlsMode};

#[cfg(feature = "runtime")]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TargetSessionAttrs {
    Any,
    ReadWrite,
    #[doc(hidden)]
    __NonExhaustive,
}

#[cfg(feature = "runtime")]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Host {
    Tcp(String),
    #[cfg(unix)]
    Unix(PathBuf),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Inner {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<Vec<u8>>,
    pub(crate) dbname: Option<String>,
    pub(crate) options: Option<String>,
    pub(crate) application_name: Option<String>,
    #[cfg(feature = "runtime")]
    pub(crate) host: Vec<Host>,
    #[cfg(feature = "runtime")]
    pub(crate) port: Vec<u16>,
    #[cfg(feature = "runtime")]
    pub(crate) connect_timeout: Option<Duration>,
    #[cfg(feature = "runtime")]
    pub(crate) keepalives: bool,
    #[cfg(feature = "runtime")]
    pub(crate) keepalives_idle: Duration,
    #[cfg(feature = "runtime")]
    pub(crate) target_session_attrs: TargetSessionAttrs,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config(pub(crate) Arc<Inner>);

impl Default for Config {
    fn default() -> Config {
        Config::new()
    }
}

impl Config {
    pub fn new() -> Config {
        Config(Arc::new(Inner {
            user: None,
            password: None,
            dbname: None,
            options: None,
            application_name: None,
            #[cfg(feature = "runtime")]
            host: vec![],
            #[cfg(feature = "runtime")]
            port: vec![],
            #[cfg(feature = "runtime")]
            connect_timeout: None,
            #[cfg(feature = "runtime")]
            keepalives: true,
            #[cfg(feature = "runtime")]
            keepalives_idle: Duration::from_secs(2 * 60 * 60),
            #[cfg(feature = "runtime")]
            target_session_attrs: TargetSessionAttrs::Any,
        }))
    }

    pub fn user(&mut self, user: &str) -> &mut Config {
        Arc::make_mut(&mut self.0).user = Some(user.to_string());
        self
    }

    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        Arc::make_mut(&mut self.0).password = Some(password.as_ref().to_vec());
        self
    }

    pub fn dbname(&mut self, dbname: &str) -> &mut Config {
        Arc::make_mut(&mut self.0).dbname = Some(dbname.to_string());
        self
    }

    pub fn options(&mut self, options: &str) -> &mut Config {
        Arc::make_mut(&mut self.0).options = Some(options.to_string());
        self
    }

    pub fn application_name(&mut self, application_name: &str) -> &mut Config {
        Arc::make_mut(&mut self.0).application_name = Some(application_name.to_string());
        self
    }

    #[cfg(feature = "runtime")]
    pub fn host(&mut self, host: &str) -> &mut Config {
        #[cfg(unix)]
        {
            if host.starts_with('/') {
                return self.host_path(host);
            }
        }

        Arc::make_mut(&mut self.0)
            .host
            .push(Host::Tcp(host.to_string()));
        self
    }

    #[cfg(all(feature = "runtime", unix))]
    pub fn host_path<T>(&mut self, host: T) -> &mut Config
    where
        T: AsRef<Path>,
    {
        Arc::make_mut(&mut self.0)
            .host
            .push(Host::Unix(host.as_ref().to_path_buf()));
        self
    }

    #[cfg(feature = "runtime")]
    pub fn port(&mut self, port: u16) -> &mut Config {
        Arc::make_mut(&mut self.0).port.push(port);
        self
    }

    #[cfg(feature = "runtime")]
    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        Arc::make_mut(&mut self.0).connect_timeout = Some(connect_timeout);
        self
    }

    #[cfg(feature = "runtime")]
    pub fn keepalives(&mut self, keepalives: bool) -> &mut Config {
        Arc::make_mut(&mut self.0).keepalives = keepalives;
        self
    }

    #[cfg(feature = "runtime")]
    pub fn keepalives_idle(&mut self, keepalives_idle: Duration) -> &mut Config {
        Arc::make_mut(&mut self.0).keepalives_idle = keepalives_idle;
        self
    }

    #[cfg(feature = "runtime")]
    pub fn target_session_attrs(
        &mut self,
        target_session_attrs: TargetSessionAttrs,
    ) -> &mut Config {
        Arc::make_mut(&mut self.0).target_session_attrs = target_session_attrs;
        self
    }

    pub fn handshake<S, T>(&self, stream: S, tls_mode: T) -> Handshake<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsMode<S>,
    {
        Handshake(HandshakeFuture::new(stream, tls_mode, self.clone()))
    }

    #[cfg(feature = "runtime")]
    pub fn connect<T>(&self, make_tls_mode: T) -> Connect<T>
    where
        T: MakeTlsMode<Socket>,
    {
        Connect(ConnectFuture::new(make_tls_mode, Ok(self.clone())))
    }
}

impl FromStr for Config {
    type Err = Error;

    fn from_str(s: &str) -> Result<Config, Error> {
        let mut parser = Parser::new(s);
        let mut builder = Config::new();

        while let Some((key, value)) = parser.parameter()? {
            match key {
                "user" => {
                    builder.user(&value);
                }
                "password" => {
                    builder.password(value);
                }
                "dbname" => {
                    builder.dbname(&value);
                }
                "options" => {
                    builder.options(&value);
                }
                "application_name" => {
                    builder.application_name(&value);
                }
                #[cfg(feature = "runtime")]
                "host" => {
                    for host in value.split(',') {
                        builder.host(host);
                    }
                }
                #[cfg(feature = "runtime")]
                "port" => {
                    for port in value.split(',') {
                        let port = if port.is_empty() {
                            5432
                        } else {
                            port.parse()
                                .map_err(|_| Error::config_parse(Box::new(InvalidValue("port"))))?
                        };
                        builder.port(port);
                    }
                }
                #[cfg(feature = "runtime")]
                "connect_timeout" => {
                    let timeout = value.parse::<i64>().map_err(|_| {
                        Error::config_parse(Box::new(InvalidValue("connect_timeout")))
                    })?;
                    if timeout > 0 {
                        builder.connect_timeout(Duration::from_secs(timeout as u64));
                    }
                }
                #[cfg(feature = "runtime")]
                "keepalives" => {
                    let keepalives = value
                        .parse::<u64>()
                        .map_err(|_| Error::config_parse(Box::new(InvalidValue("keepalives"))))?;
                    builder.keepalives(keepalives != 0);
                }
                #[cfg(feature = "runtime")]
                "keepalives_idle" => {
                    let keepalives_idle = value.parse::<i64>().map_err(|_| {
                        Error::config_parse(Box::new(InvalidValue("keepalives_idle")))
                    })?;
                    if keepalives_idle > 0 {
                        builder.keepalives_idle(Duration::from_secs(keepalives_idle as u64));
                    }
                }
                #[cfg(feature = "runtime")]
                "target_session_attrs" => {
                    let target_session_attrs = match &*value {
                        "any" => TargetSessionAttrs::Any,
                        "read-write" => TargetSessionAttrs::ReadWrite,
                        _ => {
                            return Err(Error::config_parse(Box::new(InvalidValue(
                                "target_session_attrs",
                            ))))
                        }
                    };
                    builder.target_session_attrs(target_session_attrs);
                }
                key => {
                    return Err(Error::config_parse(Box::new(UnknownOption(
                        key.to_string(),
                    ))))
                }
            }
        }

        Ok(builder)
    }
}

#[derive(Debug)]
struct UnknownOption(String);

impl fmt::Display for UnknownOption {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "unknown option `{}`", self.0)
    }
}

impl error::Error for UnknownOption {}

#[derive(Debug)]
#[cfg(feature = "runtime")]
struct InvalidValue(&'static str);

#[cfg(feature = "runtime")]
impl fmt::Display for InvalidValue {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "invalid value for option `{}`", self.0)
    }
}

#[cfg(feature = "runtime")]
impl error::Error for InvalidValue {}

struct Parser<'a> {
    s: &'a str,
    it: iter::Peekable<str::CharIndices<'a>>,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Parser<'a> {
        Parser {
            s,
            it: s.char_indices().peekable(),
        }
    }

    fn skip_ws(&mut self) {
        self.take_while(|c| c.is_whitespace());
    }

    fn take_while<F>(&mut self, f: F) -> &'a str
    where
        F: Fn(char) -> bool,
    {
        let start = match self.it.peek() {
            Some(&(i, _)) => i,
            None => return "",
        };

        loop {
            match self.it.peek() {
                Some(&(_, c)) if f(c) => {
                    self.it.next();
                }
                Some(&(i, _)) => return &self.s[start..i],
                None => return &self.s[start..],
            }
        }
    }

    fn eat(&mut self, target: char) -> Result<(), Error> {
        match self.it.next() {
            Some((_, c)) if c == target => Ok(()),
            Some((i, c)) => {
                let m = format!(
                    "unexpected character at byte {}: expected `{}` but got `{}`",
                    i, target, c
                );
                Err(Error::config_parse(m.into()))
            }
            None => Err(Error::config_parse("unexpected EOF".into())),
        }
    }

    fn eat_if(&mut self, target: char) -> bool {
        match self.it.peek() {
            Some(&(_, c)) if c == target => {
                self.it.next();
                true
            }
            _ => false,
        }
    }

    fn keyword(&mut self) -> Option<&'a str> {
        let s = self.take_while(|c| match c {
            c if c.is_whitespace() => false,
            '=' => false,
            _ => true,
        });

        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    fn value(&mut self) -> Result<String, Error> {
        let value = if self.eat_if('\'') {
            let value = self.quoted_value()?;
            self.eat('\'')?;
            value
        } else {
            self.simple_value()?
        };

        Ok(value)
    }

    fn simple_value(&mut self) -> Result<String, Error> {
        let mut value = String::new();

        while let Some(&(_, c)) = self.it.peek() {
            if c.is_whitespace() {
                break;
            }

            self.it.next();
            if c == '\\' {
                if let Some((_, c2)) = self.it.next() {
                    value.push(c2);
                }
            } else {
                value.push(c);
            }
        }

        if value.is_empty() {
            return Err(Error::config_parse("unexpected EOF".into()));
        }

        Ok(value)
    }

    fn quoted_value(&mut self) -> Result<String, Error> {
        let mut value = String::new();

        while let Some(&(_, c)) = self.it.peek() {
            if c == '\'' {
                return Ok(value);
            }

            self.it.next();
            if c == '\\' {
                if let Some((_, c2)) = self.it.next() {
                    value.push(c2);
                }
            } else {
                value.push(c);
            }
        }

        Err(Error::config_parse(
            "unterminated quoted connection parameter value".into(),
        ))
    }

    fn parameter(&mut self) -> Result<Option<(&'a str, String)>, Error> {
        self.skip_ws();
        let keyword = match self.keyword() {
            Some(keyword) => keyword,
            None => return Ok(None),
        };
        self.skip_ws();
        self.eat('=')?;
        self.skip_ws();
        let value = self.value()?;

        Ok(Some((keyword, value)))
    }
}
