use std::borrow::Cow;
use std::error;
#[cfg(all(feature = "runtime", unix))]
use std::ffi::OsStr;
use std::fmt;
use std::iter;
use std::mem;
#[cfg(all(feature = "runtime", unix))]
use std::os::unix::ffi::OsStrExt;
#[cfg(all(feature = "runtime", unix))]
use std::path::{Path, PathBuf};
use std::str::{self, FromStr};
use std::sync::Arc;
#[cfg(feature = "runtime")]
use std::time::Duration;
use tokio_io::{AsyncRead, AsyncWrite};

#[cfg(feature = "runtime")]
use crate::proto::ConnectFuture;
use crate::proto::{CancelQueryRawFuture, HandshakeFuture};
use crate::{CancelData, CancelQueryRaw, Error, Handshake, TlsMode};
#[cfg(feature = "runtime")]
use crate::{Connect, MakeTlsMode, Socket};

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

    fn param(&mut self, key: &str, value: &str) -> Result<(), Error> {
        match key {
            "user" => {
                self.user(&value);
            }
            "password" => {
                self.password(value);
            }
            "dbname" => {
                self.dbname(&value);
            }
            "options" => {
                self.options(&value);
            }
            "application_name" => {
                self.application_name(&value);
            }
            #[cfg(feature = "runtime")]
            "host" => {
                for host in value.split(',') {
                    self.host(host);
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
                    self.port(port);
                }
            }
            #[cfg(feature = "runtime")]
            "connect_timeout" => {
                let timeout = value
                    .parse::<i64>()
                    .map_err(|_| Error::config_parse(Box::new(InvalidValue("connect_timeout"))))?;
                if timeout > 0 {
                    self.connect_timeout(Duration::from_secs(timeout as u64));
                }
            }
            #[cfg(feature = "runtime")]
            "keepalives" => {
                let keepalives = value
                    .parse::<u64>()
                    .map_err(|_| Error::config_parse(Box::new(InvalidValue("keepalives"))))?;
                self.keepalives(keepalives != 0);
            }
            #[cfg(feature = "runtime")]
            "keepalives_idle" => {
                let keepalives_idle = value
                    .parse::<i64>()
                    .map_err(|_| Error::config_parse(Box::new(InvalidValue("keepalives_idle"))))?;
                if keepalives_idle > 0 {
                    self.keepalives_idle(Duration::from_secs(keepalives_idle as u64));
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
                        ))));
                    }
                };
                self.target_session_attrs(target_session_attrs);
            }
            key => {
                return Err(Error::config_parse(Box::new(UnknownOption(
                    key.to_string(),
                ))));
            }
        }

        Ok(())
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

    pub fn cancel_query_raw<S, T>(
        &self,
        stream: S,
        tls_mode: T,
        cancel_data: CancelData,
    ) -> CancelQueryRaw<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsMode<S>,
    {
        CancelQueryRaw(CancelQueryRawFuture::new(stream, tls_mode, cancel_data))
    }
}

impl FromStr for Config {
    type Err = Error;

    fn from_str(s: &str) -> Result<Config, Error> {
        match UrlParser::parse(s)? {
            Some(config) => Ok(config),
            None => Parser::parse(s),
        }
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
struct InvalidValue(&'static str);

impl fmt::Display for InvalidValue {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "invalid value for option `{}`", self.0)
    }
}

impl error::Error for InvalidValue {}

struct Parser<'a> {
    s: &'a str,
    it: iter::Peekable<str::CharIndices<'a>>,
}

impl<'a> Parser<'a> {
    fn parse(s: &'a str) -> Result<Config, Error> {
        let mut parser = Parser {
            s,
            it: s.char_indices().peekable(),
        };

        let mut config = Config::new();

        while let Some((key, value)) = parser.parameter()? {
            config.param(key, &value)?;
        }

        Ok(config)
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

// This is a pretty sloppy "URL" parser, but it matches the behavior of libpq, where things really aren't very strict
struct UrlParser<'a> {
    s: &'a str,
    config: Config,
}

impl<'a> UrlParser<'a> {
    fn parse(s: &'a str) -> Result<Option<Config>, Error> {
        let s = match Self::remove_url_prefix(s) {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut parser = UrlParser {
            s,
            config: Config::new(),
        };

        parser.parse_credentials()?;
        parser.parse_host()?;
        parser.parse_path()?;
        parser.parse_params()?;

        Ok(Some(parser.config))
    }

    fn remove_url_prefix(s: &str) -> Option<&str> {
        for prefix in &["postgres://", "postgresql://"] {
            if s.starts_with(prefix) {
                return Some(&s[prefix.len()..]);
            }
        }

        None
    }

    fn take_until(&mut self, end: &[char]) -> Option<&'a str> {
        match self.s.find(end) {
            Some(pos) => {
                let (head, tail) = self.s.split_at(pos);
                self.s = tail;
                Some(head)
            }
            None => None,
        }
    }

    fn take_all(&mut self) -> &'a str {
        mem::replace(&mut self.s, "")
    }

    fn eat_byte(&mut self) {
        self.s = &self.s[1..];
    }

    fn parse_credentials(&mut self) -> Result<(), Error> {
        let creds = match self.take_until(&['@']) {
            Some(creds) => creds,
            None => return Ok(()),
        };
        self.eat_byte();

        let mut it = creds.splitn(2, ':');
        let user = self.decode(it.next().unwrap())?;
        self.config.user(&user);

        if let Some(password) = it.next() {
            let password = Cow::from(percent_encoding::percent_decode(password.as_bytes()));
            self.config.password(password);
        }

        Ok(())
    }

    fn parse_host(&mut self) -> Result<(), Error> {
        let host = match self.take_until(&['/', '?']) {
            Some(host) => host,
            None => self.take_all(),
        };

        if host.is_empty() {
            return Ok(());
        }

        for chunk in host.split(',') {
            let (host, port) = if chunk.starts_with('[') {
                let idx = match chunk.find(']') {
                    Some(idx) => idx,
                    None => return Err(Error::config_parse(InvalidValue("host").into())),
                };

                let host = &chunk[1..idx];
                let remaining = &chunk[idx + 1..];
                let port = if remaining.starts_with(':') {
                    Some(&remaining[1..])
                } else if remaining.is_empty() {
                    None
                } else {
                    return Err(Error::config_parse(InvalidValue("host").into()));
                };

                (host, port)
            } else {
                let mut it = chunk.splitn(2, ':');
                (it.next().unwrap(), it.next())
            };

            self.host_param(host)?;
            let port = self.decode(port.unwrap_or("5432"))?;
            self.config.param("port", &port)?;
        }

        Ok(())
    }

    fn parse_path(&mut self) -> Result<(), Error> {
        if !self.s.starts_with('/') {
            return Ok(());
        }
        self.eat_byte();

        let dbname = match self.take_until(&['?']) {
            Some(dbname) => dbname,
            None => self.take_all(),
        };

        if !dbname.is_empty() {
            self.config.dbname(&self.decode(dbname)?);
        }

        Ok(())
    }

    fn parse_params(&mut self) -> Result<(), Error> {
        if !self.s.starts_with('?') {
            return Ok(());
        }
        self.eat_byte();

        while !self.s.is_empty() {
            let key = match self.take_until(&['=']) {
                Some(key) => self.decode(key)?,
                None => return Err(Error::config_parse("unterminated parameter".into())),
            };
            self.eat_byte();

            let value = match self.take_until(&['&']) {
                Some(value) => {
                    self.eat_byte();
                    value
                }
                None => self.take_all(),
            };

            if key == "host" {
                self.host_param(value)?;
            } else {
                let value = self.decode(value)?;
                self.config.param(&key, &value)?;
            }
        }

        Ok(())
    }

    #[cfg(all(feature = "runtime", unix))]
    fn host_param(&mut self, s: &str) -> Result<(), Error> {
        let decoded = Cow::from(percent_encoding::percent_decode(s.as_bytes()));
        if decoded.get(0) == Some(&b'/') {
            self.config.host_path(OsStr::from_bytes(&decoded));
        } else {
            let decoded = str::from_utf8(&decoded).map_err(|e| Error::config_parse(Box::new(e)))?;
            self.config.host(decoded);
        }

        Ok(())
    }

    #[cfg(not(all(feature = "runtime", unix)))]
    fn host_param(&mut self, s: &str) -> Result<(), Error> {
        let s = self.decode(s)?;
        self.config.param("host", &s)
    }

    fn decode(&self, s: &'a str) -> Result<Cow<'a, str>, Error> {
        percent_encoding::percent_decode(s.as_bytes())
            .decode_utf8()
            .map_err(|e| Error::config_parse(e.into()))
    }
}
