use std::collections::HashMap;
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
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Host {
    Tcp(String),
    #[cfg(unix)]
    Unix(PathBuf),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Inner {
    pub(crate) params: HashMap<String, String>,
    pub(crate) password: Option<Vec<u8>>,
    #[cfg(feature = "runtime")]
    pub(crate) host: Vec<Host>,
    #[cfg(feature = "runtime")]
    pub(crate) port: Vec<u16>,
    #[cfg(feature = "runtime")]
    pub(crate) connect_timeout: Option<Duration>,
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
        let mut params = HashMap::new();
        params.insert("client_encoding".to_string(), "UTF8".to_string());
        params.insert("timezone".to_string(), "GMT".to_string());

        Config(Arc::new(Inner {
            params,
            password: None,
            #[cfg(feature = "runtime")]
            host: vec![],
            #[cfg(feature = "runtime")]
            port: vec![],
            #[cfg(feature = "runtime")]
            connect_timeout: None,
        }))
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

    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        Arc::make_mut(&mut self.0).password = Some(password.as_ref().to_vec());
        self
    }

    pub fn param(&mut self, key: &str, value: &str) -> &mut Config {
        Arc::make_mut(&mut self.0)
            .params
            .insert(key.to_string(), value.to_string());
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
                "password" => {
                    builder.password(value);
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
                            port.parse().map_err(Error::invalid_port)?
                        };
                        builder.port(port);
                    }
                }
                #[cfg(feature = "runtime")]
                "connect_timeout" => {
                    let timeout = value
                        .parse::<i64>()
                        .map_err(Error::invalid_connect_timeout)?;
                    if timeout > 0 {
                        builder.connect_timeout(Duration::from_secs(timeout as u64));
                    }
                }
                key => {
                    builder.param(key, &value);
                }
            }
        }

        Ok(builder)
    }
}

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
                Err(Error::connection_syntax(m.into()))
            }
            None => Err(Error::connection_syntax("unexpected EOF".into())),
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
            return Err(Error::connection_syntax("unexpected EOF".into()));
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

        Err(Error::connection_syntax(
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
