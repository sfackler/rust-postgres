use std::borrow::Cow;
use std::collections::HashMap;
use std::iter;
use std::str::{self, FromStr};
use tokio_io::{AsyncRead, AsyncWrite};

use crate::proto::ConnectFuture;
use crate::{Connect, Error, TlsMode};

#[derive(Clone)]
pub struct Builder {
    params: HashMap<String, String>,
}

impl Default for Builder {
    fn default() -> Builder {
        Builder::new()
    }
}

impl Builder {
    pub fn new() -> Builder {
        let mut params = HashMap::new();
        params.insert("client_encoding".to_string(), "UTF8".to_string());
        params.insert("timezone".to_string(), "GMT".to_string());

        Builder { params }
    }

    pub fn user(&mut self, user: &str) -> &mut Builder {
        self.param("user", user)
    }

    pub fn dbname(&mut self, database: &str) -> &mut Builder {
        self.param("dbname", database)
    }

    pub fn password(&mut self, password: &str) -> &mut Builder {
        self.param("password", password)
    }

    pub fn param(&mut self, key: &str, value: &str) -> &mut Builder {
        self.params.insert(key.to_string(), value.to_string());
        self
    }

    pub fn connect<S, T>(&self, stream: S, tls_mode: T) -> Connect<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsMode<S>,
    {
        Connect(ConnectFuture::new(stream, tls_mode, self.params.clone()))
    }
}

impl FromStr for Builder {
    type Err = Error;

    fn from_str(s: &str) -> Result<Builder, Error> {
        let mut parser = Parser::new(s);
        let mut builder = Builder::new();

        while let Some((key, value)) = parser.parameter()? {
            builder.param(key, &value);
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
        while let Some(&(_, ' ')) = self.it.peek() {
            self.it.next();
        }
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
            ' ' | '=' => false,
            _ => true,
        });

        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    fn value(&mut self) -> Result<Cow<'a, str>, Error> {
        let raw = if self.eat_if('\'') {
            let s = self.take_while(|c| c != '\'');
            self.eat('\'')?;
            s
        } else {
            let s = self.take_while(|c| c != ' ');
            if s.is_empty() {
                return Err(Error::connection_syntax("unexpected EOF".into()));
            }
            s
        };

        self.unescape_value(raw)
    }

    fn unescape_value(&mut self, raw: &'a str) -> Result<Cow<'a, str>, Error> {
        if !raw.contains('\\') {
            return Ok(Cow::Borrowed(raw));
        }

        let mut s = String::with_capacity(raw.len());

        let mut it = raw.chars();
        while let Some(c) = it.next() {
            let to_push = if c == '\\' {
                match it.next() {
                    Some('\'') => '\'',
                    Some('\\') => '\\',
                    Some(c) => {
                        return Err(Error::connection_syntax(
                            format!("invalid escape `\\{}`", c).into(),
                        ));
                    }
                    None => return Err(Error::connection_syntax("unexpected EOF".into())),
                }
            } else {
                c
            };
            s.push(to_push);
        }

        Ok(Cow::Owned(s))
    }

    fn parameter(&mut self) -> Result<Option<(&'a str, Cow<'a, str>)>, Error> {
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
