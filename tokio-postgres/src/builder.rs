use std::collections::hash_map::{self, HashMap};
use std::iter;
use std::str::{self, FromStr};
use tokio_io::{AsyncRead, AsyncWrite};

use crate::proto::HandshakeFuture;
use crate::{Error, Handshake, TlsMode};

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

    /// FIXME do we want this?
    pub fn iter(&self) -> Iter<'_> {
        Iter(self.params.iter())
    }

    pub fn handshake<S, T>(&self, stream: S, tls_mode: T) -> Handshake<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsMode<S>,
    {
        Handshake(HandshakeFuture::new(stream, tls_mode, self.params.clone()))
    }
}

impl FromStr for Builder {
    type Err = Error;

    fn from_str(s: &str) -> Result<Builder, Error> {
        let mut parser = Parser::new(s);
        let mut builder = Builder::new();

        while let Some((key, value)) = parser.parameter()? {
            builder.params.insert(key.to_string(), value);
        }

        Ok(builder)
    }
}

#[derive(Debug, Clone)]
pub struct Iter<'a>(hash_map::Iter<'a, String, String>);

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<(&'a str, &'a str)> {
        self.0.next().map(|(k, v)| (&**k, &**v))
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {
    fn len(&self) -> usize {
        self.0.len()
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
