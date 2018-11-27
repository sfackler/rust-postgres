use std::collections::HashMap;
use tokio_io::{AsyncRead, AsyncWrite};

use proto::ConnectFuture;
use {Connect, TlsMode};

#[derive(Clone)]
pub struct Builder {
    params: HashMap<String, String>,
    password: Option<String>,
}

impl Builder {
    pub fn new() -> Builder {
        let mut params = HashMap::new();
        params.insert("client_encoding".to_string(), "UTF8".to_string());
        params.insert("timezone".to_string(), "GMT".to_string());

        Builder {
            params,
            password: None,
        }
    }

    pub fn user(&mut self, user: &str) -> &mut Builder {
        self.param("user", user)
    }

    pub fn database(&mut self, database: &str) -> &mut Builder {
        self.param("database", database)
    }

    pub fn param(&mut self, key: &str, value: &str) -> &mut Builder {
        self.params.insert(key.to_string(), value.to_string());
        self
    }

    pub fn password(&mut self, password: &str) -> &mut Builder {
        self.password = Some(password.to_string());
        self
    }

    pub fn connect<S, T>(&self, stream: S, tls_mode: T) -> Connect<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsMode<S>,
    {
        Connect(ConnectFuture::new(
            stream,
            tls_mode,
            self.password.clone(),
            self.params.clone(),
        ))
    }
}
