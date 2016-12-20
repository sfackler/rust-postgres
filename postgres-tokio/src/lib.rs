extern crate postgres_shared;
extern crate postgres_protocol;
extern crate tokio_core;
extern crate tokio_dns;
extern crate tokio_uds;

#[macro_use]
extern crate futures;

use futures::{Future, IntoFuture, BoxFuture, Stream, Sink, Poll, StartSend};
use futures::future::Either;
use postgres_shared::params::{ConnectParams, IntoConnectParams};
use postgres_protocol::authentication;
use postgres_protocol::message::{backend, frontend};
use std::collections::HashMap;
use std::error::Error;
use std::io;
use tokio_core::reactor::Handle;

use stream::PostgresStream;

mod stream;

#[cfg(test)]
mod test;

#[derive(Debug)]
pub enum ConnectError {
    Params(Box<Error + Sync + Send>),
    Io(io::Error),
}

#[derive(Debug, Copy, Clone)]
pub struct CancelData {
    pub process_id: i32,
    pub secret_key: i32,
}

impl From<io::Error> for ConnectError {
    fn from(e: io::Error) -> ConnectError {
        ConnectError::Io(e)
    }
}

struct InnerConnectionState {
    parameters: HashMap<String, String>,
    cancel_data: CancelData,
}

struct InnerConnection {
    stream: PostgresStream,
    state: InnerConnectionState,
}

impl InnerConnection {
    fn read(self) -> BoxFuture<(backend::Message<Vec<u8>>, InnerConnection), (io::Error, InnerConnection)> {
        self.into_future()
            .then(|r| {
                let (m, mut s) = match r {
                    Ok((m, s)) => (m, s),
                    Err((e, s)) => return Either::A(Err((e, s)).into_future()),
                };

                match m {
                    Some(backend::Message::ParameterStatus(body)) => {
                        let name = match body.name() {
                            Ok(name) => name.to_owned(),
                            Err(e) => return Either::A(Err((e, s)).into_future()),
                        };
                        let value = match body.value() {
                            Ok(value) => value.to_owned(),
                            Err(e) => return Either::A(Err((e, s)).into_future()),
                        };
                        s.state.parameters.insert(name, value);
                        Either::B(s.read())
                    }
                    Some(backend::Message::NoticeResponse(_)) => {
                        // TODO forward the error
                        Either::B(s.read())
                    }
                    Some(m) => Either::A(Ok((m, s)).into_future()),
                    None => Either::A(Err((eof(), s)).into_future()),
                }
            })
            .boxed()
    }
}

impl Stream for InnerConnection {
    type Item = backend::Message<Vec<u8>>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<backend::Message<Vec<u8>>>, io::Error> {
        self.stream.poll()
    }
}

impl Sink for InnerConnection {
    type SinkItem = Vec<u8>;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Vec<u8>) -> StartSend<Vec<u8>, io::Error> {
        self.stream.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.stream.poll_complete()
    }
}

pub struct Connection(InnerConnection);

impl Connection {
    pub fn connect<T>(params: T, handle: &Handle) -> BoxFuture<Connection, ConnectError>
        where T: IntoConnectParams
    {
        let params = match params.into_connect_params() {
            Ok(params) => params,
            Err(e) => return futures::failed(ConnectError::Params(e)).boxed(),
        };

        stream::connect(params.host(), params.port(), handle)
            .map_err(ConnectError::Io)
            .map(|s| {
                Connection(InnerConnection {
                    stream: s,
                    state: InnerConnectionState {
                        parameters: HashMap::new(),
                        cancel_data: CancelData {
                            process_id: 0,
                            secret_key: 0,
                        }
                    }
                })
            })
            .and_then(|s| s.startup(params))
            .and_then(|(s, params)| s.handle_auth(params))
            .and_then(|s| s.finish_startup())
            .boxed()
    }

    fn startup(self, params: ConnectParams) -> BoxFuture<(Connection, ConnectParams), ConnectError> {
        let mut buf = vec![];
        let result = {
            let options = [("client_encoding", "UTF8"), ("timezone", "GMT")];
            let options = options.iter().cloned();
            let options = options.chain(params.user().map(|u| ("user", u.name())));
            let options = options.chain(params.database().map(|d| ("database", d)));
            let options = options.chain(params.options().iter().map(|e| (&*e.0, &*e.1)));

            frontend::startup_message(options, &mut buf)
        };

        result
            .into_future()
            .and_then(move |()| self.0.send(buf))
            .and_then(|s| s.flush())
            .map_err(ConnectError::Io)
            .map(move |s| (Connection(s), params))
            .boxed()
    }

    fn handle_auth(self, params: ConnectParams) -> BoxFuture<Connection, ConnectError> {
        self.0.read()
            .map_err(|e| e.0.into())
            .and_then(move |(m, s)| {
                let response = match m {
                    backend::Message::AuthenticationOk => Ok(None),
                    backend::Message::AuthenticationCleartextPassword => {
                        match params.user().and_then(|u| u.password()) {
                            Some(pass) => {
                                let mut buf = vec![];
                                frontend::password_message(pass, &mut buf)
                                    .map(|()| Some(buf))
                                    .map_err(Into::into)
                            }
                            None => {
                                Err(ConnectError::Params(
                                    "password was required but not provided".into()))
                            }
                        }
                    }
                    backend::Message::AuthenticationMd5Password(body) => {
                        match params.user().and_then(|u| u.password().map(|p| (u.name(), p))) {
                            Some((user, pass)) => {
                                let pass = authentication::md5_hash(user.as_bytes(),
                                                                    pass.as_bytes(),
                                                                    body.salt());
                                let mut buf = vec![];
                                frontend::password_message(&pass, &mut buf)
                                    .map(|()| Some(buf))
                                    .map_err(Into::into)
                            }
                            None => {
                                Err(ConnectError::Params(
                                    "password was required but not provided".into()))
                            }
                        }
                    }
                    _ => Err(bad_message()),
                };

                response.map(|m| (m, Connection(s)))
            })
            .and_then(|(m, s)| {
                match m {
                    Some(m) => Either::A(s.handle_auth_response(m)),
                    None => Either::B(Ok(s).into_future())
                }
            })
            .boxed()
    }

    fn handle_auth_response(self, message: Vec<u8>) -> BoxFuture<Connection, ConnectError> {
        self.0.send(message)
            .and_then(|s| s.flush())
            .and_then(|s| s.read().map_err(|e| e.0))
            .map_err(ConnectError::Io)
            .and_then(|(m, s)| {
                match m {
                    backend::Message::AuthenticationOk => Ok(Connection(s)),
                    _ => Err(bad_message()),
                }
            })
            .boxed()
    }

    fn finish_startup(self) -> BoxFuture<Connection, ConnectError> {
        self.0.read()
            .map_err(|e| ConnectError::Io(e.0))
            .and_then(|(m, mut s)| {
                match m {
                    backend::Message::BackendKeyData(body) => {
                        s.state.cancel_data.process_id = body.process_id();
                        s.state.cancel_data.secret_key = body.secret_key();
                        Either::A(Connection(s).finish_startup())
                    }
                    backend::Message::ReadyForQuery(_) => Either::B(Ok(Connection(s)).into_future()),
                    _ => Either::B(Err(bad_message()).into_future()),
                }
            })
            .boxed()
    }

    pub fn cancel_data(&self) -> CancelData {
        self.0.state.cancel_data
    }
}

fn bad_message<T>() -> T
    where T: From<io::Error>
{
    io::Error::new(io::ErrorKind::InvalidInput, "unexpected message").into()
}

fn eof<T>() -> T
    where T: From<io::Error>
{
    io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF").into()
}
