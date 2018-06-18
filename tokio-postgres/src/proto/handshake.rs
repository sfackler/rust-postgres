use fallible_iterator::FallibleIterator;
use futures::sink;
use futures::sync::mpsc;
use futures::{Future, Poll, Sink, Stream};
use postgres_protocol::authentication;
use postgres_protocol::authentication::sasl::{self, ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use state_machine_future::RentToOwn;
use std::collections::HashMap;
use std::io;
use tokio_codec::Framed;
use want;

use error::{self, Error};
use params::{ConnectParams, User};
use proto::client::Client;
use proto::codec::PostgresCodec;
use proto::connection::Connection;
use proto::socket::{ConnectFuture, Socket};
use {bad_response, disconnected, CancelData};

#[derive(StateMachineFuture)]
pub enum Handshake {
    #[state_machine_future(start, transitions(SendingStartup))]
    Start {
        future: ConnectFuture,
        params: ConnectParams,
    },
    #[state_machine_future(transitions(ReadingAuth))]
    SendingStartup {
        future: sink::Send<Framed<Socket, PostgresCodec>>,
        user: User,
    },
    #[state_machine_future(transitions(ReadingInfo, SendingPassword, SendingSasl))]
    ReadingAuth {
        stream: Framed<Socket, PostgresCodec>,
        user: User,
    },
    #[state_machine_future(transitions(ReadingAuthCompletion))]
    SendingPassword {
        future: sink::Send<Framed<Socket, PostgresCodec>>,
    },
    #[state_machine_future(transitions(ReadingSasl))]
    SendingSasl {
        future: sink::Send<Framed<Socket, PostgresCodec>>,
        scram: ScramSha256,
    },
    #[state_machine_future(transitions(SendingSasl, ReadingAuthCompletion))]
    ReadingSasl {
        stream: Framed<Socket, PostgresCodec>,
        scram: ScramSha256,
    },
    #[state_machine_future(transitions(ReadingInfo))]
    ReadingAuthCompletion {
        stream: Framed<Socket, PostgresCodec>,
    },
    #[state_machine_future(transitions(Finished))]
    ReadingInfo {
        stream: Framed<Socket, PostgresCodec>,
        cancel_data: Option<CancelData>,
        parameters: HashMap<String, String>,
    },
    #[state_machine_future(ready)]
    Finished((Client, Connection)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollHandshake for Handshake {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let stream = try_ready!(state.future.poll());
        let state = state.take();

        let user = match state.params.user() {
            Some(user) => user.clone(),
            None => {
                return Err(error::connect(
                    "user missing from connection parameters".into(),
                ))
            }
        };

        let mut buf = vec![];
        {
            let options = state
                .params
                .options()
                .iter()
                .map(|&(ref key, ref value)| (&**key, &**value));
            let client_encoding = Some(("client_encoding", "UTF8"));
            let timezone = Some(("timezone", "GMT"));
            let user = Some(("user", user.name()));
            let database = state.params.database().map(|s| ("database", s));

            frontend::startup_message(
                options
                    .chain(client_encoding)
                    .chain(timezone)
                    .chain(user)
                    .chain(database),
                &mut buf,
            )?;
        }

        let stream = Framed::new(stream, PostgresCodec);

        transition!(SendingStartup {
            future: stream.send(buf),
            user,
        })
    }

    fn poll_sending_startup<'a>(
        state: &'a mut RentToOwn<'a, SendingStartup>,
    ) -> Poll<AfterSendingStartup, Error> {
        let stream = try_ready!(state.future.poll());
        let state = state.take();
        transition!(ReadingAuth {
            stream,
            user: state.user,
        })
    }

    fn poll_reading_auth<'a>(
        state: &'a mut RentToOwn<'a, ReadingAuth>,
    ) -> Poll<AfterReadingAuth, Error> {
        let message = try_ready!(state.stream.poll());
        let state = state.take();

        match message {
            Some(Message::AuthenticationOk) => transition!(ReadingInfo {
                stream: state.stream,
                cancel_data: None,
                parameters: HashMap::new(),
            }),
            Some(Message::AuthenticationCleartextPassword) => {
                let pass = state.user.password().ok_or_else(missing_password)?;
                let mut buf = vec![];
                frontend::password_message(pass, &mut buf)?;
                transition!(SendingPassword {
                    future: state.stream.send(buf)
                })
            }
            Some(Message::AuthenticationMd5Password(body)) => {
                let pass = state.user.password().ok_or_else(missing_password)?;
                let output = authentication::md5_hash(
                    state.user.name().as_bytes(),
                    pass.as_bytes(),
                    body.salt(),
                );
                let mut buf = vec![];
                frontend::password_message(&output, &mut buf)?;
                transition!(SendingPassword {
                    future: state.stream.send(buf)
                })
            }
            Some(Message::AuthenticationSasl(body)) => {
                let pass = state.user.password().ok_or_else(missing_password)?;

                let mut has_scram = false;
                let mut mechanisms = body.mechanisms();
                while let Some(mechanism) = mechanisms.next()? {
                    match mechanism {
                        sasl::SCRAM_SHA_256 => has_scram = true,
                        _ => {}
                    }
                }

                if !has_scram {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "unsupported SASL authentication",
                    ).into());
                }

                let mut scram = ScramSha256::new(pass.as_bytes(), ChannelBinding::unsupported())?;

                let mut buf = vec![];
                frontend::sasl_initial_response(sasl::SCRAM_SHA_256, scram.message(), &mut buf)?;

                transition!(SendingSasl {
                    future: state.stream.send(buf),
                    scram,
                })
            }
            Some(Message::AuthenticationKerberosV5)
            | Some(Message::AuthenticationScmCredential)
            | Some(Message::AuthenticationGss)
            | Some(Message::AuthenticationSspi) => Err(io::Error::new(
                io::ErrorKind::Other,
                "unsupported authentication method",
            ).into()),
            Some(Message::ErrorResponse(body)) => Err(error::__db(body)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }

    fn poll_sending_password<'a>(
        state: &'a mut RentToOwn<'a, SendingPassword>,
    ) -> Poll<AfterSendingPassword, Error> {
        let stream = try_ready!(state.future.poll());
        transition!(ReadingAuthCompletion { stream })
    }

    fn poll_sending_sasl<'a>(
        state: &'a mut RentToOwn<'a, SendingSasl>,
    ) -> Poll<AfterSendingSasl, Error> {
        let stream = try_ready!(state.future.poll());
        let state = state.take();
        transition!(ReadingSasl {
            stream,
            scram: state.scram
        })
    }

    fn poll_reading_sasl<'a>(
        state: &'a mut RentToOwn<'a, ReadingSasl>,
    ) -> Poll<AfterReadingSasl, Error> {
        let message = try_ready!(state.stream.poll());
        let mut state = state.take();

        match message {
            Some(Message::AuthenticationSaslContinue(body)) => {
                state.scram.update(body.data())?;
                let mut buf = vec![];
                frontend::sasl_response(state.scram.message(), &mut buf)?;
                transition!(SendingSasl {
                    future: state.stream.send(buf),
                    scram: state.scram,
                })
            }
            Some(Message::AuthenticationSaslFinal(body)) => {
                state.scram.finish(body.data())?;
                transition!(ReadingAuthCompletion {
                    stream: state.stream,
                })
            }
            Some(Message::ErrorResponse(body)) => Err(error::__db(body)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }

    fn poll_reading_auth_completion<'a>(
        state: &'a mut RentToOwn<'a, ReadingAuthCompletion>,
    ) -> Poll<AfterReadingAuthCompletion, Error> {
        let message = try_ready!(state.stream.poll());
        let state = state.take();

        match message {
            Some(Message::AuthenticationOk) => transition!(ReadingInfo {
                stream: state.stream,
                cancel_data: None,
                parameters: HashMap::new(),
            }),
            Some(Message::ErrorResponse(body)) => Err(error::__db(body)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }

    fn poll_reading_info<'a>(
        state: &'a mut RentToOwn<'a, ReadingInfo>,
    ) -> Poll<AfterReadingInfo, Error> {
        loop {
            let message = try_ready!(state.stream.poll());
            match message {
                Some(Message::BackendKeyData(body)) => {
                    state.cancel_data = Some(CancelData {
                        process_id: body.process_id(),
                        secret_key: body.secret_key(),
                    });
                }
                Some(Message::ParameterStatus(body)) => {
                    state
                        .parameters
                        .insert(body.name()?.to_string(), body.value()?.to_string());
                }
                Some(Message::ReadyForQuery(_)) => {
                    let state = state.take();
                    let cancel_data = state.cancel_data.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "BackendKeyData message missing")
                    })?;
                    let (sender, receiver) = mpsc::unbounded();
                    let (giver, taker) = want::new();
                    let client = Client::new(sender, giver);
                    let connection = Connection::new(
                        state.stream,
                        cancel_data,
                        state.parameters,
                        receiver,
                        taker,
                    );
                    transition!(Finished((client, connection)))
                }
                Some(Message::ErrorResponse(body)) => return Err(error::__db(body)),
                Some(Message::NoticeResponse(_)) => {}
                Some(_) => return Err(bad_response()),
                None => return Err(disconnected()),
            }
        }
    }
}

impl HandshakeFuture {
    pub fn new(params: ConnectParams) -> HandshakeFuture {
        Handshake::start(Socket::connect(&params), params)
    }
}

fn missing_password() -> Error {
    error::connect("a password was requested but not provided".into())
}
