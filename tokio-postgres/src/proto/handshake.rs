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

use params::{ConnectParams, User};
use proto::client::Client;
use proto::codec::PostgresCodec;
use proto::connect::ConnectFuture;
use proto::connection::Connection;
use tls::TlsStream;
use {CancelData, Error, TlsMode};

#[derive(StateMachineFuture)]
pub enum Handshake {
    #[state_machine_future(start, transitions(SendingStartup))]
    Start {
        future: ConnectFuture,
        params: ConnectParams,
    },
    #[state_machine_future(transitions(ReadingAuth))]
    SendingStartup {
        future: sink::Send<Framed<Box<TlsStream>, PostgresCodec>>,
        user: User,
    },
    #[state_machine_future(transitions(ReadingInfo, SendingPassword, SendingSasl))]
    ReadingAuth {
        stream: Framed<Box<TlsStream>, PostgresCodec>,
        user: User,
    },
    #[state_machine_future(transitions(ReadingAuthCompletion))]
    SendingPassword {
        future: sink::Send<Framed<Box<TlsStream>, PostgresCodec>>,
    },
    #[state_machine_future(transitions(ReadingSasl))]
    SendingSasl {
        future: sink::Send<Framed<Box<TlsStream>, PostgresCodec>>,
        scram: ScramSha256,
    },
    #[state_machine_future(transitions(SendingSasl, ReadingAuthCompletion))]
    ReadingSasl {
        stream: Framed<Box<TlsStream>, PostgresCodec>,
        scram: ScramSha256,
    },
    #[state_machine_future(transitions(ReadingInfo))]
    ReadingAuthCompletion {
        stream: Framed<Box<TlsStream>, PostgresCodec>,
    },
    #[state_machine_future(transitions(Finished))]
    ReadingInfo {
        stream: Framed<Box<TlsStream>, PostgresCodec>,
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
            None => return Err(Error::missing_user()),
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
            ).map_err(Error::encode)?;
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
        let stream = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();
        transition!(ReadingAuth {
            stream,
            user: state.user,
        })
    }

    fn poll_reading_auth<'a>(
        state: &'a mut RentToOwn<'a, ReadingAuth>,
    ) -> Poll<AfterReadingAuth, Error> {
        let message = try_ready!(state.stream.poll().map_err(Error::io));
        let state = state.take();

        match message {
            Some(Message::AuthenticationOk) => transition!(ReadingInfo {
                stream: state.stream,
                cancel_data: None,
                parameters: HashMap::new(),
            }),
            Some(Message::AuthenticationCleartextPassword) => {
                let pass = state.user.password().ok_or_else(Error::missing_password)?;
                let mut buf = vec![];
                frontend::password_message(pass, &mut buf).map_err(Error::encode)?;
                transition!(SendingPassword {
                    future: state.stream.send(buf)
                })
            }
            Some(Message::AuthenticationMd5Password(body)) => {
                let pass = state.user.password().ok_or_else(Error::missing_password)?;
                let output = authentication::md5_hash(
                    state.user.name().as_bytes(),
                    pass.as_bytes(),
                    body.salt(),
                );
                let mut buf = vec![];
                frontend::password_message(&output, &mut buf).map_err(Error::encode)?;
                transition!(SendingPassword {
                    future: state.stream.send(buf)
                })
            }
            Some(Message::AuthenticationSasl(body)) => {
                let pass = state.user.password().ok_or_else(Error::missing_password)?;

                let mut has_scram = false;
                let mut has_scram_plus = false;
                let mut mechanisms = body.mechanisms();
                while let Some(mechanism) = mechanisms.next().map_err(Error::parse)? {
                    match mechanism {
                        sasl::SCRAM_SHA_256 => has_scram = true,
                        sasl::SCRAM_SHA_256_PLUS => has_scram_plus = true,
                        _ => {}
                    }
                }
                let channel_binding = state
                    .stream
                    .get_ref()
                    .tls_unique()
                    .map(ChannelBinding::tls_unique)
                    .or_else(|| {
                        state
                            .stream
                            .get_ref()
                            .tls_server_end_point()
                            .map(ChannelBinding::tls_server_end_point)
                    });

                let (channel_binding, mechanism) = if has_scram_plus {
                    match channel_binding {
                        Some(channel_binding) => (channel_binding, sasl::SCRAM_SHA_256_PLUS),
                        None => (ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
                    }
                } else if has_scram {
                    match channel_binding {
                        Some(_) => (ChannelBinding::unrequested(), sasl::SCRAM_SHA_256),
                        None => (ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
                    }
                } else {
                    return Err(Error::unsupported_authentication());
                };

                let mut scram = ScramSha256::new(pass.as_bytes(), channel_binding);

                let mut buf = vec![];
                frontend::sasl_initial_response(mechanism, scram.message(), &mut buf)
                    .map_err(Error::encode)?;

                transition!(SendingSasl {
                    future: state.stream.send(buf),
                    scram,
                })
            }
            Some(Message::AuthenticationKerberosV5)
            | Some(Message::AuthenticationScmCredential)
            | Some(Message::AuthenticationGss)
            | Some(Message::AuthenticationSspi) => Err(Error::unsupported_authentication()),
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_sending_password<'a>(
        state: &'a mut RentToOwn<'a, SendingPassword>,
    ) -> Poll<AfterSendingPassword, Error> {
        let stream = try_ready!(state.future.poll().map_err(Error::io));
        transition!(ReadingAuthCompletion { stream })
    }

    fn poll_sending_sasl<'a>(
        state: &'a mut RentToOwn<'a, SendingSasl>,
    ) -> Poll<AfterSendingSasl, Error> {
        let stream = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();
        transition!(ReadingSasl {
            stream,
            scram: state.scram
        })
    }

    fn poll_reading_sasl<'a>(
        state: &'a mut RentToOwn<'a, ReadingSasl>,
    ) -> Poll<AfterReadingSasl, Error> {
        let message = try_ready!(state.stream.poll().map_err(Error::io));
        let mut state = state.take();

        match message {
            Some(Message::AuthenticationSaslContinue(body)) => {
                state
                    .scram
                    .update(body.data())
                    .map_err(Error::authentication)?;
                let mut buf = vec![];
                frontend::sasl_response(state.scram.message(), &mut buf).map_err(Error::encode)?;
                transition!(SendingSasl {
                    future: state.stream.send(buf),
                    scram: state.scram,
                })
            }
            Some(Message::AuthenticationSaslFinal(body)) => {
                state
                    .scram
                    .finish(body.data())
                    .map_err(Error::authentication)?;
                transition!(ReadingAuthCompletion {
                    stream: state.stream,
                })
            }
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_reading_auth_completion<'a>(
        state: &'a mut RentToOwn<'a, ReadingAuthCompletion>,
    ) -> Poll<AfterReadingAuthCompletion, Error> {
        let message = try_ready!(state.stream.poll().map_err(Error::io));
        let state = state.take();

        match message {
            Some(Message::AuthenticationOk) => transition!(ReadingInfo {
                stream: state.stream,
                cancel_data: None,
                parameters: HashMap::new(),
            }),
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_reading_info<'a>(
        state: &'a mut RentToOwn<'a, ReadingInfo>,
    ) -> Poll<AfterReadingInfo, Error> {
        loop {
            let message = try_ready!(state.stream.poll().map_err(Error::io));
            match message {
                Some(Message::BackendKeyData(body)) => {
                    state.cancel_data = Some(CancelData {
                        process_id: body.process_id(),
                        secret_key: body.secret_key(),
                    });
                }
                Some(Message::ParameterStatus(body)) => {
                    state.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                }
                Some(Message::ReadyForQuery(_)) => {
                    let state = state.take();
                    let cancel_data = state.cancel_data.ok_or_else(|| {
                        Error::parse(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "BackendKeyData message missing",
                        ))
                    })?;
                    let (sender, receiver) = mpsc::unbounded();
                    let client = Client::new(sender);
                    let connection =
                        Connection::new(state.stream, cancel_data, state.parameters, receiver);
                    transition!(Finished((client, connection)))
                }
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(Message::NoticeResponse(_)) => {}
                Some(_) => return Err(Error::unexpected_message()),
                None => return Err(Error::closed()),
            }
        }
    }
}

impl HandshakeFuture {
    pub fn new(params: ConnectParams, tls: TlsMode) -> HandshakeFuture {
        Handshake::start(ConnectFuture::new(params.clone(), tls), params)
    }
}
