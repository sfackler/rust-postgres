use fallible_iterator::FallibleIterator;
use futures::sink;
use futures::sync::mpsc;
use futures::{try_ready, Future, Poll, Sink, Stream};
use postgres_protocol::authentication;
use postgres_protocol::authentication::sasl::{self, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::collections::HashMap;
use tokio_codec::Framed;
use tokio_io::{AsyncRead, AsyncWrite};

use crate::proto::{Client, Connection, PostgresCodec, TlsFuture};
use crate::{ChannelBinding, Config, Error, TlsMode};

#[derive(StateMachineFuture)]
pub enum ConnectRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    #[state_machine_future(start, transitions(SendingStartup))]
    Start {
        future: TlsFuture<S, T>,
        config: Config,
        idx: Option<usize>,
    },
    #[state_machine_future(transitions(ReadingAuth))]
    SendingStartup {
        future: sink::Send<Framed<T::Stream, PostgresCodec>>,
        config: Config,
        idx: Option<usize>,
        channel_binding: ChannelBinding,
    },
    #[state_machine_future(transitions(ReadingInfo, SendingPassword, SendingSasl))]
    ReadingAuth {
        stream: Framed<T::Stream, PostgresCodec>,
        config: Config,
        idx: Option<usize>,
        channel_binding: ChannelBinding,
    },
    #[state_machine_future(transitions(ReadingAuthCompletion))]
    SendingPassword {
        future: sink::Send<Framed<T::Stream, PostgresCodec>>,
        config: Config,
        idx: Option<usize>,
    },
    #[state_machine_future(transitions(ReadingSasl))]
    SendingSasl {
        future: sink::Send<Framed<T::Stream, PostgresCodec>>,
        scram: ScramSha256,
        config: Config,
        idx: Option<usize>,
    },
    #[state_machine_future(transitions(SendingSasl, ReadingAuthCompletion))]
    ReadingSasl {
        stream: Framed<T::Stream, PostgresCodec>,
        scram: ScramSha256,
        config: Config,
        idx: Option<usize>,
    },
    #[state_machine_future(transitions(ReadingInfo))]
    ReadingAuthCompletion {
        stream: Framed<T::Stream, PostgresCodec>,
        config: Config,
        idx: Option<usize>,
    },
    #[state_machine_future(transitions(Finished))]
    ReadingInfo {
        stream: Framed<T::Stream, PostgresCodec>,
        process_id: i32,
        secret_key: i32,
        parameters: HashMap<String, String>,
        config: Config,
        idx: Option<usize>,
    },
    #[state_machine_future(ready)]
    Finished((Client, Connection<T::Stream>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<S, T> PollConnectRaw<S, T> for ConnectRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<S, T>>) -> Poll<AfterStart<S, T>, Error> {
        let (stream, channel_binding) = try_ready!(state.future.poll());
        let state = state.take();

        let mut params = vec![("client_encoding", "UTF8"), ("timezone", "GMT")];
        if let Some(user) = &state.config.0.user {
            params.push(("user", &**user));
        }
        if let Some(dbname) = &state.config.0.dbname {
            params.push(("database", &**dbname));
        }
        if let Some(options) = &state.config.0.options {
            params.push(("options", &**options));
        }
        if let Some(application_name) = &state.config.0.application_name {
            params.push(("application_name", &**application_name));
        }

        let mut buf = vec![];
        frontend::startup_message(params, &mut buf).map_err(Error::encode)?;

        let stream = Framed::new(stream, PostgresCodec);

        transition!(SendingStartup {
            future: stream.send(buf),
            config: state.config,
            idx: state.idx,
            channel_binding,
        })
    }

    fn poll_sending_startup<'a>(
        state: &'a mut RentToOwn<'a, SendingStartup<S, T>>,
    ) -> Poll<AfterSendingStartup<S, T>, Error> {
        let stream = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();
        transition!(ReadingAuth {
            stream,
            config: state.config,
            idx: state.idx,
            channel_binding: state.channel_binding,
        })
    }

    fn poll_reading_auth<'a>(
        state: &'a mut RentToOwn<'a, ReadingAuth<S, T>>,
    ) -> Poll<AfterReadingAuth<S, T>, Error> {
        let message = try_ready!(state.stream.poll().map_err(Error::io));
        let state = state.take();

        match message {
            Some(Message::AuthenticationOk) => transition!(ReadingInfo {
                stream: state.stream,
                process_id: 0,
                secret_key: 0,
                parameters: HashMap::new(),
                config: state.config,
                idx: state.idx,
            }),
            Some(Message::AuthenticationCleartextPassword) => {
                let pass = state
                    .config
                    .0
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::config("password missing".into()))?;
                let mut buf = vec![];
                frontend::password_message(pass, &mut buf).map_err(Error::encode)?;
                transition!(SendingPassword {
                    future: state.stream.send(buf),
                    config: state.config,
                    idx: state.idx,
                })
            }
            Some(Message::AuthenticationMd5Password(body)) => {
                let user = state
                    .config
                    .0
                    .user
                    .as_ref()
                    .ok_or_else(|| Error::config("user missing".into()))?;
                let pass = state
                    .config
                    .0
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::config("password missing".into()))?;
                let output = authentication::md5_hash(user.as_bytes(), pass, body.salt());
                let mut buf = vec![];
                frontend::password_message(output.as_bytes(), &mut buf).map_err(Error::encode)?;
                transition!(SendingPassword {
                    future: state.stream.send(buf),
                    config: state.config,
                    idx: state.idx,
                })
            }
            Some(Message::AuthenticationSasl(body)) => {
                let pass = state
                    .config
                    .0
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::config("password missing".into()))?;

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

                let channel_binding = if let Some(tls_server_end_point) =
                    state.channel_binding.tls_server_end_point
                {
                    Some(sasl::ChannelBinding::tls_server_end_point(
                        tls_server_end_point,
                    ))
                } else {
                    None
                };

                let (channel_binding, mechanism) = if has_scram_plus {
                    match channel_binding {
                        Some(channel_binding) => (channel_binding, sasl::SCRAM_SHA_256_PLUS),
                        None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
                    }
                } else if has_scram {
                    match channel_binding {
                        Some(_) => (sasl::ChannelBinding::unrequested(), sasl::SCRAM_SHA_256),
                        None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
                    }
                } else {
                    return Err(Error::authentication(
                        "unsupported authentication method".into(),
                    ));
                };

                let scram = ScramSha256::new(pass, channel_binding);

                let mut buf = vec![];
                frontend::sasl_initial_response(mechanism, scram.message(), &mut buf)
                    .map_err(Error::encode)?;

                transition!(SendingSasl {
                    future: state.stream.send(buf),
                    scram,
                    config: state.config,
                    idx: state.idx,
                })
            }
            Some(Message::AuthenticationKerberosV5)
            | Some(Message::AuthenticationScmCredential)
            | Some(Message::AuthenticationGss)
            | Some(Message::AuthenticationSspi) => Err(Error::authentication(
                "unsupported authentication method".into(),
            )),
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_sending_password<'a>(
        state: &'a mut RentToOwn<'a, SendingPassword<S, T>>,
    ) -> Poll<AfterSendingPassword<S, T>, Error> {
        let stream = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();
        transition!(ReadingAuthCompletion {
            stream,
            config: state.config,
            idx: state.idx,
        })
    }

    fn poll_sending_sasl<'a>(
        state: &'a mut RentToOwn<'a, SendingSasl<S, T>>,
    ) -> Poll<AfterSendingSasl<S, T>, Error> {
        let stream = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();
        transition!(ReadingSasl {
            stream,
            scram: state.scram,
            config: state.config,
            idx: state.idx,
        })
    }

    fn poll_reading_sasl<'a>(
        state: &'a mut RentToOwn<'a, ReadingSasl<S, T>>,
    ) -> Poll<AfterReadingSasl<S, T>, Error> {
        let message = try_ready!(state.stream.poll().map_err(Error::io));
        let mut state = state.take();

        match message {
            Some(Message::AuthenticationSaslContinue(body)) => {
                state
                    .scram
                    .update(body.data())
                    .map_err(|e| Error::authentication(Box::new(e)))?;
                let mut buf = vec![];
                frontend::sasl_response(state.scram.message(), &mut buf).map_err(Error::encode)?;
                transition!(SendingSasl {
                    future: state.stream.send(buf),
                    scram: state.scram,
                    config: state.config,
                    idx: state.idx,
                })
            }
            Some(Message::AuthenticationSaslFinal(body)) => {
                state
                    .scram
                    .finish(body.data())
                    .map_err(|e| Error::authentication(Box::new(e)))?;
                transition!(ReadingAuthCompletion {
                    stream: state.stream,
                    config: state.config,
                    idx: state.idx,
                })
            }
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_reading_auth_completion<'a>(
        state: &'a mut RentToOwn<'a, ReadingAuthCompletion<S, T>>,
    ) -> Poll<AfterReadingAuthCompletion<S, T>, Error> {
        let message = try_ready!(state.stream.poll().map_err(Error::io));
        let state = state.take();

        match message {
            Some(Message::AuthenticationOk) => transition!(ReadingInfo {
                stream: state.stream,
                process_id: 0,
                secret_key: 0,
                parameters: HashMap::new(),
                config: state.config,
                idx: state.idx,
            }),
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_reading_info<'a>(
        state: &'a mut RentToOwn<'a, ReadingInfo<S, T>>,
    ) -> Poll<AfterReadingInfo<S, T>, Error> {
        loop {
            let message = try_ready!(state.stream.poll().map_err(Error::io));
            match message {
                Some(Message::BackendKeyData(body)) => {
                    state.process_id = body.process_id();
                    state.secret_key = body.secret_key();
                }
                Some(Message::ParameterStatus(body)) => {
                    state.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                }
                Some(Message::ReadyForQuery(_)) => {
                    let state = state.take();
                    let (sender, receiver) = mpsc::unbounded();
                    let client = Client::new(
                        sender,
                        state.process_id,
                        state.secret_key,
                        state.config,
                        state.idx,
                    );
                    let connection = Connection::new(state.stream, state.parameters, receiver);
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

impl<S, T> ConnectRawFuture<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    pub fn new(
        stream: S,
        tls_mode: T,
        config: Config,
        idx: Option<usize>,
    ) -> ConnectRawFuture<S, T> {
        ConnectRaw::start(TlsFuture::new(stream, tls_mode), config, idx)
    }
}
