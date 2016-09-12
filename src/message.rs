use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use std::io;

use types::Oid;

pub enum Backend {
    AuthenticationCleartextPassword,
    AuthenticationGSS,
    AuthenticationKerberosV5,
    AuthenticationMD5Password {
        salt: [u8; 4],
    },
    AuthenticationOk,
    AuthenticationSCMCredential,
    AuthenticationSSPI,
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    BindComplete,
    CloseComplete,
    CommandComplete {
        tag: String,
    },
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    CopyInResponse {
        format: u8,
        column_formats: Vec<u16>,
    },
    CopyOutResponse {
        format: u8,
        column_formats: Vec<u16>,
    },
    DataRow {
        row: Vec<Option<Vec<u8>>>,
    },
    EmptyQueryResponse,
    ErrorResponse {
        fields: Vec<(u8, String)>,
    },
    NoData,
    NoticeResponse {
        fields: Vec<(u8, String)>,
    },
    NotificationResponse {
        process_id: i32,
        channel: String,
        payload: String,
    },
    ParameterDescription {
        types: Vec<Oid>,
    },
    ParameterStatus {
        parameter: String,
        value: String,
    },
    ParseComplete,
    PortalSuspended,
    ReadyForQuery {
        _state: u8,
    },
    RowDescription {
        descriptions: Vec<RowDescriptionEntry>,
    },
}

impl Backend {
    pub fn convert(message: Message) -> io::Result<Backend> {
        let ret = match message {
            Message::AuthenticationCleartextPassword => Backend::AuthenticationCleartextPassword,
            Message::AuthenticationGss => Backend::AuthenticationGSS,
            Message::AuthenticationKerberosV5 => Backend::AuthenticationKerberosV5,
            Message::AuthenticationMd55Password(body) => {
                Backend::AuthenticationMD5Password { salt: body.salt() }
            }
            Message::AuthenticationOk => Backend::AuthenticationOk,
            Message::AuthenticationScmCredential => Backend::AuthenticationSCMCredential,
            Message::AuthenticationSspi => Backend::AuthenticationSSPI,
            Message::BackendKeyData(body) => {
                Backend::BackendKeyData {
                    process_id: body.process_id(),
                    secret_key: body.secret_key(),
                }
            }
            Message::BindComplete => Backend::BindComplete,
            Message::CloseComplete => Backend::CloseComplete,
            Message::CommandComplete(body) => {
                Backend::CommandComplete {
                    tag: body.tag().to_owned()
                }
            }
            Message::CopyData(body) => Backend::CopyData { data: body.data().to_owned() },
            Message::CopyDone => Backend::CopyDone,
            Message::CopyInResponse(body) => {
                Backend::CopyInResponse {
                    format: body.format(),
                    column_formats: try!(body.column_formats().collect()),
                }
            }
            Message::CopyOutResponse(body) => {
                Backend::CopyOutResponse {
                    format: body.format(),
                    column_formats: try!(body.column_formats().collect()),
                }
            }
            Message::DataRow(body) => {
                Backend::DataRow {
                    row: try!(body.values().map(|r| r.map(|d| d.to_owned())).collect()),
                }
            }
            Message::EmptyQueryResponse => Backend::EmptyQueryResponse,
            Message::ErrorResponse(body) => {
                Backend::ErrorResponse {
                    fields: try!(body.fields().map(|f| (f.type_(), f.value().to_owned())).collect()),
                }
            }
            Message::NoData => Backend::NoData,
            Message::NoticeResponse(body) => {
                Backend::NoticeResponse {
                    fields: try!(body.fields().map(|f| (f.type_(), f.value().to_owned())).collect()),
                }
            }
            Message::NotificationResponse(body) => {
                Backend::NotificationResponse {
                    process_id: body.process_id(),
                    channel: body.channel().to_owned(),
                    payload: body.message().to_owned(),
                }
            }
            Message::ParameterDescription(body) => {
                Backend::ParameterDescription {
                    types: try!(body.parameters().collect()),
                }
            }
            Message::ParameterStatus(body) => {
                Backend::ParameterStatus {
                    parameter: body.name().to_owned(),
                    value: body.value().to_owned(),
                }
            }
            Message::ParseComplete => Backend::ParseComplete,
            Message::PortalSuspended => Backend::PortalSuspended,
            Message::ReadyForQuery(body) => Backend::ReadyForQuery { _state: body.status() },
            Message::RowDescription(body) => {
                let fields = body.fields()
                    .map(|f| {
                        RowDescriptionEntry {
                            name: f.name().to_owned(),
                            table_oid: f.table_oid(),
                            column_id: f.column_id(),
                            type_oid: f.type_oid(),
                            type_size: f.type_size(),
                            type_modifier: f.type_modifier(),
                            format: f.format(),
                        }
                    });
                Backend::RowDescription {
                    descriptions: try!(fields.collect()),
                }
            }
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "unknown message type")),
        };

        Ok(ret)
    }
}

pub struct RowDescriptionEntry {
    pub name: String,
    pub table_oid: Oid,
    pub column_id: i16,
    pub type_oid: Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}
