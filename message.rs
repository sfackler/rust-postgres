use std::str;
use std::io::Decorator;
use std::io::mem::{MemWriter, MemReader};
use std::mem;
use std::vec;

use super::types::Oid;

pub static PROTOCOL_VERSION: i32 = 0x0003_0000;
pub static CANCEL_CODE: i32 = 80877102;
pub static SSL_CODE: i32 = 80877103;

#[deriving(ToStr)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationKerberosV5,
    AuthenticationCleartextPassword,
    AuthenticationMD5Password {
        salt: ~[u8]
    },
    AuthenticationSCMCredential,
    AuthenticationGSS,
    AuthenticationSSPI,
    BackendKeyData {
        process_id: i32,
        secret_key: i32
    },
    BindComplete,
    CloseComplete,
    CommandComplete {
        tag: ~str
    },
    DataRow {
        row: ~[Option<~[u8]>]
    },
    EmptyQueryResponse,
    ErrorResponse {
        fields: ~[(u8, ~str)]
    },
    NoData,
    NoticeResponse {
        fields: ~[(u8, ~str)]
    },
    NotificationResponse {
        pid: i32,
        channel: ~str,
        payload: ~str
    },
    ParameterDescription {
        types: ~[Oid]
    },
    ParameterStatus {
        parameter: ~str,
        value: ~str
    },
    ParseComplete,
    PortalSuspended,
    ReadyForQuery {
        state: u8
    },
    RowDescription {
        descriptions: ~[RowDescriptionEntry]
    }
}

#[deriving(ToStr)]
pub struct RowDescriptionEntry {
    name: ~str,
    table_oid: Oid,
    column_id: i16,
    type_oid: Oid,
    type_size: i16,
    type_modifier: i32,
    format: i16
}

pub enum FrontendMessage<'a> {
    Bind {
        portal: &'a str,
        statement: &'a str,
        formats: &'a [i16],
        values: &'a [Option<~[u8]>],
        result_formats: &'a [i16]
    },
    CancelRequest {
        code: i32,
        process_id: i32,
        secret_key: i32,
    },
    Close {
        variant: u8,
        name: &'a str
    },
    Describe {
        variant: u8,
        name: &'a str
    },
    Execute {
        portal: &'a str,
        max_rows: i32
    },
    Parse {
        name: &'a str,
        query: &'a str,
        param_types: &'a [i32]
    },
    PasswordMessage {
        password: &'a str
    },
    Query {
        query: &'a str
    },
    StartupMessage {
        version: i32,
        parameters: &'a [(~str, ~str)]
    },
    SslRequest {
        code: i32
    },
    Sync,
    Terminate
}

trait WriteCStr {
    fn write_cstr(&mut self, s: &str);
}

impl<W: Writer> WriteCStr for W {
    fn write_cstr(&mut self, s: &str) {
        self.write(s.as_bytes());
        self.write_u8(0);
    }
}

pub trait WriteMessage {
    fn write_message(&mut self, &FrontendMessage);
}

impl<W: Writer> WriteMessage for W {
    fn write_message(&mut self, message: &FrontendMessage) {
        debug!("Writing message {:?}", message);
        let mut buf = MemWriter::new();
        let mut ident = None;

        match *message {
            Bind { portal, statement, formats, values, result_formats } => {
                ident = Some('B');
                buf.write_cstr(portal);
                buf.write_cstr(statement);

                buf.write_be_i16(formats.len() as i16);
                for format in formats.iter() {
                    buf.write_be_i16(*format);
                }

                buf.write_be_i16(values.len() as i16);
                for value in values.iter() {
                    match *value {
                        None => {
                            buf.write_be_i32(-1);
                        }
                        Some(ref value) => {
                            buf.write_be_i32(value.len() as i32);
                            buf.write(*value);
                        }
                    }
                }

                buf.write_be_i16(result_formats.len() as i16);
                for format in result_formats.iter() {
                    buf.write_be_i16(*format);
                }
            }
            CancelRequest { code, process_id, secret_key } => {
                buf.write_be_i32(code);
                buf.write_be_i32(process_id);
                buf.write_be_i32(secret_key);
            }
            Close { variant, name } => {
                ident = Some('C');
                buf.write_u8(variant);
                buf.write_cstr(name);
            }
            Describe { variant, name } => {
                ident = Some('D');
                buf.write_u8(variant);
                buf.write_cstr(name);
            }
            Execute { portal, max_rows } => {
                ident = Some('E');
                buf.write_cstr(portal);
                buf.write_be_i32(max_rows);
            }
            Parse { name, query, param_types } => {
                ident = Some('P');
                buf.write_cstr(name);
                buf.write_cstr(query);
                buf.write_be_i16(param_types.len() as i16);
                for ty in param_types.iter() {
                    buf.write_be_i32(*ty);
                }
            }
            PasswordMessage { password } => {
                ident = Some('p');
                buf.write_cstr(password);
            }
            Query { query } => {
                ident = Some('Q');
                buf.write_cstr(query);
            }
            StartupMessage { version, parameters } => {
                buf.write_be_i32(version);
                for &(ref k, ref v) in parameters.iter() {
                    buf.write_cstr(k.as_slice());
                    buf.write_cstr(v.as_slice());
                }
                buf.write_u8(0);
            }
            SslRequest { code } => buf.write_be_i32(code),
            Sync => {
                ident = Some('S');
            }
            Terminate => {
                ident = Some('X');
            }
        }

        match ident {
            Some(ident) => self.write_u8(ident as u8),
            None => ()
        }

        // add size of length value
        self.write_be_i32((buf.inner_ref().len() + mem::size_of::<i32>())
                           as i32);
        self.write(buf.inner());
    }
}

trait ReadCStr {
    fn read_cstr(&mut self) -> ~str;
}

impl<R: Buffer> ReadCStr for R {
    fn read_cstr(&mut self) -> ~str {
        let mut buf = self.read_until(0).unwrap();
        buf.pop();
        str::from_utf8_owned(buf)
    }
}

pub trait ReadMessage {
    fn read_message(&mut self) -> BackendMessage;
}

impl<R: Reader> ReadMessage for R {
    fn read_message(&mut self) -> BackendMessage {
        debug!("Reading message");

        let ident = self.read_u8();
        // subtract size of length value
        let len = self.read_be_i32() as uint - mem::size_of::<i32>();
        let mut buf = MemReader::new(self.read_bytes(len));

        let ret = match ident as char {
            '1' => ParseComplete,
            '2' => BindComplete,
            '3' => CloseComplete,
            'A' => NotificationResponse {
                pid: buf.read_be_i32(),
                channel: buf.read_cstr(),
                payload: buf.read_cstr()
            },
            'C' => CommandComplete { tag: buf.read_cstr() },
            'D' => read_data_row(&mut buf),
            'E' => ErrorResponse { fields: read_fields(&mut buf) },
            'I' => EmptyQueryResponse,
            'K' => BackendKeyData {
                process_id: buf.read_be_i32(),
                secret_key: buf.read_be_i32()
            },
            'n' => NoData,
            'N' => NoticeResponse { fields: read_fields(&mut buf) },
            'R' => read_auth_message(&mut buf),
            's' => PortalSuspended,
            'S' => ParameterStatus {
                parameter: buf.read_cstr(),
                value: buf.read_cstr()
            },
            't' => read_parameter_description(&mut buf),
            'T' => read_row_description(&mut buf),
            'Z' => ReadyForQuery { state: buf.read_u8() },
            ident => fail!("Unknown message identifier `{}`", ident)
        };
        assert!(buf.eof());
        debug!("Read message {:?}", ret);
        ret
    }
}

fn read_fields(buf: &mut MemReader) -> ~[(u8, ~str)] {
    let mut fields = ~[];
    loop {
        let ty = buf.read_u8();
        if ty == 0 {
            break;
        }

        fields.push((ty, buf.read_cstr()));
    }

    fields
}

fn read_data_row(buf: &mut MemReader) -> BackendMessage {
    let len = buf.read_be_i16() as uint;
    let mut values = vec::with_capacity(len);

    for _ in range(0, len) {
        let val = match buf.read_be_i32() {
            -1 => None,
            len => Some(buf.read_bytes(len as uint))
        };
        values.push(val);
    }

    DataRow { row: values }
}

fn read_auth_message(buf: &mut MemReader) -> BackendMessage {
    match buf.read_be_i32() {
        0 => AuthenticationOk,
        2 => AuthenticationKerberosV5,
        3 => AuthenticationCleartextPassword,
        5 => AuthenticationMD5Password { salt: buf.read_bytes(4) },
        6 => AuthenticationSCMCredential,
        7 => AuthenticationGSS,
        9 => AuthenticationSSPI,
        val => fail!("Invalid authentication identifier `{}`", val)
    }
}

fn read_parameter_description(buf: &mut MemReader) -> BackendMessage {
    let len = buf.read_be_i16() as uint;
    let mut types = vec::with_capacity(len);

    for _ in range(0, len) {
        types.push(buf.read_be_i32());
    }

    ParameterDescription { types: types }
}

fn read_row_description(buf: &mut MemReader) -> BackendMessage {
    let len = buf.read_be_i16() as uint;
    let mut types = vec::with_capacity(len);

    for _ in range(0, len) {
        types.push(RowDescriptionEntry {
            name: buf.read_cstr(),
            table_oid: buf.read_be_i32(),
            column_id: buf.read_be_i16(),
            type_oid: buf.read_be_i32(),
            type_size: buf.read_be_i16(),
            type_modifier: buf.read_be_i32(),
            format: buf.read_be_i16()
        });
    }

    RowDescription { descriptions: types }
}
