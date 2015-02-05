#![no_implicit_prelude]

use std::result::Result::{Ok, Err};
use std::option::Option::{self, None, Some};
use std::vec::Vec;
use std::string::String;
use std::str::StrExt;
use std::slice::SliceExt;

use std::old_io::{self, IoResult, IoError, OtherIoError, ByRefReader, Buffer};
use std::old_io::util::LimitReader;
use std::mem;
use byteorder::{BigEndian, ReaderBytesExt, WriterBytesExt};

use io::Timeout;
use types::Oid;

use self::BackendMessage::*;
use self::FrontendMessage::*;

pub const PROTOCOL_VERSION: u32 = 0x0003_0000;
pub const CANCEL_CODE: u32 = 80877102;
pub const SSL_CODE: u32 = 80877103;

pub enum BackendMessage {
    AuthenticationCleartextPassword,
    AuthenticationGSS,
    AuthenticationKerberosV5,
    AuthenticationMD5Password {
        salt: [u8; 4]
    },
    AuthenticationOk,
    AuthenticationSCMCredential,
    AuthenticationSSPI,
    BackendKeyData {
        process_id: u32,
        secret_key: u32
    },
    BindComplete,
    CloseComplete,
    CommandComplete {
        tag: String,
    },
    CopyInResponse {
        format: u8,
        column_formats: Vec<u16>,
    },
    DataRow {
        row: Vec<Option<Vec<u8>>>
    },
    EmptyQueryResponse,
    ErrorResponse {
        fields: Vec<(u8, String)>
    },
    NoData,
    NoticeResponse {
        fields: Vec<(u8, String)>
    },
    NotificationResponse {
        pid: u32,
        channel: String,
        payload: String,
    },
    ParameterDescription {
        types: Vec<Oid>
    },
    ParameterStatus {
        parameter: String,
        value: String,
    },
    ParseComplete,
    PortalSuspended,
    ReadyForQuery {
        _state: u8
    },
    RowDescription {
        descriptions: Vec<RowDescriptionEntry>
    }
}

pub struct RowDescriptionEntry {
    pub name: String,
    pub table_oid: Oid,
    pub column_id: i16,
    pub type_oid: Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16
}

pub enum FrontendMessage<'a> {
    Bind {
        portal: &'a str,
        statement: &'a str,
        formats: &'a [i16],
        values: &'a [Option<Vec<u8>>],
        result_formats: &'a [i16]
    },
    CancelRequest {
        code: u32,
        process_id: u32,
        secret_key: u32,
    },
    Close {
        variant: u8,
        name: &'a str
    },
    CopyData {
        data: &'a [u8],
    },
    CopyDone,
    CopyFail {
        message: &'a str
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
        param_types: &'a [Oid]
    },
    PasswordMessage {
        password: &'a str
    },
    Query {
        query: &'a str
    },
    SslRequest {
        code: u32
    },
    StartupMessage {
        version: u32,
        parameters: &'a [(String, String)]
    },
    Sync,
    Terminate
}

#[doc(hidden)]
trait WriteCStr {
    fn write_cstr(&mut self, s: &str) -> IoResult<()>;
}

impl<W: old_io::Writer> WriteCStr for W {
    fn write_cstr(&mut self, s: &str) -> IoResult<()> {
        try!(self.write_all(s.as_bytes()));
        self.write_u8(0)
    }
}

#[doc(hidden)]
pub trait WriteMessage {
    fn write_message(&mut self, &FrontendMessage) -> IoResult<()> ;
}

impl<W: old_io::Writer> WriteMessage for W {
    fn write_message(&mut self, message: &FrontendMessage) -> IoResult<()> {
        let mut buf = vec![];
        let mut ident = None;

        match *message {
            Bind { portal, statement, formats, values, result_formats } => {
                ident = Some(b'B');
                try!(buf.write_cstr(portal));
                try!(buf.write_cstr(statement));

                try!(buf.write_i16::<BigEndian>(formats.len() as i16));
                for format in formats.iter() {
                    try!(buf.write_i16::<BigEndian>(*format));
                }

                try!(buf.write_i16::<BigEndian>(values.len() as i16));
                for value in values.iter() {
                    match *value {
                        None => try!(buf.write_i32::<BigEndian>(-1)),
                        Some(ref value) => {
                            try!(buf.write_i32::<BigEndian>(value.len() as i32));
                            try!(old_io::Writer::write_all(&mut buf, &**value));
                        }
                    }
                }

                try!(buf.write_i16::<BigEndian>(result_formats.len() as i16));
                for format in result_formats.iter() {
                    try!(buf.write_i16::<BigEndian>(*format));
                }
            }
            CancelRequest { code, process_id, secret_key } => {
                try!(buf.write_u32::<BigEndian>(code));
                try!(buf.write_u32::<BigEndian>(process_id));
                try!(buf.write_u32::<BigEndian>(secret_key));
            }
            Close { variant, name } => {
                ident = Some(b'C');
                try!(buf.write_u8(variant));
                try!(buf.write_cstr(name));
            }
            CopyData { data } => {
                ident = Some(b'd');
                try!(old_io::Writer::write_all(&mut buf, data));
            }
            CopyDone => ident = Some(b'c'),
            CopyFail { message } => {
                ident = Some(b'f');
                try!(buf.write_cstr(message));
            }
            Describe { variant, name } => {
                ident = Some(b'D');
                try!(buf.write_u8(variant));
                try!(buf.write_cstr(name));
            }
            Execute { portal, max_rows } => {
                ident = Some(b'E');
                try!(buf.write_cstr(portal));
                try!(buf.write_i32::<BigEndian>(max_rows));
            }
            Parse { name, query, param_types } => {
                ident = Some(b'P');
                try!(buf.write_cstr(name));
                try!(buf.write_cstr(query));
                try!(buf.write_i16::<BigEndian>(param_types.len() as i16));
                for ty in param_types.iter() {
                    try!(buf.write_u32::<BigEndian>(*ty));
                }
            }
            PasswordMessage { password } => {
                ident = Some(b'p');
                try!(buf.write_cstr(password));
            }
            Query { query } => {
                ident = Some(b'Q');
                try!(buf.write_cstr(query));
            }
            StartupMessage { version, parameters } => {
                try!(buf.write_u32::<BigEndian>(version));
                for &(ref k, ref v) in parameters {
                    try!(buf.write_cstr(&**k));
                    try!(buf.write_cstr(&**v));
                }
                try!(buf.write_u8(0));
            }
            SslRequest { code } => try!(buf.write_u32::<BigEndian>(code)),
            Sync => ident = Some(b'S'),
            Terminate => ident = Some(b'X'),
        }

        if let Some(ident) = ident {
            try!(self.write_u8(ident));
        }

        // add size of length value
        try!(self.write_i32::<BigEndian>((buf.len() + mem::size_of::<i32>()) as i32));
        try!(self.write_all(&*buf));

        Ok(())
    }
}

#[doc(hidden)]
trait ReadCStr {
    fn read_cstr(&mut self) -> IoResult<String>;
}

impl<R: Buffer> ReadCStr for R {
    fn read_cstr(&mut self) -> IoResult<String> {
        let mut buf = try!(self.read_until(0));
        buf.pop();
        String::from_utf8(buf).map_err(|_| IoError {
            kind: OtherIoError,
            desc: "Received a non-utf8 string from server",
            detail: None
        })
    }
}

#[doc(hidden)]
pub trait ReadMessage {
    fn read_message(&mut self) -> IoResult<BackendMessage>;
}

impl<R: Buffer+Timeout> ReadMessage for R {
    fn read_message(&mut self) -> IoResult<BackendMessage> {
        // The first byte read is a bit complex to make
        // Notifications::next_block_for work.
        let ident = self.read_u8();
        // At this point we've got to turn off any read timeout to prevent
        // stream desynchronization. We're assuming that if we've got the first
        // byte, there's more stuff to follow.
        self.set_read_timeout(None);
        let ident = try!(ident);

        // subtract size of length value
        let len = try!(self.read_u32::<BigEndian>()) as usize - mem::size_of::<i32>();
        let mut rdr = LimitReader::new(self.by_ref(), len);

        let ret = match ident {
            b'1' => ParseComplete,
            b'2' => BindComplete,
            b'3' => CloseComplete,
            b'A' => NotificationResponse {
                pid: try!(rdr.read_u32::<BigEndian>()),
                channel: try!(rdr.read_cstr()),
                payload: try!(rdr.read_cstr())
            },
            b'C' => CommandComplete { tag: try!(rdr.read_cstr()) },
            b'D' => try!(read_data_row(&mut rdr)),
            b'E' => ErrorResponse { fields: try!(read_fields(&mut rdr)) },
            b'G' => {
                let format = try!(rdr.read_u8());
                let mut column_formats = vec![];
                for _ in 0..try!(rdr.read_u16::<BigEndian>()) {
                    column_formats.push(try!(rdr.read_u16::<BigEndian>()));
                }
                CopyInResponse {
                    format: format,
                    column_formats: column_formats,
                }
            }
            b'I' => EmptyQueryResponse,
            b'K' => BackendKeyData {
                process_id: try!(rdr.read_u32::<BigEndian>()),
                secret_key: try!(rdr.read_u32::<BigEndian>())
            },
            b'n' => NoData,
            b'N' => NoticeResponse { fields: try!(read_fields(&mut rdr)) },
            b'R' => try!(read_auth_message(&mut rdr)),
            b's' => PortalSuspended,
            b'S' => ParameterStatus {
                parameter: try!(rdr.read_cstr()),
                value: try!(rdr.read_cstr())
            },
            b't' => try!(read_parameter_description(&mut rdr)),
            b'T' => try!(read_row_description(&mut rdr)),
            b'Z' => ReadyForQuery { _state: try!(rdr.read_u8()) },
            ident => return Err(IoError {
                kind: OtherIoError,
                desc: "Unexpected message tag",
                detail: Some(format!("got {}", ident)),
            })
        };
        if rdr.limit() != 0 {
            return Err(IoError {
                kind: OtherIoError,
                desc: "didn't read entire message",
                detail: None,
            });
        }
        Ok(ret)
    }
}

fn read_fields<R: Buffer>(buf: &mut R) -> IoResult<Vec<(u8, String)>> {
    let mut fields = vec![];
    loop {
        let ty = try!(buf.read_u8());
        if ty == 0 {
            break;
        }

        fields.push((ty, try!(buf.read_cstr())));
    }

    Ok(fields)
}

fn read_data_row<R: Buffer>(buf: &mut R) -> IoResult<BackendMessage> {
    let len = try!(buf.read_be_u16()) as usize;
    let mut values = Vec::with_capacity(len);

    for _ in 0..len {
        let val = match try!(buf.read_be_i32()) {
            -1 => None,
            len => Some(try!(buf.read_exact(len as usize)))
        };
        values.push(val);
    }

    Ok(DataRow { row: values })
}

fn read_auth_message<R: Buffer>(buf: &mut R) -> IoResult<BackendMessage> {
    Ok(match try!(buf.read_be_i32()) {
        0 => AuthenticationOk,
        2 => AuthenticationKerberosV5,
        3 => AuthenticationCleartextPassword,
        5 => {
            let mut salt = [0; 4];
            try!(buf.read_at_least(salt.len(), &mut salt));
            AuthenticationMD5Password { salt: salt }
        },
        6 => AuthenticationSCMCredential,
        7 => AuthenticationGSS,
        9 => AuthenticationSSPI,
        val => return Err(IoError {
            kind: OtherIoError,
            desc: "Unexpected authentication tag",
            detail: Some(format!("got {}", val)),
        })
    })
}

fn read_parameter_description<R: Buffer>(buf: &mut R) -> IoResult<BackendMessage> {
    let len = try!(buf.read_be_i16()) as usize;
    let mut types = Vec::with_capacity(len);

    for _ in 0..len {
        types.push(try!(buf.read_u32::<BigEndian>()));
    }

    Ok(ParameterDescription { types: types })
}

fn read_row_description<R: Buffer>(buf: &mut R) -> IoResult<BackendMessage> {
    let len = try!(buf.read_be_i16()) as usize;
    let mut types = Vec::with_capacity(len);

    for _ in 0..len {
        types.push(RowDescriptionEntry {
            name: try!(buf.read_cstr()),
            table_oid: try!(buf.read_u32::<BigEndian>()),
            column_id: try!(buf.read_be_i16()),
            type_oid: try!(buf.read_u32::<BigEndian>()),
            type_size: try!(buf.read_be_i16()),
            type_modifier: try!(buf.read_be_i32()),
            format: try!(buf.read_be_i16())
        })
    }

    Ok(RowDescription { descriptions: types })
}
