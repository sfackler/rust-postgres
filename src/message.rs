use std::io;
use std::io::prelude::*;
use std::mem;
use std::time::Duration;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use types::Oid;
use priv_io::StreamOptions;

pub const CANCEL_CODE: u32 = 80877102;
pub const SSL_CODE: u32 = 80877103;

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
        process_id: u32,
        secret_key: u32,
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
        pid: u32,
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

pub struct RowDescriptionEntry {
    pub name: String,
    pub table_oid: Oid,
    pub column_id: i16,
    pub type_oid: Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

pub enum Frontend<'a> {
    Bind {
        portal: &'a str,
        statement: &'a str,
        formats: &'a [i16],
        values: &'a [Option<Vec<u8>>],
        result_formats: &'a [i16],
    },
    CancelRequest {
        code: u32,
        process_id: u32,
        secret_key: u32,
    },
    Close {
        variant: u8,
        name: &'a str,
    },
    CopyData {
        data: &'a [u8],
    },
    CopyDone,
    CopyFail {
        message: &'a str,
    },
    Describe {
        variant: u8,
        name: &'a str,
    },
    Execute {
        portal: &'a str,
        max_rows: i32,
    },
    Parse {
        name: &'a str,
        query: &'a str,
        param_types: &'a [Oid],
    },
    Query {
        query: &'a str,
    },
    SslRequest {
        code: u32,
    },
    Sync,
    Terminate,
}

#[doc(hidden)]
trait WriteCStr {
    fn write_cstr(&mut self, s: &str) -> io::Result<()>;
}

impl<W: Write> WriteCStr for W {
    fn write_cstr(&mut self, s: &str) -> io::Result<()> {
        try!(self.write_all(s.as_bytes()));
        Ok(try!(self.write_u8(0)))
    }
}

#[doc(hidden)]
pub trait WriteMessage {
    fn write_message(&mut self, &Frontend) -> io::Result<()>;
}

impl<W: Write> WriteMessage for W {
    #[allow(cyclomatic_complexity)]
    fn write_message(&mut self, message: &Frontend) -> io::Result<()> {
        let mut buf = vec![];
        let mut ident = None;

        match *message {
            Frontend::Bind { portal, statement, formats, values, result_formats } => {
                ident = Some(b'B');
                try!(buf.write_cstr(portal));
                try!(buf.write_cstr(statement));

                try!(buf.write_u16::<BigEndian>(try!(u16::from_usize(formats.len()))));
                for &format in formats {
                    try!(buf.write_i16::<BigEndian>(format));
                }

                try!(buf.write_u16::<BigEndian>(try!(u16::from_usize(values.len()))));
                for value in values {
                    match *value {
                        None => try!(buf.write_i32::<BigEndian>(-1)),
                        Some(ref value) => {
                            try!(buf.write_i32::<BigEndian>(try!(i32::from_usize(value.len()))));
                            try!(buf.write_all(&**value));
                        }
                    }
                }

                try!(buf.write_u16::<BigEndian>(try!(u16::from_usize(result_formats.len()))));
                for &format in result_formats {
                    try!(buf.write_i16::<BigEndian>(format));
                }
            }
            Frontend::CancelRequest { code, process_id, secret_key } => {
                try!(buf.write_u32::<BigEndian>(code));
                try!(buf.write_u32::<BigEndian>(process_id));
                try!(buf.write_u32::<BigEndian>(secret_key));
            }
            Frontend::Close { variant, name } => {
                ident = Some(b'C');
                try!(buf.write_u8(variant));
                try!(buf.write_cstr(name));
            }
            Frontend::CopyData { data } => {
                ident = Some(b'd');
                try!(buf.write_all(data));
            }
            Frontend::CopyDone => ident = Some(b'c'),
            Frontend::CopyFail { message } => {
                ident = Some(b'f');
                try!(buf.write_cstr(message));
            }
            Frontend::Describe { variant, name } => {
                ident = Some(b'D');
                try!(buf.write_u8(variant));
                try!(buf.write_cstr(name));
            }
            Frontend::Execute { portal, max_rows } => {
                ident = Some(b'E');
                try!(buf.write_cstr(portal));
                try!(buf.write_i32::<BigEndian>(max_rows));
            }
            Frontend::Parse { name, query, param_types } => {
                ident = Some(b'P');
                try!(buf.write_cstr(name));
                try!(buf.write_cstr(query));
                try!(buf.write_u16::<BigEndian>(try!(u16::from_usize(param_types.len()))));
                for &ty in param_types {
                    try!(buf.write_u32::<BigEndian>(ty));
                }
            }
            Frontend::Query { query } => {
                ident = Some(b'Q');
                try!(buf.write_cstr(query));
            }
            Frontend::SslRequest { code } => try!(buf.write_u32::<BigEndian>(code)),
            Frontend::Sync => ident = Some(b'S'),
            Frontend::Terminate => ident = Some(b'X'),
        }

        if let Some(ident) = ident {
            try!(self.write_u8(ident));
        }

        // add size of length value
        if buf.len() > u32::max_value() as usize - mem::size_of::<u32>() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "value too large to transmit"));
        }
        try!(self.write_u32::<BigEndian>((buf.len() + mem::size_of::<u32>()) as u32));
        try!(self.write_all(&*buf));

        Ok(())
    }
}

#[doc(hidden)]
trait ReadCStr {
    fn read_cstr(&mut self) -> io::Result<String>;
}

impl<R: BufRead> ReadCStr for R {
    fn read_cstr(&mut self) -> io::Result<String> {
        let mut buf = vec![];
        try!(self.read_until(0, &mut buf));
        buf.pop();
        String::from_utf8(buf).map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }
}

#[doc(hidden)]
pub trait ReadMessage {
    fn read_message(&mut self) -> io::Result<Backend>;

    fn read_message_timeout(&mut self, timeout: Duration) -> io::Result<Option<Backend>>;

    fn read_message_nonblocking(&mut self) -> io::Result<Option<Backend>>;

    fn finish_read_message(&mut self, ident: u8) -> io::Result<Backend>;
}

impl<R: BufRead + StreamOptions> ReadMessage for R {
    fn read_message(&mut self) -> io::Result<Backend> {
        let ident = try!(self.read_u8());
        self.finish_read_message(ident)
    }

    fn read_message_timeout(&mut self, timeout: Duration) -> io::Result<Option<Backend>> {
        try!(self.set_read_timeout(Some(timeout)));
        let ident = self.read_u8();
        try!(self.set_read_timeout(None));

        match ident {
            Ok(ident) => self.finish_read_message(ident).map(Some),
            Err(e) => {
                let e: io::Error = e.into();
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn read_message_nonblocking(&mut self) -> io::Result<Option<Backend>> {
        try!(self.set_nonblocking(true));
        let ident = self.read_u8();
        try!(self.set_nonblocking(false));

        match ident {
            Ok(ident) => self.finish_read_message(ident).map(Some),
            Err(e) => {
                let e: io::Error = e.into();
                if e.kind() == io::ErrorKind::WouldBlock {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    #[allow(cyclomatic_complexity)]
    fn finish_read_message(&mut self, ident: u8) -> io::Result<Backend> {
        // subtract size of length value
        let len = try!(self.read_u32::<BigEndian>()) - mem::size_of::<u32>() as u32;
        let mut rdr = self.by_ref().take(len as u64);

        let ret = match ident {
            b'1' => Backend::ParseComplete,
            b'2' => Backend::BindComplete,
            b'3' => Backend::CloseComplete,
            b'A' => {
                Backend::NotificationResponse {
                    pid: try!(rdr.read_u32::<BigEndian>()),
                    channel: try!(rdr.read_cstr()),
                    payload: try!(rdr.read_cstr()),
                }
            }
            b'c' => Backend::CopyDone,
            b'C' => Backend::CommandComplete { tag: try!(rdr.read_cstr()) },
            b'd' => {
                let mut data = vec![];
                try!(rdr.read_to_end(&mut data));
                Backend::CopyData { data: data }
            }
            b'D' => try!(read_data_row(&mut rdr)),
            b'E' => Backend::ErrorResponse { fields: try!(read_fields(&mut rdr)) },
            b'G' => {
                let format = try!(rdr.read_u8());
                let mut column_formats = vec![];
                for _ in 0..try!(rdr.read_u16::<BigEndian>()) {
                    column_formats.push(try!(rdr.read_u16::<BigEndian>()));
                }
                Backend::CopyInResponse {
                    format: format,
                    column_formats: column_formats,
                }
            }
            b'H' => {
                let format = try!(rdr.read_u8());
                let mut column_formats = vec![];
                for _ in 0..try!(rdr.read_u16::<BigEndian>()) {
                    column_formats.push(try!(rdr.read_u16::<BigEndian>()));
                }
                Backend::CopyOutResponse {
                    format: format,
                    column_formats: column_formats,
                }
            }
            b'I' => Backend::EmptyQueryResponse,
            b'K' => {
                Backend::BackendKeyData {
                    process_id: try!(rdr.read_u32::<BigEndian>()),
                    secret_key: try!(rdr.read_u32::<BigEndian>()),
                }
            }
            b'n' => Backend::NoData,
            b'N' => Backend::NoticeResponse { fields: try!(read_fields(&mut rdr)) },
            b'R' => try!(read_auth_message(&mut rdr)),
            b's' => Backend::PortalSuspended,
            b'S' => {
                Backend::ParameterStatus {
                    parameter: try!(rdr.read_cstr()),
                    value: try!(rdr.read_cstr()),
                }
            }
            b't' => try!(read_parameter_description(&mut rdr)),
            b'T' => try!(read_row_description(&mut rdr)),
            b'Z' => Backend::ReadyForQuery { _state: try!(rdr.read_u8()) },
            t => {
                return Err(io::Error::new(io::ErrorKind::Other,
                                          format!("unexpected message tag `{}`", t)))
            }
        };
        if rdr.limit() != 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "didn't read entire message"));
        }
        Ok(ret)
    }
}

fn read_fields<R: BufRead>(buf: &mut R) -> io::Result<Vec<(u8, String)>> {
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

fn read_data_row<R: BufRead>(buf: &mut R) -> io::Result<Backend> {
    let len = try!(buf.read_u16::<BigEndian>()) as usize;
    let mut values = Vec::with_capacity(len);

    for _ in 0..len {
        let val = match try!(buf.read_i32::<BigEndian>()) {
            -1 => None,
            len => {
                let mut data = vec![0; len as usize];
                try!(buf.read_exact(&mut data));
                Some(data)
            }
        };
        values.push(val);
    }

    Ok(Backend::DataRow { row: values })
}

fn read_auth_message<R: Read>(buf: &mut R) -> io::Result<Backend> {
    Ok(match try!(buf.read_i32::<BigEndian>()) {
        0 => Backend::AuthenticationOk,
        2 => Backend::AuthenticationKerberosV5,
        3 => Backend::AuthenticationCleartextPassword,
        5 => {
            let mut salt = [0; 4];
            try!(buf.read_exact(&mut salt));
            Backend::AuthenticationMD5Password { salt: salt }
        }
        6 => Backend::AuthenticationSCMCredential,
        7 => Backend::AuthenticationGSS,
        9 => Backend::AuthenticationSSPI,
        t => {
            return Err(io::Error::new(io::ErrorKind::Other,
                                      format!("unexpected authentication tag `{}`", t)))
        }
    })
}

fn read_parameter_description<R: Read>(buf: &mut R) -> io::Result<Backend> {
    let len = try!(buf.read_u16::<BigEndian>()) as usize;
    let mut types = Vec::with_capacity(len);

    for _ in 0..len {
        types.push(try!(buf.read_u32::<BigEndian>()));
    }

    Ok(Backend::ParameterDescription { types: types })
}

fn read_row_description<R: BufRead>(buf: &mut R) -> io::Result<Backend> {
    let len = try!(buf.read_u16::<BigEndian>()) as usize;
    let mut types = Vec::with_capacity(len);

    for _ in 0..len {
        types.push(RowDescriptionEntry {
            name: try!(buf.read_cstr()),
            table_oid: try!(buf.read_u32::<BigEndian>()),
            column_id: try!(buf.read_i16::<BigEndian>()),
            type_oid: try!(buf.read_u32::<BigEndian>()),
            type_size: try!(buf.read_i16::<BigEndian>()),
            type_modifier: try!(buf.read_i32::<BigEndian>()),
            format: try!(buf.read_i16::<BigEndian>()),
        })
    }

    Ok(Backend::RowDescription { descriptions: types })
}

trait FromUsize: Sized {
    fn from_usize(x: usize) -> io::Result<Self>;
}

macro_rules! from_usize {
    ($t:ty) => {
        impl FromUsize for $t {
            fn from_usize(x: usize) -> io::Result<$t> {
                if x > <$t>::max_value() as usize {
                    Err(io::Error::new(io::ErrorKind::InvalidInput, "value too large to transmit"))
                } else {
                    Ok(x as $t)
                }
            }
        }
    }
}

from_usize!(u16);
from_usize!(i32);
