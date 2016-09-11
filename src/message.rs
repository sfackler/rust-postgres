use std::io;
use std::io::prelude::*;
use std::mem;
use std::time::Duration;
use byteorder::{BigEndian, ReadBytesExt};

use types::Oid;
use priv_io::StreamOptions;

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
