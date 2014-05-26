use std::io::{IoResult, MemWriter, MemReader};
use std::mem;

use types::Oid;

pub static PROTOCOL_VERSION: i32 = 0x0003_0000;
pub static CANCEL_CODE: i32 = 80877102;
pub static SSL_CODE: i32 = 80877103;

pub enum BackendMessage {
    AuthenticationCleartextPassword,
    AuthenticationGSS,
    AuthenticationKerberosV5,
    AuthenticationMD5Password {
        pub salt: [u8, ..4]
    },
    AuthenticationOk,
    AuthenticationSCMCredential,
    AuthenticationSSPI,
    BackendKeyData {
        pub process_id: i32,
        pub secret_key: i32
    },
    BindComplete,
    CloseComplete,
    CommandComplete {
        pub tag: String,
    },
    DataRow {
        pub row: Vec<Option<Vec<u8>>>
    },
    EmptyQueryResponse,
    ErrorResponse {
        pub fields: Vec<(u8, String)>
    },
    NoData,
    NoticeResponse {
        pub fields: Vec<(u8, String)>
    },
    NotificationResponse {
        pub pid: i32,
        pub channel: String,
        pub payload: String,
    },
    ParameterDescription {
        pub types: Vec<Oid>
    },
    ParameterStatus {
        pub parameter: String,
        pub value: String,
    },
    ParseComplete,
    PortalSuspended,
    ReadyForQuery {
        pub state: u8
    },
    RowDescription {
        pub descriptions: Vec<RowDescriptionEntry>
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
        pub portal: &'a str,
        pub statement: &'a str,
        pub formats: &'a [i16],
        pub values: &'a [Option<Vec<u8>>],
        pub result_formats: &'a [i16]
    },
    CancelRequest {
        pub code: i32,
        pub process_id: i32,
        pub secret_key: i32,
    },
    Close {
        pub variant: u8,
        pub name: &'a str
    },
    Describe {
        pub variant: u8,
        pub name: &'a str
    },
    Execute {
        pub portal: &'a str,
        pub max_rows: i32
    },
    Parse {
        pub name: &'a str,
        pub query: &'a str,
        pub param_types: &'a [i32]
    },
    PasswordMessage {
        pub password: &'a str
    },
    Query {
        pub query: &'a str
    },
    SslRequest {
        pub code: i32
    },
    StartupMessage {
        pub version: i32,
        pub parameters: &'a [(String, String)]
    },
    Sync,
    Terminate
}

#[doc(hidden)]
trait WriteCStr {
    fn write_cstr(&mut self, s: &str) -> IoResult<()>;
}

impl<W: Writer> WriteCStr for W {
    fn write_cstr(&mut self, s: &str) -> IoResult<()> {
        try!(self.write(s.as_bytes()));
        self.write_u8(0)
    }
}

#[doc(hidden)]
pub trait WriteMessage {
    fn write_message(&mut self, &FrontendMessage) -> IoResult<()> ;
}

impl<W: Writer> WriteMessage for W {
    fn write_message(&mut self, message: &FrontendMessage) -> IoResult<()> {
        let mut buf = MemWriter::new();
        let mut ident = None;

        match *message {
            Bind { portal, statement, formats, values, result_formats } => {
                ident = Some('B');
                try!(buf.write_cstr(portal));
                try!(buf.write_cstr(statement));

                try!(buf.write_be_i16(formats.len() as i16));
                for format in formats.iter() {
                    try!(buf.write_be_i16(*format));
                }

                try!(buf.write_be_i16(values.len() as i16));
                for value in values.iter() {
                    match *value {
                        None => {
                            try!(buf.write_be_i32(-1));
                        }
                        Some(ref value) => {
                            try!(buf.write_be_i32(value.len() as i32));
                            try!(buf.write(value.as_slice()));
                        }
                    }
                }

                try!(buf.write_be_i16(result_formats.len() as i16));
                for format in result_formats.iter() {
                    try!(buf.write_be_i16(*format));
                }
            }
            CancelRequest { code, process_id, secret_key } => {
                try!(buf.write_be_i32(code));
                try!(buf.write_be_i32(process_id));
                try!(buf.write_be_i32(secret_key));
            }
            Close { variant, name } => {
                ident = Some('C');
                try!(buf.write_u8(variant));
                try!(buf.write_cstr(name));
            }
            Describe { variant, name } => {
                ident = Some('D');
                try!(buf.write_u8(variant));
                try!(buf.write_cstr(name));
            }
            Execute { portal, max_rows } => {
                ident = Some('E');
                try!(buf.write_cstr(portal));
                try!(buf.write_be_i32(max_rows));
            }
            Parse { name, query, param_types } => {
                ident = Some('P');
                try!(buf.write_cstr(name));
                try!(buf.write_cstr(query));
                try!(buf.write_be_i16(param_types.len() as i16));
                for ty in param_types.iter() {
                    try!(buf.write_be_i32(*ty));
                }
            }
            PasswordMessage { password } => {
                ident = Some('p');
                try!(buf.write_cstr(password));
            }
            Query { query } => {
                ident = Some('Q');
                try!(buf.write_cstr(query));
            }
            StartupMessage { version, parameters } => {
                try!(buf.write_be_i32(version));
                for &(ref k, ref v) in parameters.iter() {
                    try!(buf.write_cstr(k.as_slice()));
                    try!(buf.write_cstr(v.as_slice()));
                }
                try!(buf.write_u8(0));
            }
            SslRequest { code } => try!(buf.write_be_i32(code)),
            Sync => {
                ident = Some('S');
            }
            Terminate => {
                ident = Some('X');
            }
        }

        match ident {
            Some(ident) => try!(self.write_u8(ident as u8)),
            None => ()
        }

        let buf = buf.unwrap();
        // add size of length value
        try!(self.write_be_i32((buf.len() + mem::size_of::<i32>()) as i32));
        try!(self.write(buf.as_slice()));

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
        Ok(String::from_utf8(buf).unwrap())
    }
}

#[doc(hidden)]
pub trait ReadMessage {
    fn read_message(&mut self) -> IoResult<BackendMessage>;
}

impl<R: Reader> ReadMessage for R {
    fn read_message(&mut self) -> IoResult<BackendMessage> {
        let ident = try!(self.read_u8());
        // subtract size of length value
        let len = try!(self.read_be_i32()) as uint - mem::size_of::<i32>();
        let mut buf = MemReader::new(try!(self.read_exact(len)));

        let ret = match ident as char {
            '1' => ParseComplete,
            '2' => BindComplete,
            '3' => CloseComplete,
            'A' => NotificationResponse {
                pid: try!(buf.read_be_i32()),
                channel: try!(buf.read_cstr()),
                payload: try!(buf.read_cstr())
            },
            'C' => CommandComplete { tag: try!(buf.read_cstr()) },
            'D' => try!(read_data_row(&mut buf)),
            'E' => ErrorResponse { fields: try!(read_fields(&mut buf)) },
            'I' => EmptyQueryResponse,
            'K' => BackendKeyData {
                process_id: try!(buf.read_be_i32()),
                secret_key: try!(buf.read_be_i32())
            },
            'n' => NoData,
            'N' => NoticeResponse { fields: try!(read_fields(&mut buf)) },
            'R' => try!(read_auth_message(&mut buf)),
            's' => PortalSuspended,
            'S' => ParameterStatus {
                parameter: try!(buf.read_cstr()),
                value: try!(buf.read_cstr())
            },
            't' => try!(read_parameter_description(&mut buf)),
            'T' => try!(read_row_description(&mut buf)),
            'Z' => ReadyForQuery { state: try!(buf.read_u8()) },
            ident => fail!("Unknown message identifier `{}`", ident)
        };
        Ok(ret)
    }
}

fn read_fields(buf: &mut MemReader) -> IoResult<Vec<(u8, String)>> {
    let mut fields = Vec::new();
    loop {
        let ty = try!(buf.read_u8());
        if ty == 0 {
            break;
        }

        fields.push((ty, try!(buf.read_cstr())));
    }

    Ok(fields)
}

fn read_data_row(buf: &mut MemReader) -> IoResult<BackendMessage> {
    let len = try!(buf.read_be_i16()) as uint;
    let mut values = Vec::with_capacity(len);

    for _ in range(0, len) {
        let val = match try!(buf.read_be_i32()) {
            -1 => None,
            len => Some(try!(buf.read_exact(len as uint)))
        };
        values.push(val);
    }

    Ok(DataRow { row: values })
}

fn read_auth_message(buf: &mut MemReader) -> IoResult<BackendMessage> {
    Ok(match try!(buf.read_be_i32()) {
        0 => AuthenticationOk,
        2 => AuthenticationKerberosV5,
        3 => AuthenticationCleartextPassword,
        5 => {
            let mut salt = [0, 0, 0, 0];
            try!(buf.read_at_least(salt.len(), salt));
            AuthenticationMD5Password { salt: salt }
        },
        6 => AuthenticationSCMCredential,
        7 => AuthenticationGSS,
        9 => AuthenticationSSPI,
        val => fail!("Invalid authentication identifier `{}`", val)
    })
}

fn read_parameter_description(buf: &mut MemReader) -> IoResult<BackendMessage> {
    let len = try!(buf.read_be_i16()) as uint;
    let mut types = Vec::with_capacity(len);

    for _ in range(0, len) {
        types.push(try!(buf.read_be_u32()));
    }

    Ok(ParameterDescription { types: types })
}

fn read_row_description(buf: &mut MemReader) -> IoResult<BackendMessage> {
    let len = try!(buf.read_be_i16()) as uint;
    let mut types = Vec::with_capacity(len);

    for _ in range(0, len) {
        types.push(RowDescriptionEntry {
            name: try!(buf.read_cstr()),
            table_oid: try!(buf.read_be_u32()),
            column_id: try!(buf.read_be_i16()),
            type_oid: try!(buf.read_be_u32()),
            type_size: try!(buf.read_be_i16()),
            type_modifier: try!(buf.read_be_i32()),
            format: try!(buf.read_be_i16())
        });
    }

    Ok(RowDescription { descriptions: types })
}
