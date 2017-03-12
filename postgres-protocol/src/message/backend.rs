#![allow(missing_docs)]

use byteorder::{ReadBytesExt, BigEndian};
use memchr::memchr;
use fallible_iterator::FallibleIterator;
use std::io::{self, Read};
use std::marker::PhantomData;
use std::ops::Deref;
use std::str;

use Oid;

/// An enum representing Postgres backend messages.
pub enum Message<T> {
    AuthenticationCleartextPassword,
    AuthenticationGss,
    AuthenticationKerberosV5,
    AuthenticationMd5Password(AuthenticationMd5PasswordBody<T>),
    AuthenticationOk,
    AuthenticationScmCredential,
    AuthenticationSspi,
    BackendKeyData(BackendKeyDataBody<T>),
    BindComplete,
    CloseComplete,
    CommandComplete(CommandCompleteBody<T>),
    CopyData(CopyDataBody<T>),
    CopyDone,
    CopyInResponse(CopyInResponseBody<T>),
    CopyOutResponse(CopyOutResponseBody<T>),
    DataRow(DataRowBody<T>),
    EmptyQueryResponse,
    ErrorResponse(ErrorResponseBody<T>),
    NoData,
    NoticeResponse(NoticeResponseBody<T>),
    NotificationResponse(NotificationResponseBody<T>),
    ParameterDescription(ParameterDescriptionBody<T>),
    ParameterStatus(ParameterStatusBody<T>),
    ParseComplete,
    PortalSuspended,
    ReadyForQuery(ReadyForQueryBody<T>),
    RowDescription(RowDescriptionBody<T>),
    #[doc(hidden)]
    __ForExtensibility,
}

impl<'a> Message<&'a [u8]> {
    /// Attempts to parse a backend message from the buffer.
    ///
    /// This method is unfortunately difficult to use due to deficiencies in the compiler's borrow
    /// checker.
    #[inline]
    pub fn parse(buf: &'a [u8]) -> io::Result<ParseResult<&'a [u8]>> {
        Message::parse_inner(buf)
    }
}

impl Message<Vec<u8>> {
    /// Attempts to parse a backend message from the buffer.
    ///
    /// In contrast to `parse`, this method produces messages that do not reference the input,
    /// buffer by copying any necessary portions internally.
    #[inline]
    pub fn parse_owned(buf: &[u8]) -> io::Result<ParseResult<Vec<u8>>> {
        Message::parse_inner(buf)
    }
}

impl<'a, T> Message<T>
    where T: From<&'a [u8]>
{
    #[inline]
    fn parse_inner(buf: &'a [u8]) -> io::Result<ParseResult<T>> {
        if buf.len() < 5 {
            return Ok(ParseResult::Incomplete { required_size: None });
        }

        let mut r = buf;
        let tag = r.read_u8().unwrap();
        // add a byte for the tag
        let len = r.read_u32::<BigEndian>().unwrap() as usize + 1;

        if buf.len() < len {
            return Ok(ParseResult::Incomplete { required_size: Some(len) });
        }

        let mut buf = &buf[5..len];
        let message = match tag {
            b'1' => Message::ParseComplete,
            b'2' => Message::BindComplete,
            b'3' => Message::CloseComplete,
            b'A' => {
                let process_id = try!(buf.read_i32::<BigEndian>());
                let channel_end = try!(find_null(buf, 0));
                let message_end = try!(find_null(buf, channel_end + 1));
                let storage = buf[..message_end].into();
                buf = &buf[message_end + 1..];
                Message::NotificationResponse(NotificationResponseBody {
                    storage: storage,
                    process_id: process_id,
                    channel_end: channel_end,
                })
            }
            b'c' => Message::CopyDone,
            b'C' => {
                let tag_end = try!(find_null(buf, 0));
                let storage = buf[..tag_end].into();
                buf = &buf[tag_end + 1..];
                Message::CommandComplete(CommandCompleteBody {
                    storage: storage,
                })
            }
            b'd' => {
                let storage = buf.into();
                buf = &[];
                Message::CopyData(CopyDataBody { storage: storage })
            }
            b'D' => {
                let len = try!(buf.read_u16::<BigEndian>());
                let storage = buf.into();
                buf = &[];
                Message::DataRow(DataRowBody {
                    storage: storage,
                    len: len,
                })
            }
            b'E' => {
                let storage = buf.into();
                buf = &[];
                Message::ErrorResponse(ErrorResponseBody { storage: storage })
            }
            b'G' => {
                let format = try!(buf.read_u8());
                let len = try!(buf.read_u16::<BigEndian>());
                let storage = buf.into();
                buf = &[];
                Message::CopyInResponse(CopyInResponseBody {
                    format: format,
                    len: len,
                    storage: storage,
                })
            }
            b'H' => {
                let format = try!(buf.read_u8());
                let len = try!(buf.read_u16::<BigEndian>());
                let storage = buf.into();
                buf = &[];
                Message::CopyOutResponse(CopyOutResponseBody {
                    format: format,
                    len: len,
                    storage: storage,
                })
            }
            b'I' => Message::EmptyQueryResponse,
            b'K' => {
                let process_id = try!(buf.read_i32::<BigEndian>());
                let secret_key = try!(buf.read_i32::<BigEndian>());
                Message::BackendKeyData(BackendKeyDataBody {
                    process_id: process_id,
                    secret_key: secret_key,
                    _p: PhantomData,
                })
            }
            b'n' => Message::NoData,
            b'N' => {
                let storage = buf.into();
                buf = &[];
                Message::NoticeResponse(NoticeResponseBody {
                    storage: storage,
                })
            }
            b'R' => {
                match try!(buf.read_i32::<BigEndian>()) {
                    0 => Message::AuthenticationOk,
                    2 => Message::AuthenticationKerberosV5,
                    3 => Message::AuthenticationCleartextPassword,
                    5 => {
                        let mut salt = [0; 4];
                        try!(buf.read_exact(&mut salt));
                        Message::AuthenticationMd5Password(AuthenticationMd5PasswordBody {
                            salt: salt,
                            _p: PhantomData,
                        })
                    }
                    6 => Message::AuthenticationScmCredential,
                    7 => Message::AuthenticationGss,
                    9 => Message::AuthenticationSspi,
                    tag => {
                        return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                                  format!("unknown authentication tag `{}`", tag)));
                    }
                }
            }
            b's' => Message::PortalSuspended,
            b'S' => {
                let name_end = try!(find_null(buf, 0));
                let value_end = try!(find_null(buf, name_end + 1));
                let storage = buf[0..value_end].into();
                buf = &buf[value_end + 1..];
                Message::ParameterStatus(ParameterStatusBody {
                    storage: storage,
                    name_end: name_end,
                })
            }
            b't' => {
                let len = try!(buf.read_u16::<BigEndian>());
                let storage = buf.into();
                buf = &[];
                Message::ParameterDescription(ParameterDescriptionBody {
                    storage: storage,
                    len: len,
                })
            }
            b'T' => {
                let len = try!(buf.read_u16::<BigEndian>());
                let storage = buf.into();
                buf = &[];
                Message::RowDescription(RowDescriptionBody {
                    storage: storage,
                    len: len,
                })
            }
            b'Z' => {
                let status = try!(buf.read_u8());
                Message::ReadyForQuery(ReadyForQueryBody {
                    status: status,
                    _p: PhantomData,
                })
            }
            tag => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                          format!("unknown message tag `{}`", tag)));
            }
        };

        if !buf.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid message length"));
        }

        Ok(ParseResult::Complete {
            message: message,
            consumed: len,
        })
    }
}

/// The result of an attempted parse.
pub enum ParseResult<T> {
    /// The message was successfully parsed.
    Complete {
        /// The message.
        message: Message<T>,
        /// The number of bytes of the input buffer consumed to parse this message.
        consumed: usize,
    },
    /// The buffer did not contain a full message.
    Incomplete {
        /// The number of total bytes required to parse a message, if known.
        ///
        /// This value is present if the input buffer contains at least 5 bytes.
        required_size: Option<usize>,
    }
}

pub struct AuthenticationMd5PasswordBody<T> {
    salt: [u8; 4],
    _p: PhantomData<T>,
}

impl<T> AuthenticationMd5PasswordBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn salt(&self) -> [u8; 4] {
        self.salt
    }
}

pub struct BackendKeyDataBody<T> {
    process_id: i32,
    secret_key: i32,
    _p: PhantomData<T>,
}

impl<T> BackendKeyDataBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    #[inline]
    pub fn secret_key(&self) -> i32 {
        self.secret_key
    }
}

pub struct CommandCompleteBody<T> {
    storage: T,
}

impl<T> CommandCompleteBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn tag(&self) -> io::Result<&str> {
        get_str(&self.storage)
    }
}

pub struct CopyDataBody<T> {
    storage: T,
}

impl<T> CopyDataBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.storage
    }
}

pub struct CopyInResponseBody<T> {
    storage: T,
    len: u16,
    format: u8,
}

impl<T> CopyInResponseBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn format(&self) -> u8 {
        self.format
    }

    #[inline]
    pub fn column_formats<'a>(&'a self) -> ColumnFormats<'a> {
        ColumnFormats {
            remaining: self.len,
            buf: &self.storage,
        }
    }
}

pub struct ColumnFormats<'a> {
    buf: &'a [u8],
    remaining: u16,
}

impl<'a> FallibleIterator for ColumnFormats<'a> {
    type Item = u16;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<u16>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid message length"));
            }
        }

        self.remaining -= 1;
        self.buf.read_u16::<BigEndian>().map(Some)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

pub struct CopyOutResponseBody<T> {
    storage: T,
    len: u16,
    format: u8,
}

impl<T> CopyOutResponseBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn format(&self) -> u8 {
        self.format
    }

    #[inline]
    pub fn column_formats<'a>(&'a self) -> ColumnFormats<'a> {
        ColumnFormats {
            remaining: self.len,
            buf: &self.storage,
        }
    }
}

pub struct DataRowBody<T> {
    storage: T,
    len: u16,
}

impl<T> DataRowBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn values<'a>(&'a self) -> DataRowValues<'a> {
        DataRowValues {
            buf: &self.storage,
            remaining: self.len,
        }
    }
}

pub struct DataRowValues<'a> {
    buf: &'a [u8],
    remaining: u16,
}

impl<'a> FallibleIterator for DataRowValues<'a> {
    type Item = Option<&'a [u8]>;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<Option<&'a [u8]>>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid message length"));
            }
        }

        self.remaining -= 1;
        let len = try!(self.buf.read_i32::<BigEndian>());
        if len < 0 {
            Ok(Some(None))
        } else {
            let len = len as usize;
            if self.buf.len() < len {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF"));
            }
            let (head, tail) = self.buf.split_at(len);
            self.buf = tail;
            Ok(Some(Some(head)))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

pub struct ErrorResponseBody<T> {
    storage: T,
}

impl<T> ErrorResponseBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn fields<'a>(&'a self) -> ErrorFields<'a> {
        ErrorFields {
            buf: &self.storage
        }
    }
}

pub struct ErrorFields<'a> {
    buf: &'a [u8],
}

impl<'a> FallibleIterator for ErrorFields<'a> {
    type Item = ErrorField<'a>;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<ErrorField<'a>>> {
        let type_ = try!(self.buf.read_u8());
        if type_ == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid message length"));
            }
        }

        let value_end = try!(find_null(self.buf, 0));
        let value = try!(get_str(&self.buf[..value_end]));
        self.buf = &self.buf[value_end + 1..];

        Ok(Some(ErrorField {
            type_: type_,
            value: value,
        }))
    }
}

pub struct ErrorField<'a> {
    type_: u8,
    value: &'a str,
}

impl<'a> ErrorField<'a> {
    #[inline]
    pub fn type_(&self) -> u8 {
        self.type_
    }

    #[inline]
    pub fn value(&self) -> &str {
        self.value
    }
}

pub struct NoticeResponseBody<T> {
    storage: T,
}

impl<T> NoticeResponseBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn fields<'a>(&'a self) -> ErrorFields<'a> {
        ErrorFields {
            buf: &self.storage
        }
    }
}

pub struct NotificationResponseBody<T> {
    storage: T,
    process_id: i32,
    channel_end: usize,
}

impl<T> NotificationResponseBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    #[inline]
    pub fn channel(&self) -> io::Result<&str> {
        get_str(&self.storage[..self.channel_end])
    }

    #[inline]
    pub fn message(&self) -> io::Result<&str> {
        get_str(&self.storage[self.channel_end + 1..])
    }
}

pub struct ParameterDescriptionBody<T> {
    storage: T,
    len: u16,
}

impl<T> ParameterDescriptionBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn parameters<'a>(&'a self) -> Parameters<'a> {
        Parameters {
            buf: &self.storage,
            remaining: self.len,
        }
    }
}

pub struct Parameters<'a> {
    buf: &'a [u8],
    remaining: u16,
}

impl<'a> FallibleIterator for Parameters<'a> {
    type Item = Oid;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<Oid>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid message length"));
            }
        }

        self.remaining -= 1;
        self.buf.read_u32::<BigEndian>().map(Some)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

pub struct ParameterStatusBody<T> {
    storage: T,
    name_end: usize,
}

impl<T> ParameterStatusBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.storage[..self.name_end])
    }

    #[inline]
    pub fn value(&self) -> io::Result<&str> {
        get_str(&self.storage[self.name_end + 1..])
    }
}

pub struct ReadyForQueryBody<T> {
    status: u8,
    _p: PhantomData<T>,
}

impl<T> ReadyForQueryBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn status(&self) -> u8 {
        self.status
    }
}

pub struct RowDescriptionBody<T> {
    storage: T,
    len: u16,
}

impl<T> RowDescriptionBody<T>
    where T: Deref<Target = [u8]>
{
    #[inline]
    pub fn fields<'a>(&'a self) -> Fields<'a> {
        Fields {
            buf: &self.storage,
            remaining: self.len,
        }
    }
}

pub struct Fields<'a> {
    buf: &'a [u8],
    remaining: u16,
}

impl<'a> FallibleIterator for Fields<'a> {
    type Item = Field<'a>;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<Field<'a>>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid message length"));
            }
        }

        self.remaining -= 1;
        let name_end = try!(find_null(self.buf, 0));
        let name = try!(get_str(&self.buf[..name_end]));
        self.buf = &self.buf[name_end + 1..];
        let table_oid = try!(self.buf.read_u32::<BigEndian>());
        let column_id = try!(self.buf.read_i16::<BigEndian>());
        let type_oid = try!(self.buf.read_u32::<BigEndian>());
        let type_size = try!(self.buf.read_i16::<BigEndian>());
        let type_modifier = try!(self.buf.read_i32::<BigEndian>());
        let format = try!(self.buf.read_i16::<BigEndian>());

        Ok(Some(Field {
            name: name,
            table_oid: table_oid,
            column_id: column_id,
            type_oid: type_oid,
            type_size: type_size,
            type_modifier: type_modifier,
            format: format,
        }))
    }
}

pub struct Field<'a> {
    name: &'a str,
    table_oid: Oid,
    column_id: i16,
    type_oid: Oid,
    type_size: i16,
    type_modifier: i32,
    format: i16,
}

impl<'a> Field<'a> {
    #[inline]
    pub fn name(&self) -> &'a str {
        self.name
    }

    #[inline]
    pub fn table_oid(&self) -> Oid {
        self.table_oid
    }

    #[inline]
    pub fn column_id(&self) -> i16 {
        self.column_id
    }

    #[inline]
    pub fn type_oid(&self) -> Oid {
        self.type_oid
    }

    #[inline]
    pub fn type_size(&self) -> i16 {
        self.type_size
    }

    #[inline]
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }

    #[inline]
    pub fn format(&self) -> i16 {
        self.format
    }
}

#[inline]
fn find_null(buf: &[u8], start: usize) -> io::Result<usize> {
    match memchr(0, &buf[start..]) {
        Some(pos) => Ok(pos + start),
        None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF"))
    }
}

#[inline]
fn get_str(buf: &[u8]) -> io::Result<&str> {
    str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}