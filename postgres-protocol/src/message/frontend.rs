//! Frontend message serialization.
#![allow(missing_docs)]

use byteorder::{WriteBytesExt, BigEndian, ByteOrder};
use std::error::Error;
use std::io;
use std::marker;

use {Oid, FromUsize, IsNull, write_nullable};

pub enum Message<'a> {
    Bind {
        portal: &'a str,
        statement: &'a str,
        formats: &'a [i16],
        values: &'a [Option<Vec<u8>>],
        result_formats: &'a [i16],
    },
    CancelRequest { process_id: i32, secret_key: i32 },
    Close { variant: u8, name: &'a str },
    CopyData { data: &'a [u8] },
    CopyDone,
    CopyFail { message: &'a str },
    Describe { variant: u8, name: &'a str },
    Execute { portal: &'a str, max_rows: i32 },
    Parse {
        name: &'a str,
        query: &'a str,
        param_types: &'a [Oid],
    },
    PasswordMessage { password: &'a str },
    Query { query: &'a str },
    SaslInitialResponse { mechanism: &'a str, data: &'a [u8] },
    SaslResponse { data: &'a [u8] },
    SslRequest,
    StartupMessage { parameters: &'a [(String, String)] },
    Sync,
    Terminate,
    #[doc(hidden)]
    __ForExtensibility,
}

impl<'a> Message<'a> {
    #[inline]
    pub fn serialize(&self, buf: &mut Vec<u8>) -> io::Result<()> {
        match *self {
            Message::Bind {
                portal,
                statement,
                formats,
                values,
                result_formats,
            } => {
                let r = bind(
                    portal,
                    statement,
                    formats.iter().cloned(),
                    values,
                    |v, buf| match *v {
                        Some(ref v) => {
                            buf.extend_from_slice(v);
                            Ok(IsNull::No)
                        }
                        None => Ok(IsNull::Yes),
                    },
                    result_formats.iter().cloned(),
                    buf,
                );
                match r {
                    Ok(()) => Ok(()),
                    Err(BindError::Conversion(_)) => unreachable!(),
                    Err(BindError::Serialization(e)) => Err(e),
                }
            }
            Message::CancelRequest {
                process_id,
                secret_key,
            } => Ok(cancel_request(process_id, secret_key, buf)),
            Message::Close { variant, name } => close(variant, name, buf),
            Message::CopyData { data } => copy_data(data, buf),
            Message::CopyDone => Ok(copy_done(buf)),
            Message::CopyFail { message } => copy_fail(message, buf),
            Message::Describe { variant, name } => describe(variant, name, buf),
            Message::Execute { portal, max_rows } => execute(portal, max_rows, buf),
            Message::Parse {
                name,
                query,
                param_types,
            } => parse(name, query, param_types.iter().cloned(), buf),
            Message::PasswordMessage { password } => password_message(password, buf),
            Message::Query { query: q } => query(q, buf),
            Message::SaslInitialResponse { mechanism, data } => {
                sasl_initial_response(mechanism, data, buf)
            }
            Message::SaslResponse { data } => sasl_response(data, buf),
            Message::SslRequest => Ok(ssl_request(buf)),
            Message::StartupMessage { parameters } => {
                startup_message(parameters.iter().map(|&(ref k, ref v)| (&**k, &**v)), buf)
            }
            Message::Sync => Ok(sync(buf)),
            Message::Terminate => Ok(terminate(buf)),
            Message::__ForExtensibility => unreachable!(),
        }
    }
}

#[inline]
fn write_body<F, E>(buf: &mut Vec<u8>, f: F) -> Result<(), E>
where
    F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    f(buf)?;

    let size = i32::from_usize(buf.len() - base)?;
    BigEndian::write_i32(&mut buf[base..], size);
    Ok(())
}

pub enum BindError {
    Conversion(Box<Error + marker::Sync + Send>),
    Serialization(io::Error),
}

impl From<Box<Error + marker::Sync + Send>> for BindError {
    #[inline]
    fn from(e: Box<Error + marker::Sync + Send>) -> BindError {
        BindError::Conversion(e)
    }
}

impl From<io::Error> for BindError {
    #[inline]
    fn from(e: io::Error) -> BindError {
        BindError::Serialization(e)
    }
}

#[inline]
pub fn bind<I, J, F, T, K>(
    portal: &str,
    statement: &str,
    formats: I,
    values: J,
    mut serializer: F,
    result_formats: K,
    buf: &mut Vec<u8>,
) -> Result<(), BindError>
where
    I: IntoIterator<Item = i16>,
    J: IntoIterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>) -> Result<IsNull, Box<Error + marker::Sync + Send>>,
    K: IntoIterator<Item = i16>,
{
    buf.push(b'B');

    write_body(buf, |buf| {
        buf.write_cstr(portal)?;
        buf.write_cstr(statement)?;
        write_counted(formats, |f, buf| buf.write_i16::<BigEndian>(f), buf)?;
        write_counted(
            values,
            |v, buf| write_nullable(|buf| serializer(v, buf), buf),
            buf,
        )?;
        write_counted(result_formats, |f, buf| buf.write_i16::<BigEndian>(f), buf)?;

        Ok(())
    })
}

#[inline]
fn write_counted<I, T, F, E>(items: I, mut serializer: F, buf: &mut Vec<u8>) -> Result<(), E>
where
    I: IntoIterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 2]);
    let mut count = 0;
    for item in items {
        serializer(item, buf)?;
        count += 1;
    }
    let count = i16::from_usize(count)?;
    BigEndian::write_i16(&mut buf[base..], count);

    Ok(())
}

#[inline]
pub fn cancel_request(process_id: i32, secret_key: i32, buf: &mut Vec<u8>) {
    write_body(buf, |buf| {
        buf.write_i32::<BigEndian>(80877102).unwrap();
        buf.write_i32::<BigEndian>(process_id).unwrap();
        buf.write_i32::<BigEndian>(secret_key)
    }).unwrap();
}

#[inline]
pub fn close(variant: u8, name: &str, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'C');
    write_body(buf, |buf| {
        buf.push(variant);
        buf.write_cstr(name)
    })
}

// FIXME ideally this'd take a Read but it's unclear what to do at EOF
#[inline]
pub fn copy_data(data: &[u8], buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'd');
    write_body(buf, |buf| {
        buf.extend_from_slice(data);
        Ok(())
    })
}

#[inline]
pub fn copy_done(buf: &mut Vec<u8>) {
    buf.push(b'c');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
pub fn copy_fail(message: &str, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'f');
    write_body(buf, |buf| buf.write_cstr(message))
}

#[inline]
pub fn describe(variant: u8, name: &str, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'D');
    write_body(buf, |buf| {
        buf.push(variant);
        buf.write_cstr(name)
    })
}

#[inline]
pub fn execute(portal: &str, max_rows: i32, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'E');
    write_body(buf, |buf| {
        buf.write_cstr(portal)?;
        buf.write_i32::<BigEndian>(max_rows).unwrap();
        Ok(())
    })
}

#[inline]
pub fn parse<I>(name: &str, query: &str, param_types: I, buf: &mut Vec<u8>) -> io::Result<()>
where
    I: IntoIterator<Item = Oid>,
{
    buf.push(b'P');
    write_body(buf, |buf| {
        buf.write_cstr(name)?;
        buf.write_cstr(query)?;
        write_counted(param_types, |t, buf| buf.write_u32::<BigEndian>(t), buf)?;
        Ok(())
    })
}

#[inline]
pub fn password_message(password: &str, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'p');
    write_body(buf, |buf| buf.write_cstr(password))
}

#[inline]
pub fn query(query: &str, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'Q');
    write_body(buf, |buf| buf.write_cstr(query))
}

#[inline]
pub fn sasl_initial_response(mechanism: &str, data: &[u8], buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'p');
    write_body(buf, |buf| {
        buf.write_cstr(mechanism)?;
        let len = i32::from_usize(data.len())?;
        buf.write_i32::<BigEndian>(len)?;
        buf.extend_from_slice(data);
        Ok(())
    })
}

#[inline]
pub fn sasl_response(data: &[u8], buf: &mut Vec<u8>) -> io::Result<()> {
    buf.push(b'p');
    write_body(buf, |buf| Ok(buf.extend_from_slice(data)))
}

#[inline]
pub fn ssl_request(buf: &mut Vec<u8>) {
    write_body(buf, |buf| buf.write_i32::<BigEndian>(80877103)).unwrap();
}

#[inline]
pub fn startup_message<'a, I>(parameters: I, buf: &mut Vec<u8>) -> io::Result<()>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    write_body(buf, |buf| {
        buf.write_i32::<BigEndian>(196608).unwrap();
        for (key, value) in parameters {
            buf.write_cstr(key.as_ref())?;
            buf.write_cstr(value.as_ref())?;
        }
        buf.push(0);
        Ok(())
    })
}

#[inline]
pub fn sync(buf: &mut Vec<u8>) {
    buf.push(b'S');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
pub fn terminate(buf: &mut Vec<u8>) {
    buf.push(b'X');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

trait WriteCStr {
    fn write_cstr(&mut self, s: &str) -> Result<(), io::Error>;
}

impl WriteCStr for Vec<u8> {
    #[inline]
    fn write_cstr(&mut self, s: &str) -> Result<(), io::Error> {
        if s.as_bytes().contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "string contains embedded null",
            ));
        }
        self.extend_from_slice(s.as_bytes());
        self.push(0);
        Ok(())
    }
}
