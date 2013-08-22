use std::str;
use std::rt::io::{Decorator, Reader, Writer};
use std::rt::io::extensions::{ReaderUtil, ReaderByteConversions,
                              WriterByteConversions};
use std::rt::io::mem::{MemWriter, MemReader};
use std::hashmap::HashMap;
use std::sys;

pub static PROTOCOL_VERSION: i32 = 0x0003_0000;

pub enum BackendMessage {
    AuthenticationOk,
    BackendKeyData(i32, i32),
    ErrorResponse(HashMap<u8, ~str>),
    ParameterStatus(~str, ~str),
    ParseComplete,
    ReadyForQuery(u8)
}

pub enum FrontendMessage {
    /// name, query, parameter types
    Parse(~str, ~str, ~[i32]),
    Query(~str),
    StartupMessage(HashMap<~str, ~str>),
    Sync,
    Terminate
}

trait WriteString {
    fn write_string(&mut self, s: &str);
}

impl<W: Writer> WriteString for W {
    fn write_string(&mut self, s: &str) {
        self.write(s.as_bytes());
        self.write_u8_(0);
    }
}

pub trait WriteMessage {
    fn write_message(&mut self, &FrontendMessage);
}

impl<W: Writer> WriteMessage for W {
    fn write_message(&mut self, message: &FrontendMessage) {
        debug!("Writing message %?", message);
        let mut buf = MemWriter::new();
        let mut ident = None;

        match *message {
            Parse(ref name, ref query, ref param_types) => {
                ident = Some('P');
                buf.write_string(*name);
                buf.write_string(*query);
                buf.write_be_i16_(param_types.len() as i16);
                for ty in param_types.iter() {
                    buf.write_be_i32_(*ty);
                }
            }
            Query(ref query) => {
                ident = Some('Q');
                buf.write_string(*query);
            }
            StartupMessage(ref params) => {
                buf.write_be_i32_(PROTOCOL_VERSION);
                for (k, v) in params.iter() {
                    buf.write_string(*k);
                    buf.write_string(*v);
                }
                buf.write_u8_(0);
            }
            Sync => {
                ident = Some('S');
            }
            Terminate => {
                ident = Some('X');
            }
        }

        match ident {
            Some(ident) => self.write_u8_(ident as u8),
            None => ()
        }

        // add size of length value
        self.write_be_i32_((buf.inner_ref().len() + sys::size_of::<i32>())
                           as i32);
        self.write(buf.inner());
    }
}

trait ReadString {
    fn read_string(&mut self) -> ~str;
}

impl<R: Reader> ReadString for R {
    fn read_string(&mut self) -> ~str {
        let mut buf = ~[];
        loop {
            let byte = self.read_u8_();
            if byte == 0 {
                break;
            }

            buf.push(byte);
        }

        str::from_bytes_owned(buf)
    }
}

pub trait ReadMessage {
    fn read_message(&mut self) -> BackendMessage;
}

impl<R: Reader> ReadMessage for R {
    fn read_message(&mut self) -> BackendMessage {
        debug!("Reading message");

        let ident = self.read_u8_();
        debug!("Ident %?", ident);
        // subtract size of length value
        let len = self.read_be_i32_() as uint - sys::size_of::<i32>();
        let mut buf = MemReader::new(self.read_bytes(len));

        let ret = match ident as char {
            '1' => ParseComplete,
            'E' => read_error_message(&mut buf),
            'K' => BackendKeyData(buf.read_be_i32_(), buf.read_be_i32_()),
            'R' => read_auth_message(&mut buf),
            'S' => ParameterStatus(buf.read_string(), buf.read_string()),
            'Z' => ReadyForQuery(buf.read_u8_()),
            ident => fail!("Unknown message identifier `%c`", ident)
        };
        assert!(buf.eof());
        debug!("Read message %?", ret);
        ret
    }
}

fn read_error_message(buf: &mut MemReader) -> BackendMessage {
    let mut fields = HashMap::new();
    loop {
        let ty = buf.read_u8_();
        if ty == 0 {
            break;
        }

        fields.insert(ty, buf.read_string());
    }

    ErrorResponse(fields)
}

fn read_auth_message(buf: &mut MemReader) -> BackendMessage {
    match buf.read_be_i32_() {
        0 => AuthenticationOk,
        val => fail!("Unknown Authentication identifier `%?`", val)
    }
}
