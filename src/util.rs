use std::io;
use std::io::prelude::*;
use std::ascii::AsciiExt;

pub fn comma_join_quoted_idents<'a, W, I>(writer: &mut W, strs: I) -> io::Result<()>
        where W: Write, I: Iterator<Item=&'a str> {
    let mut first = true;
    for str_ in strs {
        if !first {
            try!(write!(writer, ", "));
        }
        first = false;
        try!(write_quoted_ident(writer, str_));
    }
    Ok(())
}

// See http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html for ident grammar
pub fn write_quoted_ident<W: Write>(w: &mut W, ident: &str) -> io::Result<()> {
    try!(write!(w, "U&\""));
    for ch in ident.chars() {
        match ch {
            '"' => try!(write!(w, "\"\"")),
            '\\' => try!(write!(w, "\\\\")),
            ch if ch.is_ascii() => try!(write!(w, "{}", ch)),
            ch => try!(write!(w, "\\+{:06X}", ch as u32)),
        }
    }
    write!(w, "\"")
}

pub fn parse_update_count(tag: String) -> u64 {
    tag.split(' ').last().unwrap().parse().unwrap_or(0)
}

pub fn read_all<R: Read>(r: &mut R, mut buf: &mut [u8]) -> io::Result<()> {
    let mut start = 0;
    while start != buf.len() {
        match r.read(&mut buf[start..]) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::Other, "unexpected EOF")),
            Ok(len) => start += len,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
