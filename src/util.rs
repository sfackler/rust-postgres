use std::io;
use std::io::prelude::*;

pub fn comma_join<'a, W, I>(writer: &mut W, strs: I) -> io::Result<()>
        where W: Write, I: Iterator<Item=&'a str> {
    let mut first = true;
    for str_ in strs {
        if !first {
            try!(write!(writer, ", "));
        }
        first = false;
        try!(write!(writer, "{}", str_));
    }
    Ok(())
}

pub fn parse_update_count(tag: String) -> u64 {
    tag.split(' ').last().unwrap().parse().unwrap_or(0)
}

pub fn read_all<R: Read>(r: &mut R, mut buf: &mut [u8]) -> io::Result<()> {
    let mut start = 0;
    while start != buf.len() {
        match r.read(&mut buf[start..]) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::Other, "unexpected EOF", None)),
            Ok(len) => start += len,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
