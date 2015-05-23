use std::io;
use std::io::prelude::*;

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
