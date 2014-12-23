use std::io::IoResult;

pub fn comma_join<'a, W, I>(writer: &mut W, mut strs: I) -> IoResult<()>
        where W: Writer, I: Iterator<&'a str> {
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

pub fn parse_update_count(tag: String) -> uint {
    tag.split(' ').last().unwrap().parse().unwrap_or(0)
}
