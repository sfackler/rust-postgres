extern crate time;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use self::time::Timespec;
use std::io::prelude::*;

use Result;
use types::{Type, FromSql, ToSql, IsNull, SessionInfo};

const USEC_PER_SEC: i64 = 1_000_000;
const NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
const TIME_SEC_CONVERSION: i64 = 946684800;

impl FromSql for Timespec {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<Timespec> {
        let t = try!(raw.read_i64::<BigEndian>());
        let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
        let mut usec = t % USEC_PER_SEC;

        if usec < 0 {
            sec -= 1;
            usec = USEC_PER_SEC + usec;
        }

        Ok(Timespec::new(sec, (usec * NSEC_PER_USEC) as i32))
    }

    accepts!(Type::Timestamp, Type::Timestamptz);
}

impl ToSql for Timespec {
    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut w: &mut W,
                                 _: &SessionInfo)
                                 -> Result<IsNull> {
        let t = (self.sec - TIME_SEC_CONVERSION) * USEC_PER_SEC + self.nsec as i64 / NSEC_PER_USEC;
        try!(w.write_i64::<BigEndian>(t));
        Ok(IsNull::No)
    }

    accepts!(Type::Timestamp, Type::Timestamptz);
    to_sql_checked!();
}
