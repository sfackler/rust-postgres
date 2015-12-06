extern crate bit_vec;

use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use self::bit_vec::BitVec;

use Result;
use types::{FromSql, ToSql, IsNull, Type, SessionInfo, downcast};

impl FromSql for BitVec {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<BitVec> {
        let len = try!(raw.read_i32::<BigEndian>()) as usize;
        let mut bytes = vec![];
        try!(raw.take(((len + 7) / 8) as u64).read_to_end(&mut bytes));

        let mut bitvec = BitVec::from_bytes(&bytes);
        while bitvec.len() > len {
            bitvec.pop();
        }

        Ok(bitvec)
    }

    accepts!(Type::Bit, Type::Varbit);
}

impl ToSql for BitVec {
    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut out: &mut W,
                                 _: &SessionInfo)
                                 -> Result<IsNull> {
        try!(out.write_i32::<BigEndian>(try!(downcast(self.len()))));
        try!(out.write_all(&self.to_bytes()));

        Ok(IsNull::No)
    }

    accepts!(Type::Bit, Type::Varbit);
    to_sql_checked!();
}
