extern crate bit_vec;

use postgres_protocol::types;
use self::bit_vec::BitVec;
use std::error::Error;

use types::{FromSql, ToSql, IsNull, Type, SessionInfo};

impl FromSql for BitVec {
    fn from_sql(_: &Type, raw: &[u8], _: &SessionInfo) -> Result<BitVec, Box<Error + Sync + Send>> {
        let varbit = try!(types::varbit_from_sql(raw));
        let mut bitvec = BitVec::from_bytes(varbit.bytes());
        while bitvec.len() > varbit.len() {
            bitvec.pop();
        }

        Ok(bitvec)
    }

    accepts!(Type::Bit, Type::Varbit);
}

impl ToSql for BitVec {
    fn to_sql(&self,
              _: &Type,
              mut out: &mut Vec<u8>,
              _: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        try!(types::varbit_to_sql(self.len(), self.to_bytes().into_iter(), out));
        Ok(IsNull::No)
    }

    accepts!(Type::Bit, Type::Varbit);
    to_sql_checked!();
}
