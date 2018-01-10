extern crate bit_vec;

use postgres_protocol::types;
use self::bit_vec::BitVec;
use std::error::Error;

use types::{FromSql, IsNull, ToSql, Type, BIT, VARBIT};

impl FromSql for BitVec {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<BitVec, Box<Error + Sync + Send>> {
        let varbit = types::varbit_from_sql(raw)?;
        let mut bitvec = BitVec::from_bytes(varbit.bytes());
        while bitvec.len() > varbit.len() {
            bitvec.pop();
        }

        Ok(bitvec)
    }

    accepts!(BIT, VARBIT);
}

impl ToSql for BitVec {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::varbit_to_sql(self.len(), self.to_bytes().into_iter(), out)?;
        Ok(IsNull::No)
    }

    accepts!(BIT, VARBIT);
    to_sql_checked!();
}
