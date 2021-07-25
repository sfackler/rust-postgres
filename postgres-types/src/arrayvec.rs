use arrayvec_0_7::ArrayVec;
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, Kind, ToSql, Type};

impl<'a, T, const N: usize> FromSql<'a> for ArrayVec<T, N>
where
    T: FromSql<'a>,
{
    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref inner) => T::accepts(inner),
            _ => false,
        }
    }

    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<ArrayVec<T, N>, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => return Err("expected array type".into()),
        };

        let array = types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        let mut arrayvec = ArrayVec::new();
        let mut iter = array.values();
        while let Ok(Some(value)) = iter.next() {
            let el = T::from_sql_nullable(member_type, value)?;
            arrayvec.try_push(el).map_err(|err| err.to_string())?;
        }
        Ok(arrayvec)
    }
}

impl<T, const N: usize> ToSql for ArrayVec<T, N>
where
    T: ToSql,
{
    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSql>::to_sql(&&self[..], ty, w)
    }

    to_sql_checked!();
}
