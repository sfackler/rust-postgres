use crate::{accepts, to_sql_checked, types, FromSql, IsNull, ToSql, Type};
use bytes::BytesMut;
use prost_012::Message;
use std::error::Error;

/// A wrapper type to allow for `prost::Message` types to convert to & from Postgres BYTEA values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Protobuf<T>(pub T);

impl<T: Message + Default> FromSql<'_> for Protobuf<T> {
    fn from_sql(_ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(T::decode(types::bytea_from_sql(raw))?))
    }

    accepts!(BYTEA);
}

impl<T: Message> ToSql for Protobuf<T> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[u8] as ToSql>::to_sql(&&*self.0.encode_to_vec(), ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[u8] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}
