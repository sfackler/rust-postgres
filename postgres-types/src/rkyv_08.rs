use std::{collections::HashMap, error::Error, hash::Hasher, net::IpAddr};

use crate::{Format, IsNull, ToSql, Type};
use bytes::BytesMut;
use postgres_protocol::types;

use rkyv_08::{
    collections::swiss_table::ArchivedHashMap, net::ArchivedIpAddr,
    niche::option_box::ArchivedOptionBox, option::ArchivedOption, string::ArchivedString,
    vec::ArchivedVec,
};

macro_rules! fwd_accepts {
    ($ty:ty) => {
        #[inline]
        fn accepts(ty: &Type) -> bool {
            <$ty as ToSql>::accepts(ty)
        }
    };
}

impl ToSql for ArchivedString {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.as_str().to_sql(ty, out)
    }

    fwd_accepts!(&str);
    to_sql_checked!();
}

impl<T> ToSql for ArchivedVec<T>
where
    T: ToSql,
{
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.as_slice().to_sql(ty, out)
    }

    fwd_accepts!(&[T]);
    to_sql_checked!();
}

impl<T> ToSql for ArchivedOption<T>
where
    T: ToSql,
{
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            ArchivedOption::Some(value) => value.to_sql(ty, out),
            ArchivedOption::None => Ok(IsNull::Yes),
        }
    }

    fn encode_format(&self, ty: &Type) -> Format {
        match self {
            ArchivedOption::Some(ref val) => val.encode_format(ty),
            ArchivedOption::None => Format::Binary,
        }
    }

    fwd_accepts!(Option<T>);
    to_sql_checked!();
}

impl<T> ToSql for ArchivedOptionBox<T>
where
    T: ToSql,
{
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self.as_ref() {
            Some(value) => value.to_sql(ty, out),
            None => Ok(IsNull::Yes),
        }
    }

    fn encode_format(&self, ty: &Type) -> Format {
        match self.as_ref() {
            Some(val) => val.encode_format(ty),
            None => Format::Binary,
        }
    }

    fwd_accepts!(Option<T>);
    to_sql_checked!();
}

impl ToSql for ArchivedIpAddr {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.as_ipaddr().to_sql(ty, out)
    }

    fwd_accepts!(IpAddr);
    to_sql_checked!();
}

impl<H> ToSql for ArchivedHashMap<ArchivedString, ArchivedOption<ArchivedString>, H>
where
    H: Hasher,
{
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::hstore_to_sql(
            self.iter()
                .map(|(k, v)| (k.as_ref(), v.as_ref().map(|v| v.as_ref()))),
            out,
        )?;

        Ok(IsNull::No)
    }

    fwd_accepts!(HashMap<String, Option<String>>);
    to_sql_checked!();
}
