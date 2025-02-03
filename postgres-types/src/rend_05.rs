use crate::{IsNull, ToSql, Type};
use bytes::BytesMut;

macro_rules! impl_rend {
    ($($ty:ty => $orig:ty,)*) => {$(
        impl ToSql for $ty {
            #[inline]
            fn to_sql(
                &self,
                ty: &Type,
                out: &mut BytesMut,
            ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.to_native().to_sql(ty, out)
            }

            #[inline]
            fn accepts(ty: &Type) -> bool {
                <$orig as ToSql>::accepts(ty)
            }

            to_sql_checked!();
        }
    )*};
}

impl_rend! {
    rend_05::f32_le => f32,
    rend_05::f32_be => f32,
    rend_05::f64_le => f64,
    rend_05::f64_be => f64,

    rend_05::i16_le => i16,
    rend_05::i16_be => i16,
    rend_05::i32_le => i32,
    rend_05::i32_be => i32,
    rend_05::i64_le => i64,
    rend_05::i64_be => i64,

    rend_05::u32_le => u32,
    rend_05::u32_be => u32,
}
