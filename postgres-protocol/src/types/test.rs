use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use std::collections::HashMap;

use super::*;
use crate::IsNull;

#[test]
fn bool() {
    let mut buf = BytesMut::new();
    bool_to_sql(true, &mut buf);
    assert_eq!(bool_from_sql(&buf).unwrap(), true);

    let mut buf = BytesMut::new();
    bool_to_sql(false, &mut buf);
    assert_eq!(bool_from_sql(&buf).unwrap(), false);
}

#[test]
fn int2() {
    let mut buf = BytesMut::new();
    int2_to_sql(0x0102, &mut buf);
    assert_eq!(int2_from_sql(&buf).unwrap(), 0x0102);
}

#[test]
fn int4() {
    let mut buf = BytesMut::new();
    int4_to_sql(0x0102_0304, &mut buf);
    assert_eq!(int4_from_sql(&buf).unwrap(), 0x0102_0304);
}

#[test]
fn int8() {
    let mut buf = BytesMut::new();
    int8_to_sql(0x0102_0304_0506_0708, &mut buf);
    assert_eq!(int8_from_sql(&buf).unwrap(), 0x0102_0304_0506_0708);
}

#[test]
#[allow(clippy::float_cmp)]
fn float4() {
    let mut buf = BytesMut::new();
    float4_to_sql(10343.95, &mut buf);
    assert_eq!(float4_from_sql(&buf).unwrap(), 10343.95);
}

#[test]
#[allow(clippy::float_cmp)]
fn float8() {
    let mut buf = BytesMut::new();
    float8_to_sql(10343.95, &mut buf);
    assert_eq!(float8_from_sql(&buf).unwrap(), 10343.95);
}

#[test]
fn hstore() {
    let mut map = HashMap::new();
    map.insert("hello", Some("world"));
    map.insert("hola", None);

    let mut buf = BytesMut::new();
    hstore_to_sql(map.iter().map(|(&k, &v)| (k, v)), &mut buf).unwrap();
    assert_eq!(
        hstore_from_sql(&buf)
            .unwrap()
            .collect::<HashMap<_, _>>()
            .unwrap(),
        map
    );
}

#[test]
fn varbit() {
    let len = 12;
    let bits = [0b0010_1011, 0b0000_1111];

    let mut buf = BytesMut::new();
    varbit_to_sql(len, bits.iter().cloned(), &mut buf).unwrap();
    let out = varbit_from_sql(&buf).unwrap();
    assert_eq!(out.len(), len);
    assert_eq!(out.bytes(), bits);
}

#[test]
fn array() {
    let dimensions = [
        ArrayDimension {
            len: 1,
            lower_bound: 10,
        },
        ArrayDimension {
            len: 2,
            lower_bound: 0,
        },
    ];
    let values = [None, Some(&b"hello"[..])];

    let mut buf = BytesMut::new();
    array_to_sql(
        dimensions.iter().cloned(),
        10,
        values.iter().cloned(),
        |v, buf| match v {
            Some(v) => {
                buf.extend_from_slice(v);
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        },
        &mut buf,
    )
    .unwrap();

    let array = array_from_sql(&buf).unwrap();
    assert_eq!(array.has_nulls(), true);
    assert_eq!(array.element_type(), 10);
    assert_eq!(array.dimensions().collect::<Vec<_>>().unwrap(), dimensions);
    assert_eq!(array.values().collect::<Vec<_>>().unwrap(), values);
}

#[test]
fn non_null_array() {
    let dimensions = [
        ArrayDimension {
            len: 1,
            lower_bound: 10,
        },
        ArrayDimension {
            len: 2,
            lower_bound: 0,
        },
    ];
    let values = [Some(&b"hola"[..]), Some(&b"hello"[..])];

    let mut buf = BytesMut::new();
    array_to_sql(
        dimensions.iter().cloned(),
        10,
        values.iter().cloned(),
        |v, buf| match v {
            Some(v) => {
                buf.extend_from_slice(v);
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        },
        &mut buf,
    )
    .unwrap();

    let array = array_from_sql(&buf).unwrap();
    assert_eq!(array.has_nulls(), false);
    assert_eq!(array.element_type(), 10);
    assert_eq!(array.dimensions().collect::<Vec<_>>().unwrap(), dimensions);
    assert_eq!(array.values().collect::<Vec<_>>().unwrap(), values);
}
