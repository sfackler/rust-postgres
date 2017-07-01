//! Conversions to and from Postgres's binary format for various types.
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use fallible_iterator::FallibleIterator;
use std::error::Error;
use std::str;
use std::boxed::Box as StdBox;

use {Oid, IsNull, write_nullable, FromUsize};

const RANGE_UPPER_UNBOUNDED: u8 = 0b0001_0000;
const RANGE_LOWER_UNBOUNDED: u8 = 0b0000_1000;
const RANGE_UPPER_INCLUSIVE: u8 = 0b0000_0100;
const RANGE_LOWER_INCLUSIVE: u8 = 0b0000_0010;
const RANGE_EMPTY: u8 = 0b0000_0001;

/// Serializes a `BOOL` value.
#[inline]
pub fn bool_to_sql(v: bool, buf: &mut Vec<u8>) {
    buf.push(v as u8);
}

/// Deserializes a `BOOL` value.
#[inline]
pub fn bool_from_sql(buf: &[u8]) -> Result<bool, StdBox<Error + Sync + Send>> {
    if buf.len() != 1 {
        return Err("invalid buffer size".into());
    }

    Ok(buf[0] != 0)
}

/// Serializes a `BYTEA` value.
#[inline]
pub fn bytea_to_sql(v: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(v);
}

/// Deserializes a `BYTEA value.
#[inline]
pub fn bytea_from_sql(buf: &[u8]) -> &[u8] {
    buf
}

/// Serializes a `TEXT`, `VARCHAR`, `CHAR(n)`, `NAME`, or `CITEXT` value.
#[inline]
pub fn text_to_sql(v: &str, buf: &mut Vec<u8>) {
    buf.extend_from_slice(v.as_bytes());
}

/// Deserializes a `TEXT`, `VARCHAR`, `CHAR(n)`, `NAME`, or `CITEXT` value.
#[inline]
pub fn text_from_sql(buf: &[u8]) -> Result<&str, StdBox<Error + Sync + Send>> {
    Ok(str::from_utf8(buf)?)
}

/// Serializes a `"char"` value.
#[inline]
pub fn char_to_sql(v: i8, buf: &mut Vec<u8>) {
    buf.write_i8(v).unwrap();
}

/// Deserializes a `"char"` value.
#[inline]
pub fn char_from_sql(mut buf: &[u8]) -> Result<i8, StdBox<Error + Sync + Send>> {
    let v = buf.read_i8()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `INT2` value.
#[inline]
pub fn int2_to_sql(v: i16, buf: &mut Vec<u8>) {
    buf.write_i16::<BigEndian>(v).unwrap();
}

/// Deserializes an `INT2` value.
#[inline]
pub fn int2_from_sql(mut buf: &[u8]) -> Result<i16, StdBox<Error + Sync + Send>> {
    let v = buf.read_i16::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `INT4` value.
#[inline]
pub fn int4_to_sql(v: i32, buf: &mut Vec<u8>) {
    buf.write_i32::<BigEndian>(v).unwrap();
}

/// Deserializes an `INT4` value.
#[inline]
pub fn int4_from_sql(mut buf: &[u8]) -> Result<i32, StdBox<Error + Sync + Send>> {
    let v = buf.read_i32::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `OID` value.
#[inline]
pub fn oid_to_sql(v: Oid, buf: &mut Vec<u8>) {
    buf.write_u32::<BigEndian>(v).unwrap();
}

/// Deserializes an `OID` value.
#[inline]
pub fn oid_from_sql(mut buf: &[u8]) -> Result<Oid, StdBox<Error + Sync + Send>> {
    let v = buf.read_u32::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `INT8` value.
#[inline]
pub fn int8_to_sql(v: i64, buf: &mut Vec<u8>) {
    buf.write_i64::<BigEndian>(v).unwrap();
}

/// Deserializes an `INT8` value.
#[inline]
pub fn int8_from_sql(mut buf: &[u8]) -> Result<i64, StdBox<Error + Sync + Send>> {
    let v = buf.read_i64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes a `FLOAT4` value.
#[inline]
pub fn float4_to_sql(v: f32, buf: &mut Vec<u8>) {
    buf.write_f32::<BigEndian>(v).unwrap();
}

/// Deserializes a `FLOAT4` value.
#[inline]
pub fn float4_from_sql(mut buf: &[u8]) -> Result<f32, StdBox<Error + Sync + Send>> {
    let v = buf.read_f32::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes a `FLOAT8` value.
#[inline]
pub fn float8_to_sql(v: f64, buf: &mut Vec<u8>) {
    buf.write_f64::<BigEndian>(v).unwrap();
}

/// Deserializes a `FLOAT8` value.
#[inline]
pub fn float8_from_sql(mut buf: &[u8]) -> Result<f64, StdBox<Error + Sync + Send>> {
    let v = buf.read_f64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `HSTORE` value.
#[inline]
pub fn hstore_to_sql<'a, I>(values: I, buf: &mut Vec<u8>) -> Result<(), StdBox<Error + Sync + Send>>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    let mut count = 0;
    for (key, value) in values {
        count += 1;

        write_pascal_string(key, buf)?;

        match value {
            Some(value) => {
                write_pascal_string(value, buf)?;
            }
            None => buf.write_i32::<BigEndian>(-1).unwrap(),
        }
    }

    let count = i32::from_usize(count)?;
    (&mut buf[base..base + 4])
        .write_i32::<BigEndian>(count)
        .unwrap();

    Ok(())
}

fn write_pascal_string(s: &str, buf: &mut Vec<u8>) -> Result<(), StdBox<Error + Sync + Send>> {
    let size = i32::from_usize(s.len())?;
    buf.write_i32::<BigEndian>(size).unwrap();
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

/// Deserializes an `HSTORE` value.
#[inline]
pub fn hstore_from_sql<'a>(
    mut buf: &'a [u8],
) -> Result<HstoreEntries<'a>, StdBox<Error + Sync + Send>> {
    let count = buf.read_i32::<BigEndian>()?;
    if count < 0 {
        return Err("invalid entry count".into());
    }

    Ok(HstoreEntries {
        remaining: count,
        buf: buf,
    })
}

/// A fallible iterator over `HSTORE` entries.
pub struct HstoreEntries<'a> {
    remaining: i32,
    buf: &'a [u8],
}

impl<'a> FallibleIterator for HstoreEntries<'a> {
    type Item = (&'a str, Option<&'a str>);
    type Error = StdBox<Error + Sync + Send>;

    #[inline]
    fn next(&mut self) -> Result<Option<(&'a str, Option<&'a str>)>, StdBox<Error + Sync + Send>> {
        if self.remaining == 0 {
            if !self.buf.is_empty() {
                return Err("invalid buffer size".into());
            }
            return Ok(None);
        }

        self.remaining -= 1;

        let key_len = self.buf.read_i32::<BigEndian>()?;
        if key_len < 0 {
            return Err("invalid key length".into());
        }
        let (key, buf) = self.buf.split_at(key_len as usize);
        let key = str::from_utf8(key)?;
        self.buf = buf;

        let value_len = self.buf.read_i32::<BigEndian>()?;
        let value = if value_len < 0 {
            None
        } else {
            let (value, buf) = self.buf.split_at(value_len as usize);
            let value = str::from_utf8(value)?;
            self.buf = buf;
            Some(value)
        };

        Ok(Some((key, value)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

/// Serializes a `VARBIT` or `BIT` value.
#[inline]
pub fn varbit_to_sql<I>(
    len: usize,
    v: I,
    buf: &mut Vec<u8>,
) -> Result<(), StdBox<Error + Sync + Send>>
where
    I: Iterator<Item = u8>,
{
    let len = i32::from_usize(len)?;
    buf.write_i32::<BigEndian>(len).unwrap();

    for byte in v {
        buf.push(byte);
    }

    Ok(())
}

/// Deserializes a `VARBIT` or `BIT` value.
#[inline]
pub fn varbit_from_sql<'a>(mut buf: &'a [u8]) -> Result<Varbit<'a>, StdBox<Error + Sync + Send>> {
    let len = buf.read_i32::<BigEndian>()?;
    if len < 0 {
        return Err("invalid varbit length".into());
    }
    let bytes = (len as usize + 7) / 8;
    if buf.len() != bytes {
        return Err("invalid message length".into());
    }

    Ok(Varbit {
        len: len as usize,
        bytes: buf,
    })
}

/// A `VARBIT` value.
pub struct Varbit<'a> {
    len: usize,
    bytes: &'a [u8],
}

impl<'a> Varbit<'a> {
    /// Returns the number of bits.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the bits as a slice of bytes.
    #[inline]
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// Serializes a `TIMESTAMP` or `TIMESTAMPTZ` value.
///
/// The value should represent the number of microseconds since midnight, January 1st, 2000.
#[inline]
pub fn timestamp_to_sql(v: i64, buf: &mut Vec<u8>) {
    buf.write_i64::<BigEndian>(v).unwrap();
}

/// Deserializes a `TIMESTAMP` or `TIMESTAMPTZ` value.
///
/// The value represents the number of microseconds since midnight, January 1st, 2000.
#[inline]
pub fn timestamp_from_sql(mut buf: &[u8]) -> Result<i64, StdBox<Error + Sync + Send>> {
    let v = buf.read_i64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid message length".into());
    }
    Ok(v)
}

/// Serializes a `DATE` value.
///
/// The value should represent the number of days since January 1st, 2000.
#[inline]
pub fn date_to_sql(v: i32, buf: &mut Vec<u8>) {
    buf.write_i32::<BigEndian>(v).unwrap();
}

/// Deserializes a `DATE` value.
///
/// The value represents the number of days since January 1st, 2000.
#[inline]
pub fn date_from_sql(mut buf: &[u8]) -> Result<i32, StdBox<Error + Sync + Send>> {
    let v = buf.read_i32::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid message length".into());
    }
    Ok(v)
}

/// Serializes a `TIME` or `TIMETZ` value.
///
/// The value should represent the number of microseconds since midnight.
#[inline]
pub fn time_to_sql(v: i64, buf: &mut Vec<u8>) {
    buf.write_i64::<BigEndian>(v).unwrap();
}

/// Deserializes a `TIME` or `TIMETZ` value.
///
/// The value represents the number of microseconds since midnight.
#[inline]
pub fn time_from_sql(mut buf: &[u8]) -> Result<i64, StdBox<Error + Sync + Send>> {
    let v = buf.read_i64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid message length".into());
    }
    Ok(v)
}

/// Serializes a `MACADDR` value.
#[inline]
pub fn macaddr_to_sql(v: [u8; 6], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&v);
}

/// Deserializes a `MACADDR` value.
#[inline]
pub fn macaddr_from_sql(buf: &[u8]) -> Result<[u8; 6], StdBox<Error + Sync + Send>> {
    if buf.len() != 6 {
        return Err("invalid message length".into());
    }
    let mut out = [0; 6];
    out.copy_from_slice(buf);
    Ok(out)
}

/// Serializes a `UUID` value.
#[inline]
pub fn uuid_to_sql(v: [u8; 16], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&v);
}

/// Deserializes a `UUID` value.
#[inline]
pub fn uuid_from_sql(buf: &[u8]) -> Result<[u8; 16], StdBox<Error + Sync + Send>> {
    if buf.len() != 16 {
        return Err("invalid message length".into());
    }
    let mut out = [0; 16];
    out.copy_from_slice(buf);
    Ok(out)
}

/// Serializes an array value.
#[inline]
pub fn array_to_sql<T, I, J, F>(
    dimensions: I,
    has_nulls: bool,
    element_type: Oid,
    elements: J,
    mut serializer: F,
    buf: &mut Vec<u8>,
) -> Result<(), StdBox<Error + Sync + Send>>
where
    I: IntoIterator<Item = ArrayDimension>,
    J: IntoIterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>) -> Result<IsNull, StdBox<Error + Sync + Send>>,
{
    let dimensions_idx = buf.len();
    buf.extend_from_slice(&[0; 4]);
    buf.write_i32::<BigEndian>(has_nulls as i32).unwrap();
    buf.write_u32::<BigEndian>(element_type).unwrap();

    let mut num_dimensions = 0;
    for dimension in dimensions {
        num_dimensions += 1;
        buf.write_i32::<BigEndian>(dimension.len).unwrap();
        buf.write_i32::<BigEndian>(dimension.lower_bound).unwrap();
    }

    let num_dimensions = i32::from_usize(num_dimensions)?;
    (&mut buf[dimensions_idx..dimensions_idx + 4])
        .write_i32::<BigEndian>(num_dimensions)
        .unwrap();

    for element in elements {
        write_nullable(|buf| serializer(element, buf), buf)?;
    }

    Ok(())
}

/// Deserializes an array value.
#[inline]
pub fn array_from_sql<'a>(mut buf: &'a [u8]) -> Result<Array<'a>, StdBox<Error + Sync + Send>> {
    let dimensions = buf.read_i32::<BigEndian>()?;
    if dimensions < 0 {
        return Err("invalid dimension count".into());
    }
    let has_nulls = buf.read_i32::<BigEndian>()? != 0;
    let element_type = buf.read_u32::<BigEndian>()?;

    let mut r = buf;
    let mut elements = 1i32;
    for _ in 0..dimensions {
        let len = r.read_i32::<BigEndian>()?;
        if len < 0 {
            return Err("invalid dimension size".into());
        }
        let _lower_bound = r.read_i32::<BigEndian>()?;
        elements = match elements.checked_mul(len) {
            Some(elements) => elements,
            None => return Err("too many array elements".into()),
        };
    }

    if dimensions == 0 {
        elements = 0;
    }

    Ok(Array {
        dimensions: dimensions,
        has_nulls: has_nulls,
        element_type: element_type,
        elements: elements,
        buf: buf,
    })
}

/// A Postgres array.
pub struct Array<'a> {
    dimensions: i32,
    has_nulls: bool,
    element_type: Oid,
    elements: i32,
    buf: &'a [u8],
}

impl<'a> Array<'a> {
    /// Returns true if there are `NULL` elements.
    #[inline]
    pub fn has_nulls(&self) -> bool {
        self.has_nulls
    }

    /// Returns the OID of the elements of the array.
    #[inline]
    pub fn element_type(&self) -> Oid {
        self.element_type
    }

    /// Returns an iterator over the dimensions of the array.
    #[inline]
    pub fn dimensions(&self) -> ArrayDimensions<'a> {
        ArrayDimensions(&self.buf[..self.dimensions as usize * 8])
    }

    /// Returns an iterator over the values of the array.
    #[inline]
    pub fn values(&self) -> ArrayValues<'a> {
        ArrayValues {
            remaining: self.elements,
            buf: &self.buf[self.dimensions as usize * 8..],
        }
    }
}

/// An iterator over the dimensions of an array.
pub struct ArrayDimensions<'a>(&'a [u8]);

impl<'a> FallibleIterator for ArrayDimensions<'a> {
    type Item = ArrayDimension;
    type Error = StdBox<Error + Sync + Send>;

    #[inline]
    fn next(&mut self) -> Result<Option<ArrayDimension>, StdBox<Error + Sync + Send>> {
        if self.0.is_empty() {
            return Ok(None);
        }

        let len = self.0.read_i32::<BigEndian>()?;
        let lower_bound = self.0.read_i32::<BigEndian>()?;

        Ok(Some(ArrayDimension {
            len: len,
            lower_bound: lower_bound,
        }))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.len() / 8;
        (len, Some(len))
    }
}

/// Information about a dimension of an array.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ArrayDimension {
    /// The length of this dimension.
    pub len: i32,

    /// The base value used to index into this dimension.
    pub lower_bound: i32,
}

/// An iterator over the values of an array, in row-major order.
pub struct ArrayValues<'a> {
    remaining: i32,
    buf: &'a [u8],
}

impl<'a> FallibleIterator for ArrayValues<'a> {
    type Item = Option<&'a [u8]>;
    type Error = StdBox<Error + Sync + Send>;

    #[inline]
    fn next(&mut self) -> Result<Option<Option<&'a [u8]>>, StdBox<Error + Sync + Send>> {
        if self.remaining == 0 {
            if !self.buf.is_empty() {
                return Err("invalid message length".into());
            }
            return Ok(None);
        }
        self.remaining -= 1;

        let len = self.buf.read_i32::<BigEndian>()?;
        let val = if len < 0 {
            None
        } else {
            if self.buf.len() < len as usize {
                return Err("invalid value length".into());
            }

            let (val, buf) = self.buf.split_at(len as usize);
            self.buf = buf;
            Some(val)
        };

        Ok(Some(val))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

/// Serializes an empty range.
#[inline]
pub fn empty_range_to_sql(buf: &mut Vec<u8>) {
    buf.push(RANGE_EMPTY);
}

/// Serializes a range value.
pub fn range_to_sql<F, G>(
    lower: F,
    upper: G,
    buf: &mut Vec<u8>,
) -> Result<(), StdBox<Error + Sync + Send>>
where
    F: FnOnce(&mut Vec<u8>) -> Result<RangeBound<IsNull>, StdBox<Error + Sync + Send>>,
    G: FnOnce(&mut Vec<u8>) -> Result<RangeBound<IsNull>, StdBox<Error + Sync + Send>>,
{
    let tag_idx = buf.len();
    buf.push(0);
    let mut tag = 0;

    match write_bound(lower, buf)? {
        RangeBound::Inclusive(()) => tag |= RANGE_LOWER_INCLUSIVE,
        RangeBound::Exclusive(()) => {}
        RangeBound::Unbounded => tag |= RANGE_LOWER_UNBOUNDED,
    }

    match write_bound(upper, buf)? {
        RangeBound::Inclusive(()) => tag |= RANGE_UPPER_INCLUSIVE,
        RangeBound::Exclusive(()) => {}
        RangeBound::Unbounded => tag |= RANGE_UPPER_UNBOUNDED,
    }

    buf[tag_idx] = tag;

    Ok(())
}

fn write_bound<F>(
    bound: F,
    buf: &mut Vec<u8>,
) -> Result<RangeBound<()>, StdBox<Error + Sync + Send>>
where
    F: FnOnce(&mut Vec<u8>) -> Result<RangeBound<IsNull>, StdBox<Error + Sync + Send>>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    let (null, ret) = match bound(buf)? {
        RangeBound::Inclusive(null) => (Some(null), RangeBound::Inclusive(())),
        RangeBound::Exclusive(null) => (Some(null), RangeBound::Exclusive(())),
        RangeBound::Unbounded => (None, RangeBound::Unbounded),
    };

    match null {
        Some(null) => {
            let len = match null {
                IsNull::No => i32::from_usize(buf.len() - base - 4)?,
                IsNull::Yes => -1,
            };
            (&mut buf[base..base + 4])
                .write_i32::<BigEndian>(len)
                .unwrap();
        }
        None => buf.truncate(base),
    }

    Ok(ret)
}

/// One side of a range.
pub enum RangeBound<T> {
    /// An inclusive bound.
    Inclusive(T),
    /// An exclusive bound.
    Exclusive(T),
    /// No bound.
    Unbounded,
}

/// Deserializes a range value.
#[inline]
pub fn range_from_sql<'a>(mut buf: &'a [u8]) -> Result<Range<'a>, StdBox<Error + Sync + Send>> {
    let tag = buf.read_u8()?;

    if tag == RANGE_EMPTY {
        if !buf.is_empty() {
            return Err("invalid message size".into());
        }
        return Ok(Range::Empty);
    }

    let lower = read_bound(&mut buf, tag, RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE)?;
    let upper = read_bound(&mut buf, tag, RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE)?;

    if !buf.is_empty() {
        return Err("invalid message size".into());
    }

    Ok(Range::Nonempty(lower, upper))
}

#[inline]
fn read_bound<'a>(
    buf: &mut &'a [u8],
    tag: u8,
    unbounded: u8,
    inclusive: u8,
) -> Result<RangeBound<Option<&'a [u8]>>, StdBox<Error + Sync + Send>> {
    if tag & unbounded != 0 {
        Ok(RangeBound::Unbounded)
    } else {
        let len = buf.read_i32::<BigEndian>()?;
        let value = if len < 0 {
            None
        } else {
            let len = len as usize;
            if buf.len() < len {
                return Err("invalid message size".into());
            }
            let (value, tail) = buf.split_at(len);
            *buf = tail;
            Some(value)
        };

        if tag & inclusive != 0 {
            Ok(RangeBound::Inclusive(value))
        } else {
            Ok(RangeBound::Exclusive(value))
        }
    }
}

/// A Postgres range.
pub enum Range<'a> {
    /// An empty range.
    Empty,
    /// A nonempty range.
    Nonempty(RangeBound<Option<&'a [u8]>>, RangeBound<Option<&'a [u8]>>),
}

/// Serializes a point value.
#[inline]
pub fn point_to_sql(x: f64, y: f64, buf: &mut Vec<u8>) {
    buf.write_f64::<BigEndian>(x).unwrap();
    buf.write_f64::<BigEndian>(y).unwrap();
}

/// Deserializes a point value.
#[inline]
pub fn point_from_sql(mut buf: &[u8]) -> Result<Point, StdBox<Error + Sync + Send>> {
    let x = buf.read_f64::<BigEndian>()?;
    let y = buf.read_f64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(Point { x: x, y: y })
}

/// A Postgres point.
#[derive(Copy, Clone)]
pub struct Point {
    x: f64,
    y: f64,
}

impl Point {
    /// Returns the x coordinate of the point.
    #[inline]
    pub fn x(&self) -> f64 {
        self.x
    }

    /// Returns the y coordinate of the point.
    #[inline]
    pub fn y(&self) -> f64 {
        self.y
    }
}

/// Serializes a box value.
#[inline]
pub fn box_to_sql(x1: f64, y1: f64, x2: f64, y2: f64, buf: &mut Vec<u8>) {
    buf.write_f64::<BigEndian>(x1).unwrap();
    buf.write_f64::<BigEndian>(y1).unwrap();
    buf.write_f64::<BigEndian>(x2).unwrap();
    buf.write_f64::<BigEndian>(y2).unwrap();
}

/// Deserializes a box value.
#[inline]
pub fn box_from_sql(mut buf: &[u8]) -> Result<Box, StdBox<Error + Sync + Send>> {
    let x1 = buf.read_f64::<BigEndian>()?;
    let y1 = buf.read_f64::<BigEndian>()?;
    let x2 = buf.read_f64::<BigEndian>()?;
    let y2 = buf.read_f64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(Box {
        upper_right: Point { x: x1, y: y1 },
        lower_left: Point { x: x2, y: y2 },
    })
}

/// A Postgres box.
#[derive(Copy, Clone)]
pub struct Box {
    upper_right: Point,
    lower_left: Point,
}

impl Box {
    /// Returns the upper right corner of the box.
    #[inline]
    pub fn upper_right(&self) -> Point {
        self.upper_right
    }

    /// Returns the lower left corner of the box.
    #[inline]
    pub fn lower_left(&self) -> Point {
        self.lower_left
    }
}

/// Serializes a Postgres path.
#[inline]
pub fn path_to_sql<I>(
    closed: bool,
    points: I,
    buf: &mut Vec<u8>,
) -> Result<(), StdBox<Error + Sync + Send>>
where
    I: IntoIterator<Item = (f64, f64)>,
{
    buf.push(closed as u8);
    let points_idx = buf.len();
    buf.extend_from_slice(&[0; 4]);

    let mut num_points = 0;
    for (x, y) in points {
        num_points += 1;
        buf.write_f64::<BigEndian>(x).unwrap();
        buf.write_f64::<BigEndian>(y).unwrap();
    }

    let num_points = i32::from_usize(num_points)?;
    (&mut buf[points_idx..])
        .write_i32::<BigEndian>(num_points)
        .unwrap();

    Ok(())
}

/// Deserializes a Postgres path.
#[inline]
pub fn path_from_sql<'a>(mut buf: &'a [u8]) -> Result<Path<'a>, StdBox<Error + Sync + Send>> {
    let closed = buf.read_u8()? != 0;
    let points = buf.read_i32::<BigEndian>()?;

    Ok(Path {
        closed: closed,
        points: points,
        buf: buf,
    })
}

/// A Postgres point.
pub struct Path<'a> {
    closed: bool,
    points: i32,
    buf: &'a [u8],
}

impl<'a> Path<'a> {
    /// Determines if the path is closed or open.
    #[inline]
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Returns an iterator over the points in the path.
    #[inline]
    pub fn points(&self) -> PathPoints<'a> {
        PathPoints {
            remaining: self.points,
            buf: self.buf,
        }
    }
}

/// An iterator over the points of a Postgres path.
pub struct PathPoints<'a> {
    remaining: i32,
    buf: &'a [u8],
}

impl<'a> FallibleIterator for PathPoints<'a> {
    type Item = Point;
    type Error = StdBox<Error + Sync + Send>;

    #[inline]
    fn next(&mut self) -> Result<Option<Point>, StdBox<Error + Sync + Send>> {
        if self.remaining == 0 {
            if !self.buf.is_empty() {
                return Err("invalid message length".into());
            }
            return Ok(None);
        }
        self.remaining -= 1;

        let x = self.buf.read_f64::<BigEndian>()?;
        let y = self.buf.read_f64::<BigEndian>()?;

        Ok(Some(Point { x: x, y: y }))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use fallible_iterator::FallibleIterator;

    use super::*;
    use IsNull;

    #[test]
    fn bool() {
        let mut buf = vec![];
        bool_to_sql(true, &mut buf);
        assert_eq!(bool_from_sql(&buf).unwrap(), true);

        let mut buf = vec![];
        bool_to_sql(false, &mut buf);
        assert_eq!(bool_from_sql(&buf).unwrap(), false);
    }

    #[test]
    fn int2() {
        let mut buf = vec![];
        int2_to_sql(0x0102, &mut buf);
        assert_eq!(int2_from_sql(&buf).unwrap(), 0x0102);
    }

    #[test]
    fn int4() {
        let mut buf = vec![];
        int4_to_sql(0x01020304, &mut buf);
        assert_eq!(int4_from_sql(&buf).unwrap(), 0x01020304);
    }

    #[test]
    fn int8() {
        let mut buf = vec![];
        int8_to_sql(0x0102030405060708, &mut buf);
        assert_eq!(int8_from_sql(&buf).unwrap(), 0x0102030405060708);
    }

    #[test]
    fn float4() {
        let mut buf = vec![];
        float4_to_sql(10343.95, &mut buf);
        assert_eq!(float4_from_sql(&buf).unwrap(), 10343.95);
    }

    #[test]
    fn float8() {
        let mut buf = vec![];
        float8_to_sql(10343.95, &mut buf);
        assert_eq!(float8_from_sql(&buf).unwrap(), 10343.95);
    }

    #[test]
    fn hstore() {
        let mut map = HashMap::new();
        map.insert("hello", Some("world"));
        map.insert("hola", None);

        let mut buf = vec![];
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

        let mut buf = vec![];
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

        let mut buf = vec![];
        array_to_sql(
            dimensions.iter().cloned(),
            true,
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
        ).unwrap();

        let array = array_from_sql(&buf).unwrap();
        assert_eq!(array.has_nulls(), true);
        assert_eq!(array.element_type(), 10);
        assert_eq!(array.dimensions().collect::<Vec<_>>().unwrap(), dimensions);
        assert_eq!(array.values().collect::<Vec<_>>().unwrap(), values);
    }
}
