//! Conversions to and from Postgres's binary format for the numeric type.
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::boxed::Box as StdBox;
use std::collections::VecDeque;
use std::error::Error;
use std::str::{self, FromStr};

/// Serializes a `NUMERIC` value.
#[inline]
pub fn numeric_to_sql(v: Numeric, buf: &mut BytesMut) {
    let num_digits = v.digits.len() as u16;
    buf.put_u16(num_digits);
    buf.put_i16(v.weight);
    buf.put_u16(v.sign.into_u16());
    buf.put_u16(v.scale);

    for digit in v.digits {
        buf.put_i16(digit);
    }
}

/// Deserializes a `NUMERIC` value.
#[inline]
pub fn numeric_from_sql(mut buf: &[u8]) -> Result<Numeric, StdBox<dyn Error + Sync + Send>> {
    let num_digits = buf.read_u16::<BigEndian>()?;
    let mut digits = Vec::with_capacity(num_digits.into());

    let weight = buf.read_i16::<BigEndian>()?;
    let sign = NumericSign::try_from_u16(buf.read_u16::<BigEndian>()?)?;

    let scale = buf.read_u16::<BigEndian>()?;

    for _ in 0..num_digits {
        digits.push(buf.read_i16::<BigEndian>()?);
    }

    Ok(Numeric {
        sign,
        scale,
        weight,
        digits,
    })
}

/// A Posgres numeric
#[derive(Debug, PartialEq, Eq)]
pub struct Numeric {
    sign: NumericSign,
    scale: u16,
    weight: i16,
    digits: Vec<i16>,
}

impl Numeric {
    /// Returns the number of digits.
    #[inline]
    pub fn num_digits(&self) -> usize {
        self.digits.len()
    }

    /// Returns the weight of the numeric value.
    #[inline]
    pub fn weight(&self) -> i16 {
        self.weight
    }

    /// Returns the scale of the numeric value.
    #[inline]
    pub fn scale(&self) -> u16 {
        self.scale
    }

    /// Returns the sign of the numeric value.
    #[inline]
    pub fn sign(&self) -> NumericSign {
        self.sign
    }

    fn nan() -> Self {
        Self {
            sign: NumericSign::NaN,
            scale: 0,
            weight: 0,
            digits: vec![],
        }
    }

    fn infinity() -> Self {
        Self {
            sign: NumericSign::PositiveInfinity,
            scale: 0,
            weight: 0,
            digits: vec![],
        }
    }

    fn negative_infinity() -> Self {
        Self {
            sign: NumericSign::NegativeInfinity,
            scale: 0,
            weight: 0,
            digits: vec![],
        }
    }
}

impl std::fmt::Display for Numeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.sign {
            NumericSign::Positive | NumericSign::Negative => {
                if self.sign == NumericSign::Negative {
                    write!(f, "-")?;
                }
                if self.weight >= 0 {
                    for i in 0..self.weight + 1 {
                        let digit = self.digits.get(i as usize).unwrap_or(&0);
                        if i == 0 {
                            write!(f, "{digit}")?;
                        } else {
                            write!(f, "{digit:0>4}")?;
                        }
                    }
                }

                let mut weight = self.weight;
                let mut scale = self.scale;
                if weight < 0 && scale > 0 {
                    write!(f, "0.")?;

                    while weight < -1 {
                        write!(f, "0000")?;
                        weight += 1;
                        scale -= 4;
                    }
                } else if scale > 0 {
                    write!(f, ".")?;
                }
                if scale > 0 {
                    let first_decimal_index = if weight < 0 { 0 } else { weight as usize + 1 };

                    let mut decimals = scale;
                    for i in first_decimal_index..self.digits.len() {
                        let digit = self.digits[i];
                        if decimals > 4 {
                            write!(f, "{digit:0>4}")?;
                            decimals -= 4;
                        } else {
                            let digit = digit / 10_i16.pow(4 - decimals as u32);
                            write!(f, "{digit:0>w$}", w = decimals as usize)?;
                            decimals = 0;
                        }
                    }

                    if decimals > 0 {
                        write!(f, "{:0>width$}", 0, width = decimals as usize)?;
                    }
                }

                Ok(())
            }
            NumericSign::NaN => write!(f, "NaN"),
            NumericSign::PositiveInfinity => write!(f, "Infinity"),
            NumericSign::NegativeInfinity => write!(f, "-Infinity"),
        }
    }
}

fn split_e(s: &[u8]) -> (&[u8], Option<&[u8]>) {
    let mut s = s.splitn(2, |&b| b == b'e' || b == b'E');
    let first = s.next().unwrap();
    let second = s.next();
    (first, second)
}

fn split_decimal(s: &[u8]) -> (&[u8], Option<&[u8]>) {
    let mut s = s.splitn(2, |&b| b == b'.');
    let first = s.next().unwrap();
    let second = s.next();
    (first, second)
}

impl FromStr for Numeric {
    type Err = StdBox<dyn Error + Sync + Send>;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut digits = VecDeque::new();
        let mut scale = 0;
        let mut sign = NumericSign::Positive;

        if value.eq_ignore_ascii_case("NaN") {
            return Ok(Numeric::nan());
        }
        if value.eq_ignore_ascii_case("Infinity") || value.eq_ignore_ascii_case("Inf") {
            return Ok(Numeric::infinity());
        }
        if value.eq_ignore_ascii_case("-Infinity") || value.eq_ignore_ascii_case("-Inf") {
            return Ok(Numeric::negative_infinity());
        }

        let mut s = value.as_bytes();
        if let Some(&b'-') = s.first() {
            sign = NumericSign::Negative;
            s = &s[1..];
        };

        if s.is_empty() {
            return Err("empty string".into());
        }

        let (s, e) = split_e(s);
        let (s, decimal) = split_decimal(s);
        let mut decimal: VecDeque<u8> = decimal.unwrap_or(b"").to_vec().into();
        let mut integer: VecDeque<u8> = s.to_vec().into();

        if let Some(mut e) = e {
            if e.is_empty() {
                return Err("empty scientific notation string".into());
            }

            let mut positive = true;
            let mut exp = 0;

            if let Some(&b'-') = e.first() {
                positive = false;
                e = &e[1..];
            } else if let Some(&b'+') = e.first() {
                e = &e[1..];
            }

            for &b in e {
                if !b.is_ascii_digit() {
                    return Err("scientific notation string contain non-digit character".into());
                }
                exp = exp * 10 + (b - b'0') as u16;
            }

            if positive {
                while !decimal.is_empty() && exp > 0 {
                    integer.push_back(decimal[0]);
                    decimal.pop_front();
                    exp -= 1;
                }
                for _ in 0..exp {
                    integer.push_back(b'0');
                }
            } else {
                while !(integer.is_empty() || integer == b"0") && exp > 0 {
                    decimal.push_front(integer[integer.len() - 1]);
                    integer.pop_back();
                    exp -= 1;
                }
                for _ in 0..exp {
                    decimal.push_front(b'0');
                }
            }
        }

        // remove leading zeros from integer
        while integer.len() > 1 && integer[0] == b'0' {
            integer.pop_front();
        }

        let mut weight = if integer.is_empty() {
            -1
        } else {
            integer.len().div_ceil(4) as i16 - 1
        };

        if weight >= 0 {
            let integer: Vec<u8> = integer.into();
            for chunk in integer.rchunks(4) {
                let mut digit = 0;
                for &b in chunk {
                    if !b.is_ascii_digit() {
                        return Err("integer part string contain non-digit character".into());
                    }
                    digit = digit * 10 + (b - b'0') as i16;
                }
                digits.push_front(digit);
            }
        }

        // parse the decimal part
        if !decimal.is_empty() {
            scale = decimal.len() as u16;

            let decimal: Vec<u8> = decimal.into();
            for chunk in decimal.chunks(4) {
                let mut digit = 0;
                for i in 0..4 {
                    let b = chunk.get(i).unwrap_or(&b'0');
                    if !b.is_ascii_digit() {
                        return Err("decimal part string contain non-digit character".into());
                    }
                    digit = digit * 10 + (b - b'0') as i16;
                }
                digits.push_back(digit);
            }
        }

        // drop trailing zeros
        while digits.back() == Some(&0) {
            digits.pop_back();
        }
        // drop leading zeros
        while digits.front() == Some(&0) {
            weight -= 1;
            digits.pop_front();
        }

        Ok(Numeric {
            sign,
            scale,
            weight,
            digits: digits.into(),
        })
    }
}

/// Numeric sign
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NumericSign {
    /// Positive number
    Positive,
    /// Negative number
    Negative,
    /// Not a number
    NaN,
    /// Positive infinity
    PositiveInfinity,
    /// Negative infinity
    NegativeInfinity,
}

impl NumericSign {
    #[inline]
    fn try_from_u16(sign: u16) -> Result<NumericSign, StdBox<dyn Error + Sync + Send>> {
        match sign {
            0x0000 => Ok(NumericSign::Positive),
            0x4000 => Ok(NumericSign::Negative),
            0xC000 => Ok(NumericSign::NaN),
            0xD000 => Ok(NumericSign::PositiveInfinity),
            0xF000 => Ok(NumericSign::NegativeInfinity),
            _ => Err("invalid sign in numeric value".into()),
        }
    }

    #[inline]
    fn into_u16(self) -> u16 {
        match self {
            NumericSign::Positive => 0x0000,
            NumericSign::Negative => 0x4000,
            NumericSign::NaN => 0xC000,
            NumericSign::PositiveInfinity => 0xD000,
            NumericSign::NegativeInfinity => 0xF000,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_string_deserialization_and_serialization() {
        let cases = &[
            (
                "0",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 0,
                    digits: vec![],
                },
            ),
            (
                "1",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 0,
                    digits: vec![1],
                },
            ),
            (
                "-1",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 0,
                    digits: vec![1],
                },
            ),
            (
                "10",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 0,
                    digits: vec![10],
                },
            ),
            (
                "-10",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 0,
                    digits: vec![10],
                },
            ),
            (
                "20000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 1,
                    digits: vec![2],
                },
            ),
            (
                "-20000",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 1,
                    digits: vec![2],
                },
            ),
            (
                "20001",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 1,
                    digits: vec![2, 1],
                },
            ),
            (
                "-20001",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 1,
                    digits: vec![2, 1],
                },
            ),
            (
                "200000000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 2,
                    digits: vec![2],
                },
            ),
            (
                "2.0",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 1,
                    weight: 0,
                    digits: vec![2],
                },
            ),
            (
                "2.1",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 1,
                    weight: 0,
                    digits: vec![2, 1000],
                },
            ),
            (
                "2.10",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 2,
                    weight: 0,
                    digits: vec![2, 1000],
                },
            ),
            (
                "200000000.0001",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 4,
                    weight: 2,
                    digits: vec![2, 0, 0, 1],
                },
            ),
            (
                "-200000000.0001",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 4,
                    weight: 2,
                    digits: vec![2, 0, 0, 1],
                },
            ),
            (
                "0.1",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 1,
                    weight: -1,
                    digits: vec![1000],
                },
            ),
            (
                "-0.1",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 1,
                    weight: -1,
                    digits: vec![1000],
                },
            ),
            (
                "123.456",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 3,
                    weight: 0,
                    digits: vec![123, 4560],
                },
            ),
            (
                "-123.456",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 3,
                    weight: 0,
                    digits: vec![123, 4560],
                },
            ),
            (
                "-123.0456",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 4,
                    weight: 0,
                    digits: vec![123, 456],
                },
            ),
            (
                "0.1000000000000000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 16,
                    weight: -1,
                    digits: vec![1000],
                },
            ),
            (
                "-0.1000000000000000",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 16,
                    weight: -1,
                    digits: vec![1000],
                },
            ),
            (
                "0.003159370000000000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 18,
                    weight: -1,
                    digits: vec![31, 5937],
                },
            ),
            (
                "-0.003159370000000000",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 18,
                    weight: -1,
                    digits: vec![31, 5937],
                },
            ),
            (
                "0.0000000000000002",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 16,
                    weight: -4,
                    digits: vec![2],
                },
            ),
            (
                "-0.0000000000000002",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 16,
                    weight: -4,
                    digits: vec![2],
                },
            ),
        ];

        for (str, n) in cases {
            assert_eq!(*str, n.to_string(), "numeric to string");
            let num = str.parse::<Numeric>().expect("parse numeric");
            assert_eq!(num, *n, "numeric from string");
        }
    }

    #[test]
    fn test_from_scientific_notation() {
        let cases = &[
            (
                "2e4",
                "20000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 1,
                    digits: vec![2],
                },
            ),
            (
                "2e+4",
                "20000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 1,
                    digits: vec![2],
                },
            ),
            (
                "-2e4",
                "-20000",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 1,
                    digits: vec![2],
                },
            ),
            (
                "-2e-4",
                "-0.0002",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 4,
                    weight: -1,
                    digits: vec![2],
                },
            ),
            (
                "1.234e4",
                "12340",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 1,
                    digits: vec![1, 2340],
                },
            ),
            (
                "-1.234e4",
                "-12340",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 1,
                    digits: vec![1, 2340],
                },
            ),
            (
                "1.234e5",
                "123400",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 1,
                    digits: vec![12, 3400],
                },
            ),
            (
                "-1.234e5",
                "-123400",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 1,
                    digits: vec![12, 3400],
                },
            ),
            (
                "1.234e8",
                "123400000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 2,
                    digits: vec![1, 2340],
                },
            ),
            (
                "-1.234e8",
                "-123400000",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 2,
                    digits: vec![1, 2340],
                },
            ),
            (
                "0.0001e4",
                "1",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 0,
                    digits: vec![1],
                },
            ),
            (
                "-0.0001e4",
                "-1",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 0,
                    digits: vec![1],
                },
            ),
            (
                "0.0001e5",
                "10",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 0,
                    digits: vec![10],
                },
            ),
            (
                "-0.0001e5",
                "-10",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 0,
                    digits: vec![10],
                },
            ),
            (
                "2e16",
                "20000000000000000",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 0,
                    weight: 4,
                    digits: vec![2],
                },
            ),
            (
                "-2e16",
                "-20000000000000000",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 0,
                    weight: 4,
                    digits: vec![2],
                },
            ),
            (
                "2e-16",
                "0.0000000000000002",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 16,
                    weight: -4,
                    digits: vec![2],
                },
            ),
            (
                "-2e-16",
                "-0.0000000000000002",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 16,
                    weight: -4,
                    digits: vec![2],
                },
            ),
            (
                "2e-17",
                "0.00000000000000002",
                Numeric {
                    sign: NumericSign::Positive,
                    scale: 17,
                    weight: -5,
                    digits: vec![2000],
                },
            ),
            (
                "-2e-17",
                "-0.00000000000000002",
                Numeric {
                    sign: NumericSign::Negative,
                    scale: 17,
                    weight: -5,
                    digits: vec![2000],
                },
            ),
        ];

        for (e, str, n) in cases {
            let num = e.parse::<Numeric>().expect("parse numeric");
            assert_eq!(num, *n, "{e} to numeric");
            assert_eq!(num.to_string(), *str, "{e} back to string");
        }
    }

    use proptest::prelude::*;
    proptest! {
        #[test]
        fn test_arbitrary_f64_from_string_and_back(value in any::<f64>()) {
            let prop_val = value.to_string();
            let numeric = Numeric::from_str(&prop_val).expect("parse numeric");
            let str_val = numeric.to_string();
            assert_eq!(prop_val, str_val, "proprty test value {value}");
        }
        #[test]
        fn test_arbitrary_i64_from_string_and_back(value in any::<i64>()) {
            let prop_val = value.to_string();
            let numeric = Numeric::from_str(&prop_val).expect("parse numeric");
            let str_val = numeric.to_string();
            assert_eq!(prop_val, str_val, "proprty test value {value}");
        }
    }
}
