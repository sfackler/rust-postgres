extern crate num;
extern crate rand;

use self::num::rational::BigRational;
use self::num::bigint::{BigInt};
use self::num::traits::{FromPrimitive, Zero};

use self::rand::Rng;
use self::rand::distributions::{IndependentSample, Range};

use types::test_type;

#[test]
fn test_half() {
    let half = BigRational::new(BigInt::from_u64(1).unwrap(),
                                BigInt::from_u64(2).unwrap());
    test_type("NUMERIC", &[(Some(half), "'0.5'"), (None, "NULL")])
}

#[test]
fn test_big() {
    let ratio = BigRational::new(BigInt::parse_bytes(b"21310003213000000001341001231300000032002030201030212312300002132130000000103021400002130121231230000313000003131310000012210000200303001", 10).unwrap(),
                                 BigInt::parse_bytes(b"100000000000000000000000000000000000000000000000000000000000000000", 10).unwrap());
    test_type("NUMERIC", &[(Some(ratio), "'213100032130000000013410012313000000320020302010302123123000021321300000.00103021400002130121231230000313000003131310000012210000200303001'"), (None, "NULL")]);
}

#[test]
fn test_small() {
    let ratio = BigRational::new(BigInt::parse_bytes(b"1002000000000000000000000030000540000080005120000000700409", 10).unwrap(),
                                 BigInt::parse_bytes(b"100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 10).unwrap());
    test_type("NUMERIC", &[(Some(ratio), "'0.0000000000000000000000000000000000000100200000000000000000000003000054000008000512000000070040900000'"), (None, "NULL")]);
}

#[test]
fn test_zero() {
    let zero: BigRational = Zero::zero();
    test_type("NUMERIC", &[(Some(zero), "'0.0'"), (None, "NULL")]);
}

#[test]
fn test_negative() {
    let ratio = BigRational::new(BigInt::parse_bytes(b"-5000003210000001000440000023", 10).unwrap(),
                                 BigInt::parse_bytes(b"1000000000000000000",  10).unwrap());
    test_type("NUMERIC", &[(Some(ratio), "'-5000003210.000001000440000023'"), (None, "NULL")]);
}

#[test]
#[should_panic] // will fail, arbitrary precision is impossible here as a decimal
fn test_third_meant_to_fail() {
    let third = BigRational::new(BigInt::from_u64(1).unwrap(),
                                 BigInt::from_u64(3).unwrap());
    test_type("NUMERIC", &[(Some(third), "'0.333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333'"), (None, "NULL")]);
}

fn generate_integer<R: Rng>(rng: &mut R) -> (BigRational, String) {
    let size_max = Range::new(1, 1000);        
    let mut count = 0;
    let size = size_max.ind_sample(rng);
    let mut digits: String = String::new();
    let mut char_gen = rng.gen_ascii_chars();
    while count < size {
        let character: char = char_gen.next().unwrap();
        if character.is_digit(10) {
            digits.push(character);
            count += 1;
        }
    }
    let integer = BigRational::from_integer(BigInt::parse_bytes(digits.as_bytes(), 10).unwrap());
    (integer, digits.trim_left_matches('0').to_string())
}

#[test]
fn test_battery_integer() {
    let mut rng = rand::thread_rng();
    let times = 1;  // add zeroes here if you want to test more
    let mut count = 0;
    while count < times {
        count += 1;
        let (integer, num_string) = generate_integer(&mut rng);
        test_type("NUMERIC", &[(Some(integer), &num_string[..]), (None, "NULL")]);
    }
}

fn generate_fractional<R: Rng>(rng: &mut R) -> (BigRational, String) {
    let precision_max = Range::new(1, 999);
    let mut count = 0;
    let precision = precision_max.ind_sample(rng);
    let mut digits: String = String::new();
    let mut char_gen = rng.gen_ascii_chars();
    while count < precision {
        let character: char = char_gen.next().unwrap();
        if character.is_digit(10) {
            digits.push(character);
            count += 1;
        }
    }
    let divisor = num::pow(BigInt::from_u64(10).unwrap(), precision as usize);
    let fractional = BigRational::new(BigInt::parse_bytes(digits.as_bytes(), 10).unwrap(), divisor);
    digits.insert(0, '.');
    digits.insert(0, '0');
    let trimmed = if digits.len() > 3 {
        digits.trim_right_matches('0').to_string()
    } else {
        digits
    };            
    (fractional, trimmed)
}

#[test]
fn test_battery_fractional() {
    let mut rng = rand::thread_rng();
    let times = 1;  // add zeroes here to test more
    let mut count = 0;
    while count < times {
        count += 1;
        let (fractional, num_string) = generate_fractional(&mut rng);
        test_type("NUMERIC", &[(Some(fractional), &num_string[..]), (None, "NULL")]);
    }
}

fn generate_decimal<R: Rng>(rng: &mut R) -> (BigRational, String) {
    let size_max = Range::new(1, 1000);
    let size = size_max.ind_sample(rng);
    let precision_max = Range::new(0, size);
    let precision = precision_max.ind_sample(rng);
    let integral_size = size - precision;
    let mut integral_digits: String = String::new();
    let mut fractional_digits: String = String::new();
    let mut char_gen = rng.gen_ascii_chars();
    let mut count = 0;
    while count < size {
        let character: char = char_gen.next().unwrap();
        if character.is_digit(10) {
            if count < integral_size {
                integral_digits.push(character);
            } else {
                fractional_digits.push(character);
            }
            count += 1;
        }
    }
    let divisor = num::pow(BigInt::from_u64(10).unwrap(), precision as usize);
    let combined_num = integral_digits.clone() + &fractional_digits.clone()[..];
    integral_digits = integral_digits.trim_left_matches('0').to_string();
    fractional_digits = fractional_digits.trim_right_matches('0').to_string();
    let formatted_num = if integral_digits.is_empty() && fractional_digits.is_empty() {
        "0".to_string()
    } else if !integral_digits.is_empty() && fractional_digits.is_empty() {
        integral_digits.clone()
    } else if integral_digits.is_empty() && !fractional_digits.is_empty() {
        "0.".to_string() + &fractional_digits[..]
    } else {
        integral_digits + "." + &fractional_digits[..]
    };
    let decimal = if formatted_num == "0" {
        Zero::zero()
    } else {
        BigRational::new(BigInt::parse_bytes(combined_num.as_bytes(), 10).unwrap(), divisor)
    };
    (decimal, formatted_num)
}

#[test]
fn test_battery_decimal() {
    let mut rng = rand::thread_rng();
    let times = 1;   // add zeroes here to test more
    let mut count = 0;
    while count < times {
        count += 1;
        let (decimal, num_string) = generate_decimal(&mut rng);
        test_type("NUMERIC", &[(Some(decimal), &num_string[..]), (None, "NULL")]);
    }
}
