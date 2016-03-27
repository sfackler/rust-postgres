extern crate num;

use std::io::{Read, Write};
use std::ops::Neg;

use self::num::bigint::{BigInt, Sign};
use self::num::rational::BigRational;
use self::num::traits::{FromPrimitive, ToPrimitive};
use self::num::traits::{Zero, One, Signed};

use Result;
use types::{ToSql, FromSql, Type, IsNull, SessionInfo};

/// No optimisation. Just provide the conversion and be correct.
fn get_bigint_digits(num: &BigInt) -> u64 {
    let mut digits: u64 = 0;
    let mut whittle = num.clone();
    while whittle > Zero::zero() {
        digits += 1;
        whittle = whittle / BigInt::from_u64(10).unwrap();
    }
    digits
}

fn fractional_from_fraction(precision: u64,
                            count: u64,
                            quotient: BigInt,
                            dividend: BigInt,
                            divisor: BigInt) -> (BigInt, u64) {
    let divisor_digits = get_bigint_digits(&divisor);
    if dividend > Zero::zero() && count < precision {        
        let digits = divisor_digits - get_bigint_digits(&dividend);
        let diff_mult: BigInt = if digits > 0 {
            num::pow(BigInt::from_u64(10).unwrap(), digits as usize)
        } else if dividend >= divisor {
            BigInt::from_u64(1).unwrap()
        } else {
            BigInt::from_u64(10).unwrap()
        };
        let depth = get_bigint_digits(&diff_mult) - 1; // we only want the 0's
        let new_dividend = &dividend * &diff_mult;
        let result = &new_dividend / &divisor;
        let remainder = &new_dividend - &result * &divisor;            
        let new_quotient = quotient * diff_mult + result;
        fractional_from_fraction(precision,
                                 (count + depth),
                                 new_quotient,
                                 remainder,
                                 divisor)
    } else {
        (quotient, count)
    }
}

/// We're going to write a BigRational as a numeric or decimal
impl ToSql for BigRational {
    to_sql_checked!();

    /// Since BigRational doesn't store the precision, we could exceed the limits of
    /// postgresql numeric precision. Therefore we'll limit ourselves to 1000 digits of
    /// precision. 
    fn to_sql<W: Write + ?Sized>(&self, _: &Type, w: &mut W, _: &SessionInfo)
                                 -> Result<IsNull>
    {
        let temp: BigRational = if self.is_negative() {
            self.clone().neg()
        } else {
            self.clone()
        };
        
        let mut integral: BigInt = temp.numer().clone() / temp.denom().clone();
        let fraction: BigRational =  temp.fract();

        let sign: u16 = if self.is_positive() {
            0x0000u16
        } else if self.is_negative() {
            0x4000u16
        } else if self.is_zero() {
            0x0000u16
        } else {
            0xC000u16 // NaN?
        };

        let difference = 1000 - get_bigint_digits(&integral); 
        let (mut fractional, scale) =
            fractional_from_fraction(difference,
                                     0,
                                     Zero::zero(),
                                     fraction.numer().clone(),
                                     fraction.denom().clone());     
        let itg_digits = get_bigint_digits(&integral) as f64;
        let mut itg_buf16: Vec<u16> = Vec::new();
        let segments = (itg_digits / 4f64).ceil() as u64;

        for x in 0..segments {
            let divisor: BigInt = if (segments - x) == 1 {
                One::one()
            } else {
                num::pow(BigInt::from_u64(10).unwrap(),
                         ((segments - x - 1) * 4) as usize)
            };
            let chunk: u16 = (&integral / &divisor).to_u16().unwrap();
            itg_buf16.push(chunk);
            integral = &integral % &divisor;
        }

        let mut frc_buf16: Vec<u16> = Vec::new();
        let mut scale_dec = scale as i64;
        while scale_dec > 0 {
            let frc_digits = get_bigint_digits(&fractional) as i64;
            let leading_zeros = scale_dec - frc_digits;

            if leading_zeros >= 4 {
                frc_buf16.push(0);
            } else {
                if scale_dec > 4 {
                    let divisor: BigInt =
                        num::pow(BigInt::from_u64(10).unwrap(),
                                 (leading_zeros + frc_digits - 4) as usize);
                    let chunk: u16 = (&fractional / &divisor).to_u16().unwrap();
                    fractional = &fractional % &divisor;
                    frc_buf16.push(chunk);
                } else {
                    let mult: BigInt =
                        num::pow(BigInt::from_u64(10).unwrap(),
                                 ((4 - frc_digits) - leading_zeros) as usize);
                    let tail: u16 = (&fractional * &mult).to_u16().unwrap();
                    frc_buf16.push(tail);
                }
            }
            scale_dec -= 4;
        }
        // Combine the two buffers together.
        let mut temp_buf16: Vec<u16> = Vec::new();
        let mut buf16: Vec<u16> = Vec::new();
        temp_buf16.extend_from_slice(&itg_buf16[..]);
        temp_buf16.extend_from_slice(&frc_buf16[..]);

        // prune all leading empty items in the vector
        let mut leading = true;
        for x in temp_buf16.iter() {
            if leading && *x > 0 {
                leading = false;
            }
            if !leading {
                buf16.push(*x);
            }
        }

        let weight: i16 = if itg_buf16.is_empty() && frc_buf16.is_empty() {
            0
        } else if itg_buf16.is_empty() && !frc_buf16.is_empty() {
            let mut wght: i16 = -1;
            for n in frc_buf16.iter() {
                if *n > 0 as u16 { break; }
                wght += -1;
            }
            wght
        } else {
            (itg_buf16.len() - 1) as i16
        };
        
        let digits: u16 = buf16.len() as u16;

        // push the other values to the front
        buf16.insert(0,scale as u16);
        buf16.insert(0,sign);
        buf16.insert(0,weight as u16);
        buf16.insert(0,digits);

        // convert to u8 for write
        let mut buf8: Vec<u8> = Vec::new();
        for x in buf16.iter() {
            buf8.push((x >> 8) as u8);
            buf8.push(((x << 8) >> 8) as u8);
        }

        try!(w.write_all(&buf8[..]));
        
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Numeric => true,
            _ => false,
        }
    }
}

/// We're going to store a numeric or decimal inside a BigRational
impl FromSql for BigRational {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo)
                         -> Result<Self>
    {
        let mut buf8 = vec![];
        try!(raw.read_to_end(&mut buf8));

        // we need to convert the u8 buffer into a u16 buffer since that's how
        // postgres transmits it's numeric data in binary.
        let mut buf16: Vec<u16> = buf8
            .chunks(2)
            .map(|item| item.iter().fold(0, |acc, &x| (acc << 8) + x as u16))
            .collect();       

        if buf16.len() <= 4 {
            return Ok(Zero::zero());
        }

        // assignments to help me clarify
        let digits: u16 = buf16[0];  // The first element is the number of digits
        let weight: i16 = buf16[1] as i16;  // Careful, i16 is used here
        let sign = match buf16[2] {
            0x0000u16 => Sign::Plus,
            0x4000u16 => Sign::Minus,            
            _ => return Ok(Zero::zero()),
        };

        // Seperate the digits array. Here we use the u16 buffer; but since BigNum
        // doesn't read in u16 on initializing we need to convert to u8 later.
        let mut numbers: Vec<u16> = buf16.split_off(4);

        // Now with the weight, we determine if numbers array needs padding with 0's
        // for constructing the numerator and denominator.
        if weight >= 0 {            
            for _ in digits..((weight + 1) as u16) {
                numbers.push(0);
            }
        } else {
            for _ in 0..(((weight * -1) - 1) as u16) {
                numbers.insert(0,0);
            }
        }

        // slice in half to get at the integral and fractional components of the buffer
        let (itg_buf16, frc_buf16) = if weight >= 0 {
            let split = numbers.split_off((weight + 1) as usize);
            (numbers, split)
        } else {
            (Vec::new(), numbers)
        };
             
        // construct the integral
        let mut integral: BigInt = Zero::zero();
        for x in itg_buf16.iter() {
            integral = integral * BigInt::from_u64(10000).unwrap();
            integral = integral + BigInt::from_u16(*x).unwrap();
        }

        // constructing the fractional will have to be done in reverse
        let mut fractional: BigInt = Zero::zero();
        let mut frac_digits = 0;
        let mut leading_zero = true;
        let length = frc_buf16.len();
        for x in 0..length { 
            let curr_num = frc_buf16[x];
            if curr_num == 0 && leading_zero {
                frac_digits += 4;                
            } else {
                leading_zero = false; // watch out for gaps of 0000 between digits
                if x < length - 1 {
                    fractional = fractional * BigInt::from_u64(10000).unwrap();
                    fractional = fractional + BigInt::from_u16(curr_num).unwrap();
                    frac_digits += 4;
                } else { 
                    let mut whittle_down = curr_num;
                    let mut test = whittle_down % 10;
                    let mut count = 0;
                    while test == 0 {
                        whittle_down = whittle_down / 10;
                        test = whittle_down % 10;
                        count += 1;
                    }
                    let mult = BigInt::from_u64(num::pow(10, (4 - count) as usize))
                        .unwrap();
                    fractional = fractional * mult;
                    fractional = fractional + BigInt::from_u16(whittle_down).unwrap();
                    frac_digits += 4 - count;
                }
            }
        }
        
        
        let multiplier: BigInt = num::pow(FromPrimitive::from_u64(10).unwrap(),
                                          frac_digits as usize);
        let ratio = if frac_digits == 0 {
            BigRational::new(integral, One::one()) // we have an integer
        } else {
            let numerator: BigInt = (integral * multiplier.clone()) + fractional;
            let denominator = multiplier;
            let result = BigRational::new(numerator, denominator);
            result
        };
        
        match sign {
            Sign::Minus => Ok(ratio.neg()),
            _ => Ok(ratio),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Numeric => true,
            _ => false,
        }
    }
}
