// Copyright 2013 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io::prelude::*;
use std::ptr;
use std::mem;
use std::ops::{Add, Range};
use std::iter::repeat;

#[derive(Clone)]
struct StepUp<T> {
    next: T,
    end: T,
    ammount: T,
}

impl<T> Iterator for StepUp<T> where T: Add<T, Output = T> + PartialOrd + Copy
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.next < self.end {
            let n = self.next;
            self.next = self.next + self.ammount;
            Some(n)
        } else {
            None
        }
    }
}

trait RangeExt<T> {
    fn step_up(self, ammount: T) -> StepUp<T>;
}

impl<T> RangeExt<T> for Range<T> where T: Add<T, Output = T> + PartialOrd + Copy
{
    fn step_up(self, ammount: T) -> StepUp<T> {
        StepUp {
            next: self.start,
            end: self.end,
            ammount: ammount,
        }
    }
}

/// Copy bytes from src to dest
#[inline]
fn copy_memory(src: &[u8], dst: &mut [u8]) {
    assert!(dst.len() >= src.len());
    unsafe {
        let srcp = src.as_ptr();
        let dstp = dst.as_mut_ptr();
        ptr::copy_nonoverlapping(srcp, dstp, src.len());
    }
}

/// Zero all bytes in dst
#[inline]
fn zero(dst: &mut [u8]) {
    unsafe {
        ptr::write_bytes(dst.as_mut_ptr(), 0, dst.len());
    }
}

/// Read a vector of bytes into a vector of u32s. The values are read in little-endian format.
fn read_u32v_le(dst: &mut [u32], input: &[u8]) {
    assert!(dst.len() * 4 == input.len());
    unsafe {
        let mut x: *mut u32 = dst.get_unchecked_mut(0);
        let mut y: *const u8 = input.get_unchecked(0);
        for _ in (0..dst.len()) {
            let mut tmp: u32 = mem::uninitialized();
            ptr::copy_nonoverlapping(y, &mut tmp as *mut _ as *mut u8, 4);
            *x = u32::from_le(tmp);
            x = x.offset(1);
            y = y.offset(4);
        }
    }
}

/// Write a u32 into a vector, which must be 4 bytes long. The value is written in little-endian
/// format.
fn write_u32_le(dst: &mut [u8], mut input: u32) {
    assert!(dst.len() == 4);
    input = input.to_le();
    unsafe {
        let tmp = &input as *const _ as *const u8;
        ptr::copy_nonoverlapping(tmp, dst.get_unchecked_mut(0), 4);
    }
}

/// The StandardPadding trait adds a method useful for various hash algorithms to a FixedBuffer
/// struct.
trait StandardPadding {
    /// Add standard padding to the buffer. The buffer must not be full when this method is called
    /// and is guaranteed to have exactly rem remaining bytes when it returns. If there are not at
    /// least rem bytes available, the buffer will be zero padded, processed, cleared, and then
    /// filled with zeros again until only rem bytes are remaining.
    fn standard_padding<F: FnMut(&[u8])>(&mut self, rem: usize, func: F);
}

impl<T: FixedBuffer> StandardPadding for T {
    fn standard_padding<F: FnMut(&[u8])>(&mut self, rem: usize, mut func: F) {
        let size = self.size();

        self.next(1)[0] = 128;

        if self.remaining() < rem {
            self.zero_until(size);
            func(self.full_buffer());
        }

        self.zero_until(size - rem);
    }
}

/// A FixedBuffer, likes its name implies, is a fixed size buffer. When the buffer becomes full, it
/// must be processed. The input() method takes care of processing and then clearing the buffer
/// automatically. However, other methods do not and require the caller to process the buffer. Any
/// method that modifies the buffer directory or provides the caller with bytes that can be modifies
/// results in those bytes being marked as used by the buffer.
trait FixedBuffer {
    /// Input a vector of bytes. If the buffer becomes full, process it with the provided
    /// function and then clear the buffer.
    fn input<F: FnMut(&[u8])>(&mut self, input: &[u8], func: F);

    /// Reset the buffer.
    fn reset(&mut self);

    /// Zero the buffer up until the specified index. The buffer position currently must not be
    /// greater than that index.
    fn zero_until(&mut self, idx: usize);

    /// Get a slice of the buffer of the specified size. There must be at least that many bytes
    /// remaining in the buffer.
    fn next<'s>(&'s mut self, len: usize) -> &'s mut [u8];

    /// Get the current buffer. The buffer must already be full. This clears the buffer as well.
    fn full_buffer<'s>(&'s mut self) -> &'s [u8];

    /// Get the current buffer.
    fn current_buffer<'s>(&'s mut self) -> &'s [u8];

    /// Get the current position of the buffer.
    fn position(&self) -> usize;

    /// Get the number of bytes remaining in the buffer until it is full.
    fn remaining(&self) -> usize;

    /// Get the size of the buffer
    fn size(&self) -> usize;
}

macro_rules! impl_fixed_buffer( ($name:ident, $size:expr) => (
    impl FixedBuffer for $name {
        fn input<F: FnMut(&[u8])>(&mut self, input: &[u8], mut func: F) {
            let mut i = 0;

            // FIXME: #6304 - This local variable shouldn't be necessary.
            let size = $size;

            // If there is already data in the buffer, copy as much as we can into it and process
            // the data if the buffer becomes full.
            if self.buffer_idx != 0 {
                let buffer_remaining = size - self.buffer_idx;
                if input.len() >= buffer_remaining {
                        copy_memory(
                            &input[..buffer_remaining],
                            &mut self.buffer[self.buffer_idx..size]);
                    self.buffer_idx = 0;
                    func(&self.buffer);
                    i += buffer_remaining;
                } else {
                    copy_memory(
                        input,
                        &mut self.buffer[self.buffer_idx..self.buffer_idx + input.len()]);
                    self.buffer_idx += input.len();
                    return;
                }
            }

            // While we have at least a full buffer size chunks's worth of data, process that data
            // without copying it into the buffer
            while input.len() - i >= size {
                func(&input[i..i + size]);
                i += size;
            }

            // Copy any input data into the buffer. At this point in the method, the ammount of
            // data left in the input vector will be less than the buffer size and the buffer will
            // be empty.
            let input_remaining = input.len() - i;
            copy_memory(
                &input[i..],
                &mut self.buffer[0..input_remaining]);
            self.buffer_idx += input_remaining;
        }

        fn reset(&mut self) {
            self.buffer_idx = 0;
        }

        fn zero_until(&mut self, idx: usize) {
            assert!(idx >= self.buffer_idx);
            zero(&mut self.buffer[self.buffer_idx..idx]);
            self.buffer_idx = idx;
        }

        fn next<'s>(&'s mut self, len: usize) -> &'s mut [u8] {
            self.buffer_idx += len;
            &mut self.buffer[self.buffer_idx - len..self.buffer_idx]
        }

        fn full_buffer<'s>(&'s mut self) -> &'s [u8] {
            assert!(self.buffer_idx == $size);
            self.buffer_idx = 0;
            &self.buffer[..$size]
        }

        fn current_buffer<'s>(&'s mut self) -> &'s [u8] {
            let tmp = self.buffer_idx;
            self.buffer_idx = 0;
            &self.buffer[..tmp]
        }

        fn position(&self) -> usize { self.buffer_idx }

        fn remaining(&self) -> usize { $size - self.buffer_idx }

        fn size(&self) -> usize { $size }
    }
));

/// A fixed size buffer of 64 bytes useful for cryptographic operations.
#[derive(Copy)]
struct FixedBuffer64 {
    buffer: [u8; 64],
    buffer_idx: usize,
}

impl Clone for FixedBuffer64 {
    fn clone(&self) -> FixedBuffer64 {
        *self
    }
}

impl FixedBuffer64 {
    /// Create a new buffer
    fn new() -> FixedBuffer64 {
        FixedBuffer64 {
            buffer: [0u8; 64],
            buffer_idx: 0,
        }
    }
}

impl_fixed_buffer!(FixedBuffer64, 64);

// A structure that represents that state of a digest computation for the MD5 digest function
struct Md5State {
    s0: u32,
    s1: u32,
    s2: u32,
    s3: u32,
}

impl Md5State {
    fn new() -> Md5State {
        Md5State {
            s0: 0x67452301,
            s1: 0xefcdab89,
            s2: 0x98badcfe,
            s3: 0x10325476,
        }
    }

    fn reset(&mut self) {
        self.s0 = 0x67452301;
        self.s1 = 0xefcdab89;
        self.s2 = 0x98badcfe;
        self.s3 = 0x10325476;
    }

    fn process_block(&mut self, input: &[u8]) {
        fn f(u: u32, v: u32, w: u32) -> u32 {
            (u & v) | (!u & w)
        }

        fn g(u: u32, v: u32, w: u32) -> u32 {
            (u & w) | (v & !w)
        }

        fn h(u: u32, v: u32, w: u32) -> u32 {
            u ^ v ^ w
        }

        fn i(u: u32, v: u32, w: u32) -> u32 {
            v ^ (u | !w)
        }

        fn op_f(w: u32, x: u32, y: u32, z: u32, m: u32, s: u32) -> u32 {
            w.wrapping_add(f(x, y, z)).wrapping_add(m).rotate_left(s).wrapping_add(x)
        }

        fn op_g(w: u32, x: u32, y: u32, z: u32, m: u32, s: u32) -> u32 {
            w.wrapping_add(g(x, y, z)).wrapping_add(m).rotate_left(s).wrapping_add(x)
        }

        fn op_h(w: u32, x: u32, y: u32, z: u32, m: u32, s: u32) -> u32 {
            w.wrapping_add(h(x, y, z)).wrapping_add(m).rotate_left(s).wrapping_add(x)
        }

        fn op_i(w: u32, x: u32, y: u32, z: u32, m: u32, s: u32) -> u32 {
            w.wrapping_add(i(x, y, z)).wrapping_add(m).rotate_left(s).wrapping_add(x)
        }

        let mut a = self.s0;
        let mut b = self.s1;
        let mut c = self.s2;
        let mut d = self.s3;

        let mut data = [0u32; 16];

        read_u32v_le(&mut data, input);

        // round 1
        for i in (0..16).step_up(4) {
            a = op_f(a, b, c, d, data[i].wrapping_add(C1[i]), 7);
            d = op_f(d, a, b, c, data[i + 1].wrapping_add(C1[i + 1]), 12);
            c = op_f(c, d, a, b, data[i + 2].wrapping_add(C1[i + 2]), 17);
            b = op_f(b, c, d, a, data[i + 3].wrapping_add(C1[i + 3]), 22);
        }

        // round 2
        let mut t = 1;
        for i in (0..16).step_up(4) {
            a = op_g(a, b, c, d, data[t & 0x0f].wrapping_add(C2[i]), 5);
            d = op_g(d, a, b, c, data[(t + 5) & 0x0f].wrapping_add(C2[i + 1]), 9);
            c = op_g(c,
                     d,
                     a,
                     b,
                     data[(t + 10) & 0x0f].wrapping_add(C2[i + 2]),
                     14);
            b = op_g(b,
                     c,
                     d,
                     a,
                     data[(t + 15) & 0x0f].wrapping_add(C2[i + 3]),
                     20);
            t += 20;
        }

        // round 3
        t = 5;
        for i in (0..16).step_up(4) {
            a = op_h(a, b, c, d, data[t & 0x0f].wrapping_add(C3[i]), 4);
            d = op_h(d, a, b, c, data[(t + 3) & 0x0f].wrapping_add(C3[i + 1]), 11);
            c = op_h(c, d, a, b, data[(t + 6) & 0x0f].wrapping_add(C3[i + 2]), 16);
            b = op_h(b, c, d, a, data[(t + 9) & 0x0f].wrapping_add(C3[i + 3]), 23);
            t += 12;
        }

        // round 4
        t = 0;
        for i in (0..16).step_up(4) {
            a = op_i(a, b, c, d, data[t & 0x0f].wrapping_add(C4[i]), 6);
            d = op_i(d, a, b, c, data[(t + 7) & 0x0f].wrapping_add(C4[i + 1]), 10);
            c = op_i(c,
                     d,
                     a,
                     b,
                     data[(t + 14) & 0x0f].wrapping_add(C4[i + 2]),
                     15);
            b = op_i(b,
                     c,
                     d,
                     a,
                     data[(t + 21) & 0x0f].wrapping_add(C4[i + 3]),
                     21);
            t += 28;
        }

        self.s0 = self.s0.wrapping_add(a);
        self.s1 = self.s1.wrapping_add(b);
        self.s2 = self.s2.wrapping_add(c);
        self.s3 = self.s3.wrapping_add(d);
    }
}

// Round 1 constants
static C1: [u32; 16] = [0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a,
                        0xa8304613, 0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
                        0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821];

// Round 2 constants
static C2: [u32; 16] = [0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453,
                        0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
                        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a];

// Round 3 constants
static C3: [u32; 16] = [0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9,
                        0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
                        0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665];

// Round 4 constants
static C4: [u32; 16] = [0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92,
                        0xffeff47d, 0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
                        0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391];

/// The MD5 Digest algorithm
pub struct Md5 {
    length_bytes: u64,
    buffer: FixedBuffer64,
    state: Md5State,
    finished: bool,
}

impl Md5 {
    /// Construct a new instance of the MD5 Digest.
    pub fn new() -> Md5 {
        Md5 {
            length_bytes: 0,
            buffer: FixedBuffer64::new(),
            state: Md5State::new(),
            finished: false,
        }
    }

    pub fn input(&mut self, input: &[u8]) {
        assert!(!self.finished);
        // Unlike Sha1 and Sha2, the length value in MD5 is defined as the length of the message mod
        // 2^64 - ie: integer overflow is OK.
        self.length_bytes += input.len() as u64;
        let self_state = &mut self.state;
        self.buffer.input(input, |d: &[u8]| {
            self_state.process_block(d);
        });
    }

    pub fn reset(&mut self) {
        self.length_bytes = 0;
        self.buffer.reset();
        self.state.reset();
        self.finished = false;
    }

    pub fn result(&mut self, out: &mut [u8]) {
        if !self.finished {
            let self_state = &mut self.state;
            self.buffer.standard_padding(8, |d: &[u8]| {
                self_state.process_block(d);
            });
            write_u32_le(self.buffer.next(4), (self.length_bytes << 3) as u32);
            write_u32_le(self.buffer.next(4), (self.length_bytes >> 29) as u32);
            self_state.process_block(self.buffer.full_buffer());
            self.finished = true;
        }

        write_u32_le(&mut out[0..4], self.state.s0);
        write_u32_le(&mut out[4..8], self.state.s1);
        write_u32_le(&mut out[8..12], self.state.s2);
        write_u32_le(&mut out[12..16], self.state.s3);
    }

    fn output_bits(&self) -> usize {
        128
    }

    pub fn result_str(&mut self) -> String {
        use serialize::hex::ToHex;

        let mut buf: Vec<u8> = repeat(0).take((self.output_bits() + 7) / 8).collect();
        self.result(&mut buf);
        buf[..].to_hex()
    }
}


#[cfg(test)]
mod tests {
    use md5::Md5;

    struct Test {
        input: &'static str,
        output_str: &'static str,
    }

    fn test_hash<D: Digest>(sh: &mut D, tests: &[Test]) {
        // Test that it works when accepting the message all at once
        for t in tests.iter() {
            sh.input_str(t.input);

            let out_str = sh.result_str();
            assert_eq!(out_str, t.output_str);

            sh.reset();
        }

        // Test that it works when accepting the message in pieces
        for t in tests.iter() {
            let len = t.input.len();
            let mut left = len;
            while left > 0 {
                let take = (left + 1) / 2;
                sh.input_str(&t.input[len - left..take + len - left]);
                left = left - take;
            }

            let out_str = sh.result_str();
            assert_eq!(out_str, t.output_str);

            sh.reset();
        }
    }

    #[test]
    fn test_md5() {
        // Examples from wikipedia
        let wikipedia_tests = vec![
            Test {
                input: "",
                output_str: "d41d8cd98f00b204e9800998ecf8427e"
            },
            Test {
                input: "The quick brown fox jumps over the lazy dog",
                output_str: "9e107d9d372bb6826bd81d3542a419d6"
            },
            Test {
                input: "The quick brown fox jumps over the lazy dog.",
                output_str: "e4d909c290d0fb1ca068ffaddf22cbd0"
            },
        ];

        let tests = wikipedia_tests;

        let mut sh = Md5::new();

        test_hash(&mut sh, &tests[..]);
    }
}
