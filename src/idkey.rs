//! Exact 64-bit keys for IDs (user_id, event_id, click_id).
//!
//! An ID of 1..=10 bytes drawn from `[0-9A-Z_a-z]` is packed 6 bits per byte,
//! left-aligned, into the low 60 bits of a u64. The codes ascend with the
//! ASCII order of the characters (`0-9` < `A-Z` < `_` < `a-z`) and a missing
//! (past-the-end) byte is code 0, so for two packed IDs integer order equals
//! byte-wise string order and integer equality equals string equality.
//!
//! Every other non-empty ID ("exotic": longer, or with any other byte such as
//! a space, `\r` or non-ASCII) is interned exactly and gets
//! `EXOTIC | interned id`. A packable string is always packed, so a packed key
//! and an exotic key are never the same string. Ordering involving an exotic
//! key compares the real bytes (`cmp`).

use crate::intern::Interner;
use std::cmp::Ordering;

pub const EXOTIC: u64 = 1 << 63;

const fn code_table() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        t[i] = if b >= b'0' && b <= b'9' {
            b - b'0' + 1
        } else if b >= b'A' && b <= b'Z' {
            b - b'A' + 11
        } else if b == b'_' {
            37
        } else if b >= b'a' && b <= b'z' {
            b - b'a' + 38
        } else {
            0
        };
        i += 1;
    }
    t
}

const fn decode_table() -> [u8; 64] {
    let code = code_table();
    let mut d = [0u8; 64];
    let mut i = 0;
    while i < 256 {
        if code[i] != 0 {
            d[code[i] as usize] = i as u8;
        }
        i += 1;
    }
    d
}

static CODE: [u8; 256] = code_table();
static DECODE: [u8; 64] = decode_table();

/// Pack a 1..=10 byte `[0-9A-Z_a-z]` string; `None` otherwise.
#[inline(always)]
pub fn pack(b: &[u8]) -> Option<u64> {
    let n = b.len();
    if n == 0 || n > 10 {
        return None;
    }
    let mut k = 0u64;
    let mut bad = false;
    for &c in b {
        let v = CODE[c as usize];
        bad |= v == 0;
        k = (k << 6) | v as u64;
    }
    if bad {
        None
    } else {
        Some(k << (6 * (10 - n)))
    }
}

/// Bytes of a packed key into `out`; returns the length.
#[inline]
pub fn unpack(k: u64, out: &mut [u8; 10]) -> usize {
    let mut n = 0;
    while n < 10 {
        let v = ((k >> (54 - 6 * n)) & 63) as usize;
        if v == 0 {
            break;
        }
        out[n] = DECODE[v];
        n += 1;
    }
    n
}

/// Key of a non-empty ID: packed, or interned exactly.
#[inline(always)]
pub fn key(b: &[u8], exotic: &Interner) -> u64 {
    match pack(b) {
        Some(k) => k,
        None => EXOTIC | exotic.intern(b) as u64,
    }
}

/// Byte-wise comparison of the strings behind two keys.
pub fn cmp(a: u64, b: u64, exotic: &Interner) -> Ordering {
    if a & EXOTIC == 0 && b & EXOTIC == 0 {
        return a.cmp(&b);
    }
    let (mut ba, mut bb) = ([0u8; 10], [0u8; 10]);
    let sa: &[u8] = if a & EXOTIC == 0 {
        let n = unpack(a, &mut ba);
        &ba[..n]
    } else {
        exotic.bytes(a as u32)
    };
    let sb: &[u8] = if b & EXOTIC == 0 {
        let n = unpack(b, &mut bb);
        &bb[..n]
    } else {
        exotic.bytes(b as u32)
    };
    sa.cmp(sb)
}

/// Append the bytes of a key to `out`.
pub fn write(k: u64, exotic: &Interner, out: &mut Vec<u8>) {
    if k & EXOTIC == 0 {
        let mut buf = [0u8; 10];
        let n = unpack(k, &mut buf);
        out.extend_from_slice(&buf[..n]);
    } else {
        out.extend_from_slice(exotic.bytes(k as u32));
    }
}

/// A well-mixed 64-bit hash of a key (for table slots and partitions).
#[inline(always)]
pub fn mix(k: u64) -> u64 {
    let x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_and_equality_match_bytes() {
        let words: Vec<&[u8]> = vec![
            b"0", b"9", b"A", b"Z", b"_", b"a", b"z", b"u1", b"U1", b"u10", b"u2", b"u", b"uu", b"zzzzzzzzzz",
            b"0000000000", b"e42huhoug", b"E42huhoug", b"k1q2awe54", b"a_b", b"aZ", b"a0",
        ];
        for a in &words {
            let ka = pack(a).unwrap();
            let mut buf = [0u8; 10];
            let n = unpack(ka, &mut buf);
            assert_eq!(&buf[..n], *a);
            for b in &words {
                let kb = pack(b).unwrap();
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{:?} {:?}", a, b);
            }
        }
        for bad in [&b""[..], b" u1", b"u1 ", b"u-1", b"u1\r", b"abcdefghijk", "é".as_bytes()] {
            assert_eq!(pack(bad), None);
        }
    }

    #[test]
    fn exotic_keys() {
        let ex = Interner::new();
        let a = key(b"u1 ", &ex);
        let b = key(b" u1", &ex);
        let c = key(b"u1", &ex);
        assert_ne!(a, b);
        assert_eq!(a, key(b"u1 ", &ex));
        assert_eq!(cmp(b, c, &ex), (&b" u1"[..]).cmp(&b"u1"[..]));
        assert_eq!(cmp(a, c, &ex), (&b"u1 "[..]).cmp(&b"u1"[..]));
        assert_eq!(cmp(c, a, &ex), (&b"u1"[..]).cmp(&b"u1 "[..]));
        let mut out = Vec::new();
        write(a, &ex, &mut out);
        write(c, &ex, &mut out);
        assert_eq!(out, b"u1 u1");
    }
}
