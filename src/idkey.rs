//! Exact 64-bit keys for IDs (user_id, event_id, click_id).
//!
//! An ID of 1..=10 bytes drawn from `[0-9A-Z_a-z]` is packed 6 bits per byte,
//! left-aligned, into the low 60 bits of a u64. The codes ascend with the
//! ASCII order of the characters (`0-9` < `A-Z` < `_` < `a-z`) and a missing
//! (past-the-end) byte is code 0, so for two packed IDs integer order equals
//! byte-wise string order and integer equality equals string equality.
//!
//! Every other non-empty ID ("exotic": longer, or with any other byte such as
//! a space, `\r` or non-ASCII) gets `EXOTIC | key bits` from intern.rs: a
//! 62-bit hash, verified collision-free after parsing (or, after a collision,
//! an exactly interned index). A packable string is always packed, so a packed
//! key and an exotic key are never the same string. Ordering involving an
//! exotic key compares the real bytes (`cmp`).

use crate::intern::{IdLog, Interner};
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
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
    {
        let n = b.len();
        // 16 bytes starting at b lie in b's page, so they can be read even
        // past the end of b (the extra lanes are masked off).
        if n.wrapping_sub(1) < 10 && (b.as_ptr() as usize & 4095) <= 4096 - 16 {
            // SAFETY: the 16-byte read stays inside a mapped page (above).
            return unsafe { pack_simd(b.as_ptr(), n) };
        }
    }
    pack_scalar(b)
}

/// SIMD `pack` for 1..=10 bytes: classify all 16 lanes at once, then fold
/// the 6-bit codes together with two multiply-add steps.
#[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
#[inline(always)]
unsafe fn pack_simd(p: *const u8, n: usize) -> Option<u64> {
    use std::arch::x86_64::*;
    let v = _mm_loadu_si128(p as *const __m128i);
    let lanes = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    let inlen = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), lanes);
    let range = |lo: u8, span: u8| {
        let d = _mm_sub_epi8(v, _mm_set1_epi8(lo as i8));
        (d, _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(span as i8)), d))
    };
    let (d, is_d) = range(b'0', 9);
    let (u, is_u) = range(b'A', 25);
    let (l, is_l) = range(b'a', 25);
    let is_us = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'_' as i8));
    let valid = _mm_or_si128(_mm_or_si128(is_d, is_u), _mm_or_si128(is_l, is_us));
    if _mm_movemask_epi8(_mm_andnot_si128(valid, inlen)) != 0 {
        return None;
    }
    let code = _mm_or_si128(
        _mm_or_si128(
            _mm_and_si128(is_d, _mm_add_epi8(d, _mm_set1_epi8(1))),
            _mm_and_si128(is_u, _mm_add_epi8(u, _mm_set1_epi8(11))),
        ),
        _mm_or_si128(
            _mm_and_si128(is_l, _mm_add_epi8(l, _mm_set1_epi8(38))),
            _mm_and_si128(is_us, _mm_set1_epi8(37)),
        ),
    );
    let code = _mm_and_si128(code, inlen);
    // 16-bit lanes: c[2j] * 64 + c[2j+1]; 32-bit lanes: pairs of those * 4096.
    let w = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140));
    let q = _mm_madd_epi16(w, _mm_set1_epi32(0x0001_1000));
    let q0 = _mm_cvtsi128_si32(q) as u32 as u64;
    let q1 = _mm_extract_epi32::<1>(q) as u32 as u64;
    let q2 = _mm_extract_epi32::<2>(q) as u32 as u64;
    Some((q0 << 36) | (q1 << 12) | (q2 >> 12))
}

/// Scalar `pack` (also the reference for the SIMD one).
#[inline(always)]
pub fn pack_scalar(b: &[u8]) -> Option<u64> {
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

/// Key of a non-empty ID: packed, or an exotic key (intern.rs). `NAME`:
/// a user or click ID, whose bytes are kept (an event ID's are not).
#[inline(always)]
pub fn key<const NAME: bool>(b: &[u8], exotic: &Interner, log: &mut IdLog) -> u64 {
    match pack(b) {
        Some(k) => k,
        None => EXOTIC | exotic.key::<NAME>(b, log),
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
        exotic.bytes(a)
    };
    let sb: &[u8] = if b & EXOTIC == 0 {
        let n = unpack(b, &mut bb);
        &bb[..n]
    } else {
        exotic.bytes(b)
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
        out.extend_from_slice(exotic.bytes(k));
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
    fn simd_pack_matches_scalar() {
        // Random strings over valid and invalid bytes, lengths 0..=12, at
        // every alignment (including next to a page end).
        let alphabet = b"09AZaz_mQ5 -\r\xff\x80/:@[`{";
        let mut x: u64 = 0xdead_beef_1234_5678;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut page = vec![0u8; 3 * 4096];
        let base = (4096 - (page.as_ptr() as usize & 4095)) % 4096; // page-aligned offset
        for _ in 0..400_000 {
            let n = (rnd() % 13) as usize;
            let off = base + (rnd() % 4096) as usize;
            for i in 0..16 {
                page[off + i] = alphabet[(rnd() % alphabet.len() as u64) as usize];
            }
            if rnd() % 3 == 0 {
                for i in 0..n {
                    page[off + i] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"[(rnd() % 63) as usize];
                }
            }
            let s = &page[off..off + n];
            assert_eq!(pack(s), pack_scalar(s), "{:?}", s);
        }
    }

    #[test]
    fn exotic_keys() {
        for locked in [false, true] {
            let ex = Interner::with_mode(locked);
            let mut log = IdLog::new();
            let a = key::<true>(b"u1 ", &ex, &mut log);
            let b = key::<true>(b" u1", &ex, &mut log);
            let c = key::<true>(b"u1", &ex, &mut log);
            let d = key::<true>(b"u5fstpucv1829340564ab6c7d8e9f0a1b2c3d4", &ex, &mut log);
            assert_ne!(a, b);
            assert_eq!(a, key::<true>(b"u1 ", &ex, &mut log));
            assert!(a & EXOTIC != 0 && c & EXOTIC == 0 && a != u64::MAX && d != u64::MAX);
            ex.verify_all(&mut [&mut log]).unwrap();
            assert_eq!(cmp(b, c, &ex), (&b" u1"[..]).cmp(&b"u1"[..]));
            assert_eq!(cmp(a, c, &ex), (&b"u1 "[..]).cmp(&b"u1"[..]));
            assert_eq!(cmp(c, a, &ex), (&b"u1"[..]).cmp(&b"u1 "[..]));
            assert_eq!(cmp(d, a, &ex), (&b"u5fstpucv1829340564ab6c7d8e9f0a1b2c3d4"[..]).cmp(&b"u1 "[..]));
            let mut out = Vec::new();
            write(a, &ex, &mut out);
            write(c, &ex, &mut out);
            write(d, &ex, &mut out);
            assert_eq!(out, b"u1 u1u5fstpucv1829340564ab6c7d8e9f0a1b2c3d4");
        }
    }

    #[test]
    fn hash_collisions_are_detected() {
        // With 4 hash bits, 200 different IDs must collide.
        let ex = Interner::with_mode_bits(false, 4);
        let mut log = IdLog::new();
        for i in 0..200 {
            let s = format!("user number {i}");
            key::<true>(s.as_bytes(), &ex, &mut log);
        }
        assert!(ex.verify_all(&mut [&mut log]).is_err());
        let ex = Interner::with_mode_bits(false, 4);
        let mut log = IdLog::new();
        for i in 0..200 {
            let s = format!("event number {i}");
            key::<false>(s.as_bytes(), &ex, &mut log);
        }
        assert!(ex.verify_all(&mut [&mut log]).is_err());
        // The same IDs seen many times never collide with themselves.
        let ex = Interner::with_mode(false);
        let mut log = IdLog::new();
        for _ in 0..3 {
            for i in 0..5000 {
                let s = format!("event number {i}");
                key::<false>(s.as_bytes(), &ex, &mut log);
                key::<true>(s.as_bytes(), &ex, &mut log);
            }
        }
        assert!(ex.verify_all(&mut [&mut log]).is_ok());
    }
}
