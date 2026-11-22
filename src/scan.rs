//! One-pass SIMD line/field scanner implementing exactly the dialect of
//! `csv::split` (which stays as the simple, obviously-correct version and is
//! used to fuzz this one in the tests).
//!
//! The chunk is processed 64 bytes at a time. For each block we build three
//! bitmasks (positions of `\n`, `,` and `"`) and walk the set bits of
//! `\n | ,` in order with `trailing_zeros`. Each separator ends a field,
//! which is unwrapped and stored at once (no second pass over the line);
//! quotes are counted per line with popcount.
//!
//! Quote rule: a field is "wrapped" when it is at least 2 bytes long and
//! starts and ends with `"`. A line is well-formed iff it has exactly `N`
//! fields and its total number of `"` equals twice the number of wrapped
//! fields: every wrapped field accounts for its 2 boundary quotes, so any
//! other quote anywhere (inside a wrapped field, or in an unwrapped field)
//! makes the counts differ. This is equivalent to `csv::split`.
//!
//! Cost: O(bytes / 64) block operations + O(1) per comma/newline. 64-byte
//! blocks and per-field emission (instead of 32-byte blocks and a per-line
//! commas array walked again at the line end) made the scanner ~1.3x faster
//! on generated event files.

/// Plain version of `masks` (the non-x86 build, and the tests' reference).
#[cfg_attr(target_arch = "x86_64", allow(dead_code))]
#[inline(always)]
fn masks_scalar(b: &[u8; 64]) -> (u64, u64, u64) {
    let (mut nl, mut cm, mut qt) = (0u64, 0u64, 0u64);
    for (i, &c) in b.iter().enumerate() {
        nl |= ((c == b'\n') as u64) << i;
        cm |= ((c == b',') as u64) << i;
        qt |= ((c == b'"') as u64) << i;
    }
    (nl, cm, qt)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn masks(b: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    // SAFETY: avx2 is enabled at compile time; the loads are unaligned and in bounds.
    unsafe {
        let lo = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
        let hi = _mm256_loadu_si256(b.as_ptr().add(32) as *const __m256i);
        let m = |c: u8| -> u64 {
            let s = _mm256_set1_epi8(c as i8);
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, s)) as u32 as u64)
                | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, s)) as u32 as u64) << 32)
        };
        (m(b'\n'), m(b','), m(b'"'))
    }
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
fn masks(b: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    // SAFETY: sse2 is part of the x86_64 baseline; loads are in bounds.
    unsafe {
        let v = [
            _mm_loadu_si128(b.as_ptr() as *const __m128i),
            _mm_loadu_si128(b.as_ptr().add(16) as *const __m128i),
            _mm_loadu_si128(b.as_ptr().add(32) as *const __m128i),
            _mm_loadu_si128(b.as_ptr().add(48) as *const __m128i),
        ];
        let m = |c: u8| {
            let s = _mm_set1_epi8(c as i8);
            let mut r = 0u64;
            for (i, x) in v.iter().enumerate() {
                r |= (_mm_movemask_epi8(_mm_cmpeq_epi8(*x, s)) as u32 as u64) << (16 * i);
            }
            r
        };
        (m(b'\n'), m(b','), m(b'"'))
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn masks(b: &[u8; 64]) -> (u64, u64, u64) {
    masks_scalar(b)
}

/// The field `[p, q)` of `data` with its wrapping quotes removed, and
/// whether it was wrapped. `q` must be a separator position (`q < len`) or
/// `len` with `p < len`, so both reads below are in bounds: `data[p]` when
/// the field is non-empty, else `data[q]`; `data[q - 1]` when non-empty.
#[inline(always)]
fn field(data: &[u8], p: usize, q: usize) -> (&[u8], u32) {
    let first = data[p.min(q).min(data.len() - 1)];
    let last = data[if q > p { q - 1 } else { q.min(data.len() - 1) }];
    let w = ((q - p >= 2) & (first == b'"') & (last == b'"')) as usize;
    (&data[p + w..q - w], w as u32)
}

/// Call `f` with the `N` unquoted fields of every well-formed line of
/// `data`. Lines end at `\n`; a last line without `\n` counts; malformed
/// lines (wrong field count, stray quotes, empty) are skipped.
#[inline(always)]
pub fn scan<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let n = data.len();
    let mut out: [&'a [u8]; N] = [&[]; N];
    // Field index in the line, start of the current field, 2 x wrapped
    // fields so far and quotes counted so far, all for the current line.
    let mut k = 0usize;
    let mut p = 0usize;
    let mut expected = 0u32;
    let mut quotes = 0u32;
    let mut base = 0usize;
    let mut tail = [0u8; 64];
    while base < n {
        let block: &[u8; 64] = if base + 64 <= n {
            data[base..base + 64].try_into().unwrap()
        } else {
            // Only the final, partial block lands here (once), so the rest
            // of `tail` is still zero, and zero bytes match no delimiter.
            tail[..n - base].copy_from_slice(&data[base..]);
            &tail
        };
        let (nl, cm, mut qt) = masks(block);
        let mut m = nl | cm;
        while m != 0 {
            let i = m.trailing_zeros();
            let q = base + i as usize;
            let (v, w) = field(data, p, q);
            expected += 2 * w;
            if k < N {
                out[k] = v;
            }
            if (nl >> i) & 1 != 0 {
                let below = (1u64 << i) - 1;
                quotes += (qt & below).count_ones();
                qt &= !below;
                if k == N - 1 && quotes == expected {
                    f(&out);
                }
                k = 0;
                expected = 0;
                quotes = 0;
            } else {
                k += 1;
            }
            p = q + 1;
            m &= m - 1;
        }
        quotes += qt.count_ones();
        base += 64;
    }
    // A last line without a final `\n` (possibly ending in a comma, so with
    // an empty last field).
    if p < n || (p == n && k > 0) {
        let (v, w) = if p < n { field(data, p, n) } else { (&data[n..n], 0) };
        expected += 2 * w;
        if k < N {
            out[k] = v;
        }
        if k == N - 1 && quotes == expected {
            f(&out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv;

    fn via_split<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        csv::for_each_line(data, |l| {
            let mut f = [&b""[..]; N];
            if csv::split(l, &mut f) {
                v.push(f.iter().map(|x| x.to_vec()).collect());
            }
        });
        v
    }

    fn via_scan<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        scan::<N>(data, |f| v.push(f.iter().map(|x| x.to_vec()).collect()));
        v
    }

    #[test]
    fn masks_match() {
        let mut x: u64 = 0x1234_5678;
        for _ in 0..10_000 {
            let mut b = [0u8; 64];
            for c in b.iter_mut() {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                *c = b"ab,\"\n\r 0"[(x % 8) as usize];
            }
            assert_eq!(masks(&b), masks_scalar(&b));
        }
    }

    #[test]
    fn fuzz_against_split() {
        let alphabet = b"ab,,\"\"\n\n\r 1";
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for round in 0..20_000 {
            let len = (round % 200) as usize;
            let mut d = Vec::with_capacity(len);
            for _ in 0..len {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                d.push(alphabet[(x % alphabet.len() as u64) as usize]);
            }
            assert_eq!(via_scan::<2>(&d), via_split::<2>(&d), "{:?}", String::from_utf8_lossy(&d));
            assert_eq!(via_scan::<4>(&d), via_split::<4>(&d), "{:?}", String::from_utf8_lossy(&d));
            assert_eq!(via_scan::<7>(&d), via_split::<7>(&d), "{:?}", String::from_utf8_lossy(&d));
        }
    }

    #[test]
    fn realistic() {
        let d = b"e1,\"u1\",2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,\n\n\"e2\",u1,\"\",x,y,z,\"1000\r\"\na\"b,c,d,e,f,g,h\ne3,u,t,i,purchase,p,5";
        assert_eq!(via_scan::<7>(d), via_split::<7>(d));
        assert_eq!(via_scan::<7>(d).len(), 3);
    }
}
