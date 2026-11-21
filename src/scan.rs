//! One-pass SIMD line/field scanner implementing exactly the dialect of
//! `csv::split` (which stays as the simple, obviously-correct version and is
//! used to fuzz this one in the tests).
//!
//! The chunk is processed 32 bytes at a time. For each block we build three
//! bitmasks (positions of `\n`, `,` and `"`) and walk the set bits of
//! `\n | ,` in order with `trailing_zeros`. Per line we record the first
//! `N - 1` comma positions, count all commas, and count quotes with popcount.
//!
//! Quote rule: a field is "wrapped" when it is at least 2 bytes long and
//! starts and ends with `"`. A line is well-formed iff it has exactly `N - 1`
//! commas and its total number of `"` equals twice the number of wrapped
//! fields: every wrapped field accounts for its 2 boundary quotes, so any
//! other quote anywhere (inside a wrapped field, or in an unwrapped field)
//! makes the counts differ. This is equivalent to `csv::split`.
//!
//! Cost: O(bytes / 32) block operations + O(1) per comma/newline, instead of
//! one `memchr` call per field.

#[inline(always)]
fn masks_scalar(b: &[u8; 32]) -> (u32, u32, u32) {
    let (mut nl, mut cm, mut qt) = (0u32, 0u32, 0u32);
    for (i, &c) in b.iter().enumerate() {
        nl |= ((c == b'\n') as u32) << i;
        cm |= ((c == b',') as u32) << i;
        qt |= ((c == b'"') as u32) << i;
    }
    (nl, cm, qt)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn masks(b: &[u8; 32]) -> (u32, u32, u32) {
    use std::arch::x86_64::*;
    // SAFETY: avx2 is enabled at compile time; the load is unaligned and in bounds.
    unsafe {
        let v = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
        let nl = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\n' as i8))) as u32;
        let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8))) as u32;
        let qt = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8))) as u32;
        (nl, cm, qt)
    }
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
fn masks(b: &[u8; 32]) -> (u32, u32, u32) {
    use std::arch::x86_64::*;
    // SAFETY: sse2 is part of the x86_64 baseline; loads are in bounds.
    unsafe {
        let lo = _mm_loadu_si128(b.as_ptr() as *const __m128i);
        let hi = _mm_loadu_si128(b.as_ptr().add(16) as *const __m128i);
        let m = |c: u8| {
            let s = _mm_set1_epi8(c as i8);
            (_mm_movemask_epi8(_mm_cmpeq_epi8(lo, s)) as u32)
                | ((_mm_movemask_epi8(_mm_cmpeq_epi8(hi, s)) as u32) << 16)
        };
        (m(b'\n'), m(b','), m(b'"'))
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn masks(b: &[u8; 32]) -> (u32, u32, u32) {
    masks_scalar(b)
}

/// Call `f` with the `N` unquoted fields of every well-formed line of
/// `data`. Lines end at `\n`; a last line without `\n` counts; malformed
/// lines (wrong field count, stray quotes, empty) are skipped.
#[inline(always)]
pub fn scan<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let n = data.len();
    let mut commas = [0usize; N];
    let mut ncomma = 0usize;
    let mut quotes = 0u32;
    let mut line_start = 0usize;
    let mut base = 0usize;
    let mut tail = [0u8; 32];

    let mut finish = |line_start: usize, line_end: usize, ncomma: usize, quotes: u32, commas: &[usize; N]| {
        if ncomma != N - 1 {
            return;
        }
        let mut out: [&'a [u8]; N] = [&[]; N];
        let mut s = line_start;
        for k in 0..N {
            let e = if k + 1 < N { commas[k] } else { line_end };
            out[k] = &data[s..e];
            s = e + 1;
        }
        if quotes != 0 {
            let mut wrapped = 0u32;
            for fld in out.iter_mut() {
                let l = fld.len();
                if l >= 2 && fld[0] == b'"' && fld[l - 1] == b'"' {
                    wrapped += 1;
                    *fld = &fld[1..l - 1];
                }
            }
            if quotes != 2 * wrapped {
                return;
            }
        }
        f(&out);
    };

    while base < n {
        let block: &[u8; 32] = if base + 32 <= n {
            data[base..base + 32].try_into().unwrap()
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
            let below = (1u32 << i) - 1;
            if (nl >> i) & 1 != 0 {
                quotes += (qt & below).count_ones();
                qt &= !below;
                let end = base + i as usize;
                finish(line_start, end, ncomma, quotes, &commas);
                line_start = end + 1;
                ncomma = 0;
                quotes = 0;
            } else {
                if ncomma < N - 1 {
                    commas[ncomma] = base + i as usize;
                }
                ncomma += 1;
            }
            m &= m - 1;
        }
        quotes += qt.count_ones();
        base += 32;
    }
    if line_start < n {
        finish(line_start, n, ncomma, quotes, &commas);
    }
}

#[allow(dead_code)]
pub fn masks_reference(b: &[u8; 32]) -> (u32, u32, u32) {
    masks_scalar(b)
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
            let mut b = [0u8; 32];
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

/// Line-at-a-time scanner: for each line, one 128-byte window gives the
/// newline, comma and quote masks at once; the line end is the first newline
/// bit, the N-1 commas must be exactly the comma bits before it, and quotes
/// are validated by popcount as in `scan`. Lines that do not fit in the
/// window, and the tail of the buffer where a 128-byte load is not allowed,
/// fall back to `scan`. `readable_end` is one past the last byte that may be
/// loaded (the end of the file mapping `data` lives in).
#[inline(always)]
pub fn scan_lines<'a, const N: usize>(data: &'a [u8], readable_end: *const u8, mut f: impl FnMut(&[&'a [u8]; N])) {
    let base = data.as_ptr();
    let n = data.len();
    let mut s = 0usize;
    while s < n {
        // SAFETY: pointer arithmetic only; the load below is bounds-checked.
        let p = unsafe { base.add(s) };
        if (p as usize) + 128 > readable_end as usize {
            break;
        }
        let (nl, cm, qt) = masks128(p);
        if nl == 0 {
            break; // a line of 128+ bytes: let the general scanner handle the rest
        }
        let len = nl.trailing_zeros() as usize;
        if s + len >= n && s + len != n {
            break; // (cannot happen for line-aligned chunks; be safe)
        }
        let below = (1u128 << len) - 1;
        let mut c = cm & below;
        let q = qt & below;
        let line_end = s + len;
        s = line_end + 1;
        if c.count_ones() as usize != N - 1 {
            continue;
        }
        let mut out: [&'a [u8]; N] = [&[]; N];
        let mut a = line_end - len;
        for k in 0..N - 1 {
            let i = c.trailing_zeros() as usize;
            c &= c - 1;
            let e = line_end - len + i;
            out[k] = &data[a..e];
            a = e + 1;
        }
        out[N - 1] = &data[a..line_end];
        if q != 0 {
            let mut wrapped = 0u32;
            for fld in out.iter_mut() {
                let l = fld.len();
                if l >= 2 && fld[0] == b'"' && fld[l - 1] == b'"' {
                    wrapped += 1;
                    *fld = &fld[1..l - 1];
                }
            }
            if q.count_ones() != 2 * wrapped {
                continue;
            }
        }
        f(&out);
    }
    if s < n {
        scan::<N>(&data[s..], f);
    }
}

#[inline(always)]
fn masks128(p: *const u8) -> (u128, u128, u128) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let nlv = _mm256_set1_epi8(b'\n' as i8);
        let cmv = _mm256_set1_epi8(b',' as i8);
        let qtv = _mm256_set1_epi8(b'"' as i8);
        let mut nl = 0u128;
        let mut cm = 0u128;
        let mut qt = 0u128;
        for k in 0..4 {
            let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
            nl |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nlv)) as u32 as u128) << (32 * k);
            cm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, cmv)) as u32 as u128) << (32 * k);
            qt |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, qtv)) as u32 as u128) << (32 * k);
        }
        (nl, cm, qt)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    unsafe {
        let b: &[u8; 128] = &*(p as *const [u8; 128]);
        let (mut nl, mut cm, mut qt) = (0u128, 0u128, 0u128);
        for (i, &c) in b.iter().enumerate() {
            nl |= ((c == b'\n') as u128) << i;
            cm |= ((c == b',') as u128) << i;
            qt |= ((c == b'"') as u128) << i;
        }
        (nl, cm, qt)
    }
}

#[cfg(test)]
mod line_tests {
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
    fn via_lines<const N: usize>(data: &[u8], pad: usize) -> Vec<Vec<Vec<u8>>> {
        // place data in a buffer with `pad` readable bytes after it
        let mut buf = data.to_vec();
        buf.extend(std::iter::repeat(b'x').take(pad));
        let d = &buf[..data.len()];
        let end = buf.as_ptr_range().end;
        let mut v = Vec::new();
        scan_lines::<N>(d, end, |f| v.push(f.iter().map(|x| x.to_vec()).collect()));
        v
    }

    #[test]
    fn fuzz_against_split() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for round in 0..60_000 {
            let alphabet: &[u8] = if round % 3 == 0 { b"abcdefghijklmnopqrstuvwxyz0123456789,,,,\"\n" } else { b"abcdefgh,,,\"\"\n\n\r 1" };
            let len = (round % 700) as usize;
            let mut d = Vec::with_capacity(len);
            for _ in 0..len {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                d.push(alphabet[(x % alphabet.len() as u64) as usize]);
            }
            // chunks end at a newline, as in the pipeline (or at the end)
            for pad in [0usize, 5, 200] {
                assert_eq!(via_lines::<2>(&d, pad), via_split::<2>(&d), "{:?}", String::from_utf8_lossy(&d));
                assert_eq!(via_lines::<4>(&d, pad), via_split::<4>(&d));
                assert_eq!(via_lines::<7>(&d, pad), via_split::<7>(&d));
            }
        }
    }
}
