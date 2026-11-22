//! Timestamp parsing (rules.md "Timestamps"). Every function returns integer
//! milliseconds since the Unix epoch, UTC, or `None` for a bad timestamp.
//!
//! Forms:
//!   F1 `YYYY-MM-DDTHH:MM:SSZ`        (exactly 20 bytes)
//!   F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`   (exactly 25 bytes, sign `+` or `-`)
//!   F3 ASCII digits only, epoch milliseconds, leading zeros allowed.
//!
//! An F3 value too large for an i64 is "not malformed, only outside the
//! window"; since every caller rejects out-of-window values too, returning
//! `None` for it is equivalent.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

/// Two ASCII digits at `b[i..i+2]` (callers guarantee the length).
#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some(a as i64 * 10 + c as i64)
    } else {
        None
    }
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant).
#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Per-byte "is an ASCII digit" over the bytes selected by `mask` (SWAR).
#[inline(always)]
fn all_digits(x: u64, mask: u64) -> bool {
    let hi = 0xF0F0_F0F0_F0F0_F0F0 & mask;
    let t = 0x3030_3030_3030_3030 & mask;
    // A byte is 0x30..=0x39 iff its high nibble is 3 and adding 6 keeps it 3.
    (x & hi) == t && (x.wrapping_add(0x0606_0606_0606_0606 & mask) & hi) == t
}

thread_local! {
    /// Direct-mapped memo of validated dates: (first 8 bytes, bytes 8..10, days).
    /// One `Cell` per slot, so a lookup copies one 16-byte entry (a single
    /// `Cell` around the whole array copied all 32 entries on every call).
    static DATES: [std::cell::Cell<(u64, u16, i32)>; 32] = const { [const { std::cell::Cell::new((0, 0, 0)) }; 32] };
}

/// The `YYYY-MM-DDTHH:MM:SS` prefix (19 bytes; caller checks length).
/// Fast path: the date is looked up in a small memo (timestamps cluster in a
/// few days), the time is checked and converted with SWAR.
#[inline(always)]
fn civil_prefix(b: &[u8]) -> Option<i64> {
    let lo = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let mid = u16::from_le_bytes([b[8], b[9]]);
    let t = u64::from_le_bytes(b[11..19].try_into().unwrap());
    if b[10] != b'T' {
        return None;
    }
    // HH:MM:SS
    const COLONS: u64 = 0x0000_FF00_00FF_0000;
    const DIG: u64 = 0xFFFF_00FF_FF00_FFFF;
    if t & COLONS != 0x0000_3A00_003A_0000 || !all_digits(t, DIG) {
        return None;
    }
    let d = t.wrapping_sub(0x3030_0030_3000_3030);
    let byte = |i: u32| ((d >> (8 * i)) & 0xFF) as i64;
    let h = byte(0) * 10 + byte(1);
    let mi = byte(3) * 10 + byte(4);
    let s = byte(6) * 10 + byte(7);
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let secs = h * 3_600 + mi * 60 + s;
    let slot = ((lo ^ (lo >> 29) ^ mid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 59) as usize;
    let memo = DATES.with(|c| c[slot].get());
    let days = if memo.0 == lo && memo.1 == mid && lo != 0 {
        memo.2 as i64
    } else {
        let days = civil_date(b)?;
        DATES.with(|c| c[slot].set((lo, mid, days as i32)));
        days
    };
    Some((days * 86_400 + secs) * SECOND)
}

/// Days since the epoch of a `YYYY-MM-DD` prefix, validated.
#[inline(never)]
fn civil_date(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    // Years 0000..=9999 give days well inside i32.
    Some(days)
}

/// Reference version of `civil_prefix` (tests only).
#[cfg(test)]
fn civil_prefix_slow(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3_600 + mi * 60 + s) * SECOND)
}

/// Eight ASCII digits (first byte most significant, little-endian load) to
/// their value; the caller has checked they are digits.
#[inline(always)]
fn swar8(x: u64) -> u64 {
    let x = x.wrapping_sub(0x3030_3030_3030_3030);
    let x = (x.wrapping_mul(10) + (x >> 8)) & 0x00FF_00FF_00FF_00FF;
    let x = (x.wrapping_mul(100) + (x >> 16)) & 0x0000_FFFF_0000_FFFF;
    (x.wrapping_mul(10_000) + (x >> 32)) & 0xFFFF_FFFF
}

/// F3: digits only, any number of leading zeros. Values of up to 16 bytes
/// (every in-window value is 13 digits, plus any leading zeros that fit) are
/// right-aligned into a zero-digit-padded 16-byte buffer and converted with
/// SWAR; longer ones take the byte loop.
#[inline(always)]
fn epoch_digits(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n.wrapping_sub(1) < 16 {
        let mut buf = [b'0'; 16];
        buf[16 - n..].copy_from_slice(b);
        let hi = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let lo = u64::from_le_bytes(buf[8..].try_into().unwrap());
        if !all_digits(hi, !0) || !all_digits(lo, !0) {
            return None;
        }
        return Some((swar8(hi) * 100_000_000 + swar8(lo)) as i64);
    }
    epoch_digits_slow(b)
}

/// F3 by a byte loop (any length; also the reference for the fast path).
fn epoch_digits_slow(b: &[u8]) -> Option<i64> {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let first = b.iter().position(|&c| c != b'0').unwrap_or(b.len());
    let rest = &b[first..];
    if rest.len() > 19 {
        return None; // > i64::MAX: outside every window
    }
    let mut v: u64 = 0;
    for &c in rest {
        v = v * 10 + (c - b'0') as u64; // 19 digits fit in u64
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

/// Parse any of F1, F2, F3.
#[inline(always)]
pub fn parse(b: &[u8]) -> Option<i64> {
    match b.len() {
        20 if b[19] == b'Z' => civil_prefix(b),
        25 if b[10] == b'T' => {
            let sign = b[19];
            if (sign != b'+' && sign != b'-') || b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let local = civil_prefix(b)?;
            let off = (oh * 60 + om) * MINUTE;
            // Local time = UTC + offset, so UTC = local - offset.
            Some(if sign == b'+' { local - off } else { local + off })
        }
        _ => epoch_digits(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_fast_matches_slow() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789000/:.-eE x\x00";
        for _ in 0..300_000 {
            let n = (rnd() % 22) as usize;
            let digits_only = rnd() % 3 != 0;
            let s: Vec<u8> = (0..n)
                .map(|_| if digits_only { b'0' + (rnd() % 10) as u8 } else { alphabet[(rnd() % alphabet.len() as u64) as usize] })
                .collect();
            assert_eq!(epoch_digits(&s), epoch_digits_slow(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        for s in ["1791071890000", "0001791071890000", "0", "9999999999999999", "0000000000000000", "1", "86400000"] {
            assert_eq!(epoch_digits(s.as_bytes()), epoch_digits_slow(s.as_bytes()), "{s}");
            assert_eq!(epoch_digits(s.as_bytes()), Some(s.parse::<i64>().unwrap()));
        }
    }

    #[test]
    fn fast_matches_slow() {
        // Random and mutated prefixes, including every single-byte change
        // of a valid one, must agree with the straightforward parser.
        let mut x: u64 = 0x1234_5678_9ABC_DEF1;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789-T:Z+ /:;9";
        for _ in 0..300_000 {
            let mut b = *b"2027-12-18T22:22:54";
            let r = rnd();
            b[0] = b'0' + (r % 10) as u8;
            b[3] = b'0' + (r / 10 % 10) as u8;
            b[5] = b'0' + (r / 100 % 2) as u8;
            b[6] = b'0' + (r / 200 % 10) as u8;
            b[8] = b'0' + (r / 2000 % 4) as u8;
            b[9] = b'0' + (r / 8000 % 10) as u8;
            b[11] = b'0' + (r / 80000 % 3) as u8;
            b[12] = b'0' + (r / 240000 % 10) as u8;
            b[14] = b'0' + (r / 2400000 % 7) as u8;
            b[17] = b'0' + (r / 16800000 % 7) as u8;
            if r >> 60 < 6 {
                let pos = (rnd() % 19) as usize;
                b[pos] = match rnd() % 3 {
                    0 => alphabet[(rnd() % alphabet.len() as u64) as usize],
                    1 => (rnd() & 0xFF) as u8,
                    _ => b[pos].wrapping_add(1),
                };
            }
            assert_eq!(civil_prefix(&b), civil_prefix_slow(&b), "{:?}", String::from_utf8_lossy(&b));
        }
    }

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse(b"86400000"), Some(86_400_000));
        assert_eq!(parse(b"0"), Some(0));
        assert_eq!(parse(b"2028-02-29T00:00:00Z").is_some(), true);
        for bad in [
            &b""[..],
            b"2026-10-03T23:58:10.000Z",
            b"2026-10-03T23:58:10z",
            b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10",
            b"1791071890000.0",
            b"1.79e12",
            b"-5",
            b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z",
            b"2027-02-29T23:58:10Z",
            b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:60:00Z",
            b"2026-10-03T23:58:60Z",
            b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200",
            b"99999999999999999999",
            b"2026-10-03T23:58:10Z\r",
            b"\xd9\xa2\xd9\xa5",
        ] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }
}
