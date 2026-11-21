//! Timestamp parsing (rules.md "Timestamps"): F1 `YYYY-MM-DDTHH:MM:SSZ`,
//! F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 ASCII digits (epoch ms). Returns epoch
//! milliseconds, or None for a bad timestamp. Branch-light: digit validity is
//! OR-ed together and checked once.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    // Howard Hinnant; y >= 0 here (4 digits), so plain division is fine.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn dim(y: i64, m: i64) -> i64 {
    // days in month without a table
    if m == 2 {
        let leap = (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0));
        28 + leap as i64
    } else {
        30 + ((m + (m >> 3)) & 1)
    }
}

/// The `YYYY-MM-DDTHH:MM:SS` prefix of `b` (len >= 19 guaranteed).
#[inline(always)]
fn civil(b: &[u8]) -> Option<i64> {
    let d = |i: usize| b[i].wrapping_sub(b'0');
    let ds = [d(0), d(1), d(2), d(3), d(5), d(6), d(8), d(9), d(11), d(12), d(14), d(15), d(17), d(18)];
    let mut bad = 0u8;
    for &x in &ds {
        bad |= (x > 9) as u8;
    }
    bad |= (b[4] != b'-') as u8 | (b[7] != b'-') as u8 | (b[10] != b'T') as u8 | (b[13] != b':') as u8 | (b[16] != b':') as u8;
    if bad != 0 {
        return None;
    }
    let v = |i: usize| ds[i] as i64;
    let y = v(0) * 1000 + v(1) * 100 + v(2) * 10 + v(3);
    let mo = v(4) * 10 + v(5);
    let da = v(6) * 10 + v(7);
    let h = v(8) * 10 + v(9);
    let mi = v(10) * 10 + v(11);
    let s = v(12) * 10 + v(13);
    if mo < 1 || mo > 12 || da < 1 || h > 23 || mi > 59 || s > 59 || da > dim(y, mo) {
        return None;
    }
    Some((days_from_civil(y, mo, da) * 86_400 + h * 3_600 + mi * 60 + s) * SECOND)
}

#[inline]
fn f3(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    let rest = &b[i..];
    let mut v: u64 = 0;
    let mut bad = 0u8;
    for &c in rest {
        let x = c.wrapping_sub(b'0');
        bad |= (x > 9) as u8;
        v = v.wrapping_mul(10).wrapping_add(x as u64);
    }
    if bad != 0 || rest.len() > 19 || v > i64::MAX as u64 {
        // > 19 significant digits: too big for i64 -> outside every window
        return None;
    }
    Some(v as i64)
}

#[inline]
pub fn parse(b: &[u8]) -> Option<i64> {
    match b.len() {
        20 if b[10] == b'T' => {
            if b[19] != b'Z' {
                return None;
            }
            civil(b)
        }
        25 if b[10] == b'T' => {
            let sign = b[19];
            if (sign != b'+' && sign != b'-') || b[22] != b':' {
                return None;
            }
            let (a, c, e, f) = (b[20].wrapping_sub(b'0'), b[21].wrapping_sub(b'0'), b[23].wrapping_sub(b'0'), b[24].wrapping_sub(b'0'));
            if a > 9 || c > 9 || e > 9 || f > 9 {
                return None;
            }
            let oh = (a * 10 + c) as i64;
            let om = (e * 10 + f) as i64;
            if oh > 23 || om > 59 {
                return None;
            }
            let local = civil(b)?;
            let off = (oh * 60 + om) * MINUTE;
            Some(if sign == b'+' { local - off } else { local + off })
        }
        _ => f3(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse(b"86400000"), Some(86_400_000));
        assert_eq!(parse(b"00000000000000000000"), Some(0));
        assert_eq!(parse(b"12345678901234567890"), None);
        assert_eq!(parse(b"09223372036854775807"), Some(i64::MAX));
        assert_eq!(parse(b"0000000000001791071890000"), Some(f1));
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        assert!(parse(b"2000-02-29T00:00:00Z").is_some());
        assert!(parse(b"1900-02-29T00:00:00Z").is_none());
        for (m, n) in [(1, 31), (2, 28), (3, 31), (4, 30), (5, 31), (6, 30), (7, 31), (8, 31), (9, 30), (10, 31), (11, 30), (12, 31)] {
            assert_eq!(dim(2027, m), n);
        }
        for bad in [
            &b""[..], b"2026-10-03T23:58:10.000Z", b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T23:58:10Z", b"2026-00-03T23:58:10Z", b"2026-02-30T23:58:10Z", b"2027-02-29T23:58:10Z",
            b"2026-04-31T23:58:10Z", b"2026-10-00T23:58:10Z",
            b"2026-10-03T24:00:00Z", b"2026-10-03T23:60:00Z", b"2026-10-03T23:58:60Z",
            b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200",
            b"99999999999999999999", b"2026-10-03T23:58:10Z\r", b"\xd9\xa2\xd9\xa5", b"2026-10-03T23:58:1aZ",
        ] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }
}
