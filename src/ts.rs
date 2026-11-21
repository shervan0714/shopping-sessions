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

/// The `YYYY-MM-DDTHH:MM:SS` prefix (19 bytes; caller checks length).
#[inline(always)]
fn civil_prefix(b: &[u8]) -> Option<i64> {
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

/// F3: digits only, any number of leading zeros.
#[inline]
fn epoch_digits(b: &[u8]) -> Option<i64> {
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
#[inline]
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
