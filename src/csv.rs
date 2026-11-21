//! The CSV dialect of rules.md. Only `\n` ends a line; `\r` is data. No value
//! ever holds a comma, quote or newline, so every comma separates fields and a
//! field is either bare (no `"` at all) or wrapped in exactly one pair of
//! quotes around the whole field (`"..."`, with no `"` inside). Anything else,
//! or the wrong number of fields, makes the whole line malformed.

fn memchr(c: u8, s: &[u8]) -> Option<usize> { s.iter().position(|&x| x == c) }
fn memchr_iter(c: u8, s: &[u8]) -> impl Iterator<Item = usize> + '_ { s.iter().enumerate().filter(move |x| *x.1 == c).map(|x| x.0) }

/// Split `line` into exactly `N` fields, removing syntactic quotes. Returns
/// `false` for a malformed line (wrong field count, stray quote). An empty
/// line has one field, so with `N >= 2` it is malformed, as required.
#[inline]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut n = 0usize;
    let mut start = 0usize;
    for i in memchr_iter(b',', line) {
        if n + 1 >= N {
            return false; // too many fields
        }
        out[n] = &line[start..i];
        n += 1;
        start = i + 1;
    }
    if n + 1 != N {
        return false; // too few fields
    }
    out[N - 1] = &line[start..];
    if memchr(b'"', line).is_some() {
        for f in out.iter_mut() {
            match unquote(f) {
                Some(v) => *f = v,
                None => return false,
            }
        }
    }
    true
}

#[inline]
fn unquote(f: &[u8]) -> Option<&[u8]> {
    if memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Call `f` for every line in `data` (lines end at `\n`; a final line with no
/// `\n` still counts; the empty string after a final `\n` is not a line).
#[inline]
pub fn for_each_line<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut start = 0usize;
    for i in memchr_iter(b'\n', data) {
        f(&data[start..i]);
        start = i + 1;
    }
    if start < data.len() {
        f(&data[start..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s<const N: usize>(l: &str) -> Option<Vec<String>> {
        let mut out = [&b""[..]; N];
        if split::<N>(l.as_bytes(), &mut out) {
            Some(out.iter().map(|f| String::from_utf8_lossy(f).into_owned()).collect())
        } else {
            None
        }
    }

    #[test]
    fn dialect() {
        assert_eq!(s::<2>("a,b"), Some(vec!["a".into(), "b".into()]));
        assert_eq!(s::<2>("\"a\",\"\""), Some(vec!["a".into(), "".into()]));
        assert_eq!(s::<2>("\" a\",b\r"), Some(vec![" a".into(), "b\r".into()]));
        assert_eq!(s::<2>(""), None);
        assert_eq!(s::<2>("a"), None);
        assert_eq!(s::<2>("a,b,c"), None);
        assert_eq!(s::<2>("a\"x,b"), None);
        assert_eq!(s::<2>("\"a,b"), None);
        assert_eq!(s::<2>("\"a\"x,b"), None);
        assert_eq!(s::<2>("\"\"\",b"), None);
        assert_eq!(s::<2>("\",b"), None);
        assert_eq!(s::<2>(","), Some(vec!["".into(), "".into()]));
    }
}
