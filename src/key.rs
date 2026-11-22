//! Inline key fingerprints and a read-only lookup table built on them.
//!
//! `pack` turns a byte string into two u64 words. For a given length up to 16
//! bytes the words are a lossless encoding of the bytes (two overlapping
//! 8-byte loads for lengths 8..=16, a zero-padded load below 8), so comparing
//! (len, lo, hi) is an exact equality test that never dereferences the key
//! stored in the table — the dereference is the cache miss we are avoiding.
//! Longer keys fall back to a full byte comparison.

#[inline(always)]
fn load8(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Packed {
    pub lo: u64,
    pub hi: u64,
    pub len: u32,
}

impl Packed {
    /// True when (len, lo, hi) alone decide equality.
    #[inline(always)]
    pub fn exact(&self) -> bool {
        self.len <= 16
    }
}

#[inline(always)]
pub fn pack(b: &[u8]) -> Packed {
    let n = b.len();
    let (lo, hi) = if n >= 8 {
        (load8(b, 0), load8(b, n - 8))
    } else {
        let mut v = 0u64;
        for (i, &c) in b.iter().enumerate() {
            v |= (c as u64) << (8 * i);
        }
        (v, 0)
    };
    Packed { lo, hi, len: n as u32 }
}

/// An immutable open-addressing (linear probing) map from byte strings to
/// `V`, for the small lookup tables (products, campaigns).
pub struct StaticMap<V: Copy> {
    mask: usize,
    slots: Vec<Option<(u64, Packed, &'static [u8], V)>>,
}

impl<V: Copy> StaticMap<V> {
    pub fn build(items: &[(&'static [u8], V)]) -> Self {
        let cap = (items.len() * 2 + 16).next_power_of_two();
        let mut slots = vec![None; cap];
        let mask = cap - 1;
        for &(k, v) in items {
            let h = crate::sys::hash_bytes(k);
            let mut i = h as usize & mask;
            loop {
                match &slots[i] {
                    None => {
                        slots[i] = Some((h, pack(k), k, v));
                        break;
                    }
                    Some((_, _, existing, _)) if *existing == k => break, // keep the first
                    _ => i = (i + 1) & mask,
                }
            }
        }
        StaticMap { mask, slots }
    }

    #[inline]
    pub fn get(&self, k: &[u8]) -> Option<V> {
        let h = crate::sys::hash_bytes(k);
        let p = pack(k);
        let mut i = h as usize & self.mask;
        loop {
            match &self.slots[i] {
                None => return None,
                Some((sh, sp, sk, v)) => {
                    if *sh == h && *sp == p && (p.exact() || *sk == k) {
                        return Some(*v);
                    }
                }
            }
            i = (i + 1) & self.mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_is_exact_up_to_16() {
        let words: Vec<&[u8]> = vec![
            b"", b"a", b"a\0", b"\0a", b"u1", b"U1", b"u1 ", b" u1", b"abcdefgh", b"abcdefghi",
            b"abcdefghij", b"bbcdefghij", b"abcdefghijklmnop", b"abcdefghijklmnoq", b"zbcdefghijklmnop",
        ];
        for a in &words {
            for b in &words {
                assert_eq!(pack(a) == pack(b), a == b, "{:?} {:?}", a, b);
            }
        }
    }

    #[test]
    fn static_map() {
        let long: &'static [u8] = b"a-very-long-product-identifier-1";
        let long2: &'static [u8] = b"a-very-long-product-identifier-2";
        let m = StaticMap::build(&[(&b"p1"[..], 1u16), (&b"p2"[..], 2), (long, 3), (&b"p1"[..], 9)]);
        assert_eq!(m.get(b"p1"), Some(1));
        assert_eq!(m.get(b"p2"), Some(2));
        assert_eq!(m.get(long), Some(3));
        assert_eq!(m.get(long2), None);
        assert_eq!(m.get(b"P1"), None);
        assert_eq!(m.get(b"p1 "), None);
        assert_eq!(m.get(b""), None);
    }
}
