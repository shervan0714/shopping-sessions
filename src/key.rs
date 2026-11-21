//! Exact, order-preserving 128-bit keys for short strings.
//!
//! A string of 1..=15 bytes packs losslessly into `Key { hi, lo }`: the bytes
//! big-endian from the top, zero padding, and the length in the lowest byte.
//! Comparing keys as u128 is then exactly byte-wise string order (padding
//! with zeros and then comparing lengths is the same as lexicographic order),
//! and equality is string equality. Longer strings (never produced by the
//! generator, but legal) go to a global interner and get `lo = LONG`,
//! `hi = index`; equality stays exact, ordering then needs `cmp`.
//! The empty string packs to 0, which is never stored.

use std::sync::Mutex;

pub const LONG: u64 = 0xFE;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
#[repr(C)]
pub struct Key {
    pub lo: u64,
    pub hi: u64,
}

impl Key {
    #[inline(always)]
    pub fn is_long(&self) -> bool {
        self.lo == LONG
    }
    #[inline(always)]
    pub fn u128(&self) -> u128 {
        ((self.hi as u128) << 64) | self.lo as u128
    }
    #[inline(always)]
    pub fn hash(&self) -> u64 {
        let x = (self.lo ^ 0x9E37_79B9_7F4A_7C15).wrapping_mul(0xBF58_476D_1CE4_E5B9) ^ self.hi.rotate_left(29);
        let x = (x ^ (x >> 31)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 29)
    }
    /// The string this key stands for.
    pub fn bytes(&self, out: &mut Vec<u8>) {
        if self.is_long() {
            let g = LONGS.lock().unwrap();
            out.extend_from_slice(&g.as_ref().unwrap().1[self.hi as usize]);
            return;
        }
        let n = (self.lo & 0xFF) as usize;
        let v = self.u128().to_be_bytes();
        out.extend_from_slice(&v[..n]);
    }
    /// Byte-wise string order.
    pub fn cmp(&self, o: &Key) -> std::cmp::Ordering {
        if !self.is_long() && !o.is_long() {
            return self.u128().cmp(&o.u128());
        }
        let (mut a, mut b) = (Vec::new(), Vec::new());
        self.bytes(&mut a);
        o.bytes(&mut b);
        a.cmp(&b)
    }
}

type LongMap = (std::collections::HashMap<Vec<u8>, u64>, Vec<Vec<u8>>);
static LONGS: Mutex<Option<LongMap>> = Mutex::new(None);

#[cold]
#[inline(never)]
fn long_key(b: &[u8], insert: bool) -> Key {
    let mut g = LONGS.lock().unwrap();
    let (map, list) = g.get_or_insert_with(Default::default);
    if let Some(&i) = map.get(b) {
        return Key { lo: LONG, hi: i };
    }
    if !insert {
        return Key { lo: LONG, hi: u64::MAX }; // matches nothing stored
    }
    let i = list.len() as u64;
    list.push(b.to_vec());
    map.insert(b.to_vec(), i);
    Key { lo: LONG, hi: i }
}

/// Pack `b`. `end` is one past the last readable byte of the buffer `b`
/// lives in, so a 16-byte load can be used when it stays in bounds.
#[inline(always)]
pub fn pack_in(b: &[u8], end: *const u8, insert_long: bool) -> Key {
    let n = b.len();
    if n > 15 {
        return long_key(b, insert_long);
    }
    let p = b.as_ptr();
    let raw: u128 = if (p as usize) + 16 <= end as usize {
        // SAFETY: 16 bytes from p are inside the readable buffer.
        unsafe { u128::from_be_bytes(std::ptr::read_unaligned(p as *const [u8; 16])) }
    } else {
        let mut t = [0u8; 16];
        t[..n].copy_from_slice(b);
        u128::from_be_bytes(t)
    };
    let mask = if n == 0 { 0 } else { !(u128::MAX >> (8 * n)) };
    let v = (raw & mask) | n as u128;
    Key { lo: v as u64, hi: (v >> 64) as u64 }
}

#[inline]
pub fn pack(b: &[u8], insert_long: bool) -> Key {
    pack_in(b, b.as_ptr_range().end, insert_long)
}

/// Read-only open-addressing map Key -> u32 for the small lookup tables.
pub struct StaticMap {
    mask: usize,
    slots: Vec<(Key, u32)>,
}

impl StaticMap {
    pub fn build(items: &[(Key, u32)]) -> Self {
        let cap = (items.len() * 3 + 16).next_power_of_two();
        let mut slots = vec![(Key::default(), u32::MAX); cap];
        let mask = cap - 1;
        for &(k, v) in items {
            let mut i = k.hash() as usize & mask;
            loop {
                if slots[i].0 == Key::default() {
                    slots[i] = (k, v);
                    break;
                }
                if slots[i].0 == k {
                    break; // keep the first
                }
                i = (i + 1) & mask;
            }
        }
        StaticMap { mask, slots }
    }
    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        crate::mem::prefetch(&self.slots[h as usize & self.mask]);
    }
    #[inline(always)]
    pub fn get(&self, k: Key, h: u64) -> Option<u32> {
        if k == Key::default() {
            return None;
        }
        let mut i = h as usize & self.mask;
        loop {
            let s = &self.slots[i];
            if s.0 == k {
                return Some(s.1);
            }
            if s.0 == Key::default() {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn order_and_roundtrip() {
        let words: Vec<&[u8]> = vec![
            b"a", b"a\0", b"\0a", b"u1", b"U1", b"u1 ", b" u1", b"abcdefgh", b"abcdefghi", b"ab",
            b"abcdefghijklmno", b"abcdefghijklmnop", b"abcdefghijklmnoq", b"zbcdefghijklmnop", b"\xff", b"a\x01",
        ];
        for a in &words {
            let ka = pack(a, true);
            let mut s = Vec::new();
            ka.bytes(&mut s);
            assert_eq!(&s[..], *a);
            for b in &words {
                let kb = pack(b, true);
                assert_eq!(ka == kb, a == b);
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{:?} {:?}", a, b);
            }
        }
        assert_eq!(pack(b"", true), Key::default());
    }
}
