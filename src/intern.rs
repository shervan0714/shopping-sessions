//! A concurrent string interner, used only for "exotic" IDs (see idkey.rs).
//!
//! 1024 independently locked shards, each a hash map from the key's bytes to
//! its index in the shard. With 8 threads and 1024 shards the locks are
//! almost never contended (and exotic IDs are rare to begin with).
//!
//! An interned ID is `shard << LOCAL_BITS | local index`.
//!
//! Keys are copied into a per-shard arena on first sight, so later stages
//! (choosing each person's smallest ID, writing names) read compact memory
//! and the input mappings can be released after parsing.

use crate::sys::{hash_bytes, FastMap};
use std::sync::Mutex;

pub const SHARD_BITS: u32 = 10;
pub const SHARDS: usize = 1 << SHARD_BITS;
pub const LOCAL_BITS: u32 = 32 - SHARD_BITS;
pub const LOCAL_MASK: u32 = (1 << LOCAL_BITS) - 1;

#[derive(Default)]
struct Shard {
    map: FastMap<&'static [u8], u32>,
    keys: Vec<&'static [u8]>,
    arena: Vec<u8>,
}

pub struct Interner {
    shards: Vec<Mutex<Shard>>,
}

impl Interner {
    pub fn new() -> Self {
        Interner { shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect() }
    }

    #[inline]
    pub fn intern(&self, key: &[u8]) -> u32 {
        let s = ((hash_bytes(key) >> 32) as usize) & (SHARDS - 1);
        let mut guard = self.shards[s].lock().unwrap();
        let shard = &mut *guard;
        let local = match shard.map.get(key) {
            Some(&i) => i,
            None => {
                let i = shard.keys.len() as u32;
                assert!(i <= LOCAL_MASK, "interner shard overflow");
                let arena = &mut shard.arena;
                if arena.capacity() - arena.len() < key.len() {
                    let cap = (64 << 10).max(key.len());
                    std::mem::forget(std::mem::replace(arena, Vec::with_capacity(cap)));
                }
                let start = arena.len();
                arena.extend_from_slice(key);
                // SAFETY: appended within capacity; the buffer is never moved or freed.
                let owned: &'static [u8] = unsafe { std::slice::from_raw_parts(arena.as_ptr().add(start), key.len()) };
                shard.keys.push(owned);
                shard.map.insert(owned, i);
                i
            }
        };
        ((s as u32) << LOCAL_BITS) | local
    }

    /// The bytes of an interned ID (slow path: takes the shard's lock).
    pub fn bytes(&self, packed: u32) -> &'static [u8] {
        let s = (packed >> LOCAL_BITS) as usize;
        self.shards[s].lock().unwrap().keys[(packed & LOCAL_MASK) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interns_exactly() {
        let it = Interner::new();
        let a = it.intern(b"user with spaces ");
        let b = it.intern(b" user with spaces");
        assert_ne!(a, b);
        assert_eq!(a, it.intern(b"user with spaces "));
        assert_eq!(it.bytes(a), b"user with spaces ");
        assert_eq!(it.bytes(b), b" user with spaces");
    }
}
