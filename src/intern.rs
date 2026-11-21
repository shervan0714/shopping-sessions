//! A concurrent string interner, used only for "exotic" IDs (see idkey.rs).
//!
//! 1024 independently locked shards, each a hashbrown table of indices into a
//! key vector. Keys are `&'static [u8]` slices of the memory-mapped inputs, so
//! interning never copies a string. With 8 threads and 1024 shards the locks
//! are almost never contended.
//!
//! An interned ID is "packed": `shard << LOCAL_BITS | local index`. After all
//! inputs are parsed, `finish` assigns every packed ID a dense global index.
//!
//! Shard selection uses bits 32..42 of the hash, because hashbrown uses the low
//! bits for its bucket index and the top 7 bits for its tag bytes; using either
//! for the shard would make every key in a shard share them.

use crate::key::{pack, Packed};
use hashbrown::HashTable;
use std::sync::Mutex;

pub const SHARD_BITS: u32 = 10;
pub const SHARDS: usize = 1 << SHARD_BITS;
pub const LOCAL_BITS: u32 = 32 - SHARD_BITS;
pub const LOCAL_MASK: u32 = (1 << LOCAL_BITS) - 1;

/// Each table entry holds the hash, an exact inline encoding of the key
/// (`key::pack`, lossless up to 16 bytes) and the local index, so a lookup
/// touches only the table bucket. Longer keys are also compared byte-wise.
///
/// Keys are copied into a per-shard arena on first sight, so later stages
/// (choosing each person's smallest ID, writing names) read compact memory
/// instead of scattered pages of the input files.
#[derive(Default)]
struct Shard {
    table: HashTable<(u64, Packed, u32)>,
    keys: Vec<&'static [u8]>,
    arena: Vec<u8>,
}

pub struct Interner {
    shards: Vec<Mutex<Shard>>,
}

#[inline(always)]
pub fn hash(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

impl Interner {
    pub fn new() -> Self {
        Interner { shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect() }
    }

    #[inline]
    pub fn intern(&self, key: &[u8]) -> u32 {
        let h = hash(key);
        let s = ((h >> 32) as usize) & (SHARDS - 1);
        let mut guard = self.shards[s].lock().unwrap();
        let p = pack(key);
        let shard = &mut *guard;
        let keys = &shard.keys;
        let entry = shard.table.entry(h, |e| e.0 == h && e.1 == p && (p.exact() || keys[e.2 as usize] == key), |e| e.0);
        let local = match entry {
            hashbrown::hash_table::Entry::Occupied(e) => e.get().2,
            hashbrown::hash_table::Entry::Vacant(v) => {
                let i = shard.keys.len() as u32;
                assert!(i <= LOCAL_MASK, "interner shard overflow");
                let owned = {
                    // (arena and keys are disjoint fields of the shard)
                    let arena = &mut shard.arena;
                    if arena.capacity() - arena.len() < key.len() {
                        let cap = (64 << 10).max(key.len());
                        std::mem::forget(std::mem::replace(arena, Vec::with_capacity(cap)));
                    }
                    let start = arena.len();
                    arena.extend_from_slice(key);
                    // SAFETY: appended within capacity, never moved or freed.
                    unsafe { std::slice::from_raw_parts(arena.as_ptr().add(start), key.len()) }
                };
                shard.keys.push(owned);
                v.insert((h, p, i));
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
