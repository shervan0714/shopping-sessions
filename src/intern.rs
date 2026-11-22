//! Keys for "exotic" IDs (see idkey.rs): every ID that does not pack into
//! 60 bits. Since the Nov 21 generator, 1 in 8 distinct user, event and
//! click IDs carries a 32-character hex suffix, so this path is hot.
//!
//! Default (hash mode): an exotic ID's key is `EXOTIC | h1`, where `h1` is
//! a 62-bit hash of its bytes. Parsing never takes a lock or touches shared
//! memory: each worker appends `(key, h2)` to its own `IdLog`, where `h2` is
//! a second, independent 64-bit hash, plus (for user and click IDs, whose
//! bytes are needed later) where the bytes are in the input mapping.
//!
//! Exactness: keys stay plain integers, compared with `==` everywhere after
//! parsing, so two different IDs must never share a key. `verify` sorts all
//! logs by key after parsing: every pair of entries with the same key must
//! have the same `h2`. If any pair differs, two different IDs collided on
//! `h1`; the run is then restarted in locked mode (below), which is exact by
//! construction. A collision goes unnoticed only if both hashes collide at
//! once (126 bits): about n^2 / 2^127, i.e. ~1e-25 for 10M distinct long IDs.
//! With 10M long IDs even `h1` alone collides with probability ~1e-5, so the
//! restart essentially never runs (SS_HASH_BITS forces it, for tests).
//!
//! `verify` also copies the bytes of every distinct user and click ID once
//! into a read-only table (`Names`), which `bytes` searches; the input
//! mappings can be released afterwards. Event IDs are only ever compared for
//! equality, so their bytes are never kept.
//!
//! Locked mode (fallback, or SS_INTERN=locked): the old exact interner, 1024
//! independently locked shards of a hash map from bytes to an index.

use crate::sys::{hash_bytes, FastMap};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

pub const SHARD_BITS: u32 = 10;
pub const SHARDS: usize = 1 << SHARD_BITS;
pub const LOCAL_BITS: u32 = 32 - SHARD_BITS;
pub const LOCAL_MASK: u32 = (1 << LOCAL_BITS) - 1;

/// Hash-mode keys use the low 62 bits, so a key is never `u64::MAX` (the
/// empty slot in every table).
const HASH_MASK: u64 = (1 << 62) - 1;

/// Set when a hash collision was found: every later `Interner` is locked.
static FORCE_LOCKED: AtomicBool = AtomicBool::new(false);
/// SS_HASH_BITS: keep only this many bits of `h1` (tests the restart).
static HASH_BITS: AtomicU32 = AtomicU32::new(62);

/// Called once at start-up with the environment's settings.
pub fn configure() {
    if std::env::var("SS_INTERN").as_deref() == Ok("locked") {
        FORCE_LOCKED.store(true, Relaxed);
    }
    if let Some(b) = std::env::var("SS_HASH_BITS").ok().and_then(|v| v.trim().parse::<u32>().ok()) {
        HASH_BITS.store(b.clamp(1, 62), Relaxed);
    }
}

/// Switch every later run to locked mode (after a collision).
pub fn force_locked() {
    FORCE_LOCKED.store(true, Relaxed);
}

#[derive(Default)]
struct Shard {
    map: FastMap<&'static [u8], u32>,
    keys: Vec<&'static [u8]>,
    arena: Vec<u8>,
}

/// Where the bytes of a user or click ID are: in an input mapping (still
/// mapped when `verify` runs).
#[derive(Clone, Copy)]
pub struct NameRef {
    key: u64,
    h2: u64,
    ptr: usize,
    len: usize,
}

/// One worker's log of the exotic IDs it saw (hash mode only).
pub struct IdLog {
    /// Event IDs: `(key, h2)`.
    ev: Vec<(u64, u64)>,
    /// User and click IDs.
    names: Vec<NameRef>,
    /// Recently logged `(key, h2)` of names, so a user seen again by the same
    /// worker is not logged again (direct-mapped by `h1`).
    recent: Box<[(u64, u64)]>,
    /// Added to a parsed byte's address to get its address in the input
    /// mapping (non-zero when the chunk was read into a buffer).
    delta: isize,
}

const RECENT: usize = 1 << 12;

impl IdLog {
    pub fn new() -> Self {
        IdLog { ev: Vec::new(), names: Vec::new(), recent: vec![(u64::MAX, 0); RECENT].into_boxed_slice(), delta: 0 }
    }
    /// The chunk being parsed is at `parsed`; its bytes in the mapping are at `mapped`.
    pub fn set_chunk(&mut self, parsed: *const u8, mapped: *const u8) {
        self.delta = (mapped as isize).wrapping_sub(parsed as isize);
    }
}

impl Default for IdLog {
    fn default() -> Self {
        Self::new()
    }
}

/// The bytes of every distinct exotic user and click ID, by key: sorted
/// `(key, bytes)` per part (the part is the key's top hash bits).
struct Names {
    parts: Vec<Vec<(u64, &'static [u8])>>,
}

const PART_BITS: u32 = 8;
const PARTS: usize = 1 << PART_BITS;

#[inline(always)]
fn part_of(k: u64) -> usize {
    ((k & HASH_MASK) >> (62 - PART_BITS)) as usize
}

impl Names {
    fn get(&self, k: u64) -> Option<&'static [u8]> {
        let v = &self.parts[part_of(k)];
        v.binary_search_by_key(&k, |e| e.0).ok().map(|i| v[i].1)
    }
}

/// Two hash functions disagreed: two different IDs share a key.
#[derive(Debug)]
pub struct Collision;

impl std::fmt::Display for Collision {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "exotic ID hash collision (restart in locked mode)")
    }
}
impl std::error::Error for Collision {}

pub struct Interner {
    locked: bool,
    hash_mask: u64,
    shards: Vec<Mutex<Shard>>,
    /// Names of link IDs (set before the people table is built), then of
    /// every user and click ID (set after parsing).
    link_names: OnceLock<Names>,
    all_names: OnceLock<Names>,
}

impl Interner {
    pub fn new() -> Self {
        Self::with_mode_bits(FORCE_LOCKED.load(Relaxed), HASH_BITS.load(Relaxed))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_mode(locked: bool) -> Self {
        Self::with_mode_bits(locked, 62)
    }

    pub fn with_mode_bits(locked: bool, bits: u32) -> Self {
        Interner {
            locked,
            hash_mask: HASH_MASK & (u64::MAX >> (64 - bits)),
            shards: if locked { (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect() } else { Vec::new() },
            link_names: OnceLock::new(),
            all_names: OnceLock::new(),
        }
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    /// The key bits (below EXOTIC) of an exotic event ID (`NAME == false`)
    /// or user/click ID (`NAME == true`).
    #[inline(always)]
    pub fn key<const NAME: bool>(&self, b: &[u8], log: &mut IdLog) -> u64 {
        if self.locked {
            return self.intern(b) as u64;
        }
        let (h1, h2) = hash128(b);
        let k = crate::idkey::EXOTIC | (h1 & self.hash_mask);
        if NAME {
            let slot = &mut log.recent[(h1 >> 40) as usize & (RECENT - 1)];
            if *slot != (k, h2) {
                *slot = (k, h2);
                let ptr = (b.as_ptr() as isize).wrapping_add(log.delta) as usize;
                log.names.push(NameRef { key: k, h2, ptr, len: b.len() });
            }
        } else {
            log.ev.push((k, h2));
        }
        k & !crate::idkey::EXOTIC
    }

    fn intern(&self, key: &[u8]) -> u32 {
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

    /// The bytes of an exotic user or click ID key (with or without EXOTIC).
    pub fn bytes(&self, k: u64) -> &'static [u8] {
        if self.locked {
            let packed = k as u32;
            let s = (packed >> LOCAL_BITS) as usize;
            return self.shards[s].lock().unwrap().keys[(packed & LOCAL_MASK) as usize];
        }
        let k = k | crate::idkey::EXOTIC;
        let names = self.all_names.get().or(self.link_names.get());
        names.and_then(|n| n.get(k)).expect("exotic name not in the verified table")
    }

    /// Check the links' logs and publish their names (before the people
    /// table is built, which orders link IDs).
    pub fn verify_links(&self, logs: &mut [&mut IdLog]) -> Result<(), Collision> {
        if self.locked {
            return Ok(());
        }
        let names = verify(logs, false)?;
        let _ = self.link_names.set(names);
        Ok(())
    }

    /// Check every log (links, events, clicks) and publish all names. Must
    /// run while the input mappings are still mapped.
    pub fn verify_all(&self, logs: &mut [&mut IdLog]) -> Result<(usize, usize), Collision> {
        if self.locked {
            return Ok((0, 0));
        }
        let names = verify(logs, true)?;
        let n = names.parts.iter().map(Vec::len).sum();
        let ev: usize = logs.iter().map(|l| l.ev.len()).sum();
        let _ = self.all_names.set(names);
        for l in logs.iter_mut() {
            l.ev = Vec::new();
            l.names = Vec::new();
        }
        Ok((ev, n))
    }
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}

/// Scatter `items` of every log into one buffer, part-major (`part_of`), so
/// each part is a contiguous range; returns the buffer and part offsets.
fn scatter<T: Copy + Send + Sync>(srcs: &[&[T]], key: impl Fn(&T) -> u64 + Sync) -> (Vec<T>, Vec<usize>) {
    let counts: Vec<Vec<usize>> = crate::par::each(
        srcs.len(),
        |_| Vec::new(),
        |out: &mut Vec<(usize, Vec<usize>)>, i| {
            let mut c = vec![0usize; PARTS];
            for x in srcs[i] {
                c[part_of(key(x))] += 1;
            }
            out.push((i, c));
        },
    )
    .into_iter()
    .flatten()
    .fold(vec![Vec::new(); srcs.len()], |mut acc, (i, c)| {
        acc[i] = c;
        acc
    });
    let total: usize = srcs.iter().map(|s| s.len()).sum();
    // offs[i][p]: where source i's entries of part p start.
    let mut offs = vec![vec![0usize; PARTS]; srcs.len()];
    let mut starts = vec![0usize; PARTS + 1];
    let mut at = 0;
    for p in 0..PARTS {
        starts[p] = at;
        for i in 0..srcs.len() {
            offs[i][p] = at;
            at += counts[i][p];
        }
    }
    starts[PARTS] = at;
    let mut buf: Vec<T> = Vec::with_capacity(total);
    let out = crate::SendPtr(buf.as_mut_ptr());
    crate::par::each(srcs.len(), |_| (), |_, i| {
        let mut o = offs[i].clone();
        for x in srcs[i] {
            let p = part_of(key(x));
            // SAFETY: the ranges of (i, p) are disjoint and cover 0..total.
            unsafe { out.get().add(o[p]).write(*x) };
            o[p] += 1;
        }
    });
    // SAFETY: every slot in 0..total was written once.
    unsafe { buf.set_len(total) };
    (buf, starts)
}

/// Check the logs for collisions and collect the names (see module docs).
fn verify(logs: &[&mut IdLog], events: bool) -> Result<Names, Collision> {
    let collided = AtomicBool::new(false);
    if events {
        let srcs: Vec<&[(u64, u64)]> = logs.iter().map(|l| &l.ev[..]).collect();
        let (mut buf, starts) = scatter(&srcs, |e| e.0);
        let out = crate::SendPtr(buf.as_mut_ptr());
        crate::par::each(PARTS, |_| (), |_, p| {
            // SAFETY: parts are disjoint ranges of buf.
            let s = unsafe { std::slice::from_raw_parts_mut(out.get().add(starts[p]), starts[p + 1] - starts[p]) };
            s.sort_unstable();
            if s.windows(2).any(|w| w[0].0 == w[1].0 && w[0].1 != w[1].1) {
                collided.store(true, Relaxed);
            }
        });
    }
    let srcs: Vec<&[NameRef]> = logs.iter().map(|l| &l.names[..]).collect();
    let (mut buf, starts) = scatter(&srcs, |e| e.key);
    let out = crate::SendPtr(buf.as_mut_ptr());
    let mut parts: Vec<(usize, Vec<(u64, &'static [u8])>)> = crate::par::each(
        PARTS,
        |_| Vec::new(),
        |res: &mut Vec<(usize, Vec<(u64, &'static [u8])>)>, p| {
            // SAFETY: parts are disjoint ranges of buf.
            let s = unsafe { std::slice::from_raw_parts_mut(out.get().add(starts[p]), starts[p + 1] - starts[p]) };
            s.sort_unstable_by_key(|e| (e.key, e.h2));
            let mut uniq: Vec<(u64, &'static [u8])> = Vec::new();
            let mut arena: Vec<u8> = Vec::new();
            let mut i = 0;
            while i < s.len() {
                let e = s[i];
                let mut j = i + 1;
                while j < s.len() && s[j].key == e.key {
                    if s[j].h2 != e.h2 {
                        collided.store(true, Relaxed);
                    }
                    j += 1;
                }
                // SAFETY: the ID's bytes are in an input mapping that is
                // still mapped (verify runs before the inputs are released).
                let src = unsafe { std::slice::from_raw_parts(e.ptr as *const u8, e.len) };
                if arena.capacity() - arena.len() < src.len() {
                    let cap = (256 << 10).max(src.len());
                    std::mem::forget(std::mem::replace(&mut arena, Vec::with_capacity(cap)));
                }
                let start = arena.len();
                arena.extend_from_slice(src);
                // SAFETY: appended within capacity; the buffer is never moved or freed.
                let owned: &'static [u8] = unsafe { std::slice::from_raw_parts(arena.as_ptr().add(start), src.len()) };
                uniq.push((e.key, owned));
                i = j;
            }
            std::mem::forget(arena);
            res.push((p, uniq));
        },
    )
    .into_iter()
    .flatten()
    .collect();
    if collided.load(Relaxed) {
        return Err(Collision);
    }
    parts.sort_unstable_by_key(|x| x.0);
    Ok(Names { parts: parts.into_iter().map(|x| x.1).collect() })
}

/// 128 bits of hash of `b` as two 64-bit words from independent lanes
/// (wyhash-style folded 64x64->128 multiplies, different constants). Reads
/// only inside `b`: 16-byte blocks, then the last 16 bytes (overlapping), or
/// for short strings two overlapping 8- or 4-byte words.
#[inline(always)]
pub fn hash128(b: &[u8]) -> (u64, u64) {
    const K: [u64; 8] = [
        0xa076_1d64_78bd_642f,
        0xe703_7ed1_a0b4_28db,
        0x8ebc_6af0_9c88_c6e3,
        0x5899_65cc_7537_4cc3,
        0x1d8e_4e27_c47d_124f,
        0x9e37_79b9_7f4a_7c15,
        0xbf58_476d_1ce4_e5b9,
        0x94d0_49bb_1331_11eb,
    ];
    #[inline(always)]
    fn mum(a: u64, b: u64) -> u64 {
        let r = (a as u128).wrapping_mul(b as u128);
        (r as u64) ^ ((r >> 64) as u64)
    }
    let n = b.len();
    let p = b.as_ptr();
    // SAFETY: every read below lies within b (offsets checked against n).
    let r8 = |i: usize| unsafe { (p.add(i) as *const u64).read_unaligned().to_le() };
    let r4 = |i: usize| unsafe { (p.add(i) as *const u32).read_unaligned().to_le() as u64 };
    let (mut x, mut y) = (K[0] ^ (n as u64).wrapping_mul(K[5]), K[1] ^ (n as u64).wrapping_mul(K[6]));
    let (a, c) = if n > 16 {
        let mut i = 0;
        while i + 16 < n {
            let (a, c) = (r8(i), r8(i + 8));
            x = mum(a ^ x ^ K[2], c ^ K[3]);
            y = mum(c ^ y ^ K[4], a ^ K[7]);
            i += 16;
        }
        (r8(n - 16), r8(n - 8))
    } else if n >= 8 {
        (r8(0), r8(n - 8))
    } else if n >= 4 {
        (r4(0), r4(n - 4))
    } else if n > 0 {
        // SAFETY: 0 < n < 4, so indices 0, n/2 and n-1 are in b.
        let (b0, b1, b2) = unsafe { (*p as u64, *p.add(n / 2) as u64, *p.add(n - 1) as u64) };
        (b0 << 16 | b1 << 8 | b2, 0)
    } else {
        (0, 0)
    };
    x = mum(a ^ x ^ K[2], c ^ K[3]);
    y = mum(c ^ y ^ K[4], a ^ K[7]);
    (mum(x ^ K[5], y ^ K[6]), mum(y ^ K[0], x ^ K[1]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_differs_on_tails_and_lengths() {
        let mut strings = std::collections::HashSet::new();
        for n in 0..40usize {
            for v in 0..=255u8 {
                let mut s = vec![b'a'; n];
                if n > 0 {
                    s[n - 1] = v;
                }
                strings.insert(s);
                strings.insert(vec![v; n]);
            }
        }
        let hashes: std::collections::HashSet<(u64, u64)> = strings.iter().map(|s| hash128(s)).collect();
        let h1: std::collections::HashSet<u64> = strings.iter().map(|s| hash128(s).0).collect();
        let h2: std::collections::HashSet<u64> = strings.iter().map(|s| hash128(s).1).collect();
        assert_eq!(hashes.len(), strings.len());
        assert_eq!(h1.len(), strings.len());
        assert_eq!(h2.len(), strings.len());
    }

    #[test]
    fn locked_interns_exactly() {
        let it = Interner::with_mode(true);
        let mut log = IdLog::new();
        let a = it.key::<true>(b"user with spaces ", &mut log);
        let b = it.key::<true>(b" user with spaces", &mut log);
        assert_ne!(a, b);
        assert_eq!(a, it.key::<true>(b"user with spaces ", &mut log));
        assert_eq!(it.bytes(a), b"user with spaces ");
        assert_eq!(it.bytes(b), b" user with spaces");
    }
}
