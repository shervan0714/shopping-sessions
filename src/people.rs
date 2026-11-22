//! Identity resolution (rules.md "Linked IDs") on 64-bit ID keys.
//!
//! Only IDs that appear in links can belong to a multi-ID person, so the
//! union-find runs over link IDs only. The result is a table from each linked
//! ID's key to its person's key (the byte-wise smallest ID of its component);
//! every other ID is its own person, so `person(k)` is `k` for them. A small
//! bit filter answers most lookups for unlinked IDs without touching the
//! table.

use crate::idkey::{self, mix};
use crate::intern::Interner;
use crate::sys::FastMap;

const EMPTY: u64 = u64::MAX;

/// Person partitions for the session stage (by hash of the person key).
/// Sized from the input before parsing (`set_pp_bits`): about 20K events
/// each, so a partition's grouping table and rows stay cache-resident.
static PP_BITS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(8);
pub fn set_pp_bits(b: u32) {
    PP_BITS.store(b, std::sync::atomic::Ordering::Relaxed);
}
/// The number of person partitions.
pub fn pp() -> usize {
    1 << PP_BITS.load(std::sync::atomic::Ordering::Relaxed)
}
#[inline(always)]
pub fn ppart(k: u64) -> usize {
    (mix(k) >> (64 - PP_BITS.load(std::sync::atomic::Ordering::Relaxed))) as usize
} // never a key: packed keys use 60 bits, exotic ones 63+32

/// Every ID that appears in a link, as a bit filter (no false negatives),
/// built right after the links are parsed and before events are: an event
/// whose user misses it is its own person, so its payload goes straight to
/// its person partition without waiting for (or looking up) the people
/// table. About 16 bits per link endpoint, so it stays cache-resident.
pub struct LinkFilter {
    shift: u32,
    words: Vec<u64>,
}

impl LinkFilter {
    /// A filter nothing is in (no links).
    pub fn empty() -> Self {
        LinkFilter { shift: 64, words: Vec::new() }
    }

    pub fn build(links: &[Vec<(u64, u64)>]) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
        let n: usize = links.iter().map(Vec::len).sum();
        if n == 0 {
            return Self::empty();
        }
        let bits = (32 * n).next_power_of_two().max(1 << 16).trailing_zeros();
        let words: Vec<AtomicU64> = (0..(1usize << bits) / 64).map(|_| AtomicU64::new(0)).collect();
        let shift = 64 - bits;
        crate::par::each(links.len(), |_| (), |_, i| {
            for &(a, b) in &links[i] {
                for k in [a, b] {
                    let f = (mix(k) >> shift) as usize;
                    words[f / 64].fetch_or(1 << (f % 64), Relaxed);
                }
            }
        });
        LinkFilter { shift, words: words.into_iter().map(AtomicU64::into_inner).collect() }
    }

    /// Whether `k` may appear in a link (false: it certainly does not).
    #[inline(always)]
    pub fn maybe(&self, k: u64) -> bool {
        if self.shift == 64 {
            return false;
        }
        let f = (mix(k) >> self.shift) as usize;
        // SAFETY: f < 2^(64 - shift) = words.len() * 64.
        unsafe { *self.words.get_unchecked(f / 64) & (1 << (f % 64)) != 0 }
    }

    pub fn bytes(&self) -> usize {
        self.words.len() * 8
    }
}

/// The people table, published once by the thread that builds it; readers
/// block (no spinning) until it is there.
#[derive(Default)]
pub struct PeopleSlot {
    table: std::sync::OnceLock<People>,
    ready: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
}

impl PeopleSlot {
    pub fn set(&self, p: People) {
        let _ = self.table.set(p);
        *self.ready.lock().unwrap() = true;
        self.cv.notify_all();
    }
    /// The table if it is built already (never blocks).
    pub fn get(&self) -> Option<&People> {
        self.table.get()
    }
    /// Wait until `set` has run.
    pub fn wait(&self) -> &People {
        if let Some(p) = self.table.get() {
            return p;
        }
        let mut r = self.ready.lock().unwrap();
        while !*r {
            r = self.cv.wait(r).unwrap();
        }
        drop(r);
        self.table.get().expect("people table")
    }
}

pub struct People {
    mask: usize,
    slots: Vec<(u64, u64)>,
    fbits: u32,
    filter: Vec<u64>,
    pub n_ids: usize,
    pub n_merged: usize,
}

struct UnionFind {
    parent: Vec<u32>,
    size: Vec<u32>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        UnionFind { parent: (0..n as u32).collect(), size: vec![1; n] }
    }
    #[inline]
    fn find(&mut self, mut x: u32) -> u32 {
        // Path halving: no recursion, so a chain of any length is fine.
        while self.parent[x as usize] != x {
            let gp = self.parent[self.parent[x as usize] as usize];
            self.parent[x as usize] = gp;
            x = gp;
        }
        x
    }
    #[inline]
    fn union(&mut self, a: u32, b: u32) {
        let (mut ra, mut rb) = (self.find(a), self.find(b));
        if ra == rb {
            return;
        }
        if self.size[ra as usize] < self.size[rb as usize] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
        self.size[ra as usize] += self.size[rb as usize];
    }
}

impl People {
    pub fn build(links: &[Vec<(u64, u64)>], exotic: &Interner) -> People {
        let n_links: usize = links.iter().map(Vec::len).sum();
        let mut index: FastMap<u64, u32> = FastMap::with_capacity_and_hasher(n_links + n_links / 2, Default::default());
        let mut ids: Vec<u64> = Vec::with_capacity(n_links + n_links / 2);
        let mut pairs: Vec<(u32, u32)> = Vec::with_capacity(n_links);
        for v in links {
            for &(a, b) in v {
                let mut id = |k: u64| {
                    *index.entry(k).or_insert_with(|| {
                        ids.push(k);
                        (ids.len() - 1) as u32
                    })
                };
                let (ia, ib) = (id(a), id(b));
                pairs.push((ia, ib));
            }
        }
        drop(index);
        let n = ids.len();
        let mut uf = UnionFind::new(n);
        for &(a, b) in &pairs {
            uf.union(a, b);
        }
        drop(pairs);
        let roots: Vec<u32> = (0..n as u32).map(|i| uf.find(i)).collect();
        drop(uf);
        // Byte-wise smallest ID of each component.
        let mut best: Vec<u64> = vec![EMPTY; n];
        for i in 0..n {
            let r = roots[i] as usize;
            let b = best[r];
            if b == EMPTY || idkey::cmp(ids[i], b, exotic).is_lt() {
                best[r] = ids[i];
            }
        }
        let n_merged = (0..n).filter(|&i| best[roots[i] as usize] != ids[i]).count();
        let cap = (2 * n_merged).next_power_of_two().max(1024);
        let mut slots = vec![(EMPTY, 0u64); cap];
        let fbits = ((16 * n_merged).next_power_of_two().max(1 << 16)).trailing_zeros();
        let mut filter = vec![0u64; (1usize << fbits) / 64];
        let mask = cap - 1;
        for i in 0..n {
            let k = ids[i];
            let name = best[roots[i] as usize];
            if name == k {
                // Its own person: needs no entry, `person` returns it as is.
                continue;
            }
            let h = mix(k);
            let f = (h >> (64 - fbits)) as usize;
            filter[f / 64] |= 1 << (f % 64);
            let mut s = h as usize & mask;
            while slots[s].0 != EMPTY {
                s = (s + 1) & mask;
            }
            slots[s] = (k, name);
        }
        People { mask, slots, fbits, filter, n_ids: n, n_merged }
    }

    /// Stage 1 of a prefetched lookup: hint the filter word for `k`.
    #[inline(always)]
    pub fn prefetch_filter(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let f = (mix(k) >> (64 - self.fbits)) as usize;
            // SAFETY: prefetch of an in-bounds address; nothing is read.
            unsafe { _mm_prefetch(self.filter.as_ptr().add(f / 64) as *const i8, _MM_HINT_T0) };
        }
    }

    /// Stage 2 (after stage 1 had time to land): if the filter says `k` may
    /// be linked, hint its table slot.
    #[inline(always)]
    pub fn prefetch_slot(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let h = mix(k);
            let f = (h >> (64 - self.fbits)) as usize;
            // SAFETY: in-bounds read and prefetch.
            unsafe {
                if *self.filter.get_unchecked(f / 64) & (1 << (f % 64)) != 0 {
                    _mm_prefetch(self.slots.as_ptr().add(h as usize & self.mask) as *const i8, _MM_HINT_T0);
                }
            }
        }
    }

    /// The person key of an ID key.
    #[inline(always)]
    pub fn person(&self, k: u64) -> u64 {
        let h = mix(k);
        let f = (h >> (64 - self.fbits)) as usize;
        // SAFETY: f < 2^fbits = filter.len() * 64.
        if unsafe { *self.filter.get_unchecked(f / 64) } & (1 << (f % 64)) == 0 {
            return k;
        }
        let mut s = h as usize & self.mask;
        loop {
            let (x, v) = self.slots[s];
            if x == k {
                return v;
            }
            if x == EMPTY {
                return k;
            }
            s = (s + 1) & self.mask;
        }
    }
}
