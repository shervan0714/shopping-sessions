//! Lock-free insert-only table of user IDs, with an in-place concurrent
//! union-find. The slot index is the ID's number. Each slot is 32 bytes (two
//! per cache line): the key, `val` (the person: the slot of the person's
//! name), the union-find parent and the component's best name.
//!
//! Insert protocol: `lo` is 0 when empty (a stored key's low byte is its
//! length 1..=15 or `LONG`, never 0). A writer CASes `lo` 0 -> LOCKED, writes
//! `hi` and `val`, then publishes `lo` with Release. Readers that see LOCKED
//! spin briefly; readers that see a key read `hi`/`val` after an Acquire.

use crate::key::Key;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::*};

const LOCKED: u64 = 0xFF;

#[repr(C, align(32))]
pub struct Slot {
    lo: AtomicU64,
    hi: AtomicU64,
    val: AtomicU32,
    parent: AtomicU32, // 0 = root, else parent slot + 1
    best: AtomicU32,   // 0 = none, else slot + 1 of the smallest key seen
    _pad: u32,
}

pub struct UTab {
    slots: &'static [Slot],
    mask: usize,
    pub inserted: AtomicUsize,
    pub limit: usize,
    pub overflow: AtomicBool,
}

impl UTab {
    pub fn new(bits: u32) -> Self {
        let n = 1usize << bits;
        UTab {
            slots: crate::mem::alloc_slice::<Slot>(n),
            mask: n - 1,
            inserted: AtomicUsize::new(0),
            limit: n / 4 * 3,
            overflow: AtomicBool::new(false),
        }
    }
    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        crate::mem::prefetch(&self.slots[h as usize & self.mask]);
    }
    /// The slot of `k` (hash `h`), inserting it if absent. `local` counts this
    /// thread's inserts; flushed into `inserted` every 256 so the load limit
    /// is enforced without a shared counter on the hot path.
    #[inline(always)]
    pub fn intern(&self, k: Key, h: u64, local: &mut u32) -> u32 {
        self.intern2(k, h, local).0
    }
    /// Like `intern`, also telling whether this call inserted the key.
    #[inline(always)]
    pub fn intern2(&self, k: Key, h: u64, local: &mut u32) -> (u32, bool) {
        let mut i = h as usize & self.mask;
        let mut probes = 0usize;
        loop {
            probes += 1;
            if probes > self.mask {
                // full table: give up; the caller sees `overflow` and reruns
                self.overflow.store(true, Relaxed);
                return (0, false);
            }
            let s = &self.slots[i];
            let mut lo = s.lo.load(Acquire);
            if lo == k.lo && s.hi.load(Relaxed) == k.hi {
                return (i as u32, false);
            }
            if lo == 0 {
                match s.lo.compare_exchange(0, LOCKED, Acquire, Acquire) {
                    Ok(_) => {
                        s.hi.store(k.hi, Relaxed);
                        s.val.store(i as u32, Relaxed);
                        s.lo.store(k.lo, Release);
                        *local += 1;
                        if *local >= 256 {
                            self.flush(local);
                        }
                        return (i as u32, true);
                    }
                    Err(v) => lo = v,
                }
            }
            while lo == LOCKED {
                std::hint::spin_loop();
                lo = s.lo.load(Acquire);
            }
            if lo == k.lo && s.hi.load(Relaxed) == k.hi {
                return (i as u32, false);
            }
            i = (i + 1) & self.mask;
        }
    }
    pub fn flush(&self, local: &mut u32) {
        let n = self.inserted.fetch_add(*local as usize, Relaxed) + *local as usize;
        *local = 0;
        if n > self.limit {
            self.overflow.store(true, Relaxed);
        }
    }
    #[inline(always)]
    pub fn val(&self, slot: u32) -> u32 {
        self.slots[slot as usize].val.load(Relaxed)
    }
    pub fn key(&self, slot: u32) -> Key {
        let s = &self.slots[slot as usize];
        Key { lo: s.lo.load(Relaxed), hi: s.hi.load(Relaxed) }
    }

    // ---- union-find (lock-free; links point higher slots at lower ones) ----
    #[inline]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.slots[x as usize].parent.load(Relaxed);
            if p == 0 {
                return x;
            }
            let px = p - 1;
            let gp = self.slots[px as usize].parent.load(Relaxed);
            if gp == 0 {
                return px;
            }
            // path halving; a lost race only skips the shortcut
            let _ = self.slots[x as usize].parent.compare_exchange_weak(p, gp, Relaxed, Relaxed);
            x = gp - 1;
        }
    }
    pub fn union(&self, a: u32, b: u32) {
        loop {
            let (ra, rb) = (self.find(a), self.find(b));
            if ra == rb {
                return;
            }
            let (hi, lo) = if ra > rb { (ra, rb) } else { (rb, ra) };
            if self.slots[hi as usize].parent.compare_exchange(0, lo + 1, AcqRel, Relaxed).is_ok() {
                return;
            }
        }
    }
    /// After all unions: every slot in `range` offers its key to its root's
    /// best (lock-free minimum).
    pub fn offer_names(&self, slots: &[u32]) {
        for &i in slots {
            let i = i as usize;
            let r = self.find(i as u32) as usize;
            let me = self.key(i as u32);
            let best = &self.slots[r].best;
            let mut cur = best.load(Relaxed);
            loop {
                if cur != 0 && self.key(cur - 1).cmp(&me) != std::cmp::Ordering::Greater {
                    break;
                }
                match best.compare_exchange_weak(cur, i as u32 + 1, Relaxed, Relaxed) {
                    Ok(_) => break,
                    Err(v) => cur = v,
                }
            }
        }
    }
    /// After `offer_names`: each slot's person is its component's best slot.
    pub fn set_persons(&self, slots: &[u32]) {
        for &i in slots {
            let i = i as usize;
            let r = self.find(i as u32) as usize;
            let b = self.slots[r].best.load(Relaxed);
            self.slots[i].val.store(b - 1, Relaxed);
        }
    }
}
