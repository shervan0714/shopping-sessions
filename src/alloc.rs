//! Memory helpers: big buffers on transparent huge pages.
//!
//! Page faults dominated the kernel time (a 4 KB fault per page, zeroed, for
//! gigabytes of row buffers). Allocations of `BIG` bytes or more get their own
//! anonymous mapping advised `MADV_HUGEPAGE` (the scorer's and WSL's THP mode
//! is usually `madvise`), so one fault maps 2 MB. Everything smaller goes to
//! the system allocator. Both are hints only: without THP it is plain mmap.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;

const BIG: usize = 1 << 20;
const HUGE: usize = 2 << 20;

#[cfg(target_os = "linux")]
mod sys {
    extern "C" {
        pub fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
        pub fn munmap(addr: *mut u8, len: usize) -> i32;
        pub fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    }
    pub const PROT_RW: i32 = 0x1 | 0x2;
    pub const MAP_PRIVATE_ANON_NORESERVE: i32 = 0x02 | 0x20 | 0x4000;
    pub const MADV_HUGEPAGE: i32 = 14;
}

/// Map `len` bytes of zeroed, huge-page-advised memory (2 MB aligned).
/// Returns null on failure.
#[cfg(target_os = "linux")]
pub fn map_huge(len: usize) -> *mut u8 {
    use sys::*;
    // SAFETY: plain anonymous mappings; the over-allocation is trimmed so
    // the returned range starts on a 2 MB boundary.
    unsafe {
        let total = len + HUGE;
        let p = mmap(std::ptr::null_mut(), total, PROT_RW, MAP_PRIVATE_ANON_NORESERVE, -1, 0);
        if p as isize == -1 {
            return std::ptr::null_mut();
        }
        let a = ((p as usize + HUGE - 1) & !(HUGE - 1)) as *mut u8;
        let head = a as usize - p as usize;
        if head > 0 {
            munmap(p, head);
        }
        let tail = total - head - len;
        if tail > 0 {
            munmap(a.add(len), tail);
        }
        madvise(a, len, MADV_HUGEPAGE);
        a
    }
}

#[cfg(target_os = "linux")]
fn unmap(p: *mut u8, len: usize) {
    // SAFETY: `p..p+len` is exactly a range returned by map_huge.
    unsafe { sys::munmap(p, len) };
}

#[cfg(not(target_os = "linux"))]
pub fn map_huge(len: usize) -> *mut u8 {
    // SAFETY: plain zeroed allocation, never freed through System.
    unsafe { System.alloc_zeroed(Layout::from_size_align(len, 4096).unwrap()) }
}
#[cfg(not(target_os = "linux"))]
fn unmap(p: *mut u8, len: usize) {
    unsafe { System.dealloc(p, Layout::from_size_align(len, 4096).unwrap()) }
}

pub struct Alloc;

/// Big allocations are rounded up to a power of two (virtual size only:
/// untouched pages cost nothing) so that a freed one can serve any later
/// request of its class. Freed big buffers are kept per class and handed out
/// again instead of being unmapped and faulted in fresh (dedup tables,
/// grouping scratch and output columns of one stage feed the next).
const CLASSES: usize = 48;
static FREE: [Mutex<Vec<usize>>; CLASSES] = [const { Mutex::new(Vec::new()) }; CLASSES];

#[inline(always)]
fn class(size: usize) -> usize {
    size.next_power_of_two().trailing_zeros() as usize
}

unsafe fn big_alloc(size: usize, zero: bool) -> *mut u8 {
    let c = class(size);
    if let Some(p) = FREE[c].lock().ok().and_then(|mut f| f.pop()) {
        if zero {
            std::ptr::write_bytes(p as *mut u8, 0, size);
        }
        return p as *mut u8;
    }
    map_huge(1usize << c) // fresh anonymous memory is zero
}

unsafe fn big_free(p: *mut u8, size: usize) {
    let c = class(size);
    match FREE[c].lock() {
        Ok(mut f) => f.push(p as usize),
        Err(_) => unmap(p, 1 << c),
    }
}

unsafe impl GlobalAlloc for Alloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= HUGE {
            big_alloc(l.size(), false)
        } else {
            System.alloc(l)
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= HUGE {
            big_alloc(l.size(), true)
        } else {
            System.alloc_zeroed(l)
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if l.size() >= BIG && l.align() <= HUGE {
            big_free(p, l.size())
        } else {
            System.dealloc(p, l)
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        if l.size() < BIG && new_size < BIG {
            return System.realloc(p, l, new_size);
        }
        // Still in the same big class: the mapping already has room.
        if l.size() >= BIG && new_size >= BIG && l.align() <= HUGE && class(l.size()) == class(new_size) {
            return p;
        }
        let nl = Layout::from_size_align_unchecked(new_size, l.align());
        let q = self.alloc(nl);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
            self.dealloc(p, l);
        }
        q
    }
}

/// Rows appended to `n` partitions, each a chain of arena blocks.
pub struct Parts<T: 'static> {
    /// Rows pushed so far.
    pub count: usize,
    open: Vec<*mut T>,
    len: Vec<u32>,
    /// Full blocks (as raw pointers: rows may still be written through
    /// pointers from `push_ptr` until `finish`).
    full: Vec<Vec<*mut T>>,
    arena: Blocks<T>,
}

unsafe impl<T: 'static> Send for Parts<T> {}

impl<T: Copy + 'static> Parts<T> {
    pub fn new(n: usize, block: usize) -> Self {
        Parts {
            count: 0,
            open: vec![std::ptr::null_mut(); n],
            len: vec![0; n],
            full: (0..n).map(|_| Vec::new()).collect(),
            arena: Blocks::new(block),
        }
    }
    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        self.push_ptr(p, x);
    }
    /// `push`, returning where the row went (blocks never move).
    #[inline(always)]
    pub fn push_ptr(&mut self, p: usize, x: T) -> *mut T {
        let block = self.arena.block;
        // SAFETY: p < n (bounds-checked on `open`); a block is replaced as
        // soon as it fills, so the write is in bounds.
        unsafe {
            let o = &mut self.open[p];
            let n = self.len.get_unchecked_mut(p);
            if o.is_null() {
                *o = self.arena.take();
            }
            let at = o.add(*n as usize);
            at.write(x);
            *n += 1;
            self.count += 1;
            if *n as usize == block {
                self.full[p].push(*o);
                *o = std::ptr::null_mut();
                *n = 0;
            }
            at
        }
    }
    /// The blocks of each partition.
    pub fn finish(self) -> Vec<Vec<&'static [T]>> {
        let block = self.arena.block;
        let mut out: Vec<Vec<&'static [T]>> = Vec::with_capacity(self.full.len());
        for (p, f) in self.full.into_iter().enumerate() {
            // SAFETY: full blocks hold `block` written rows; the first len
            // rows of the open block are written. Blocks are never freed.
            let mut v: Vec<&'static [T]> = f.into_iter().map(|b| unsafe { std::slice::from_raw_parts(b as *const T, block) }).collect();
            if self.len[p] > 0 {
                v.push(unsafe { std::slice::from_raw_parts(self.open[p], self.len[p] as usize) });
            }
            out.push(v);
        }
        out
    }
}

/// Every row block is one `POOL_BLOCK`-byte slot. Slots freed by one stage
/// (event blocks after dedup, survivor and click blocks once a person
/// partition is grouped) are reused by the next (survivor blocks, user rows),
/// so later stages write into memory that is already mapped instead of
/// faulting in and zeroing fresh pages.
///
/// One pool per worker thread (`par::tid`), so a worker puts and takes its
/// own slots without contention; a worker whose pool is empty tries the
/// others (non-blocking) before mapping fresh memory.
///
/// Small on purpose: each worker keeps one open block per event bucket and
/// per person partition, and with 6 KB slots those open blocks (and the
/// fresh pages behind them) stay small enough to remain cache-resident.
/// Measured on 20M events: 80 KB slots 1326 ms, 12 KB 1271, 6 KB 1222,
/// 3 KB 1232 (median total, 8 threads).
pub const POOL_BLOCK: usize = 6144;
const NPOOL: usize = 64;
static POOLS: [Mutex<Vec<usize>>; NPOOL] = [const { Mutex::new(Vec::new()) }; NPOOL];
/// Slots in all pools (approximate): skip the scan when there are none.
static POOLED: AtomicUsize = AtomicUsize::new(0);

/// Return a slot (the start of a block) to this worker's pool.
pub fn pool_put(p: *const u8) {
    POOLS[crate::par::tid() % NPOOL].lock().unwrap().push(p as usize);
    POOLED.fetch_add(1, Relaxed);
}

fn pool_take() -> Option<usize> {
    if POOLED.load(Relaxed) == 0 {
        return None;
    }
    let t = crate::par::tid() % NPOOL;
    for i in 0..NPOOL {
        let q = &POOLS[(t + i) % NPOOL];
        let got = if i == 0 { q.lock().ok().and_then(|mut g| g.pop()) } else { q.try_lock().ok().and_then(|mut g| g.pop()) };
        if let Some(x) = got {
            POOLED.fetch_sub(1, Relaxed);
            return Some(x);
        }
    }
    None
}

/// A bump allocator of fixed-size row blocks carved from huge-page regions
/// (or reused pool slots); never unmapped (the process exits right after
/// writing its outputs).
pub struct Blocks<T> {
    cur: *mut u8,
    left: usize,
    block: usize,
    _t: std::marker::PhantomData<T>,
}

unsafe impl<T> Send for Blocks<T> {}

const REGION: usize = 64 << 20;

impl<T> Blocks<T> {
    /// Blocks of `block` rows; must fit one pool slot.
    pub fn new(block: usize) -> Self {
        assert!(block * std::mem::size_of::<T>() <= POOL_BLOCK);
        Blocks { cur: std::ptr::null_mut(), left: 0, block, _t: std::marker::PhantomData }
    }
    #[inline]
    pub fn take(&mut self) -> *mut T {
        if let Some(p) = pool_take() {
            return p as *mut T;
        }
        if self.left == 0 {
            let per = REGION / POOL_BLOCK;
            let p = map_huge(per * POOL_BLOCK);
            assert!(!p.is_null(), "out of memory");
            self.cur = p;
            self.left = per;
        }
        let b = self.cur;
        // SAFETY: stays inside the current region.
        self.cur = unsafe { self.cur.add(POOL_BLOCK) };
        self.left -= 1;
        b as *mut T
    }
}
