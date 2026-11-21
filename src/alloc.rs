//! Memory helpers: big buffers on transparent huge pages.
//!
//! Page faults dominated the kernel time (a 4 KB fault per page, zeroed, for
//! gigabytes of row buffers). Allocations of `BIG` bytes or more get their own
//! anonymous mapping advised `MADV_HUGEPAGE` (the scorer's and WSL's THP mode
//! is usually `madvise`), so one fault maps 2 MB. Everything smaller goes to
//! the system allocator. Both are hints only: without THP it is plain mmap.

use std::alloc::{GlobalAlloc, Layout, System};

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

#[inline(always)]
fn rounded(size: usize) -> usize {
    (size + 4095) & !4095
}

unsafe impl GlobalAlloc for Alloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= HUGE {
            map_huge(rounded(l.size()))
        } else {
            System.alloc(l)
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= HUGE {
            map_huge(rounded(l.size())) // fresh anonymous memory is zero
        } else {
            System.alloc_zeroed(l)
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if l.size() >= BIG && l.align() <= HUGE {
            unmap(p, rounded(l.size()))
        } else {
            System.dealloc(p, l)
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        if l.size() < BIG && new_size < BIG {
            return System.realloc(p, l, new_size);
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
    open: Vec<*mut T>,
    len: Vec<u32>,
    full: Vec<Vec<&'static [T]>>,
    arena: Blocks<T>,
}

unsafe impl<T: 'static> Send for Parts<T> {}

impl<T: Copy + 'static> Parts<T> {
    pub fn new(n: usize, block: usize) -> Self {
        Parts {
            open: vec![std::ptr::null_mut(); n],
            len: vec![0; n],
            full: (0..n).map(|_| Vec::new()).collect(),
            arena: Blocks::new(block),
        }
    }
    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        let block = self.arena.block;
        // SAFETY: p < n (bounds-checked on `open`); a block is replaced as
        // soon as it fills, so the write is in bounds.
        unsafe {
            let o = &mut self.open[p];
            let n = self.len.get_unchecked_mut(p);
            if o.is_null() {
                *o = self.arena.take();
            }
            o.add(*n as usize).write(x);
            *n += 1;
            if *n as usize == block {
                self.full[p].push(std::slice::from_raw_parts(*o, block));
                *o = std::ptr::null_mut();
                *n = 0;
            }
        }
    }
    /// The blocks of each partition.
    pub fn finish(self) -> Vec<Vec<&'static [T]>> {
        let mut full = self.full;
        for p in 0..full.len() {
            if self.len[p] > 0 {
                // SAFETY: the first len rows of the open block are written.
                full[p].push(unsafe { std::slice::from_raw_parts(self.open[p], self.len[p] as usize) });
            }
        }
        full
    }
}

/// A bump allocator of fixed-size row blocks carved from huge-page regions;
/// never freed (the process exits right after writing its outputs).
pub struct Blocks<T> {
    cur: *mut T,
    left: usize,
    block: usize,
}

unsafe impl<T> Send for Blocks<T> {}

const REGION: usize = 64 << 20;

impl<T> Blocks<T> {
    pub fn new(block: usize) -> Self {
        Blocks { cur: std::ptr::null_mut(), left: 0, block }
    }
    #[inline]
    pub fn take(&mut self) -> *mut T {
        if self.left == 0 {
            let per = (REGION / (self.block * std::mem::size_of::<T>())).max(1);
            let p = map_huge(per * self.block * std::mem::size_of::<T>());
            assert!(!p.is_null(), "out of memory");
            self.cur = p as *mut T;
            self.left = per;
        }
        let b = self.cur;
        // SAFETY: stays inside the current region.
        self.cur = unsafe { self.cur.add(self.block) };
        self.left -= 1;
        b
    }
}
