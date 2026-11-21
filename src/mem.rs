//! Raw memory helpers: file mapping, anonymous huge-page backed regions,
//! prefetch, and a per-thread block store used for partitioned scatter.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}
const PROT_READ: i32 = 1;
const PROT_WRITE: i32 = 2;
const MAP_PRIVATE: i32 = 2;
const MAP_ANON: i32 = 0x20;
const MAP_NORESERVE: i32 = 0x4000;
const MADV_SEQUENTIAL: i32 = 2;
const MADV_HUGEPAGE: i32 = 14;

/// Map a whole file read-only for the life of the process.
pub fn map_file(path: &Path) -> io::Result<&'static [u8]> {
    let f = File::open(path)?;
    let len = f.metadata()?.len() as usize;
    if len == 0 {
        return Ok(&[]);
    }
    // SAFETY: read-only private mapping of a file nobody modifies; leaked.
    unsafe {
        let p = mmap(std::ptr::null_mut(), len, PROT_READ, MAP_PRIVATE, f.as_raw_fd(), 0);
        if p as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        madvise(p, len, MADV_SEQUENTIAL);
        Ok(std::slice::from_raw_parts(p, len))
    }
}

/// Zeroed anonymous memory, transparent-huge-page backed where possible.
/// Never freed (the process exits right after writing its output).
pub fn alloc_zeroed(bytes: usize) -> *mut u8 {
    let bytes = (bytes.max(1) + (2 << 20) - 1) & !((2 << 20) - 1);
    // SAFETY: plain anonymous mapping.
    unsafe {
        let p = mmap(std::ptr::null_mut(), bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
        assert!(p as isize != -1, "mmap of {bytes} bytes failed");
        madvise(p, bytes, MADV_HUGEPAGE);
        p
    }
}

pub fn alloc_slice<T>(n: usize) -> &'static mut [T] {
    let p = alloc_zeroed(n * std::mem::size_of::<T>()) as *mut T;
    // SAFETY: zeroed memory; only used for plain-old-data T where all-zero is valid.
    unsafe { std::slice::from_raw_parts_mut(p, n) }
}

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

/// Bump allocator handing out fixed-size blocks from big huge-page regions.
pub struct Arena {
    cur: *mut u8,
    left: usize,
    region: usize,
}
unsafe impl Send for Arena {}

impl Arena {
    pub fn new(region: usize) -> Self {
        Arena { cur: std::ptr::null_mut(), left: 0, region: region.max(4 << 20) }
    }
    #[inline]
    pub fn take(&mut self, bytes: usize) -> *mut u8 {
        if self.left < bytes {
            let r = self.region.max(bytes);
            self.cur = alloc_zeroed(r);
            self.left = (r + (2 << 20) - 1) & !((2 << 20) - 1);
        }
        let p = self.cur;
        // SAFETY: inside the current region.
        self.cur = unsafe { self.cur.add(bytes) };
        self.left -= bytes;
        p
    }
}

/// Per-thread scatter into `parts` partitions, rows of type T, in blocks of
/// `B` rows. Each partition keeps a list of full blocks and one open block.
pub struct Scatter<T: Copy, const B: usize> {
    arena: Arena,
    open: Vec<*mut T>,
    fill: Vec<u32>,
    pub full: Vec<Vec<*mut T>>,
    pub rows: usize,
    shift: u32,
}
unsafe impl<T: Copy, const B: usize> Send for Scatter<T, B> {}
unsafe impl<T: Copy, const B: usize> Sync for Scatter<T, B> {}

impl<T: Copy, const B: usize> Scatter<T, B> {
    /// `bits`: log2 of the partition count; the partition is the top `bits`
    /// bits of the hash passed to `push`.
    pub fn new(bits: u32, region: usize) -> Self {
        let parts = 1usize << bits;
        Scatter {
            arena: Arena::new(region),
            open: vec![std::ptr::null_mut(); parts],
            fill: vec![B as u32; parts],
            full: (0..parts).map(|_| Vec::new()).collect(),
            rows: 0,
            shift: 64 - bits,
        }
    }
    #[inline(always)]
    pub fn part_of(&self, h: u64) -> usize {
        if self.shift == 64 {
            0
        } else {
            (h >> self.shift) as usize
        }
    }
    #[inline(always)]
    pub fn push(&mut self, h: u64, row: T) {
        let p = self.part_of(h);
        self.push_to(p, row);
    }
    #[inline(always)]
    pub fn push_to(&mut self, p: usize, row: T) {
        let f = self.fill[p];
        if f as usize == B {
            self.new_block(p);
        }
        let f = self.fill[p];
        // SAFETY: f < B, the open block has room for B rows.
        unsafe { self.open[p].add(f as usize).write(row) };
        self.fill[p] = f + 1;
        self.rows += 1;
    }
    #[cold]
    fn new_block(&mut self, p: usize) {
        if !self.open[p].is_null() {
            self.full[p].push(self.open[p]);
        }
        self.open[p] = self.arena.take(B * std::mem::size_of::<T>()) as *mut T;
        self.fill[p] = 0;
    }
    /// All rows of partition `p` as slices (full blocks, then the open one).
    pub fn slices(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        let open = if self.open[p].is_null() {
            &[][..]
        } else {
            // SAFETY: the first fill[p] rows of the open block were written.
            unsafe { std::slice::from_raw_parts(self.open[p] as *const T, self.fill[p] as usize) }
        };
        self.full[p]
            .iter()
            // SAFETY: full blocks hold B written rows.
            .map(|&b| unsafe { std::slice::from_raw_parts(b as *const T, B) })
            .chain(std::iter::once(open))
    }
    pub fn count(&self, p: usize) -> usize {
        self.full[p].len() * B + if self.open[p].is_null() { 0 } else { self.fill[p] as usize }
    }
}
