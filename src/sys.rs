//! Small OS and hashing helpers (replaces memmap2, memchr, hashbrown and
//! xxhash): read-only file mappings, madvise, memchr, and hashers for the
//! std collections.

use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::io;

#[cfg(target_os = "linux")]
mod ffi {
    extern "C" {
        pub fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
        pub fn munmap(addr: *mut u8, len: usize) -> i32;
        pub fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    }
}

pub const MADV_SEQUENTIAL: i32 = 2;
pub const MADV_WILLNEED: i32 = 3;
pub const MADV_DONTNEED: i32 = 4;
pub const MADV_POPULATE_READ: i32 = 22;

/// Map all of `file` read-only (private). Never unmapped implicitly; see
/// `unmap`. An empty file maps to an empty slice.
pub fn map_file(file: &File) -> io::Result<&'static [u8]> {
    let len = file.metadata()?.len() as usize;
    if len == 0 {
        return Ok(&[]);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: read-only private mapping of a file nobody modifies while we run.
        unsafe {
            let p = ffi::mmap(std::ptr::null_mut(), len, 1, 2, file.as_raw_fd(), 0);
            if p as isize == -1 {
                return Err(io::Error::last_os_error());
            }
            ffi::madvise(p, len, MADV_SEQUENTIAL);
            Ok(std::slice::from_raw_parts(p, len))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::io::Read;
        let mut v = Vec::with_capacity(len);
        let mut f = file;
        f.read_to_end(&mut v)?;
        Ok(Box::leak(v.into_boxed_slice()))
    }
}

/// `madvise` on whole pages inside `data` (rounded inward). A hint: failures
/// are ignored and results never depend on it.
pub fn advise(data: &[u8], advice: i32) -> bool {
    const PAGE: usize = 4096;
    let a = (data.as_ptr() as usize + PAGE - 1) & !(PAGE - 1);
    let b = (data.as_ptr() as usize + data.len()) & !(PAGE - 1);
    if b <= a {
        return true;
    }
    #[cfg(target_os = "linux")]
    // SAFETY: [a, b) lies inside a live mapping.
    unsafe {
        return ffi::madvise(a as *mut u8, b - a, advice) == 0;
    }
    #[allow(unreachable_code)]
    {
        let _ = advice;
        false
    }
}

/// Unmap a whole mapping returned by `map_file`. Nothing may use it after.
pub unsafe fn unmap(data: &'static [u8]) {
    #[cfg(target_os = "linux")]
    if !data.is_empty() {
        ffi::munmap(data.as_ptr() as *mut u8, data.len());
    }
    #[cfg(not(target_os = "linux"))]
    let _ = data;
}

/// First index of `c` in `s`.
#[inline]
pub fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let n = s.len();
        let p = s.as_ptr();
        let needle = _mm_set1_epi8(c as i8);
        let mut i = 0;
        while i + 16 <= n {
            let v = _mm_loadu_si128(p.add(i) as *const __m128i);
            let m = _mm_movemask_epi8(_mm_cmpeq_epi8(v, needle)) as u32;
            if m != 0 {
                return Some(i + m.trailing_zeros() as usize);
            }
            i += 16;
        }
        return s[i..].iter().position(|&x| x == c).map(|j| i + j);
    }
    #[allow(unreachable_code)]
    s.iter().position(|&x| x == c)
}

/// Every index of `c` in `s`.
pub fn memchr_iter(c: u8, s: &[u8]) -> impl Iterator<Item = usize> + '_ {
    let mut at = 0;
    std::iter::from_fn(move || {
        let i = memchr(c, &s[at..])? + at;
        at = i + 1;
        Some(i)
    })
}

#[inline(always)]
pub fn mix64(x: u64) -> u64 {
    let x = (x ^ (x >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    let x = (x ^ (x >> 32)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    x ^ (x >> 32)
}

/// A fast 64-bit hash of a byte string (all bytes count).
#[inline]
pub fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = 0x9E37_79B9_7F4A_7C15u64 ^ (b.len() as u64).wrapping_mul(0xA076_1D64_78BD_642F);
    let mut c = b.chunks_exact(8);
    for w in &mut c {
        h = (h ^ u64::from_le_bytes(w.try_into().unwrap())).wrapping_mul(0xE703_7ED1_A0B4_28DB).rotate_left(31);
    }
    let r = c.remainder();
    let mut t = 0u64;
    for (i, &x) in r.iter().enumerate() {
        t |= (x as u64) << (8 * i);
    }
    mix64(h ^ t)
}

/// Hasher for the std collections: bytes via `hash_bytes`, integers mixed.
#[derive(Default)]
pub struct FastHasher(u64);

impl Hasher for FastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        mix64(self.0)
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0 = self.0.rotate_left(17) ^ hash_bytes(bytes);
    }
    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(17) ^ x).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    #[inline]
    fn write_usize(&mut self, x: usize) {
        self.write_u64(x as u64)
    }
    #[inline]
    fn write_u32(&mut self, x: u32) {
        self.write_u64(x as u64)
    }
    #[inline]
    fn write_u8(&mut self, x: u8) {
        self.write_u64(x as u64)
    }
}

pub type Fast = BuildHasherDefault<FastHasher>;
pub type FastMap<K, V> = std::collections::HashMap<K, V, Fast>;
pub type FastSet<K> = std::collections::HashSet<K, Fast>;
