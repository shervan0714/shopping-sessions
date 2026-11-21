//! Input discovery, memory mapping, chunking, and the small lookup tables
//! (`products.csv`, `campaigns.csv`, `manifest.json`).

use crate::csv;
use crate::key::StaticMap;
use crate::ts;
use hashbrown::HashMap;
use memmap2::{Advice, Mmap};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

/// Map a whole file and leak the mapping: inputs stay mapped (read-only, in
/// the page cache) for the life of the process, so every parsed string can be
/// a `&'static [u8]` slice of the file with no copying.
pub fn map_file(path: &Path) -> io::Result<&'static [u8]> {
    let file = File::open(path)?;
    if file.metadata()?.len() == 0 {
        return Ok(&[]);
    }
    // SAFETY: /data is read-only and nothing modifies the inputs while we run.
    let map = unsafe { Mmap::map(&file)? };
    let _ = map.advise(Advice::Sequential);
    let map: &'static Mmap = Box::leak(Box::new(map));
    Ok(&map[..])
}

pub const WILLNEED: i32 = 3;
pub const DONTNEED: i32 = 4;

/// `madvise` over the whole pages inside `data` (part of a leaked, read-only
/// file mapping). Rounds inward so neighbouring chunks' pages are untouched.
/// WILLNEED starts asynchronous readahead; DONTNEED drops clean file pages
/// from our mapping (they refault transparently if read again). Both are
/// hints: results never depend on them.
pub fn advise(data: &'static [u8], advice: i32) {
    const PAGE: usize = 4096;
    let a = (data.as_ptr() as usize + PAGE - 1) & !(PAGE - 1);
    let b = (data.as_ptr() as usize + data.len()) & !(PAGE - 1);
    if b > a {
        // SAFETY: [a, b) lies inside a live mapping; neither advice writes.
        unsafe { sys_madvise(a as *mut u8, b - a, advice) };
    }
}

#[cfg(target_os = "linux")]
unsafe fn sys_madvise(addr: *mut u8, len: usize, advice: i32) {
    extern "C" {
        fn madvise(addr: *mut std::ffi::c_void, len: usize, advice: i32) -> i32;
    }
    madvise(addr as *mut std::ffi::c_void, len, advice);
}

#[cfg(not(target_os = "linux"))]
unsafe fn sys_madvise(_addr: *mut u8, _len: usize, _advice: i32) {}

/// Every `*.csv` regular file (or symlink to one) directly in `dir`, sorted.
/// A missing directory holds no rows.
pub fn list_csv(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            eprintln!("warning: {} does not exist; treating it as empty", dir.display());
            return Ok(out);
        }
        Err(e) => return Err(e),
    };
    for entry in entries {
        let path = entry?.path();
        let is_csv = path.extension().map_or(false, |e| e == "csv");
        if is_csv && path.is_file() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

/// The rows of a file: everything after its first line (the header).
pub fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &[], // empty file, or a header with no newline: no rows
    }
}

/// Split a file body into pieces of roughly `target` bytes, each ending just
/// after a `\n` (or at the end of the body), so no line is cut.
pub fn chunks(body: &'static [u8], target: usize, out: &mut Vec<&'static [u8]>) {
    let mut start = 0usize;
    while start < body.len() {
        let mut end = (start + target).min(body.len());
        if end < body.len() {
            end = match memchr::memchr(b'\n', &body[end..]) {
                Some(i) => end + i + 1,
                None => body.len(),
            };
        }
        out.push(&body[start..end]);
        start = end;
    }
}

pub struct Lookups {
    /// product_id -> category index
    pub products: StaticMap<u16>,
    /// category names; includes `unknown`
    pub categories: Vec<&'static [u8]>,
    pub unknown_category: u16,
    /// rank of each category in byte-wise order (for top_category ties)
    pub category_rank: Vec<u32>,
    /// campaign_id -> campaign index
    pub campaigns: StaticMap<u32>,
    pub campaign_ids: Vec<&'static [u8]>,
    pub campaign_channels: Vec<&'static [u8]>,
}

pub fn load_lookups(data_dir: &Path) -> io::Result<Lookups> {
    let products_data = map_file(&data_dir.join("products.csv"))?;
    let mut products: Vec<(&'static [u8], u16)> = Vec::new();
    let mut products_seen: hashbrown::HashSet<&'static [u8]> = hashbrown::HashSet::new();
    let mut cat_index: HashMap<&'static [u8], u16> = HashMap::new();
    let mut categories: Vec<&'static [u8]> = Vec::new();
    let mut intern_cat = |name: &'static [u8], cats: &mut Vec<&'static [u8]>| -> u16 {
        *cat_index.entry(name).or_insert_with(|| {
            cats.push(name);
            assert!(cats.len() < u16::MAX as usize, "too many categories");
            (cats.len() - 1) as u16
        })
    };
    csv::for_each_line(body(products_data), |line| {
        let mut f = [&b""[..]; 2];
        if csv::split(line, &mut f) && !f[0].is_empty() {
            let c = intern_cat(f[1], &mut categories);
            if products_seen.insert(f[0]) {
                products.push((f[0], c));
            }
        }
    });
    let unknown_category = intern_cat(b"unknown", &mut categories);
    let mut order: Vec<u32> = (0..categories.len() as u32).collect();
    order.sort_by(|&a, &b| categories[a as usize].cmp(categories[b as usize]));
    let mut category_rank = vec![0u32; categories.len()];
    for (rank, &c) in order.iter().enumerate() {
        category_rank[c as usize] = rank as u32;
    }

    let campaigns_data = map_file(&data_dir.join("campaigns.csv"))?;
    let mut campaigns = HashMap::new();
    let mut campaign_ids = Vec::new();
    let mut campaign_channels = Vec::new();
    csv::for_each_line(body(campaigns_data), |line| {
        let mut f = [&b""[..]; 2];
        if csv::split(line, &mut f) && !f[0].is_empty() && !campaigns.contains_key(f[0]) {
            campaigns.insert(f[0], campaign_ids.len() as u32);
            campaign_ids.push(f[0]);
            campaign_channels.push(f[1]);
        }
    });

    Ok(Lookups {
        products: StaticMap::build(&products),
        categories,
        unknown_category,
        category_rank,
        campaigns: StaticMap::build(&campaign_ids.iter().enumerate().map(|(i, &k)| (k, i as u32)).collect::<Vec<_>>()),
        campaign_ids,
        campaign_channels,
    })
}

/// `window_start` and `window_end` from manifest.json, in epoch milliseconds.
pub fn load_manifest(data_dir: &Path) -> io::Result<(i64, i64)> {
    let text = fs::read_to_string(data_dir.join("manifest.json"))?;
    let get = |key: &str| -> io::Result<i64> {
        let bad = || io::Error::new(io::ErrorKind::InvalidData, format!("manifest.json: bad {key}"));
        let at = text.find(&format!("\"{key}\"")).ok_or_else(bad)?;
        let rest = &text[at + key.len() + 2..];
        let colon = rest.find(':').ok_or_else(bad)?;
        let rest = &rest[colon + 1..];
        let open = rest.find('"').ok_or_else(bad)?;
        let rest = &rest[open + 1..];
        let close = rest.find('"').ok_or_else(bad)?;
        ts::parse(rest[..close].as_bytes()).ok_or_else(bad)
    };
    Ok((get("window_start")?, get("window_end")?))
}
