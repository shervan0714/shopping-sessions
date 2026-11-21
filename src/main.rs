//! Shopping Sessions pipeline. See README.md for the architecture and
//! rules.md for the specification. Stages:
//!
//!  1. map every input file, cut the bodies into ~4 MB line-aligned chunks;
//!  2. parse all chunks in parallel: events are validated and hash-partitioned
//!     on event_id, clicks validated, links collected; user IDs interned;
//!  3. union-find over links; name each person by its byte-wise smallest ID;
//!  4. dedup each event partition in parallel (sort by hash, ingest_ts);
//!  5. group surviving events and valid clicks by person (counting sort);
//!  6. per person, in parallel: sort, sessionize, credit, assist, aggregate;
//!  7. write Parquet in parallel.

mod csv;
mod input;
mod intern;
mod key;
mod output;
mod parse;
mod scan;
mod ts;

use input::Lookups;
use parquet::data_type::ByteArray;
use parse::{ClickRow, EvRow, Worker, EV_PARTS, KIND_PURCHASE};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;
use ts::{DAY, MINUTE};

type Error = Box<dyn std::error::Error + Send + Sync>;

const GAP: i64 = 30 * MINUTE;
const WEEK: i64 = 7 * DAY;
const CHUNK: usize = 4 << 20;
const NO_CATEGORY: u16 = u16::MAX;

/// A surviving event, tagged with its person, before grouping.
#[derive(Clone, Copy)]
struct Surv {
    ts: i64,
    person: u32,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A surviving event, grouped by person.
#[derive(Clone, Copy)]
struct Ev {
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A valid click, grouped by person.
#[derive(Clone, Copy)]
struct Click {
    ts: i64,
    cid: usize,
    cid_len: u32,
    campaign: u32,
}

impl Click {
    #[inline(always)]
    fn cid(&self) -> &'static [u8] {
        // SAFETY: points into a leaked, never-unmapped input mapping.
        unsafe { std::slice::from_raw_parts(self.cid as *const u8, self.cid_len as usize) }
    }
}

/// A raw pointer that may be shared between threads; every user writes or
/// borrows disjoint ranges only.
#[derive(Clone, Copy)]
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}
impl<T> SendPtr<T> {
    #[inline(always)]
    fn get(self) -> *mut T {
        self.0
    }
}

fn log(t0: Instant, what: &str) {
    eprintln!("[{:8.3}s] {what}", t0.elapsed().as_secs_f64());
}

/// Make glibc serve every allocation of 64 KB or more with its own mmap and
/// unmap it on free (a fixed threshold also disables glibc's dynamic raising
/// of it). Big transient buffers then really leave the process when dropped.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn tune_malloc() {
    extern "C" {
        fn mallopt(param: i32, value: i32) -> i32;
    }
    const M_MMAP_THRESHOLD: i32 = -3;
    // SAFETY: plain libc call made before any other thread exists.
    unsafe { mallopt(M_MMAP_THRESHOLD, 64 << 10) };
}
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn tune_malloc() {}

fn main() {
    tune_malloc();
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (PathBuf::from(env("DATA_DIR", "/data")), PathBuf::from(env("OUTPUT_DIR", "/output")))
    };
    let ncpu = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().expect("thread pool");
    match run(&data_dir, &out_dir, t0) {
        Ok(()) => {
            log(t0, "done");
            // Skip freeing gigabytes of heap and unmapping inputs one by one.
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
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
        // Path halving: amortised near-constant time, no recursion, so a
        // chain of any length is fine.
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

fn run(data_dir: &Path, out_dir: &Path, t0: Instant) -> Result<(), Error> {
    let (ws, we) = input::load_manifest(data_dir)?;
    let lookups = input::load_lookups(data_dir)?;

    // ---- 1. map and chunk -------------------------------------------------
    let mut files: Vec<(u8, PathBuf)> = Vec::new();
    for (kind, dir) in [(0u8, "events"), (1, "clicks"), (2, "links")] {
        for p in input::list_csv(&data_dir.join(dir))? {
            files.push((kind, p));
        }
    }
    let mapped: Vec<(u8, &'static [u8])> = files
        .par_iter()
        .map(|(k, p)| input::map_file(p).map(|d| (*k, d)))
        .collect::<Result<_, _>>()?;
    let mut tasks: Vec<(u8, &'static [u8])> = Vec::new();
    let mut total_bytes = 0usize;
    for &(k, data) in &mapped {
        total_bytes += data.len();
        let mut cs = Vec::new();
        input::chunks(input::body(data), CHUNK, &mut cs);
        tasks.extend(cs.into_iter().map(|c| (k, c)));
    }


    // Releasing parsed chunks (MADV_DONTNEED) costs TLB shootdowns, so it is
    // only worth it when the inputs plus our working set (~1.2x the input
    // size) would crowd the memory limit (MEM_GB, default 24).
    let mem_bytes = std::env::var("MEM_GB").ok().and_then(|v| v.trim().parse::<f64>().ok()).unwrap_or(24.0) * 1e9;
    let release_chunks = (total_bytes as f64) * 2.2 > mem_bytes * 0.8;
    log(t0, &format!("mapped {} files, {:.1} MB, {} chunks, release chunks: {release_chunks}", files.len(), total_bytes as f64 / 1e6, tasks.len()));
    // Leaked so the readahead thread can follow the parsers' progress.
    let tasks: &'static [(u8, &'static [u8])] = Box::leak(tasks.into_boxed_slice());
    let next: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));

    // Background readahead: hint the kernel to read chunks ahead of the
    // parsers, but at most RA_AHEAD chunks (~512 MB) ahead, so the inputs'
    // page-cache footprint stays bounded. A hint only; never needed for
    // correctness.
    if std::env::var_os("SS_NO_READAHEAD").is_none() {
        const RA_AHEAD: usize = 128;
        std::thread::spawn(move || {
            for (i, &(_, chunk)) in tasks.iter().enumerate() {
                while i > next.load(Relaxed) + RA_AHEAD {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                input::advise(chunk, input::WILLNEED);
            }
        });
    }

    // ---- 2. parse ---------------------------------------------------------
    let interner = intern::Interner::new();
    let pctx = parse::Ctx { lookups: &lookups, interner: &interner, ws, we };
    let nthreads = rayon::current_num_threads();
    // Reserve generously so the per-worker vectors never reallocate and copy
    // (an event line is at least ~60 bytes; unused capacity is never touched,
    // so it costs address space only).
    let cl_bytes: usize = mapped.iter().filter(|m| m.0 == 1).map(|m| m.1.len()).sum();
    let workers: Vec<Worker> = (0..nthreads)
        .into_par_iter()
        .map(|_| {
            let mut w = Worker::new();
            w.clicks.reserve(cl_bytes / 30 * 2 / nthreads + 1024);
            loop {
                let i = next.fetch_add(1, Relaxed);
                let Some(&(k, chunk)) = tasks.get(i) else { break };
                match k {
                    0 => parse::events_chunk(chunk, &pctx, &mut w),
                    1 => parse::clicks_chunk(chunk, &pctx, &mut w),
                    _ => parse::links_chunk(chunk, &pctx, &mut w),
                }
                // Done with this chunk's pages: let the kernel drop them from
                // our mapping now (clean, file-backed: anything referenced
                // later, such as an event_id or click_id, refaults from the
                // page cache or the file).
                if release_chunks {
                    input::advise(chunk, input::DONTNEED);
                }
            }
            w
        })
        .collect();
    let n_ev_rows: usize = workers.iter().map(|w| w.n_events).sum();
    let n_clicks: usize = workers.iter().map(|w| w.clicks.len()).sum();
    let n_links: usize = workers.iter().map(|w| w.links.len()).sum();
    log(t0, &format!("parsed: {n_ev_rows} event rows, {n_clicks} valid clicks, {n_links} links"));

    // ---- 3. people ----------------------------------------------------------
    let dense = interner.finish();
    let n_ids = dense.keys.len();
    let mut uf = UnionFind::new(n_ids);
    for w in &workers {
        for &(a, b) in &w.links {
            uf.union(dense.index(a), dense.index(b));
        }
    }
    let roots: Vec<u32> = (0..n_ids as u32).map(|i| uf.find(i)).collect();
    drop(uf);
    // Byte-wise smallest ID of each component.
    let mut best = vec![u32::MAX; n_ids];
    for i in 0..n_ids {
        let r = roots[i] as usize;
        let b = best[r];
        if b == u32::MAX || dense.keys[i] < dense.keys[b as usize] {
            best[r] = i as u32;
        }
    }
    let mut person_of_root = vec![u32::MAX; n_ids];
    let mut names: Vec<&'static [u8]> = Vec::new();
    let mut person_of = vec![0u32; n_ids];
    for i in 0..n_ids {
        let r = roots[i] as usize;
        if person_of_root[r] == u32::MAX {
            person_of_root[r] = names.len() as u32;
            names.push(dense.keys[best[r] as usize]);
        }
        person_of[i] = person_of_root[r];
    }
    drop((roots, best, person_of_root));
    let np = names.len();
    let person = |packed: u32| person_of[dense.index(packed) as usize];
    log(t0, &format!("people: {n_ids} ids, {np} persons"));

    // ---- 4. dedup events -----------------------------------------------------
    // Transpose worker blocks into per-bucket block lists (moves handles only).
    let mut buckets: Vec<Vec<Vec<EvRow>>> = (0..EV_PARTS).map(|_| Vec::new()).collect();
    let mut click_vecs: Vec<Vec<ClickRow>> = Vec::with_capacity(nthreads);
    for w in workers {
        for (p, (open, full)) in w.open.into_iter().zip(w.full).enumerate() {
            buckets[p].extend(full);
            buckets[p].push(open);
        }
        click_vecs.push(w.clicks);
    }
    let survivors: Vec<Vec<Surv>> = buckets.into_par_iter().map(|blocks| dedup(blocks, &person)).collect();
    let n_surv: usize = survivors.iter().map(Vec::len).sum();
    log(t0, &format!("dedup: {n_surv} surviving events"));

    // ---- 5. group by person -------------------------------------------------
    let counts: Vec<AtomicU32> = (0..np).into_par_iter().map(|_| AtomicU32::new(0)).collect();
    survivors.par_iter().for_each(|v| {
        for s in v {
            counts[s.person as usize].fetch_add(1, Relaxed);
        }
    });
    let ev_off = offsets(&counts);
    let mut events: Vec<Ev> = Vec::with_capacity(n_surv);
    {
        let ptr = SendPtr(events.as_mut_ptr());
        survivors.par_iter().for_each(|v| {
            for s in v {
                let pos = counts[s.person as usize].fetch_add(1, Relaxed) as usize;
                // SAFETY: each position in 0..n_surv is claimed exactly once.
                unsafe { ptr.get().add(pos).write(Ev { ts: s.ts, price: s.price, cat: s.cat, kind: s.kind }) };
            }
        });
        // SAFETY: all n_surv slots were written above.
        unsafe { events.set_len(n_surv) };
    }
    drop(survivors);

    counts.par_iter().for_each(|c| c.store(0, Relaxed));
    click_vecs.par_iter().for_each(|v| {
        v.par_chunks(1 << 16).for_each(|cs| {
            for c in cs {
                counts[person(c.user) as usize].fetch_add(1, Relaxed);
            }
        })
    });
    let cl_off = offsets(&counts);
    let mut clicks: Vec<Click> = Vec::with_capacity(n_clicks);
    {
        let ptr = SendPtr(clicks.as_mut_ptr());
        click_vecs.par_iter().for_each(|v| {
            v.par_chunks(1 << 16).for_each(|cs| {
                for c in cs {
                    let pos = counts[person(c.user) as usize].fetch_add(1, Relaxed) as usize;
                    // SAFETY: each position in 0..n_clicks is claimed exactly once.
                    unsafe {
                        ptr.get().add(pos).write(Click { ts: c.ts, cid: c.cid, cid_len: c.cid_len, campaign: c.campaign })
                    };
                }
            })
        });
        unsafe { clicks.set_len(n_clicks) };
    }
    drop(click_vecs);
    drop(counts);
    log(t0, "grouped by person");

    // ---- 6. sessions, attribution, aggregation ------------------------------
    let day0 = ws.div_euclid(DAY);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY) - day0 + 1) as usize } else { 0 };
    let ncamp = lookups.campaign_ids.len();
    let sctx = SessCtx {
        day0,
        ndays,
        organic: ncamp as u32,
        rank: &lookups.category_rank,
    };
    let accs: Vec<Mutex<Acc>> = (0..nthreads).map(|_| Mutex::new(Acc::new((ncamp + 1) * ndays))).collect();
    {
        let evp = SendPtr(events.as_mut_ptr());
        let clp = SendPtr(clicks.as_mut_ptr());
        (0..np).into_par_iter().with_min_len(32).for_each(|p| {
            let (a, b) = (ev_off[p] as usize, ev_off[p + 1] as usize);
            if a == b {
                return;
            }
            let (ca, cb) = (cl_off[p] as usize, cl_off[p + 1] as usize);
            // SAFETY: person ranges are disjoint; each person is visited once.
            let evs = unsafe { std::slice::from_raw_parts_mut(evp.get().add(a), b - a) };
            let cls = unsafe { std::slice::from_raw_parts_mut(clp.get().add(ca), cb - ca) };
            // Sort before taking this thread's accumulator: a parallel sort may
            // run other persons on this same thread while it waits.
            if evs.len() > 1 << 18 {
                evs.par_sort_unstable_by_key(|e| e.ts);
            } else {
                evs.sort_unstable_by_key(|e| e.ts);
            }
            let by_ts_cid = |x: &Click, y: &Click| x.ts.cmp(&y.ts).then_with(|| x.cid().cmp(y.cid()));
            if cls.len() > 1 << 18 {
                cls.par_sort_unstable_by(by_ts_cid);
            } else {
                cls.sort_unstable_by(by_ts_cid);
            }
            let t = rayon::current_thread_index().unwrap_or(0);
            let mut acc = accs[t].lock().unwrap();
            process_person(p as u32, evs, cls, &mut acc, &sctx);
        });
    }
    drop(events);
    drop(clicks);
    log(t0, "sessions done");

    // ---- 7. output -------------------------------------------------------------
    let accs: Vec<Acc> = accs.into_iter().map(|m| m.into_inner().unwrap()).collect();
    write_outputs(out_dir, &accs, &names, &lookups, &sctx, ncamp)?;
    log(t0, "outputs written");
    Ok(())
}

/// Exclusive prefix sums of `counts`, and each counter reset to its offset
/// so it can be used as an insertion cursor.
fn offsets(counts: &[AtomicU32]) -> Vec<u32> {
    let mut off = Vec::with_capacity(counts.len() + 1);
    let mut acc: u64 = 0;
    for c in counts {
        off.push(acc as u32);
        let n = c.load(Relaxed) as u64;
        c.store(acc as u32, Relaxed);
        acc += n;
    }
    assert!(acc < u32::MAX as u64, "too many rows for u32 offsets");
    off.push(acc as u32);
    off
}

/// Dedup one hash partition (rules.md events steps 2-4): for each event_id,
/// keep the copy with the smallest ingest_ts; drop it if invalid or late.
fn dedup(blocks: Vec<Vec<EvRow>>, person: &(impl Fn(u32) -> u32 + Sync)) -> Vec<Surv> {
    let total: usize = blocks.iter().map(Vec::len).sum();
    let mut rows: Vec<EvRow> = Vec::with_capacity(total);
    for b in blocks {
        rows.extend_from_slice(&b); // each block is freed right after its copy
    }
    rows.sort_unstable_by_key(|r| (r.h, r.ingest));
    let mut out = Vec::with_capacity(total);
    let mut keep = |r: &EvRow| {
        // Invalid and late copies were marked KIND_INVALID while parsing.
        if r.kind() != parse::KIND_INVALID {
            out.push(Surv { ts: r.ts, person: person(r.user), price: r.price(), cat: r.cat(), kind: r.kind() });
        }
    };
    let n = rows.len();
    let mut i = 0;
    while i < n {
        let h = rows[i].h;
        let mut j = i + 1;
        while j < n && rows[j].h == h {
            j += 1;
        }
        if j - i == 1 {
            keep(&rows[i]);
        } else {
            let first = rows[i].eid();
            if rows[i + 1..j].iter().all(|r| r.eid() == first) {
                // All copies of one event_id, sorted by ingest_ts: keep the first.
                // (Copies with equal ingest_ts are identical, so any is right.)
                keep(&rows[i]);
            } else {
                // 64-bit hash collision between distinct event_ids: regroup
                // exactly by the bytes.
                let g = &mut rows[i..j];
                g.sort_by(|a, b| a.eid().cmp(b.eid()).then(a.ingest.cmp(&b.ingest)));
                let mut k = 0;
                while k < g.len() {
                    keep(&g[k]);
                    let e = g[k].eid();
                    k += 1;
                    while k < g.len() && g[k].eid() == e {
                        k += 1;
                    }
                }
            }
        }
        i = j;
    }
    out
}

struct SessCtx<'a> {
    day0: i64,
    ndays: usize,
    organic: u32,
    rank: &'a [u32],
}

#[derive(Clone, Copy)]
struct UserRow {
    person: u32,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: u16,
}

/// Per-thread accumulator plus reusable scratch buffers.
struct Acc {
    /// [sessions, purchasing_sessions, revenue, assisted] per (campaign, day)
    camp: Vec<[i64; 4]>,
    users: Vec<UserRow>,
    starts: Vec<i64>,
    segs: Vec<(usize, u32)>, // (day index, first session index)
    cats: Vec<(u16, i64)>,
    tmp: Vec<(u32, i64)>,
}

impl Acc {
    fn new(slots: usize) -> Self {
        Acc {
            camp: vec![[0; 4]; slots],
            users: Vec::new(),
            starts: Vec::new(),
            segs: Vec::new(),
            cats: Vec::new(),
            tmp: Vec::new(),
        }
    }
}

/// The click credited with a session starting at `s` (rules.md clicks step
/// 4): among clicks with `s - 7d <= ts <= s`, the latest ts, then the
/// smallest click_id. `cls` is sorted by (ts, click_id).
#[inline]
fn credit(cls: &[Click], s: i64, organic: u32) -> u32 {
    let ub = cls.partition_point(|c| c.ts <= s);
    if ub == 0 {
        return organic;
    }
    let t = cls[ub - 1].ts;
    if s - t > WEEK {
        return organic;
    }
    let lb = cls[..ub].partition_point(|c| c.ts < t);
    cls[lb].campaign
}

fn top_category(cats: &[(u16, i64)], rank: &[u32]) -> u16 {
    let mut best = NO_CATEGORY;
    let mut best_sum = i64::MIN;
    for &(c, s) in cats {
        if s > best_sum || (s == best_sum && rank[c as usize] < rank[best as usize]) {
            best = c;
            best_sum = s;
        }
    }
    best
}

/// One person: `evs` sorted by ts, `cls` sorted by (ts, click_id).
fn process_person(p: u32, evs: &[Ev], cls: &[Click], acc: &mut Acc, cx: &SessCtx) {
    let Acc { camp, users, starts, segs, cats, tmp } = acc;
    starts.clear();
    segs.clear();
    cats.clear();
    let ndays = cx.ndays;

    let mut cur_day: i64 = i64::MIN;
    let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0i64);
    let n = evs.len();
    let mut i = 0;
    while i < n {
        // A new session starts when the gap to the previous event is over 30
        // minutes; equal timestamps have gap 0 and always stay together.
        let start = evs[i].ts;
        let mut j = i + 1;
        while j < n && evs[j].ts - evs[j - 1].ts <= GAP {
            j += 1;
        }
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if d_sessions > 0 {
                users.push(UserRow {
                    person: p,
                    day: cur_day as i32,
                    sessions: d_sessions,
                    events: d_events,
                    revenue: d_revenue,
                    top: top_category(cats, cx.rank),
                });
            }
            cats.clear();
            d_sessions = 0;
            d_events = 0;
            d_revenue = 0;
            cur_day = day;
            segs.push(((day - cx.day0) as usize, starts.len() as u32));
        }
        let mut rev = 0i64;
        let mut purchased = false;
        for e in &evs[i..j] {
            if e.kind == KIND_PURCHASE {
                rev += e.price as i64;
                purchased = true;
                match cats.iter_mut().find(|(c, _)| *c == e.cat) {
                    Some(x) => x.1 += e.price as i64,
                    None => cats.push((e.cat, e.price as i64)),
                }
            }
        }
        d_sessions += 1;
        d_events += (j - i) as u32;
        d_revenue += rev;

        let c = credit(cls, start, cx.organic);
        let slot = &mut camp[c as usize * ndays + (day - cx.day0) as usize];
        slot[0] += 1;
        slot[1] += purchased as i64;
        slot[2] += rev;
        starts.push(start);
        i = j;
    }
    if d_sessions > 0 {
        users.push(UserRow {
            person: p,
            day: cur_day as i32,
            sessions: d_sessions,
            events: d_events,
            revenue: d_revenue,
            top: top_category(cats, cx.rank),
        });
    }

    // Assists (rules.md clicks step 7). A campaign assists a session starting
    // at S when one of its clicks has S - 7d <= ts <= S, i.e. S lies in
    // [ts, ts + 7d]. Per campaign, merge those intervals; every session whose
    // start falls in a merged interval is assisted exactly once. Sessions in a
    // range are counted per day with the day segments of `starts`, so the
    // cost is O(clicks log clicks + intervals * (log sessions + days)).
    if cls.is_empty() {
        return;
    }
    tmp.clear();
    tmp.extend(cls.iter().map(|c| (c.campaign, c.ts)));
    tmp.sort_unstable();
    let nsess = starts.len();
    let mut add_range = |c: u32, a: i64, b: i64| {
        let lo = starts.partition_point(|&s| s < a);
        let hi = starts.partition_point(|&s| s <= b);
        if lo >= hi {
            return;
        }
        for k in 0..segs.len() {
            let s = segs[k].1 as usize;
            let e = if k + 1 < segs.len() { segs[k + 1].1 as usize } else { nsess };
            let from = s.max(lo);
            let to = e.min(hi);
            if from < to {
                camp[c as usize * ndays + segs[k].0][3] += (to - from) as i64;
            }
        }
    };
    let m = tmp.len();
    let mut k = 0;
    while k < m {
        let c = tmp[k].0;
        let mut a = tmp[k].1;
        let mut b = a + WEEK;
        k += 1;
        while k < m && tmp[k].0 == c {
            let t = tmp[k].1;
            if t <= b {
                b = t + WEEK;
            } else {
                add_range(c, a, b);
                a = t;
                b = t + WEEK;
            }
            k += 1;
        }
        add_range(c, a, b);
    }
}

fn write_outputs(
    out_dir: &Path,
    accs: &[Acc],
    names: &[&'static [u8]],
    lookups: &Lookups,
    cx: &SessCtx,
    ncamp: usize,
) -> Result<(), Error> {
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;

    // campaign_daily: sum the per-thread arrays (tiny), then one file.
    let ndays = cx.ndays;
    let slots = (ncamp + 1) * ndays;
    let mut camp = vec![[0i64; 4]; slots];
    for a in accs {
        for (x, y) in camp.iter_mut().zip(&a.camp) {
            for f in 0..4 {
                x[f] += y[f];
            }
        }
    }
    let mut cc = output::CampaignCols {
        campaign_id: Vec::new(),
        channel: Vec::new(),
        day: Vec::new(),
        sessions: Vec::new(),
        purchasing: Vec::new(),
        revenue: Vec::new(),
        assisted: Vec::new(),
    };
    for c in 0..=ncamp {
        let (id, ch): (&[u8], &[u8]) =
            if c == ncamp { (b"organic", b"organic") } else { (lookups.campaign_ids[c], lookups.campaign_channels[c]) };
        for d in 0..ndays {
            let v = camp[c * ndays + d];
            if v[0] > 0 || v[3] > 0 {
                cc.campaign_id.push(ByteArray::from(id));
                cc.channel.push(ByteArray::from(ch));
                cc.day.push((cx.day0 + d as i64) as i32);
                cc.sessions.push(v[0]);
                cc.purchasing.push(v[1]);
                cc.revenue.push(v[2]);
                cc.assisted.push(v[3]);
            }
        }
    }
    let campaign_job = || output::write_campaign(&cd.join("part-00000.parquet"), &cc);

    // user_daily: one file per thread accumulator, written in parallel.
    let total: usize = accs.iter().map(|a| a.users.len()).sum();
    let user_job = || -> Result<(), Error> {
        accs.par_iter()
            .enumerate()
            .filter(|(i, a)| !a.users.is_empty() || (total == 0 && *i == 0))
            .map(|(i, a)| {
                let rows = &a.users;
                let mut c = output::UserCols {
                    user_id: Vec::with_capacity(rows.len()),
                    day: Vec::with_capacity(rows.len()),
                    sessions: Vec::with_capacity(rows.len()),
                    events: Vec::with_capacity(rows.len()),
                    revenue: Vec::with_capacity(rows.len()),
                    top: Vec::with_capacity(rows.len()),
                    top_def: Vec::with_capacity(rows.len()),
                };
                for r in rows {
                    c.user_id.push(ByteArray::from(names[r.person as usize]));
                    c.day.push(r.day);
                    c.sessions.push(r.sessions as i64);
                    c.events.push(r.events as i64);
                    c.revenue.push(r.revenue);
                    if r.top == NO_CATEGORY {
                        c.top_def.push(0);
                    } else {
                        c.top_def.push(1);
                        c.top.push(ByteArray::from(lookups.categories[r.top as usize]));
                    }
                }
                output::write_user(&ud.join(format!("part-{i:05}.parquet")), &c)
            })
            .collect::<Result<Vec<()>, Error>>()?;
        Ok(())
    };
    let (a, b) = rayon::join(campaign_job, user_job);
    a?;
    b?;
    Ok(())
}
