//! Shopping Sessions pipeline. See README.md for the architecture and
//! rules.md for the specification. Stages:
//!
//!  1. map every input file, cut the bodies into ~4 MB line-aligned chunks;
//!  2. parse the link chunks, then build the people table (union-find over
//!     linked IDs) on its own thread while all event and click chunks are
//!     parsed in parallel; every ID becomes an exact 64-bit key (idkey.rs) and
//!     events are hash-partitioned on event_id into huge-page block chains;
//!  3. dedup each event partition in parallel with cache-sized hash tables;
//!     survivors go straight into per-person-partition block chains;
//!  4. scatter valid clicks into the same person partitions;
//!  5. per person partition, in parallel: group by person (hash table), sort
//!     each person's events and clicks, sessionize, credit, assist, aggregate;
//!  6. write Parquet in parallel.

mod alloc;
mod csv;
mod idkey;
mod input;
mod intern;
mod key;
mod output;
mod parse;
mod people;
mod scan;
mod ts;

use input::Lookups;
use parquet::data_type::ByteArray;
use parse::{ClickRow, EvRow, Worker, EV_PARTS, EV_PART_BITS, KIND_PURCHASE};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;
use ts::{DAY, MINUTE};

#[global_allocator]
static GLOBAL: alloc::Alloc = alloc::Alloc;

type Error = Box<dyn std::error::Error + Send + Sync>;

const GAP: i64 = 30 * MINUTE;
const WEEK: i64 = 7 * DAY;
const CHUNK: usize = 4 << 20;
const NO_CATEGORY: u32 = u32::MAX;

/// A surviving event, tagged with its person key.
#[derive(Clone, Copy)]
struct Surv {
    person: u64,
    ts: i64,
    price_kind: u32,
    cat: u32,
}

/// Person partitions for the session stage.
const PP_BITS: u32 = 8;
const PP: usize = 1 << PP_BITS;
#[inline(always)]
fn ppart(k: u64) -> usize {
    (idkey::mix(k) >> (64 - PP_BITS)) as usize
}

/// A raw pointer that may be shared between threads; every user writes or
/// borrows disjoint ranges only.
#[derive(Clone, Copy)]
pub struct SendPtr<T>(pub *mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}
impl<T> SendPtr<T> {
    #[inline(always)]
    pub fn get(self) -> *mut T {
        self.0
    }
}

fn log(t0: Instant, what: &str) {
    eprintln!("[{:8.3}s] {what}", t0.elapsed().as_secs_f64());
}

fn main() {
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

fn run(data_dir: &Path, out_dir: &Path, t0: Instant) -> Result<(), Error> {
    let (ws, we) = input::load_manifest(data_dir)?;
    let lookups = input::load_lookups(data_dir)?;

    // ---- 1. map and chunk -------------------------------------------------
    let mut files: Vec<(u8, PathBuf)> = Vec::new();
    for (kind, dir) in [(2u8, "links"), (0, "events"), (1, "clicks")] {
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
    let exotic = intern::Interner::new();
    let pctx = parse::Ctx { lookups: &lookups, exotic: &exotic, ws, we };
    let nthreads = rayon::current_num_threads();
    // Reserve generously so the per-worker vectors never reallocate and copy
    // (an event line is at least ~60 bytes; unused capacity is never touched,
    // so it costs address space only).
    let cl_bytes: usize = mapped.iter().filter(|m| m.0 == 1).map(|m| m.1.len()).sum();
    // Links first (small), so the people table can be built on its own
    // thread while the events and clicks are parsed.
    let n_link_tasks = tasks.iter().take_while(|t| t.0 == 2).count();
    let link_vecs: Vec<Vec<(u64, u64)>> = tasks[..n_link_tasks]
        .par_iter()
        .map(|&(_, chunk)| {
            let mut w = parse::LinkSink::default();
            parse::links_chunk(chunk, &pctx, &mut w.links);
            w.links
        })
        .collect();
    next.store(n_link_tasks, Relaxed);
    let n_links: usize = link_vecs.iter().map(Vec::len).sum();
    let (people, workers) = std::thread::scope(|sc| {
        let h = sc.spawn(|| people::People::build(&link_vecs, &exotic));
        let workers: Vec<Worker> = parse_all(nthreads, tasks, next, &pctx, cl_bytes, release_chunks);
        (h.join().expect("people thread"), workers)
    });
    drop(link_vecs);
    let n_ev_rows: usize = workers.iter().map(|w| w.n_events).sum();
    let n_clicks: usize = workers.iter().map(|w| w.clicks.len()).sum();
    log(t0, &format!("parsed: {n_ev_rows} event rows, {n_clicks} valid clicks, {n_links} links"));
    log(t0, &format!("people: {} linked ids, {} merged into another person", people.n_ids, people.n_merged));

    // ---- 3. regroup worker output ---------------------------------------------
    let mut buckets: Vec<Vec<&'static [EvRow]>> = (0..EV_PARTS).map(|_| Vec::new()).collect();
    let mut click_vecs: Vec<Vec<ClickRow>> = Vec::with_capacity(nthreads);
    for w in workers {
        let (blocks, clicks) = w.into_parts();
        for (p, b) in blocks.into_iter().enumerate() {
            buckets[p].extend(b);
        }
        click_vecs.push(clicks);
    }

    // ---- 4. dedup events -----------------------------------------------------
    // Survivors go straight into per-thread block chains, one per person
    // partition (no intermediate array, no second scatter pass).
    let writers: Vec<Mutex<alloc::Parts<Surv>>> = (0..nthreads).map(|_| Mutex::new(alloc::Parts::new(PP, 1024))).collect();
    let n_surv: usize = buckets
        .into_par_iter()
        .map(|blocks| {
            let t = rayon::current_thread_index().unwrap_or(0);
            dedup(blocks, &people, &mut writers[t].lock().unwrap())
        })
        .sum();
    let mut ev_parts: Vec<Vec<&'static [Surv]>> = (0..PP).map(|_| Vec::new()).collect();
    for w in writers {
        for (p, b) in w.into_inner().unwrap().finish().into_iter().enumerate() {
            ev_parts[p].extend(b);
        }
    }
    log(t0, &format!("dedup: {n_surv} surviving events"));

    // ---- 5. partition by person -----------------------------------------------
    click_vecs.par_iter_mut().for_each(|v| {
        for c in v.iter_mut() {
            c.user = people.person(c.user);
        }
    });
    let srcs: Vec<&[ClickRow]> = click_vecs.iter().flat_map(|v| v.chunks(1 << 16)).collect();
    let (mut clicks, cl_off) = people::partition::<ClickRow, PP>(&srcs, |c| ppart(c.user));
    drop(srcs);
    drop(click_vecs);
    log(t0, "partitioned by person");

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
        let clp = SendPtr(clicks.as_mut_ptr());
        // Largest partitions first, so no big one starts last.
        let mut order: Vec<usize> = (0..PP).collect();
        let ev_len: Vec<usize> = ev_parts.iter().map(|v| v.iter().map(|b| b.len()).sum()).collect();
        order.sort_unstable_by_key(|&p| std::cmp::Reverse(ev_len[p]));
        let ev_parts = &ev_parts;
        let exotic = &exotic;
        order.into_par_iter().with_max_len(1).for_each(|p| {
            // SAFETY: partition ranges are disjoint; each is visited once.
            let cls = unsafe { std::slice::from_raw_parts(clp.get().add(cl_off[p]) as *const ClickRow, cl_off[p + 1] - cl_off[p]) };
            let mut g = GROUP.with(|c| std::mem::take(&mut *c.borrow_mut()));
            group_partition(&ev_parts[p], cls, &mut g);
            let t = rayon::current_thread_index().unwrap_or(0);
            let mut acc = accs[t].lock().unwrap();
            let Group { persons, ev_off, evs, cl_off, cls: gcls, .. } = &mut g;
            for (pi, &person) in persons.iter().enumerate() {
                let e = &mut evs[ev_off[pi] as usize..ev_off[pi + 1] as usize];
                let c = &mut gcls[cl_off[pi] as usize..cl_off[pi + 1] as usize];
                if e.len() > 1 {
                    e.sort_unstable_by_key(|x| x.ts);
                }
                if c.len() > 1 {
                    c.sort_unstable_by(|x, y| x.ts.cmp(&y.ts).then_with(|| idkey::cmp(x.cid, y.cid, exotic)));
                }
                process_person(person, e, c, &mut acc, &sctx);
            }
            drop(acc);
            GROUP.with(|c| *c.borrow_mut() = g);
        });
    }
    drop(clicks);
    log(t0, "sessions done");

    // ---- 7. output -------------------------------------------------------------
    let accs: Vec<Acc> = accs.into_iter().map(|m| m.into_inner().unwrap()).collect();
    write_outputs(out_dir, &accs, &exotic, &lookups, &sctx, ncamp)?;
    log(t0, "outputs written");
    Ok(())
}

/// Parse every remaining (event and click) chunk on `nthreads` workers.
fn parse_all(
    nthreads: usize,
    tasks: &'static [(u8, &'static [u8])],
    next: &'static AtomicUsize,
    pctx: &parse::Ctx,
    cl_bytes: usize,
    release_chunks: bool,
) -> Vec<Worker> {
    (0..nthreads)
        .into_par_iter()
        .map(|_| {
            let mut w = Worker::new();
            w.clicks.reserve(cl_bytes / 30 * 2 / nthreads + 1024);
            loop {
                let i = next.fetch_add(1, Relaxed);
                let Some(&(k, chunk)) = tasks.get(i) else { break };
                match k {
                    0 => parse::events_chunk(chunk, pctx, &mut w),
                    _ => parse::clicks_chunk(chunk, pctx, &mut w),
                }
                // Done with this chunk's pages: let the kernel drop them from
                // our mapping (clean, file-backed; refaults if ever needed).
                if release_chunks {
                    input::advise(chunk, input::DONTNEED);
                }
            }
            w
        })
        .collect()
}

/// One person partition grouped by person (reused per-thread scratch).
#[derive(Default)]
struct Group {
    table: Vec<(u64, u32)>,
    persons: Vec<u64>,
    pidx: Vec<u32>,
    ev_off: Vec<u32>,
    evs: Vec<Surv>,
    cpidx: Vec<u32>,
    cl_off: Vec<u32>,
    cls: Vec<ClickRow>,
}

/// Group a partition's events by person with a hash table (persons in first
/// seen order) and attach each person's clicks; clicks of persons with no
/// event are dropped (they cannot credit or assist anything).
fn group_partition(blocks: &[&'static [Surv]], cls: &[ClickRow], g: &mut Group) {
    const EMPTY: u64 = u64::MAX;
    let n: usize = blocks.iter().map(|b| b.len()).sum();
    let cap = (2 * n).next_power_of_two().max(16);
    let mask = cap - 1;
    g.table.clear();
    g.table.resize(cap, (EMPTY, 0));
    g.persons.clear();
    g.pidx.clear();
    g.ev_off.clear();
    let find = |table: &[(u64, u32)], k: u64| -> Option<u32> {
        let mut h = idkey::mix(k) as usize & mask;
        loop {
            let (x, v) = table[h];
            if x == k {
                return Some(v);
            }
            if x == EMPTY {
                return None;
            }
            h = (h + 1) & mask;
        }
    };
    for b in blocks {
        for e in b.iter() {
            let k = e.person;
            let mut h = idkey::mix(k) as usize & mask;
            let pi = loop {
                let slot = &mut g.table[h];
                if slot.0 == k {
                    break slot.1;
                }
                if slot.0 == EMPTY {
                    let pi = g.persons.len() as u32;
                    *slot = (k, pi);
                    g.persons.push(k);
                    g.ev_off.push(0);
                    break pi;
                }
                h = (h + 1) & mask;
            };
            g.ev_off[pi as usize] += 1;
            g.pidx.push(pi);
        }
    }
    let np = g.persons.len();
    // Counts -> exclusive offsets (+ end), then scatter.
    let mut acc = 0u32;
    for c in g.ev_off.iter_mut() {
        let x = *c;
        *c = acc;
        acc += x;
    }
    g.ev_off.push(acc);
    g.evs.clear();
    g.evs.reserve(n);
    {
        let mut pos: Vec<u32> = g.ev_off[..np].to_vec();
        let out = g.evs.as_mut_ptr();
        let mut i = 0;
        for b in blocks {
            for e in b.iter() {
                let pi = g.pidx[i] as usize;
                // SAFETY: every slot in 0..n is written exactly once.
                unsafe { out.add(pos[pi] as usize).write(*e) };
                pos[pi] += 1;
                i += 1;
            }
        }
        // SAFETY: all n slots were written.
        unsafe { g.evs.set_len(n) };
    }
    g.cpidx.clear();
    g.cl_off.clear();
    g.cl_off.resize(np + 1, 0);
    for c in cls {
        match find(&g.table, c.user) {
            Some(pi) => {
                g.cl_off[pi as usize] += 1;
                g.cpidx.push(pi);
            }
            None => g.cpidx.push(u32::MAX),
        }
    }
    let mut acc = 0u32;
    for c in g.cl_off.iter_mut() {
        let x = *c;
        *c = acc;
        acc += x;
    }
    g.cls.clear();
    g.cls.reserve(acc as usize);
    {
        let mut pos: Vec<u32> = g.cl_off[..np].to_vec();
        let out = g.cls.as_mut_ptr();
        for (c, &pi) in cls.iter().zip(&g.cpidx) {
            if pi != u32::MAX {
                // SAFETY: every slot in 0..acc is written exactly once.
                unsafe { out.add(pos[pi as usize] as usize).write(*c) };
                pos[pi as usize] += 1;
            }
        }
        // SAFETY: all acc slots were written.
        unsafe { g.cls.set_len(acc as usize) };
    }
}

/// Reused per-thread dedup scratch.
#[derive(Default)]
struct DedupScratch {
    tmp: Vec<(u64, u32)>,
    pairs: Vec<(u64, u32)>,
    table: Vec<(u64, u32)>,
    win: Vec<u64>,
    keep: Vec<u32>,
}

thread_local! {
    static GROUP: std::cell::RefCell<Group> = std::cell::RefCell::new(Group::default());
    static DEDUP: std::cell::RefCell<DedupScratch> = std::cell::RefCell::new(DedupScratch::default());
}

const DD_SUB_BITS: u32 = 4;
const DD_SUB: usize = 1 << DD_SUB_BITS;

/// Dedup one hash partition (rules.md events steps 2-4): for each event_id,
/// keep the copy with the smallest ingest_ts; drop it if invalid or late.
/// The rows are split once more into DD_SUB sub-partitions so each
/// open-addressing table (event_id key -> row) stays cache-resident. Rows stay
/// in their blocks (row index = block << 11 | offset).
fn dedup(blocks: Vec<&'static [EvRow]>, people: &people::People, out: &mut alloc::Parts<Surv>) -> usize {
    const EMPTY: u64 = u64::MAX;
    const _: () = assert!(parse::BLOCK == 1 << 11);
    let row = |i: u32| -> &EvRow { &blocks[(i >> 11) as usize][(i & 2047) as usize] };
    let sub = |k: u64| ((idkey::mix(k) >> (64 - EV_PART_BITS - DD_SUB_BITS)) as usize) & (DD_SUB - 1);
    DEDUP.with(|cell| {
        let sc = &mut *cell.borrow_mut();
        let total: usize = blocks.iter().map(|b| b.len()).sum();
        sc.tmp.clear();
        // One bit per row index (all zero between calls).
        sc.win.resize(blocks.len() * parse::BLOCK / 64, 0);
        let mut cnt = [0usize; DD_SUB + 1];
        for (bi, b) in blocks.iter().enumerate() {
            for (o, r) in b.iter().enumerate() {
                sc.tmp.push((r.eid, (bi as u32) << 11 | o as u32));
                cnt[sub(r.eid) + 1] += 1;
            }
        }
        for i in 0..DD_SUB {
            cnt[i + 1] += cnt[i];
        }
        sc.pairs.clear();
        sc.pairs.resize(total, (0, 0));
        let mut pos = cnt;
        for &(k, i) in &sc.tmp {
            let s = sub(k);
            sc.pairs[pos[s]] = (k, i);
            pos[s] += 1;
        }
        let mut kept = 0usize;
        for s in 0..DD_SUB {
            let part = &sc.pairs[cnt[s]..cnt[s + 1]];
            let cap = (part.len() * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            sc.table.clear();
            sc.table.resize(cap, (EMPTY, 0));
            let table = &mut sc.table;
            for &(k, me) in part {
                let mut h = (idkey::mix(k) as usize) & mask;
                loop {
                    let slot = &mut table[h];
                    if slot.0 == EMPTY {
                        *slot = (k, me);
                        break;
                    }
                    if slot.0 == k {
                        // Keep the smaller ingest_ts (copies with equal
                        // ingest_ts are identical, so either is right).
                        if row(me).ingest < row(slot.1).ingest {
                            slot.1 = me;
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
            // Mark the kept copies; rows are then read in block order.
            for &(k, i) in table.iter() {
                if k != EMPTY {
                    sc.win[(i >> 6) as usize] |= 1 << (i & 63);
                }
            }
        }
        // Emit the kept valid copies (invalid and late ones were marked
        // KIND_INVALID while parsing). Person lookups are random reads, so
        // they are prefetched a few rows ahead.
        const AHEAD: usize = 16;
        sc.keep.clear();
        for (wi, w) in sc.win.iter_mut().enumerate() {
            let mut bits = std::mem::take(w);
            while bits != 0 {
                let i = (wi as u32) << 6 | bits.trailing_zeros();
                bits &= bits - 1;
                let r = row(i);
                if r.kind() != parse::KIND_INVALID {
                    sc.keep.push(i);
                }
            }
        }
        let n = sc.keep.len();
        for a in 0..n.min(2 * AHEAD) {
            people.prefetch_filter(row(sc.keep[a]).user);
        }
        for a in 0..n.min(AHEAD) {
            people.prefetch_slot(row(sc.keep[a]).user);
        }
        for a in 0..n {
            if a + 2 * AHEAD < n {
                people.prefetch_filter(row(sc.keep[a + 2 * AHEAD]).user);
            }
            if a + AHEAD < n {
                people.prefetch_slot(row(sc.keep[a + AHEAD]).user);
            }
            let r = row(sc.keep[a]);
            let person = people.person(r.user);
            out.push(ppart(person), Surv { person, ts: r.ts, price_kind: r.price_kind, cat: r.cat });
        }
        kept += n;
        kept
    })
}

struct SessCtx<'a> {
    day0: i64,
    ndays: usize,
    organic: u32,
    rank: &'a [u32],
}

#[derive(Clone, Copy)]
struct UserRow {
    person: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: u32,
}

/// Per-thread accumulator plus reusable scratch buffers.
struct Acc {
    /// [sessions, purchasing_sessions, revenue, assisted] per (campaign, day)
    camp: Vec<[i64; 4]>,
    users: Vec<UserRow>,
    starts: Vec<i64>,
    segs: Vec<(usize, u32)>, // (day index, first session index)
    cats: Vec<(u32, i64)>,
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
fn credit(cls: &[ClickRow], s: i64, organic: u32) -> u32 {
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

fn top_category(cats: &[(u32, i64)], rank: &[u32]) -> u32 {
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
fn process_person(p: u64, evs: &[Surv], cls: &[ClickRow], acc: &mut Acc, cx: &SessCtx) {
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
            if (e.price_kind >> 24) as u8 == KIND_PURCHASE {
                let price = (e.price_kind & 0x00FF_FFFF) as i64;
                rev += price;
                purchased = true;
                match cats.iter_mut().find(|(c, _)| *c == e.cat) {
                    Some(x) => x.1 += price,
                    None => cats.push((e.cat, price)),
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
    exotic: &intern::Interner,
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

    // user_daily: the rows are cut into ~4 pieces per thread, one file each,
    // written in parallel. Names are written into one buffer per piece and
    // handed to Parquet as zero-copy slices of it.
    let total: usize = accs.iter().map(|a| a.users.len()).sum();
    let piece = total.div_ceil(4 * rayon::current_num_threads()).max(1 << 14);
    let pieces: Vec<&[UserRow]> = accs.iter().flat_map(|a| a.users.chunks(piece)).collect();
    let cats: Vec<bytes::Bytes> = lookups.categories.iter().map(|c| bytes::Bytes::copy_from_slice(c)).collect();
    let user_job = || -> Result<(), Error> {
        let empty: [&[UserRow]; 1] = [&[]];
        let pieces: &[&[UserRow]] = if pieces.is_empty() { &empty } else { &pieces };
        pieces
            .par_iter()
            .enumerate()
            .map(|(i, rows)| {
                let mut buf = Vec::with_capacity(rows.len() * 10);
                let mut ends = Vec::with_capacity(rows.len());
                for r in rows.iter() {
                    idkey::write(r.person, exotic, &mut buf);
                    ends.push(buf.len());
                }
                let buf = bytes::Bytes::from(buf);
                let mut c = output::UserCols {
                    user_id: Vec::with_capacity(rows.len()),
                    day: Vec::with_capacity(rows.len()),
                    sessions: Vec::with_capacity(rows.len()),
                    events: Vec::with_capacity(rows.len()),
                    revenue: Vec::with_capacity(rows.len()),
                    top: Vec::with_capacity(rows.len()),
                    top_def: Vec::with_capacity(rows.len()),
                };
                let mut start = 0;
                for (r, &end) in rows.iter().zip(&ends) {
                    c.user_id.push(ByteArray::from(buf.slice(start..end)));
                    start = end;
                    c.day.push(r.day);
                    c.sessions.push(r.sessions as i64);
                    c.events.push(r.events as i64);
                    c.revenue.push(r.revenue);
                    if r.top == NO_CATEGORY {
                        c.top_def.push(0);
                    } else {
                        c.top_def.push(1);
                        c.top.push(ByteArray::from(cats[r.top as usize].clone()));
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
