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
mod pack;
mod par;
mod parse;
mod people;
mod pq;
mod scan;
mod sys;
mod ts;

use input::Lookups;
use parse::{Cand, ClickRow, Surv, Worker, DEAD};
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
/// Chunk size for the default input mode (`pread` into a reused buffer):
/// small enough that a worker's read buffer stays in its L2.
const CHUNK: usize = 512 << 10;
/// Chunk size when parsing straight from the mapping (`SS_INPUT=populate`).
const POPULATE_CHUNK: usize = 4 << 20;

/// How event and click chunks reach the parser.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum InputMode {
    /// `pread` each chunk into a small reused buffer (cache-hot for the
    /// parser), with a background readahead thread hinting ahead.
    Pread,
    /// `MADV_POPULATE_READ` each 4 MB chunk (one call maps all its pages from
    /// the page cache, no per-page faults), then parse from the mapping.
    Populate,
}
const NO_CATEGORY: u32 = u32::MAX;

use pack::Pack;

use people::ppart;

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
    par::set_threads(ncpu);
    intern::configure();
    let mut result = run(&data_dir, &out_dir, t0);
    if matches!(&result, Err(e) if e.is::<intern::Collision>()) {
        // Two different exotic IDs share a hash key (intern.rs): redo the
        // run with the exact locked interner. Nothing has been written yet.
        log(t0, "exotic ID hash collision: restarting with the locked interner");
        intern::force_locked();
        result = run(&data_dir, &out_dir, t0);
    }
    match result {
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
    // Events are packed into `T` words while they are parsed (pack.rs).
    // SS_WIDE=1 forces the general u128 layout (to test it).
    if pack::narrow_fits(ws, we, lookups.categories.len(), WEEK) && std::env::var_os("SS_WIDE").is_none() {
        run_with::<u64>(data_dir, out_dir, t0, ws, we, lookups)
    } else {
        log(t0, "wide window or many categories: u128 rows");
        run_with::<u128>(data_dir, out_dir, t0, ws, we, lookups)
    }
}

fn run_with<T: Pack>(data_dir: &Path, out_dir: &Path, t0: Instant, ws: i64, we: i64, lookups: Lookups) -> Result<(), Error> {

    // ---- 1. map and chunk -------------------------------------------------
    let mut files: Vec<(u8, PathBuf)> = Vec::new();
    for (kind, dir) in [(2u8, "links"), (0, "events"), (1, "clicks")] {
        for p in input::list_csv(&data_dir.join(dir))? {
            files.push((kind, p));
        }
    }
    let mut mapped_parts = par::each(
        files.len(),
        |_| Vec::new(),
        |v: &mut Vec<(usize, std::io::Result<(&'static [u8], &'static std::fs::File)>)>, i| v.push((i, input::map_open(&files[i].1))),
    )
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    mapped_parts.sort_unstable_by_key(|x| x.0);
    let mut mapped: Vec<(u8, &'static [u8], &'static std::fs::File)> = Vec::with_capacity(files.len());
    for (i, r) in mapped_parts {
        let (d, f) = r?;
        mapped.push((files[i].0, d, f));
    }
    // SS_INPUT=populate (default) | pread: see InputMode. On a 2-vCPU box
    // populate was equal (warm, 2 threads; cold) or ~7% faster (warm, 1
    // thread) than pread + readahead; confirm on scorer-like hardware.
    let mode = match std::env::var("SS_INPUT").as_deref() {
        Ok("pread") => InputMode::Pread,
        _ => InputMode::Populate,
    };
    let mut tasks: Vec<Task> = Vec::new();
    let mut total_bytes = 0usize;
    for &(k, data, file) in &mapped {
        total_bytes += data.len();
        let mut cs = Vec::new();
        input::chunks(input::body(data), if mode == InputMode::Populate { POPULATE_CHUNK } else { CHUNK }, &mut cs);
        tasks.extend(cs.into_iter().map(|c| (k, c, file, c.as_ptr() as usize - data.as_ptr() as usize)));
    }


    // Releasing parsed chunks (MADV_DONTNEED) costs TLB shootdowns, so it is
    // only worth it when the inputs plus our working set (~1.2x the input
    // size) would crowd the memory limit (MEM_GB, default 24).
    let mem_bytes = std::env::var("MEM_GB").ok().and_then(|v| v.trim().parse::<f64>().ok()).unwrap_or(24.0) * 1e9;
    let release_chunks = (total_bytes as f64) * 2.2 > mem_bytes * 0.8;
    let build = if cfg!(target_feature = "avx512f") { "avx512" } else if cfg!(target_feature = "avx2") { "avx2" } else { "baseline" };
    log(t0, &format!("mapped {} files, {:.1} MB, {} chunks, input {mode:?}, release chunks: {release_chunks}, build: {build}", files.len(), total_bytes as f64 / 1e6, tasks.len()));
    // Bucket counts from the input size (an event line is ~95 bytes): about
    // 40K rows per dedup bucket and 20K events per person partition, powers
    // of two, so each stage's per-bucket table stays in cache at any scale.
    let ev_bytes: usize = mapped.iter().filter(|m| m.0 == 0).map(|m| m.1.len()).sum();
    let ev_rows = ev_bytes / 95 + 1;
    let log2_near = |x: usize| (x.max(1) as f64).log2().round() as u32;
    let env_bits = |k: &str| std::env::var(k).ok().and_then(|v| v.trim().parse::<u32>().ok()).map(|b| b.clamp(0, 16));
    // SS_EV_BITS / SS_PP_BITS override the sizing (for tuning on scorer-like hardware).
    let ev_bits = env_bits("SS_EV_BITS").unwrap_or_else(|| log2_near(ev_rows / 40_000).clamp(4, 14));
    let pp_bits = env_bits("SS_PP_BITS").unwrap_or_else(|| log2_near(ev_rows / 20_000).clamp(4, 14));
    parse::set_ev_bits(ev_bits);
    people::set_pp_bits(pp_bits);
    log(t0, &format!("buckets: {} dedup, {} person (~{ev_rows} event rows)", 1usize << ev_bits, 1usize << pp_bits));
    // Leaked so the readahead thread can follow the parsers' progress.
    let tasks: &'static [Task] = Box::leak(tasks.into_boxed_slice());
    let next: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));

    // Background readahead: hint the kernel to read chunks ahead of the
    // parsers, but at most RA_AHEAD chunks (~512 MB) ahead, so the inputs'
    // page-cache footprint stays bounded. A hint only; never needed for
    // correctness.
    // Readahead helps the pread mode; populate reads each chunk itself.
    // SS_READAHEAD=1 / SS_NO_READAHEAD=1 force it on / off.
    let want_ra = match (std::env::var_os("SS_READAHEAD"), std::env::var_os("SS_NO_READAHEAD")) {
        (_, Some(_)) => false,
        (Some(_), None) => true,
        (None, None) => mode == InputMode::Pread,
    };
    let readahead = want_ra.then(|| {
        const RA_AHEAD: usize = 128;
        std::thread::spawn(move || {
            for (i, &(_, chunk, _, _)) in tasks.iter().enumerate() {
                while i > next.load(Relaxed) + RA_AHEAD {
                    if next.load(Relaxed) >= tasks.len() {
                        return; // parsing finished
                    }
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                input::advise(chunk, input::WILLNEED);
            }
        })
    });

    // ---- 2. parse ---------------------------------------------------------
    let exotic = intern::Interner::new();
    let people_cell = people::PeopleSlot::default();
    let no_links = people::LinkFilter::empty();
    let pctx = parse::Ctx { lookups: &lookups, exotic: &exotic, linked: &no_links, people: &people_cell, ws, we };
    let nthreads = par::threads();
    // Links first (small), so the people table can be built on its own
    // thread while the events and clicks are parsed.
    let n_link_tasks = tasks.iter().take_while(|t| t.0 == 2).count();
    let link_states: Vec<(Vec<(u64, u64)>, intern::IdLog)> = par::each(
        n_link_tasks,
        |_| (Vec::new(), intern::IdLog::new()),
        |(v, l), i| parse::links_chunk(tasks[i].1, &pctx, v, l),
    );
    next.store(n_link_tasks, Relaxed);
    let (link_vecs, mut link_logs): (Vec<Vec<(u64, u64)>>, Vec<intern::IdLog>) = link_states.into_iter().unzip();
    // The people table orders link IDs by their bytes: check the links'
    // exotic IDs and publish their names first (small: links only).
    exotic.verify_links(&mut link_logs.iter_mut().collect::<Vec<_>>())?;
    let n_links: usize = link_vecs.iter().map(Vec::len).sum();
    // Which users may be linked: the others' events need no person lookup.
    let linked = people::LinkFilter::build(&link_vecs);
    let ectx = parse::Ctx { linked: &linked, ..pctx };
    let workers: Vec<Worker<T>> = std::thread::scope(|sc| {
        sc.spawn(|| {
            people_cell.set(people::People::build(&link_vecs, &exotic));
        });
        parse_all(nthreads, tasks, next, &ectx, release_chunks, mode)
    });
    let people = people_cell.wait();
    drop(link_vecs);
    let n_ev_rows: usize = workers.iter().map(|w| w.n_events).sum();
    let n_clicks: usize = workers.iter().map(|w| w.n_clicks).sum();
    log(t0, &format!("parsed: {n_ev_rows} event rows, {n_clicks} valid clicks, {n_links} links"));
    log(t0, &format!("people: {} linked ids, {} merged into another person; link filter {} KB", people.n_ids, people.n_merged, linked.bytes() >> 10));

    // ---- 3. regroup worker output ---------------------------------------------
    let (ev_parts_n, pp) = (parse::ev_parts(), people::pp());
    let mut buckets: Vec<Vec<&'static [Cand]>> = (0..ev_parts_n).map(|_| Vec::new()).collect();
    let mut cl_parts: Vec<Vec<&'static [ClickRow]>> = (0..pp).map(|_| Vec::new()).collect();
    let mut logs = link_logs;
    let mut surv = Vec::new();
    let mut deferred = Vec::new();
    for w in workers {
        let (blocks, clicks, s, d, ids) = w.into_parts();
        surv.push(s);
        deferred.push(d);
        for (p, b) in blocks.into_iter().enumerate() {
            buckets[p].extend(b);
        }
        for (p, b) in clicks.into_iter().enumerate() {
            cl_parts[p].extend(b);
        }
        logs.push(ids);
    }
    // Check every exotic ID key for collisions and copy the names that later
    // stages read (intern.rs), while the inputs are still mapped.
    if !exotic.is_locked() {
        let (n_ev, n_names) = exotic.verify_all(&mut logs.iter_mut().collect::<Vec<_>>())?;
        let pct = 100.0 * n_ev as f64 / n_ev_rows.max(1) as f64;
        log(t0, &format!("exotic IDs verified: {n_ev} event IDs ({pct:.1}% of event rows), {n_names} distinct user/click IDs"));
    }
    drop(logs);

    // The inputs are fully parsed (IDs are packed, or verified and their
    // names copied; products and campaigns have their own mappings). Unmap them
    // on a background thread, after the readahead thread has stopped, so the
    // page-table teardown overlaps the remaining stages instead of adding to
    // process exit.
    if std::env::var_os("SS_KEEP_MAPS").is_none() {
        let maps: Vec<&'static [u8]> = mapped.iter().map(|m| m.1).collect();
        std::thread::spawn(move || {
            if let Some(h) = readahead {
                let _ = h.join();
            }
            for m in maps {
                // SAFETY: nothing reads the input mappings after parsing.
                unsafe { sys::unmap(m) };
            }
        });
    }

    // ---- 4.. dedup, sessions and output on packed rows (pack.rs) -----------
    let st = Stage { buckets, surv, deferred, cl_parts, people, exotic: &exotic, lookups: &lookups, ws, we, out_dir, t0 };
    finish::<T>(st)
}

/// Everything the stages after parsing need.
struct Stage<'a, T: Pack> {
    buckets: Vec<Vec<&'static [Cand]>>,
    /// Payloads by person partition (users in no link), per worker.
    surv: Vec<alloc::Parts<Surv<T>>>,
    /// Payloads of possibly linked users, per worker (one list each).
    deferred: Vec<alloc::Parts<Surv<T>>>,
    cl_parts: Vec<Vec<&'static [ClickRow]>>,
    people: &'a people::People,
    exotic: &'a intern::Interner,
    lookups: &'a Lookups,
    ws: i64,
    we: i64,
    out_dir: &'a Path,
    t0: Instant,
}

/// Dedup, sessions, attribution, aggregation and output, with events and
/// clicks packed as `T` words.
fn finish<T: Pack>(st: Stage<T>) -> Result<(), Error> {
    let Stage { buckets, surv, deferred, cl_parts, people, exotic, lookups, ws, we, out_dir, t0 } = st;
    let pp = people::pp();
    // ---- 4. dedup events -----------------------------------------------------
    // Only the dedup records are read; dropped copies' payloads are marked
    // DEAD in place (they were written once, while parsing).
    let n_surv: usize = par::each(buckets.len(), |_| 0usize, |n, p| *n += dedup(&buckets[p])).into_iter().sum();
    // Payloads of users in no link are already in their person partitions.
    let mut ev_parts: Vec<Vec<&'static [Surv<T>]>> = (0..pp).map(|_| Vec::new()).collect();
    let n_direct: usize = surv.iter().map(|s| s.count).sum();
    for s in surv {
        for (p, b) in s.finish().into_iter().enumerate() {
            ev_parts[p].extend(b);
        }
    }
    // The others' users may be linked: resolve each live one to its person
    // and move it to that person's partition.
    let dblocks: Vec<&'static [Surv<T>]> = deferred.into_iter().flat_map(|d| d.finish().into_iter().flatten()).collect();
    let n_deferred: usize = dblocks.iter().map(|b| b.len()).sum();
    let writers = par::each(
        dblocks.len(),
        |_| alloc::Parts::<Surv<T>>::new(pp, alloc::POOL_BLOCK / std::mem::size_of::<Surv<T>>()),
        |out, i| resolve(dblocks[i], people, out),
    );
    for w in writers {
        for (p, b) in w.finish().into_iter().enumerate() {
            ev_parts[p].extend(b);
        }
    }
    log(t0, &format!("dedup: {n_surv} surviving events ({n_direct} payloads placed while parsing, {n_deferred} resolved to a person after dedup)"));
    let _ = ws;

    // ---- 5. sessions, attribution, aggregation ------------------------------
    let day0 = ws.div_euclid(DAY);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY) - day0 + 1) as usize } else { 0 };
    let ncamp = lookups.campaign_ids.len();
    let sctx = SessCtx {
        day0,
        ndays,
        organic: ncamp as u32,
        rank: &lookups.category_rank,
        ws,
        cbase: ws - WEEK,
        // SS_SMALL_CLICKS overrides (0 / huge force either assist path, for tests).
        small_clicks: std::env::var("SS_SMALL_CLICKS").ok().and_then(|v| v.parse().ok()).unwrap_or(16),
    };
    let uw = UserWriter {
        dir: out_dir.join("user_daily"),
        cats: lookups.categories.clone(),
        files: AtomicUsize::new(0),
    };
    std::fs::create_dir_all(&uw.dir)?;
    let write_err: Mutex<Option<Error>> = Mutex::new(None);
    let mut accs: Vec<Acc<T>>;
    {
        let cl_parts = &cl_parts;
        // Largest partitions first, so no big one starts last.
        let mut order: Vec<usize> = (0..pp).collect();
        let ev_len: Vec<usize> = ev_parts.iter().map(|v| v.iter().map(|b| b.len()).sum()).collect();
        order.sort_unstable_by_key(|&p| std::cmp::Reverse(ev_len[p]));
        let ev_parts = &ev_parts;
        let order = &order;
        let states = par::each(order.len(), |_| (Acc::<T>::new((ncamp + 1) * ndays), Group::<T>::default()), |(acc, g), oi| {
            let p = order[oi];
            group_partition(&ev_parts[p], &cl_parts[p], g);
            // Grouping copied this partition's rows: its blocks are dead, so
            // they go back to this worker's pool (user rows reuse them).
            for b in &ev_parts[p] {
                alloc::pool_put(b.as_ptr() as *const u8);
            }
            for b in &cl_parts[p] {
                alloc::pool_put(b.as_ptr() as *const u8);
            }
            let Group { persons, ev_off, evs, cl_off, cls: gcls, .. } = g;
            for (pi, &person) in persons.iter().enumerate() {
                let e = &mut evs[ev_off[pi] as usize..ev_off[pi + 1] as usize];
                let c = &mut gcls[cl_off[pi] as usize..cl_off[pi + 1] as usize];
                if e.len() > 1 {
                    e.sort_unstable();
                }
                pack_clicks(c, &mut acc.ck, sctx.cbase, exotic);
                let ck = std::mem::take(&mut acc.ck);
                process_person(person, e, &ck, acc, &sctx);
                acc.ck = ck;
            }
            if acc.users.count >= USER_FLUSH {
                let blocks = acc.take_users();
                if let Err(e) = uw.write(&blocks, exotic) {
                    *write_err.lock().unwrap() = Some(e);
                }
                for b in &blocks {
                    alloc::pool_put(b.as_ptr() as *const u8);
                }
            }
        });
        accs = states.into_iter().map(|s| s.0).collect();
    }
    if let Some(e) = write_err.into_inner().unwrap() {
        return Err(e);
    }
    log(t0, "sessions done");

    // ---- 6. output -------------------------------------------------------------
    write_outputs(out_dir, &mut accs, exotic, lookups, &sctx, ncamp, &uw)?;
    log(t0, "outputs written");
    Ok(())
}

/// A person's clicks as packed words in (ts, click_id) order. Sorting the
/// words orders by ts then campaign; only when two clicks share a ts does
/// click_id decide (credit takes the smallest), so then the rows themselves
/// are sorted by (ts, click_id) and packed in that order.
fn pack_clicks<T: Pack>(c: &mut [ClickRow], out: &mut Vec<T>, cbase: i64, exotic: &intern::Interner) {
    out.clear();
    out.extend(c.iter().map(|x| T::click((x.ts - cbase) as u64, x.campaign)));
    if out.len() > 1 {
        out.sort_unstable();
        if out.windows(2).any(|w| w[0].click_ts() == w[1].click_ts()) {
            c.sort_unstable_by(|x, y| x.ts.cmp(&y.ts).then_with(|| idkey::cmp(x.cid, y.cid, exotic)));
            out.clear();
            out.extend(c.iter().map(|x| T::click((x.ts - cbase) as u64, x.campaign)));
        }
    }
}

/// (kind, chunk of the mapping, its file, offset of the chunk in the file)
type Task = (u8, &'static [u8], &'static std::fs::File, usize);

/// `len` bytes of `file` at `off`, read into `buf`; `None` on any error.
fn read_at<'b>(file: &std::fs::File, off: usize, len: usize, buf: &'b mut Vec<u8>) -> Option<&'b [u8]> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        if buf.len() < len {
            buf.resize(len, 0);
        }
        file.read_exact_at(&mut buf[..len], off as u64).ok()?;
        Some(&buf[..len])
    }
    #[cfg(not(unix))]
    {
        let _ = (file, off, len, buf);
        None
    }
}

/// Parse every remaining (event and click) chunk on `nthreads` workers.
fn parse_all<T: Pack>(
    nthreads: usize,
    tasks: &'static [Task],
    next: &'static AtomicUsize,
    pctx: &parse::Ctx,
    release_chunks: bool,
    mode: InputMode,
) -> Vec<Worker<T>> {
    par::run(nthreads, |_| {
            let mut w = Worker::<T>::new();
            let mut buf: Vec<u8> = Vec::new();
            loop {
                let i = next.fetch_add(1, Relaxed);
                let Some(&(k, chunk, file, off)) = tasks.get(i) else { break };
                // Read the chunk into a small reused buffer instead of
                // parsing through the file mapping: one streaming kernel
                // copy, then the parser reads cache-hot memory (through the
                // mapping it stalled on every 4 KB page).
                let data: &[u8] = match mode {
                    InputMode::Pread => match read_at(file, off, chunk.len(), &mut buf) {
                        Some(d) => d,
                        None => chunk, // fall back to the mapping
                    },
                    InputMode::Populate => {
                        if sys::advise(chunk, sys::MADV_POPULATE_READ) {
                            chunk
                        } else {
                            // Kernel without it (< 5.14): read this chunk instead.
                            read_at(file, off, chunk.len(), &mut buf).unwrap_or(chunk)
                        }
                    }
                };
                w.ids.set_chunk(data.as_ptr(), chunk.as_ptr());
                match k {
                    0 => parse::events_chunk(data, pctx, &mut w),
                    _ => parse::clicks_chunk(data, pctx, &mut w),
                }
                // Done with this chunk's pages: let the kernel drop them from
                // our mapping (clean, file-backed; refaults if ever needed).
                if release_chunks {
                    input::advise(chunk, input::DONTNEED);
                }
            }
            w
        })
}

/// One person partition grouped by person (reused per-thread scratch).
struct Group<T> {
    table: Vec<(u64, u32)>,
    persons: Vec<u64>,
    pidx: Vec<u32>,
    ev_off: Vec<u32>,
    evs: Vec<T>,
    cpidx: Vec<u32>,
    cl_off: Vec<u32>,
    cls: Vec<ClickRow>,
}

impl<T> Default for Group<T> {
    fn default() -> Self {
        Group {
            table: Vec::new(),
            persons: Vec::new(),
            pidx: Vec::new(),
            ev_off: Vec::new(),
            evs: Vec::new(),
            cpidx: Vec::new(),
            cl_off: Vec::new(),
            cls: Vec::new(),
        }
    }
}

/// Group a partition's events by person with a hash table (persons in first
/// seen order) and attach each person's clicks; clicks of persons with no
/// event are dropped (they cannot credit or assist anything).
fn group_partition<T: Pack>(blocks: &[&'static [Surv<T>]], cls: &[&'static [ClickRow]], g: &mut Group<T>) {
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
            if k == DEAD {
                continue;
            }
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
    let n = g.pidx.len(); // live rows
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
                if e.person == DEAD {
                    continue;
                }
                let pi = g.pidx[i] as usize;
                // SAFETY: every slot in 0..n is written exactly once.
                unsafe { out.add(pos[pi] as usize).write(e.w) };
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
    for c in cls.iter().flat_map(|b| b.iter()) {
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
        for (c, &pi) in cls.iter().flat_map(|b| b.iter()).zip(&g.cpidx) {
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
    table: Vec<(u64, u32)>,
}

thread_local! {
    static DEDUP: std::cell::RefCell<DedupScratch> = std::cell::RefCell::new(DedupScratch::default());
}

/// Dedup one hash bucket (rules.md events steps 2-4): for each event_id,
/// keep the copy with the smallest ingest_ts; drop it if invalid or late
/// (no payload). Every other copy's payload is marked DEAD. Buckets are
/// sized (~40K records) so one open-addressing table (event_id key ->
/// record) per bucket stays cache-resident. Records stay in their blocks
/// (index = block << 11 | offset). Returns the number of kept valid copies.
fn dedup(blocks: &[&'static [Cand]]) -> usize {
    const EMPTY: u64 = u64::MAX;
    const _: () = assert!(parse::BLOCK == 1 << 11);
    let row = |i: u32| -> &Cand { &blocks[(i >> 11) as usize][(i & 2047) as usize] };
    // SAFETY: a payload belongs to exactly one record, and each record is in
    // one bucket, so no two threads write the same payload; nothing reads
    // payloads until dedup has finished.
    let kill = |c: &Cand| {
        if !c.pay.is_null() {
            unsafe { c.pay.write(DEAD) };
        }
    };
    DEDUP.with(|cell| {
        let sc = &mut *cell.borrow_mut();
        let total: usize = blocks.iter().map(|b| b.len()).sum();
        let cap = (total + total / 2).next_power_of_two().max(16);
        let mask = cap - 1;
        sc.table.clear();
        sc.table.resize(cap, (EMPTY, 0));
        let table = &mut sc.table;
        for (bi, b) in blocks.iter().enumerate() {
            for (o, r) in b.iter().enumerate() {
                let (k, me) = (r.eid, (bi as u32) << 11 | o as u32);
                let mut h = (idkey::mix(k) as usize) & mask;
                loop {
                    let slot = &mut table[h];
                    if slot.0 == EMPTY {
                        *slot = (k, me);
                        break;
                    }
                    if slot.0 == k {
                        // Keep the smaller ingest_ts; on a tie the copy seen
                        // first (copies with equal ingest_ts are identical,
                        // so either is right).
                        let best = row(slot.1);
                        if r.ingest < best.ingest {
                            kill(best);
                            slot.1 = me;
                        } else {
                            kill(r);
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
        }
        let kept = table.iter().filter(|&&(k, i)| k != EMPTY && !row(i).pay.is_null()).count();
        // This bucket's record blocks are dead now: recycle them.
        for b in blocks {
            alloc::pool_put(b.as_ptr() as *const u8);
        }
        kept
    })
}

/// Move one block of deferred payloads (users that may be linked) to their
/// person's partition, skipping dead ones. Person lookups are random reads,
/// so they are prefetched a few rows ahead.
fn resolve<T: Pack>(block: &'static [Surv<T>], people: &people::People, out: &mut alloc::Parts<Surv<T>>) {
    const AHEAD: usize = 16;
    let n = block.len();
    for a in 0..n.min(2 * AHEAD) {
        people.prefetch_filter(block[a].person);
    }
    for a in 0..n.min(AHEAD) {
        people.prefetch_slot(block[a].person);
    }
    for a in 0..n {
        if a + 2 * AHEAD < n {
            people.prefetch_filter(block[a + 2 * AHEAD].person);
        }
        if a + AHEAD < n {
            people.prefetch_slot(block[a + AHEAD].person);
        }
        let s = block[a];
        if s.person == DEAD {
            continue;
        }
        let person = people.person(s.person);
        out.push(ppart(person), Surv { person, w: s.w });
    }
    alloc::pool_put(block.as_ptr() as *const u8);
}

struct SessCtx<'a> {
    day0: i64,
    ndays: usize,
    organic: u32,
    rank: &'a [u32],
    /// Bases of the packed event and click timestamps.
    ws: i64,
    cbase: i64,
    /// People with at most this many clicks take the direct assist path.
    small_clicks: usize,
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
struct Acc<T> {
    /// [sessions, purchasing_sessions, revenue, assisted] per (campaign, day)
    camp: Vec<[i64; 4]>,
    /// The current person's packed clicks.
    ck: Vec<T>,
    /// User rows, in pool blocks (reusing dead partition blocks).
    users: alloc::Parts<UserRow>,
    starts: Vec<i64>,
    segs: Vec<(usize, u32)>, // (day index, first session index)
    cats: Vec<(u32, i64)>,
    tmp: Vec<(u32, i64)>,
}

const USER_BLOCK: usize = alloc::POOL_BLOCK / std::mem::size_of::<UserRow>();

impl<T> Acc<T> {
    /// The user rows so far (as blocks); starts a new chain.
    fn take_users(&mut self) -> Vec<&'static [UserRow]> {
        std::mem::replace(&mut self.users, alloc::Parts::new(1, USER_BLOCK)).finish().pop().unwrap_or_default()
    }
    fn new(slots: usize) -> Self {
        Acc {
            camp: vec![[0; 4]; slots],
            ck: Vec::new(),
            users: alloc::Parts::new(1, USER_BLOCK),
            starts: Vec::new(),
            segs: Vec::new(),
            cats: Vec::new(),
            tmp: Vec::new(),
        }
    }
}

/// The click credited with a session starting at `s` (rules.md clicks step
/// 4): among clicks with `s - 7d <= ts <= s`, the latest ts, then the
/// smallest click_id. `cls` is packed and in (ts, click_id) order; `s` is
/// relative to the click base. Sessions come in increasing start order, so
/// `cp` (the number of clicks with ts <= the previous start) only moves
/// forward: one pass over the clicks for all of a person's sessions.
#[inline]
fn credit<T: Pack>(cls: &[T], s: u64, organic: u32, cp: &mut usize) -> u32 {
    while *cp < cls.len() && cls[*cp].click_ts() <= s {
        *cp += 1;
    }
    let ub = *cp;
    if ub == 0 {
        return organic;
    }
    let t = cls[ub - 1].click_ts();
    if s - t > WEEK as u64 {
        return organic;
    }
    // First of the run of clicks at ts t: the smallest click_id.
    let mut lb = ub - 1;
    while lb > 0 && cls[lb - 1].click_ts() == t {
        lb -= 1;
    }
    cls[lb].click_campaign()
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

/// One person: `evs` (packed) sorted by ts, `cls` (packed) in (ts, click_id) order.
fn process_person<T: Pack>(p: u64, evs: &[T], cls: &[T], acc: &mut Acc<T>, cx: &SessCtx) {
    let Acc { camp, users, starts, segs, cats, tmp, .. } = acc;
    starts.clear();
    segs.clear();
    cats.clear();
    let ndays = cx.ndays;

    let mut cur_day: i64 = i64::MIN;
    let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0i64);
    let n = evs.len();
    let mut i = 0;
    let mut cp = 0usize;
    while i < n {
        // A new session starts when the gap to the previous event is over 30
        // minutes; equal timestamps have gap 0 and always stay together.
        let start = cx.ws + evs[i].ev_ts() as i64;
        let mut j = i + 1;
        while j < n && evs[j].ev_ts() - evs[j - 1].ev_ts() <= GAP as u64 {
            j += 1;
        }
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if d_sessions > 0 {
                users.push(0, UserRow {
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
            // A valid purchase has price >= 1; views and carts carry 0.
            let price = e.ev_price() as i64;
            if price != 0 {
                rev += price;
                purchased = true;
                let cat = e.ev_cat();
                match cats.iter_mut().find(|(c, _)| *c == cat) {
                    Some(x) => x.1 += price,
                    None => cats.push((cat, price)),
                }
            }
        }
        d_sessions += 1;
        d_events += (j - i) as u32;
        d_revenue += rev;

        let c = credit(cls, (start - cx.cbase) as u64, cx.organic, &mut cp);
        let slot = &mut camp[c as usize * ndays + (day - cx.day0) as usize];
        slot[0] += 1;
        slot[1] += purchased as i64;
        slot[2] += rev;
        starts.push(start);
        i = j;
    }
    if d_sessions > 0 {
        users.push(0, UserRow {
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
    let nsess = starts.len();
    // Few clicks (most people): slide a 7-day window over the clicks with
    // the sessions (both in time order) and count each distinct campaign in
    // a session's window once. O(sessions * w^2) with w <= small_clicks.
    if cls.len() <= cx.small_clicks {
        let (mut lo, mut hi) = (0usize, 0usize);
        for &s in starts.iter() {
            let srel = (s - cx.cbase) as u64;
            while hi < cls.len() && cls[hi].click_ts() <= srel {
                hi += 1;
            }
            while lo < hi && cls[lo].click_ts() + (WEEK as u64) < srel {
                lo += 1;
            }
            let d = (s.div_euclid(DAY) - cx.day0) as usize;
            for a in lo..hi {
                let c = cls[a].click_campaign();
                if !cls[lo..a].iter().any(|x| x.click_campaign() == c) {
                    camp[c as usize * ndays + d][3] += 1;
                }
            }
        }
        return;
    }
    tmp.clear();
    tmp.extend(cls.iter().map(|c| (c.click_campaign(), cx.cbase + c.click_ts() as i64)));
    tmp.sort_unstable();
    let mut add_range = |c: u32, a: i64, b: i64| {
        let lo = starts.partition_point(|&s| s < a);
        let hi = starts.partition_point(|&s| s <= b);
        if lo >= hi {
            return;
        }
        // Start at the day segment holding session `lo`, stop once a
        // segment starts at or after `hi`.
        let mut k = segs.partition_point(|sg| (sg.1 as usize) <= lo) - 1;
        while k < segs.len() && (segs[k].1 as usize) < hi {
            let s = segs[k].1 as usize;
            let e = if k + 1 < segs.len() { segs[k + 1].1 as usize } else { nsess };
            let (from, to) = (s.max(lo), e.min(hi));
            if from < to {
                camp[c as usize * ndays + segs[k].0][3] += (to - from) as i64;
            }
            k += 1;
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

/// Writes user_daily files (one per call), numbered in call order.
struct UserWriter {
    dir: PathBuf,
    cats: Vec<&'static [u8]>,
    files: AtomicUsize,
}

impl UserWriter {
    fn write(&self, blocks: &[&[UserRow]], exotic: &intern::Interner) -> Result<(), Error> {
        let n: usize = blocks.iter().map(|b| b.len()).sum();
        let rows = || blocks.iter().flat_map(|b| b.iter());
        let i = self.files.fetch_add(1, Relaxed);
        // Names go into one buffer, handed to Parquet as zero-copy slices.
        let mut buf = Vec::with_capacity(n * 10);
        let mut ends = Vec::with_capacity(n);
        for r in rows() {
            idkey::write(r.person, exotic, &mut buf);
            ends.push(buf.len());
        }
        let mut c = output::UserCols {
            user_id: Vec::with_capacity(n),
            day: Vec::with_capacity(n),
            sessions: Vec::with_capacity(n),
            events: Vec::with_capacity(n),
            revenue: Vec::with_capacity(n),
            top: Vec::with_capacity(n),
        };
        let mut start = 0;
        for (r, &end) in rows().zip(&ends) {
            c.user_id.push(&buf[start..end]);
            start = end;
            c.day.push(r.day);
            c.sessions.push(r.sessions as i64);
            c.events.push(r.events as i64);
            c.revenue.push(r.revenue);
            c.top.push(if r.top == NO_CATEGORY { None } else { Some(self.cats[r.top as usize]) });
        }
        output::write_user(&self.dir.join(format!("part-{i:05}.parquet")), &c)
    }
}

/// A thread's user rows are written out as soon as this many pile up, so
/// most of user_daily is written while other threads are still sessionizing.
const USER_FLUSH: usize = 1 << 18;

fn write_outputs<T>(
    out_dir: &Path,
    accs: &mut [Acc<T>],
    exotic: &intern::Interner,
    lookups: &Lookups,
    cx: &SessCtx,
    ncamp: usize,
    uw: &UserWriter,
) -> Result<(), Error> {
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&cd)?;

    // campaign_daily: sum the per-thread arrays (tiny), then one file.
    let ndays = cx.ndays;
    let slots = (ncamp + 1) * ndays;
    let mut camp = vec![[0i64; 4]; slots];
    for a in accs.iter() {
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
                cc.campaign_id.push(id);
                cc.channel.push(ch);
                cc.day.push((cx.day0 + d as i64) as i32);
                cc.sessions.push(v[0]);
                cc.purchasing.push(v[1]);
                cc.revenue.push(v[2]);
                cc.assisted.push(v[3]);
            }
        }
    }

    // user_daily: the rows are cut into ~4 pieces per thread, one file each,
    // written in parallel. Names are written into one buffer per piece and
    // handed to Parquet as zero-copy slices of it.
    let total: usize = accs.iter().map(|a| a.users.count).sum();
    let piece = total.div_ceil(4 * par::threads()).max(1 << 14);
    // Each piece: consecutive blocks of one thread's chain, ~`piece` rows.
    let mut pieces: Vec<Vec<&[UserRow]>> = Vec::new();
    for a in accs.iter_mut() {
        let mut cur: Vec<&[UserRow]> = Vec::new();
        let mut rows = 0;
        for b in a.take_users() {
            rows += b.len();
            cur.push(b);
            if rows >= piece {
                pieces.push(std::mem::take(&mut cur));
                rows = 0;
            }
        }
        if !cur.is_empty() {
            pieces.push(cur);
        }
    }
    if pieces.is_empty() && uw.files.load(Relaxed) == 0 {
        pieces.push(Vec::new()); // an empty user_daily still gets one (empty) file
    }
    // Item 0 is campaign_daily, the rest are user_daily pieces.
    let errs = par::each(pieces.len() + 1, |_| Vec::<Error>::new(), |errs, i| {
        let r = if i == 0 { output::write_campaign(&cd.join("part-00000.parquet"), &cc) } else { uw.write(&pieces[i - 1], exotic) };
        if let Err(e) = r {
            errs.push(e);
        }
    });
    match errs.into_iter().flatten().next() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
