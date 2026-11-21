//! Shopping Sessions, restructured for throughput.
//!
//!  1. links first: intern both IDs into a lock-free table and union them
//!     (concurrent union-find); name each component by its smallest key, so
//!     every linked ID's table value is its person before events are read;
//!  2. events + clicks in parallel: each event (valid or not) is scattered
//!     by event_id hash into P1 partitions; valid rows carry their person
//!     (table lookups are batched behind prefetches). Valid clicks are
//!     scattered by person into P2 partitions;
//!  3. dedup each P1 partition with a cache-resident hash table (smallest
//!     ingest_ts wins), scatter surviving valid events by person into P2;
//!  4. each P2 partition: group by person, sort, sessionize, credit, assist,
//!     aggregate;
//!  5. write Parquet in parallel.

mod csv;
mod key;
mod mem;
mod pq;
mod scan;
mod ts;
mod utab;

use key::{pack_in, Key, StaticMap};
use mem::Scatter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;
use ts::DAY;
use utab::UTab;

type Error = Box<dyn std::error::Error + Send + Sync>;

const GAP: i64 = 30 * ts::MINUTE;
const WEEK: i64 = 7 * DAY;
const CHUNK: usize = 2 << 20;
const NO_CAT: u16 = u16::MAX;
const K_INVALID: u8 = 0;
const K_PURCHASE: u8 = 3;

/// An event copy in a P1 (event_id) partition. kind 0 = invalid copy.
#[derive(Clone, Copy)]
#[repr(C)]
struct ERow {
    key: Key,
    ingest: i64,
    ts: i64,
    person: u32,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A surviving event in a P2 (person) partition.
#[derive(Clone, Copy)]
#[repr(C)]
struct SRow {
    ts: i64,
    person: u32,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A valid click in a P2 (person) partition. `cid` orders byte-wise.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRow {
    ts: i64,
    cid: Key,
    person: u32,
    campaign: u32,
}

const EB: usize = 128;
const SB: usize = 256;
const CB: usize = 128;

fn log(t0: Instant, what: &str) {
    eprintln!("[{:8.3}s] {what}", t0.elapsed().as_secs_f64());
}

#[inline(always)]
fn mix32(x: u32) -> u64 {
    let x = (x as u64 ^ 0x9E37_79B9_7F4A_7C15).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^ (x >> 31)
}

/// Run `f(thread_index)` on `n` threads and collect the results in order.
fn par<R: Send>(n: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..n).map(|t| { let f = &f; s.spawn(move || f(t)) }).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
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
    let ncpu = std::env::var("NCPU").ok().and_then(|v| v.trim().parse::<usize>().ok()).filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    match run(&data_dir, &out_dir, ncpu, t0) {
        Ok(true) => {
            log(t0, "done");
            std::process::exit(0);
        }
        Ok(false) => {
            // The user table passed its load limit: run again with a bigger one.
            let bits: u32 = env("SS_UTAB_BITS_NEXT", "0").parse().unwrap_or(0);
            eprintln!("user table overflow; retrying with 2^{bits} slots");
            let err = std::process::Command::new(std::env::current_exe().unwrap())
                .args(&args[1..])
                .env("SS_UTAB_BITS", bits.to_string())
                .status();
            std::process::exit(err.map(|s| s.code().unwrap_or(1)).unwrap_or(1));
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

struct Lookups {
    products: StaticMap,
    categories: Vec<Vec<u8>>,
    unknown: u16,
    rank: Vec<u32>,
    campaigns: StaticMap,
    campaign_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
}

fn body(d: &'static [u8]) -> &'static [u8] {
    match d.iter().position(|&c| c == b'\n') {
        Some(i) => &d[i + 1..],
        None => &[],
    }
}

fn load_lookups(dir: &Path) -> Result<Lookups, Error> {
    let pd = mem::map_file(&dir.join("products.csv"))?;
    let mut cat_ix: std::collections::HashMap<Vec<u8>, u16> = Default::default();
    let mut categories: Vec<Vec<u8>> = Vec::new();
    let mut items = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut cat = |name: &[u8], cats: &mut Vec<Vec<u8>>| -> u16 {
        *cat_ix.entry(name.to_vec()).or_insert_with(|| {
            cats.push(name.to_vec());
            assert!(cats.len() < NO_CAT as usize);
            (cats.len() - 1) as u16
        })
    };
    csv::for_each_line(body(pd), |l| {
        let mut f = [&b""[..]; 2];
        if csv::split(l, &mut f) && !f[0].is_empty() {
            let c = cat(f[1], &mut categories);
            let k = key::pack(f[0], true);
            if seen.insert(k) {
                items.push((k, c as u32));
            }
        }
    });
    let unknown = cat(b"unknown", &mut categories);
    let mut order: Vec<usize> = (0..categories.len()).collect();
    order.sort_by(|&a, &b| categories[a].cmp(&categories[b]));
    let mut rank = vec![0u32; categories.len() + 1];
    for (r, &c) in order.iter().enumerate() {
        rank[c] = r as u32;
    }
    let cd = mem::map_file(&dir.join("campaigns.csv"))?;
    let (mut ids, mut chans, mut citems) = (Vec::new(), Vec::new(), Vec::new());
    let mut cseen = std::collections::HashSet::new();
    csv::for_each_line(body(cd), |l| {
        let mut f = [&b""[..]; 2];
        if csv::split(l, &mut f) && !f[0].is_empty() {
            let k = key::pack(f[0], true);
            if cseen.insert(k) {
                citems.push((k, ids.len() as u32));
                ids.push(f[0].to_vec());
                chans.push(f[1].to_vec());
            }
        }
    });
    Ok(Lookups {
        products: StaticMap::build(&items),
        categories,
        unknown,
        rank,
        campaigns: StaticMap::build(&citems),
        campaign_ids: ids,
        channels: chans,
    })
}

fn load_manifest(dir: &Path) -> Result<(i64, i64), Error> {
    let text = std::fs::read_to_string(dir.join("manifest.json"))?;
    let get = |k: &str| -> Result<i64, Error> {
        let bad = || -> Error { format!("manifest.json: bad {k}").into() };
        let at = text.find(&format!("\"{k}\"")).ok_or_else(bad)?;
        let rest = &text[at + k.len() + 2..];
        let rest = &rest[rest.find(':').ok_or_else(bad)? + 1..];
        let rest = &rest[rest.find('"').ok_or_else(bad)? + 1..];
        let close = rest.find('"').ok_or_else(bad)?;
        ts::parse(rest[..close].as_bytes()).ok_or_else(bad)
    };
    Ok((get("window_start")?, get("window_end")?))
}

fn list_csv(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut v = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(v),
        Err(e) => return Err(e.into()),
    };
    for e in rd {
        let p = e?.path();
        if p.extension().map_or(false, |x| x == "csv") && p.is_file() {
            v.push(p);
        }
    }
    v.sort();
    Ok(v)
}

#[derive(Clone, Copy)]
struct Task {
    kind: u8, // 0 events, 1 clicks, 2 links
    data: &'static [u8],
    end: *const u8,
}
unsafe impl Send for Task {}
unsafe impl Sync for Task {}

fn chunks(kind: u8, file: &'static [u8], out: &mut Vec<Task>) {
    let b = body(file);
    let end = file.as_ptr_range().end;
    let mut s = 0;
    while s < b.len() {
        let mut e = (s + CHUNK).min(b.len());
        if e < b.len() {
            e = match b[e..].iter().position(|&c| c == b'\n') {
                Some(i) => e + i + 1,
                None => b.len(),
            };
        }
        out.push(Task { kind, data: &b[s..e], end });
        s = e;
    }
}

#[inline]
fn price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    let rest = &b[i..];
    if rest.len() > 8 {
        // still must be all digits to be "bad" vs "too big": both invalid
        return None;
    }
    let mut v = 0u32;
    let mut bad = 0u8;
    for &c in rest {
        let x = c.wrapping_sub(b'0');
        bad |= (x > 9) as u8;
        v = v.wrapping_mul(10).wrapping_add(x as u32);
    }
    if bad != 0 || v == 0 || v > 10_000_000 {
        return None;
    }
    Some(v)
}

struct Ctx<'a> {
    lk: &'a Lookups,
    ut: &'a UTab,
    ws: i64,
    we: i64,
}

/// Valid-so-far rows waiting for their user/product lookups (prefetched).
struct Pend {
    row: ERow,
    eh: u64,
    uk: Key,
    uh: u64,
    pk: Key,
    ph: u64,
}
const PB: usize = 16;

fn events_chunk(t: &Task, cx: &Ctx, out: &mut Scatter<ERow, EB>, local: &mut u32) {
    let mut pend: [std::mem::MaybeUninit<Pend>; PB] = unsafe { std::mem::MaybeUninit::uninit().assume_init() };
    let mut np = 0usize;
    let end = t.end;
    let flush = |pend: &mut [std::mem::MaybeUninit<Pend>; PB], np: &mut usize, out: &mut Scatter<ERow, EB>, local: &mut u32| {
        for i in 0..*np {
            // SAFETY: the first np entries were written.
            let p = unsafe { pend[i].assume_init_ref() };
            let slot = cx.ut.intern(p.uk, p.uh, local);
            let mut row = p.row;
            row.person = cx.ut.val(slot);
            row.cat = cx.lk.products.get(p.pk, p.ph).map_or(cx.lk.unknown, |c| c as u16);
            out.push(p.eh, row);
        }
        *np = 0;
    };
    scan::scan_lines::<7>(t.data, end, |f| {
        let &[eid, user, tsf, ingf, typ, product, pricef] = f;
        if eid.is_empty() {
            return;
        }
        let ek = pack_in(eid, end, true);
        let eh = ek.hash();
        let ingest = ts::parse(ingf);
        let kind = match typ {
            b"view" => 1u8,
            b"cart" => 2,
            b"purchase" => 3,
            _ => 0,
        };
        let pr = if kind == K_PURCHASE { price(pricef) } else { Some(0) };
        let t = ts::parse(tsf);
        let mut row = ERow { key: ek, ingest: ingest.unwrap_or(i64::MAX), ts: 0, person: 0, price: 0, cat: 0, kind: K_INVALID };
        let ok = match (ingest, t, pr) {
            (Some(ing), Some(t), Some(p)) => {
                if kind != 0 && !user.is_empty() && !product.is_empty() && t >= cx.ws && t < cx.we && ing - t <= DAY {
                    row.ts = t;
                    row.price = p;
                    row.kind = kind;
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
        if !ok {
            out.push(eh, row);
            return;
        }
        let uk = pack_in(user, end, true);
        let uh = uk.hash();
        cx.ut.prefetch(uh);
        let pk = pack_in(product, end, false);
        let ph = pk.hash();
        cx.lk.products.prefetch(ph);
        pend[np].write(Pend { row, eh, uk, uh, pk, ph });
        np += 1;
        if np == PB {
            flush(&mut pend, &mut np, out, local);
        }
    });
    flush(&mut pend, &mut np, out, local);
}

fn clicks_chunk(t: &Task, cx: &Ctx, out: &mut Scatter<CRow, CB>, local: &mut u32) {
    let lo = cx.ws - WEEK;
    let end = t.end;
    let mut pend: Vec<(CRow, Key, u64)> = Vec::with_capacity(PB);
    let flush = |pend: &mut Vec<(CRow, Key, u64)>, out: &mut Scatter<CRow, CB>, local: &mut u32| {
        for &(mut r, uk, uh) in pend.iter() {
            let slot = cx.ut.intern(uk, uh, local);
            r.person = cx.ut.val(slot);
            out.push(mix32(r.person), r);
        }
        pend.clear();
    };
    scan::scan_lines::<4>(t.data, end, |f| {
        let &[cid, user, campaign, tsf] = f;
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(ts) = ts::parse(tsf) else { return };
        if ts < lo || ts >= cx.we {
            return;
        }
        let ck = pack_in(campaign, end, false);
        let Some(c) = cx.lk.campaigns.get(ck, ck.hash()) else { return };
        let uk = pack_in(user, end, true);
        let uh = uk.hash();
        cx.ut.prefetch(uh);
        let r = CRow { ts, cid: pack_in(cid, end, true), person: 0, campaign: c };
        pend.push((r, uk, uh));
        if pend.len() == PB {
            flush(&mut pend, out, local);
        }
    });
    flush(&mut pend, out, local);
}

fn links_chunk(t: &Task, ut: &UTab, local: &mut u32, mine: &mut Vec<u32>) {
    let end = t.end;
    let mut pend: Vec<(Key, u64, Key, u64)> = Vec::with_capacity(PB);
    let mut flush = |pend: &mut Vec<(Key, u64, Key, u64)>, local: &mut u32| {
        for &(a, ha, b, hb) in pend.iter() {
            let (sa, na) = ut.intern2(a, ha, local);
            if na {
                mine.push(sa);
            }
            if a != b {
                let (sb, nb) = ut.intern2(b, hb, local);
                if nb {
                    mine.push(sb);
                }
                ut.union(sa, sb);
            }
        }
        pend.clear();
    };
    scan::scan_lines::<2>(t.data, end, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let a = pack_in(f[0], end, true);
        let b = pack_in(f[1], end, true);
        let (ha, hb) = (a.hash(), b.hash());
        ut.prefetch(ha);
        ut.prefetch(hb);
        pend.push((a, ha, b, hb));
        if pend.len() == PB {
            flush(&mut pend, local);
        }
    });
    flush(&mut pend, local);
}

fn run(data_dir: &Path, out_dir: &Path, nt: usize, t0: Instant) -> Result<bool, Error> {
    let (ws, we) = load_manifest(data_dir)?;
    let lk = load_lookups(data_dir)?;

    // ---- map and chunk ------------------------------------------------------
    let mut files: Vec<(u8, PathBuf)> = Vec::new();
    for (k, d) in [(0u8, "events"), (1, "clicks"), (2, "links")] {
        for p in list_csv(&data_dir.join(d))? {
            files.push((k, p));
        }
    }
    let mut link_tasks = Vec::new();
    let mut main_tasks = Vec::new();
    let mut bytes = [0usize; 3];
    for (k, p) in &files {
        let m = mem::map_file(p)?;
        bytes[*k as usize] += m.len();
        chunks(*k, m, if *k == 2 { &mut link_tasks } else { &mut main_tasks });
    }
    // Rough row counts (an event line is ~95 bytes, a click ~50).
    let ev_rows = bytes[0] / 90 + 1024;
    let cl_rows = bytes[1] / 45 + 1024;
    let bits_env: u32 = std::env::var("SS_UTAB_BITS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let est_ids = (ev_rows + cl_rows) / 12 + bytes[2] / 10 + 4096;
    let mut ubits = bits_env.max((est_ids.next_power_of_two().trailing_zeros()).max(16));
    if bits_env == 0 {
        if let Some(b) = std::env::var("SS_UTAB_TEST_BITS").ok().and_then(|v| v.parse().ok()) {
            ubits = b; // testing the overflow retry
        }
    }
    std::env::set_var("SS_UTAB_BITS_NEXT", (ubits + 2).to_string());
    let ut = UTab::new(ubits);
    // P1 / P2 partitions of ~8K events each.
    let pbits = ((ev_rows / 8192).max(1).next_power_of_two().trailing_zeros()).clamp(4, 13);
    log(t0, &format!("mapped {} files, {:.1} MB; utab 2^{ubits}, parts 2^{pbits}", files.len(), bytes.iter().sum::<usize>() as f64 / 1e6));

    // ---- 1. links -------------------------------------------------------------
    let next = AtomicUsize::new(0);
    let linked: Vec<Vec<u32>> = par(nt, |_| {
        let mut local = 0u32;
        let mut mine = Vec::new();
        loop {
            let i = next.fetch_add(1, Relaxed);
            let Some(t) = link_tasks.get(i) else { break };
            links_chunk(t, &ut, &mut local, &mut mine);
        }
        ut.flush(&mut local);
        mine
    });
    if ut.overflow.load(Relaxed) {
        return Ok(false);
    }
    par(nt, |t| ut.offer_names(&linked[t]));
    par(nt, |t| ut.set_persons(&linked[t]));
    log(t0, &format!("links: {} linked ids", linked.iter().map(Vec::len).sum::<usize>()));

    // ---- 2. events and clicks -------------------------------------------------
    // Biggest chunks first is implicit (all ~CHUNK); events and clicks interleave.
    let cx = Ctx { lk: &lk, ut: &ut, ws, we };
    let next = AtomicUsize::new(0);
    let ev_region = (ev_rows * std::mem::size_of::<ERow>() / nt) * 5 / 4 + (16 << 20);
    let cl_region = (cl_rows * std::mem::size_of::<CRow>() / nt) * 5 / 4 + (8 << 20);
    let parsed: Vec<(Scatter<ERow, EB>, Scatter<CRow, CB>)> = par(nt, |_| {
        let mut ev = Scatter::<ERow, EB>::new(pbits, ev_region);
        let mut cl = Scatter::<CRow, CB>::new(pbits, cl_region);
        let mut local = 0u32;
        loop {
            if ut.overflow.load(Relaxed) {
                break;
            }
            let i = next.fetch_add(1, Relaxed);
            let Some(t) = main_tasks.get(i) else { break };
            if t.kind == 0 {
                events_chunk(t, &cx, &mut ev, &mut local);
            } else {
                clicks_chunk(t, &cx, &mut cl, &mut local);
            }
        }
        ut.flush(&mut local);
        (ev, cl)
    });
    if ut.overflow.load(Relaxed) {
        return Ok(false);
    }
    let n_ev: usize = parsed.iter().map(|p| p.0.rows).sum();
    let n_cl: usize = parsed.iter().map(|p| p.1.rows).sum();
    log(t0, &format!("parsed: {n_ev} event rows, {n_cl} valid clicks, {} ids", ut.inserted.load(Relaxed)));

    // ---- 3. dedup per P1 partition --------------------------------------------
    let nparts = 1usize << pbits;
    let next = AtomicUsize::new(0);
    let s_region = (n_ev * std::mem::size_of::<SRow>() / nt) * 5 / 4 + (8 << 20);
    let survivors: Vec<Scatter<SRow, SB>> = par(nt, |_| {
        let mut out = Scatter::<SRow, SB>::new(pbits, s_region);
        let mut table: Vec<(u64, *const ERow)> = Vec::new();
        loop {
            let p = next.fetch_add(1, Relaxed);
            if p >= nparts {
                break;
            }
            let n: usize = parsed.iter().map(|w| w.0.count(p)).sum();
            let cap = (n * 2).next_power_of_two().max(64);
            table.clear();
            table.resize(cap, (0, std::ptr::null()));
            let mask = cap - 1;
            for w in &parsed {
                for sl in w.0.slices(p) {
                    for r in sl {
                        let h = r.key.hash();
                        let mut i = (h as usize) & mask;
                        loop {
                            let e = &mut table[i];
                            if e.1.is_null() {
                                *e = (h, r as *const ERow);
                                break;
                            }
                            // SAFETY: table pointers point at rows in the scatter arenas.
                            let o = unsafe { &*e.1 };
                            if e.0 == h && o.key == r.key {
                                if r.ingest < o.ingest {
                                    e.1 = r;
                                }
                                break;
                            }
                            i = (i + 1) & mask;
                        }
                    }
                }
            }
            for e in &table {
                if e.1.is_null() {
                    continue;
                }
                let r = unsafe { &*e.1 };
                if r.kind != K_INVALID {
                    out.push(mix32(r.person), SRow { ts: r.ts, person: r.person, price: r.price, cat: r.cat, kind: r.kind });
                }
            }
        }
        out
    });
    let n_surv: usize = survivors.iter().map(|s| s.rows).sum();
    log(t0, &format!("dedup: {n_surv} surviving events"));

    // ---- 4. per P2 partition: sessions, attribution, aggregation ------------------
    let day0 = ws.div_euclid(DAY);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY) - day0 + 1) as usize } else { 0 };
    let ncamp = lk.campaign_ids.len();
    let sctx = SessCtx { day0, ndays, organic: ncamp as u32, rank: &lk.rank };
    let mut order: Vec<(usize, usize)> = (0..nparts)
        .map(|q| (survivors.iter().map(|s| s.count(q)).sum::<usize>() + parsed.iter().map(|w| w.1.count(q)).sum::<usize>() / 4, q))
        .collect();
    order.sort_unstable_by(|a, b| b.cmp(a)); // biggest first
    let next = AtomicUsize::new(0);
    let accs: Vec<Acc> = par(nt, |_| {
        let mut acc = Acc::new((ncamp + 1) * ndays);
        let mut g = Group::default();
        loop {
            let i = next.fetch_add(1, Relaxed);
            let Some(&(_, q)) = order.get(i) else { break };
            g.load(q, &survivors, &parsed);
            for (pi, &person) in g.persons.iter().enumerate() {
                let (a, b) = (g.ev_off[pi] as usize, g.ev_off[pi + 1] as usize);
                let (ca, cb) = (g.cl_off[pi] as usize, g.cl_off[pi + 1] as usize);
                let evs = &mut g.evs[a..b];
                evs.sort_unstable_by_key(|e| e.ts);
                let cls = &mut g.cls[ca..cb];
                cls.sort_unstable_by(|x, y| x.ts.cmp(&y.ts).then_with(|| x.cid.cmp(&y.cid)));
                process_person(person, evs, cls, &mut acc, &sctx);
            }
        }
        acc
    });
    log(t0, "sessions done");

    // ---- 5. output ------------------------------------------------------------------
    write_outputs(out_dir, &accs, &ut, &lk, &sctx, ncamp, nt)?;
    log(t0, "outputs written");
    Ok(true)
}

/// Per-thread scratch for one P2 partition, grouped by person.
#[derive(Default)]
struct Group {
    persons: Vec<u32>,
    ev_off: Vec<u32>,
    cl_off: Vec<u32>,
    evs: Vec<SRow>,
    cls: Vec<CRow>,
    map: Vec<(u32, u32)>, // (person + 1, local index)
    pix_e: Vec<u32>,
}

impl Group {
    fn load(&mut self, q: usize, surv: &[Scatter<SRow, SB>], parsed: &[(Scatter<ERow, EB>, Scatter<CRow, CB>)]) {
        self.persons.clear();
        self.pix_e.clear();
        let n: usize = surv.iter().map(|s| s.count(q)).sum();
        let cap = (n * 2).next_power_of_two().max(64);
        self.map.clear();
        self.map.resize(cap, (0, 0));
        let mask = cap - 1;
        let mut counts: Vec<u32> = std::mem::take(&mut self.ev_off);
        counts.clear();
        // pass 1: local person index for every event, counts
        for s in surv {
            for sl in s.slices(q) {
                for r in sl {
                    let mut i = (mix32(r.person) as usize) & mask;
                    let ix = loop {
                        let e = &mut self.map[i];
                        if e.0 == 0 {
                            *e = (r.person + 1, self.persons.len() as u32);
                            self.persons.push(r.person);
                            counts.push(0);
                            break e.1;
                        }
                        if e.0 == r.person + 1 {
                            break e.1;
                        }
                        i = (i + 1) & mask;
                    };
                    counts[ix as usize] += 1;
                    self.pix_e.push(ix);
                }
            }
        }
        let np = self.persons.len();
        // exclusive prefix sums -> offsets (np + 1) and cursors
        let mut off = Vec::with_capacity(np + 1);
        let mut acc = 0u32;
        for c in counts.iter_mut() {
            off.push(acc);
            let n = *c;
            *c = acc;
            acc += n;
        }
        off.push(acc);
        self.evs.clear();
        self.evs.reserve(n);
        // SAFETY: every slot in 0..n is written exactly once below.
        unsafe { self.evs.set_len(n) };
        let mut k = 0;
        for s in surv {
            for sl in s.slices(q) {
                for r in sl {
                    let ix = self.pix_e[k] as usize;
                    k += 1;
                    self.evs[counts[ix] as usize] = *r;
                    counts[ix] += 1;
                }
            }
        }
        self.ev_off = off;
        // clicks of persons who have events here
        let mut ccount = vec![0u32; np + 1];
        let mut cix: Vec<u32> = Vec::new();
        for w in parsed {
            for sl in w.1.slices(q) {
                for r in sl {
                    let mut i = (mix32(r.person) as usize) & mask;
                    let ix = loop {
                        let e = &self.map[i];
                        if e.0 == 0 {
                            break u32::MAX;
                        }
                        if e.0 == r.person + 1 {
                            break e.1;
                        }
                        i = (i + 1) & mask;
                    };
                    cix.push(ix);
                    if ix != u32::MAX {
                        ccount[ix as usize] += 1;
                    }
                }
            }
        }
        let mut coff = Vec::with_capacity(np + 1);
        let mut acc = 0u32;
        for c in ccount.iter_mut().take(np) {
            coff.push(acc);
            let n = *c;
            *c = acc;
            acc += n;
        }
        coff.push(acc);
        self.cls.clear();
        self.cls.reserve(acc as usize);
        unsafe { self.cls.set_len(acc as usize) };
        let mut k = 0;
        for w in parsed {
            for sl in w.1.slices(q) {
                for r in sl {
                    let ix = cix[k];
                    k += 1;
                    if ix != u32::MAX {
                        self.cls[ccount[ix as usize] as usize] = *r;
                        ccount[ix as usize] += 1;
                    }
                }
            }
        }
        self.cl_off = coff;
    }
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

struct Acc {
    camp: Vec<[i64; 4]>,
    users: Vec<UserRow>,
    starts: Vec<i64>,
    segs: Vec<(usize, u32)>,
    cats: Vec<(u16, i64)>,
    tmp: Vec<(u32, i64)>,
}

impl Acc {
    fn new(slots: usize) -> Self {
        Acc { camp: vec![[0; 4]; slots], users: Vec::new(), starts: Vec::new(), segs: Vec::new(), cats: Vec::new(), tmp: Vec::new() }
    }
}

fn top_category(cats: &[(u16, i64)], rank: &[u32]) -> u16 {
    let mut best = NO_CAT;
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
fn process_person(p: u32, evs: &[SRow], cls: &[CRow], acc: &mut Acc, cx: &SessCtx) {
    let Acc { camp, users, starts, segs, cats, tmp } = acc;
    starts.clear();
    segs.clear();
    cats.clear();
    let ndays = cx.ndays;
    let mut cur_day = i64::MIN;
    let (mut d_s, mut d_e, mut d_r) = (0u32, 0u32, 0i64);
    let n = evs.len();
    let mut i = 0;
    let mut cp = 0usize; // clicks with ts <= current start
    while i < n {
        let start = evs[i].ts;
        let mut j = i + 1;
        while j < n && evs[j].ts - evs[j - 1].ts <= GAP {
            j += 1;
        }
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if d_s > 0 {
                users.push(UserRow { person: p, day: cur_day as i32, sessions: d_s, events: d_e, revenue: d_r, top: top_category(cats, cx.rank) });
            }
            cats.clear();
            d_s = 0;
            d_e = 0;
            d_r = 0;
            cur_day = day;
            segs.push(((day - cx.day0) as usize, starts.len() as u32));
        }
        let mut rev = 0i64;
        let mut purchased = false;
        for e in &evs[i..j] {
            if e.kind == K_PURCHASE {
                rev += e.price as i64;
                purchased = true;
                match cats.iter_mut().find(|(c, _)| *c == e.cat) {
                    Some(x) => x.1 += e.price as i64,
                    None => cats.push((e.cat, e.price as i64)),
                }
            }
        }
        d_s += 1;
        d_e += (j - i) as u32;
        d_r += rev;
        // credit: the latest click with ts <= start (within 7 days); ties on
        // ts go to the smallest click_id, the first of the equal-ts run.
        while cp < cls.len() && cls[cp].ts <= start {
            cp += 1;
        }
        let c = if cp == 0 || start - cls[cp - 1].ts > WEEK {
            cx.organic
        } else {
            let t = cls[cp - 1].ts;
            let mut k = cp - 1;
            while k > 0 && cls[k - 1].ts == t {
                k -= 1;
            }
            cls[k].campaign
        };
        let slot = &mut camp[c as usize * ndays + (day - cx.day0) as usize];
        slot[0] += 1;
        slot[1] += purchased as i64;
        slot[2] += rev;
        starts.push(start);
        i = j;
    }
    if d_s > 0 {
        users.push(UserRow { person: p, day: cur_day as i32, sessions: d_s, events: d_e, revenue: d_r, top: top_category(cats, cx.rank) });
    }
    if cls.is_empty() {
        return;
    }
    // Assists: per campaign, merge the click intervals [ts, ts + 7d]; each
    // session whose start falls in a merged interval is assisted once.
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
        // first day segment that can hold `lo`
        let mut k = segs.partition_point(|s| (s.1 as usize) <= lo).saturating_sub(1);
        while k < segs.len() {
            let s = segs[k].1 as usize;
            if s >= hi {
                break;
            }
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

fn write_outputs(out_dir: &Path, accs: &[Acc], ut: &UTab, lk: &Lookups, cx: &SessCtx, ncamp: usize, nt: usize) -> Result<(), Error> {
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;
    let ndays = cx.ndays;
    let errs: Mutex<Vec<Error>> = Mutex::new(Vec::new());
    let total: usize = accs.iter().map(|a| a.users.len()).sum();
    par(nt + 1, |t| {
        let r: Result<(), Error> = (|| {
            if t == nt {
                // campaign_daily
                let mut camp = vec![[0i64; 4]; (ncamp + 1) * ndays];
                for a in accs {
                    for (x, y) in camp.iter_mut().zip(&a.camp) {
                        for f in 0..4 {
                            x[f] += y[f];
                        }
                    }
                }
                let (mut id, mut ch, mut day, mut s, mut ps, mut rv, mut asd) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
                for c in 0..=ncamp {
                    let (i, h): (&[u8], &[u8]) = if c == ncamp { (b"organic", b"organic") } else { (&lk.campaign_ids[c], &lk.channels[c]) };
                    for d in 0..ndays {
                        let v = camp[c * ndays + d];
                        if v[0] > 0 || v[3] > 0 {
                            id.push(i);
                            ch.push(h);
                            day.push((cx.day0 + d as i64) as i32);
                            s.push(v[0]);
                            ps.push(v[1]);
                            rv.push(v[2]);
                            asd.push(v[3]);
                        }
                    }
                }
                use pq::{Col, Field};
                return pq::write(&cd.join("part-00000.parquet"), "campaign_daily", &[
                    Field { name: "campaign_id", col: Col::Str(&id), converted: Some(0) },
                    Field { name: "channel", col: Col::Str(&ch), converted: Some(0) },
                    Field { name: "day", col: Col::I32(&day), converted: Some(6) },
                    Field { name: "sessions", col: Col::I64(&s), converted: None },
                    Field { name: "purchasing_sessions", col: Col::I64(&ps), converted: None },
                    Field { name: "revenue_cents", col: Col::I64(&rv), converted: None },
                    Field { name: "assisted_sessions", col: Col::I64(&asd), converted: None },
                ]);
            }
            let rows = &accs[t].users;
            if rows.is_empty() && !(total == 0 && t == 0) {
                return Ok(());
            }
            let mut names: Vec<u8> = Vec::with_capacity(rows.len() * 12);
            let mut ends: Vec<usize> = Vec::with_capacity(rows.len());
            for r in rows {
                ut.key(r.person).bytes(&mut names);
                ends.push(names.len());
            }
            let mut uid: Vec<&[u8]> = Vec::with_capacity(rows.len());
            let mut s = 0;
            for &e in &ends {
                uid.push(&names[s..e]);
                s = e;
            }
            let day: Vec<i32> = rows.iter().map(|r| r.day).collect();
            let ses: Vec<i64> = rows.iter().map(|r| r.sessions as i64).collect();
            let evn: Vec<i64> = rows.iter().map(|r| r.events as i64).collect();
            let rev: Vec<i64> = rows.iter().map(|r| r.revenue).collect();
            let top: Vec<Option<&[u8]>> = rows.iter().map(|r| if r.top == NO_CAT { None } else { Some(&lk.categories[r.top as usize][..]) }).collect();
            use pq::{Col, Field};
            pq::write(&ud.join(format!("part-{t:05}.parquet")), "user_daily", &[
                Field { name: "user_id", col: Col::Str(&uid), converted: Some(0) },
                Field { name: "day", col: Col::I32(&day), converted: Some(6) },
                Field { name: "sessions", col: Col::I64(&ses), converted: None },
                Field { name: "events", col: Col::I64(&evn), converted: None },
                Field { name: "revenue_cents", col: Col::I64(&rev), converted: None },
                Field { name: "top_category", col: Col::OptStr(&top), converted: Some(0) },
            ])
        })();
        if let Err(e) = r {
            errs.lock().unwrap().push(e);
        }
    });
    if let Some(e) = errs.into_inner().unwrap().pop() {
        return Err(e);
    }
    Ok(())
}
