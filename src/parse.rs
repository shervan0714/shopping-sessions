//! Row parsing for events, clicks and links (rules.md "Events", "Clicks",
//! "Linked IDs"). Runs on many threads at once, one chunk at a time.

use crate::scan::scan;
use crate::input::Lookups;
use crate::idkey::{key, mix};
use crate::alloc::{Blocks, Parts, POOL_BLOCK};
use crate::pack::Pack;
use crate::people::{pp, ppart, LinkFilter, PeopleSlot};
use crate::intern::{IdLog, Interner};
use crate::ts::{self, DAY};

pub const KIND_INVALID: u8 = 0;
pub const KIND_VIEW: u8 = 1;
pub const KIND_CART: u8 = 2;
pub const KIND_PURCHASE: u8 = 3;

/// Every copy of an event writes two things while it is parsed:
///
/// - a small dedup record (`Cand`: event_id key, ingest_ts, where its
///   payload is), hash-partitioned on `event_id` into this many buckets, so
///   every copy of an event lands in the same bucket and the buckets can be
///   deduplicated independently;
/// - if the copy is valid, its payload (`Surv`: user key, packed event),
///   written once: straight into its person partition when the user is in no
///   link (then the user is the person), or else into this worker's deferred
///   list, resolved to a person after dedup (main.rs).
///
/// Dedup then reads only the records and marks the payloads of the copies it
/// drops as dead (`DEAD`); nothing is copied. Each worker appends to one
/// fixed-capacity block per bucket (never reallocated); a full block is moved
/// to the bucket's list. Only the tail of each open block is hot in cache.
/// The count is sized from the input before parsing (`set_ev_bits`): about
/// 40K rows per bucket, so one dedup table per bucket stays cache-resident.
static EV_PART_BITS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(8);
pub fn set_ev_bits(b: u32) {
    EV_PART_BITS.store(b, std::sync::atomic::Ordering::Relaxed);
}
/// The number of event buckets.
pub fn ev_parts() -> usize {
    1 << EV_PART_BITS.load(std::sync::atomic::Ordering::Relaxed)
}
pub const BLOCK: usize = 2048;

/// The dedup record of one parsed (well-formed, non-empty `event_id`) event
/// line. Every copy of an event takes part in dedup, valid or not; a copy that
/// fails validation (rules.md events steps 3-4: invalid or late) has no
/// payload (`pay` null), so it is dropped if dedup keeps it. The late check
/// depends only on the copy's own ts and ingest_ts, so it is decided here.
/// `eid` is an exact 64-bit key (`idkey`).
#[derive(Clone, Copy)]
pub struct Cand {
    pub eid: u64,
    /// ingest_ts (i64::MAX if unparsable: such a copy loses every tie).
    pub ingest: i64,
    /// The `person` field of this copy's payload, or null if invalid.
    pub pay: *mut u64,
}

// SAFETY: `pay` is written only by the dedup of the record's own bucket
// (one thread), and read by no one before dedup has finished.
unsafe impl Send for Cand {}
unsafe impl Sync for Cand {}

/// The `person` of a payload whose copy dedup dropped. Never a key: packed
/// keys use 60 bits, exotic ones at most 63.
pub const DEAD: u64 = u64::MAX;

/// A valid event copy's payload: its person (or, in a deferred list, its
/// user) key and the packed event (pack.rs: ts offset, price, category).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Surv<T> {
    pub person: u64,
    pub w: T,
}

/// A valid click (rules.md clicks step 3). Clicks are not deduplicated: copies
/// of a click have identical values, and neither crediting (latest ts, then
/// smallest click_id) nor assisting (distinct campaigns) can change when an
/// identical row appears twice.
#[derive(Clone, Copy)]
pub struct ClickRow {
    pub ts: i64,
    pub cid: u64,
    pub user: u64,
    pub campaign: u32,
}

pub struct Ctx<'a> {
    pub lookups: &'a Lookups,
    pub exotic: &'a Interner,
    /// Every ID that appears in a link (a superset filter): an event whose
    /// user misses it is its own person.
    pub linked: &'a LinkFilter,
    /// Set by the people thread; clicks (parsed after links and events)
    /// are resolved to their person right away.
    pub people: &'a PeopleSlot,
    pub ws: i64,
    pub we: i64,
}

pub struct Worker<T: Pack> {
    /// Open block (and its fill) per event bucket; blocks come from a
    /// per-worker huge-page arena.
    open: Vec<*mut Cand>,
    open_len: Vec<u32>,
    /// Full blocks per event bucket.
    full: Vec<Vec<&'static [Cand]>>,
    arena: Blocks<Cand>,
    /// Payloads of valid copies whose user is in no link, by person partition.
    pub surv: Parts<Surv<T>>,
    /// Payloads of valid copies whose user may be linked (one list).
    pub deferred: Parts<Surv<T>>,
    /// 64 - log2(bucket count): the bucket is the top bits of mix(eid).
    shift: u32,
    pub n_events: usize,
    /// Valid clicks, already in person partitions.
    pub clicks: Parts<ClickRow>,
    pub n_clicks: usize,
    /// The exotic IDs this worker saw (intern.rs).
    pub ids: IdLog,
}

// SAFETY: the raw block pointers are owned by this worker alone.
unsafe impl<T: Pack> Send for Worker<T> {}

impl<T: Pack> Worker<T> {
    pub fn new() -> Self {
        let mut arena = Blocks::new(BLOCK);
        let n = ev_parts();
        Worker {
            open: (0..n).map(|_| arena.take()).collect(),
            open_len: vec![0; n],
            full: (0..n).map(|_| Vec::new()).collect(),
            arena,
            surv: Parts::new(pp(), POOL_BLOCK / std::mem::size_of::<Surv<T>>()),
            deferred: Parts::new(1, POOL_BLOCK / std::mem::size_of::<Surv<T>>()),
            shift: 64 - n.trailing_zeros(),
            n_events: 0,
            clicks: Parts::new(pp(), POOL_BLOCK / std::mem::size_of::<ClickRow>()),
            n_clicks: 0,
            ids: IdLog::new(),
        }
    }

    #[inline(always)]
    fn push_event(&mut self, row: Cand) {
        let p = if self.shift == 64 { 0 } else { (mix(row.eid) >> self.shift) as usize };
        // SAFETY: p < the bucket count; the open block has room (it is replaced as
        // soon as it fills).
        unsafe {
            let n = self.open_len.get_unchecked_mut(p);
            let b = *self.open.get_unchecked(p);
            b.add(*n as usize).write(row);
            *n += 1;
            if *n as usize == BLOCK {
                self.full[p].push(std::slice::from_raw_parts(b, BLOCK));
                *self.open.get_unchecked_mut(p) = self.arena.take();
                *n = 0;
            }
        }
        self.n_events += 1;
    }

    /// Every dedup-record block of each bucket (full ones and the open one),
    /// the click blocks, the payloads (still open: dedup writes `DEAD` into
    /// them through `Cand::pay`) and the ID log.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (Vec<Vec<&'static [Cand]>>, Vec<Vec<&'static [ClickRow]>>, Parts<Surv<T>>, Parts<Surv<T>>, IdLog) {
        let mut full = self.full;
        for p in 0..self.open.len() {
            if self.open_len[p] > 0 {
                // SAFETY: the first open_len rows of the open block are written.
                full[p].push(unsafe { std::slice::from_raw_parts(self.open[p], self.open_len[p] as usize) });
            }
        }
        (full, self.clicks.finish(), self.surv, self.deferred, self.ids)
    }
}

/// `price_cents` of a purchase: digits only, value 1..=10,000,000.
#[inline]
fn price(b: &[u8]) -> Option<u32> {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let first = b.iter().position(|&c| c != b'0').unwrap_or(b.len());
    let rest = &b[first..];
    if rest.len() > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in rest {
        v = v * 10 + (c - b'0') as u32;
    }
    (1..=10_000_000).contains(&v).then_some(v)
}

pub fn events_chunk<T: Pack>(chunk: &[u8], ctx: &Ctx, w: &mut Worker<T>) {
    // Once the people table is built (on its own thread, usually early in
    // parsing), a valid copy whose user may be linked is resolved to its
    // person right here, in batches with the lookups prefetched (like
    // clicks); before that, it goes to the deferred list.
    let people = ctx.people.get();
    const BATCH: usize = 64;
    let mut batch = [(Cand { eid: 0, ingest: 0, pay: std::ptr::null_mut() }, Surv { person: 0, w: T::default() }); BATCH];
    let mut nb = 0usize;
    let flush = |batch: &[(Cand, Surv<T>)], w: &mut Worker<T>| {
        let Some(people) = people else { return };
        for x in batch {
            people.prefetch_filter(x.1.person);
        }
        for x in batch {
            people.prefetch_slot(x.1.person);
        }
        for &(mut c, s) in batch {
            let person = people.person(s.person);
            c.pay = w.surv.push_ptr(ppart(person), Surv { person, w: s.w }) as *mut u64;
            w.push_event(c);
        }
    };
    scan::<7>(chunk, |f| {
        let &[eid, user, tsf, ingf, typ, product, pricef] = f;
        if eid.is_empty() {
            return;
        }
        let ek = key::<false>(eid, ctx.exotic, &mut w.ids);
        // ingest_ts is always valid F1 per the rules; if it ever is not, the
        // copy sorts last in dedup and is invalid if kept.
        let ingest = ts::parse(ingf);
        let mut row = Cand { eid: ek, ingest: ingest.unwrap_or(i64::MAX), pay: std::ptr::null_mut() };
        if let Some(ing) = ingest {
            if !user.is_empty() && !product.is_empty() {
                let kind = match typ {
                    b"view" => KIND_VIEW,
                    b"cart" => KIND_CART,
                    b"purchase" => KIND_PURCHASE,
                    _ => KIND_INVALID,
                };
                let p = if kind == KIND_PURCHASE { price(pricef) } else { Some(0) };
                if kind != KIND_INVALID {
                    if let (Some(t), Some(p)) = (ts::parse(tsf), p) {
                        // In the event window, and not late (ingest - ts over 24 h).
                        if t >= ctx.ws && t < ctx.we && ing - t <= DAY {
                            let cat = if kind == KIND_PURCHASE {
                                ctx.lookups.products.get(product).unwrap_or(ctx.lookups.unknown_category) as u32
                            } else {
                                0
                            };
                            let u = key::<true>(user, ctx.exotic, &mut w.ids);
                            let s = Surv { person: u, w: T::ev((t - ctx.ws) as u64, p, cat) };
                            if !ctx.linked.maybe(u) {
                                // In no link: the user is the person.
                                row.pay = w.surv.push_ptr(ppart(u), s) as *mut u64;
                            } else if people.is_some() {
                                batch[nb] = (row, s);
                                nb += 1;
                                if nb == BATCH {
                                    flush(&batch, w);
                                    nb = 0;
                                }
                                return;
                            } else {
                                row.pay = w.deferred.push_ptr(0, s) as *mut u64;
                            }
                        }
                    }
                }
            }
        }
        w.push_event(row);
    });
    flush(&batch[..nb], w);
}

pub fn clicks_chunk<T: Pack>(chunk: &[u8], ctx: &Ctx, w: &mut Worker<T>) {
    let lo = ctx.ws - 7 * DAY;
    let people = ctx.people.wait();
    // Person lookups are random reads: resolve clicks in batches, with the
    // filter words and then the table slots prefetched for the whole batch.
    const BATCH: usize = 64;
    let mut buf = [ClickRow { ts: 0, cid: 0, user: 0, campaign: 0 }; BATCH];
    let mut nb = 0usize;
    let flush = |buf: &[ClickRow], w: &mut Worker<T>| {
        for c in buf {
            people.prefetch_filter(c.user);
        }
        for c in buf {
            people.prefetch_slot(c.user);
        }
        for c in buf {
            let person = people.person(c.user);
            w.clicks.push(ppart(person), ClickRow { user: person, ..*c });
        }
        w.n_clicks += buf.len();
    };
    scan::<4>(chunk, |f| {
        let &[cid, user, campaign, tsf] = f;
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(t) = ts::parse(tsf) else { return };
        if t < lo || t >= ctx.we {
            return;
        }
        let Some(c) = ctx.lookups.campaigns.get(campaign) else { return };
        let cid = key::<true>(cid, ctx.exotic, &mut w.ids);
        let user = key::<true>(user, ctx.exotic, &mut w.ids);
        buf[nb] = ClickRow { ts: t, cid, user, campaign: c };
        nb += 1;
        if nb == BATCH {
            flush(&buf, w);
            nb = 0;
        }
    });
    flush(&buf[..nb], w);
}

/// Links of one chunk (parsed straight from the mapping) into `links`.
pub fn links_chunk(chunk: &[u8], ctx: &Ctx, links: &mut Vec<(u64, u64)>, log: &mut IdLog) {
    log.set_chunk(chunk.as_ptr(), chunk.as_ptr());
    scan::<2>(chunk, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let a = key::<true>(f[0], ctx.exotic, log);
        let b = key::<true>(f[1], ctx.exotic, log);
        if a != b {
            links.push((a, b));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::price;
    #[test]
    fn prices() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        assert_eq!(price(b"000000000010000000"), Some(10_000_000));
        assert_eq!(price(b"10000001"), None);
        assert_eq!(price(b"0"), None);
        assert_eq!(price(b""), None);
        assert_eq!(price(b"-5"), None);
        assert_eq!(price(b"12.5"), None);
        assert_eq!(price(b"1000\r"), None);
        assert_eq!(price("２５０".as_bytes()), None);
    }
}
