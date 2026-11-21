//! Row parsing for events, clicks and links (rules.md "Events", "Clicks",
//! "Linked IDs"). Runs on many threads at once, one chunk at a time.

use crate::scan::scan;
use crate::input::Lookups;
use crate::idkey::{key, mix};
use crate::alloc::Blocks;
use crate::intern::Interner;
use crate::ts::{self, DAY};

pub const KIND_INVALID: u8 = 0;
pub const KIND_VIEW: u8 = 1;
pub const KIND_CART: u8 = 2;
pub const KIND_PURCHASE: u8 = 3;

/// Events are hash-partitioned on `event_id` into this many buckets while
/// they are parsed, so every copy of an event lands in the same bucket and the
/// buckets can be deduplicated independently. Each worker appends to one
/// fixed-capacity block per bucket (never reallocated); a full block is moved
/// to the bucket's list. A block is 2048 rows (80 KB): above the fixed mmap
/// threshold set in main, so each block is its own mapping and is returned to
/// the OS the moment dedup has copied it (keeping peak memory near the size
/// of the parsed rows). Only the tail of each open block is hot in cache.
pub const EV_PART_BITS: u32 = 8;
pub const EV_PARTS: usize = 1 << EV_PART_BITS;
pub const BLOCK: usize = 2048;

/// One parsed (well-formed, non-empty `event_id`) event line. Every copy of
/// an event takes part in dedup, valid or not; `kind == KIND_INVALID` marks a
/// copy that fails validation (rules.md events steps 3-4: invalid or late),
/// which is dropped only if dedup keeps it. The late check depends only on the
/// copy's own ts and ingest_ts, so it is decided here. IDs are exact 64-bit
/// keys (`idkey`). `price_kind` = price (24 bits, max 10,000,000 < 2^24) |
/// kind << 24; `cat` is only set for purchases (only they feed top_category).
#[derive(Clone, Copy)]
pub struct EvRow {
    pub eid: u64,
    pub ingest: i64,     // ingest_ts
    pub ts: i64,         // ts (only meaningful when valid)
    pub user: u64,       // user key (only when valid)
    pub price_kind: u32,
    pub cat: u32,
}

impl EvRow {
    #[inline(always)]
    pub fn part(&self) -> usize {
        (mix(self.eid) >> (64 - EV_PART_BITS)) as usize
    }
    #[inline(always)]
    pub fn kind(&self) -> u8 {
        (self.price_kind >> 24) as u8
    }
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
    pub ws: i64,
    pub we: i64,
}

pub struct Worker {
    /// Open block (and its fill) per event bucket; blocks come from a
    /// per-worker huge-page arena.
    open: Vec<*mut EvRow>,
    open_len: Vec<u32>,
    /// Full blocks per event bucket.
    full: Vec<Vec<&'static [EvRow]>>,
    arena: Blocks<EvRow>,
    pub n_events: usize,
    pub clicks: Vec<ClickRow>,
}

#[derive(Default)]
pub struct LinkSink {
    pub links: Vec<(u64, u64)>,
}

// SAFETY: the raw block pointers are owned by this worker alone.
unsafe impl Send for Worker {}

impl Worker {
    pub fn new() -> Self {
        let mut arena = Blocks::new(BLOCK);
        Worker {
            open: (0..EV_PARTS).map(|_| arena.take()).collect(),
            open_len: vec![0; EV_PARTS],
            full: (0..EV_PARTS).map(|_| Vec::new()).collect(),
            arena,
            n_events: 0,
            clicks: Vec::new(),
        }
    }

    #[inline(always)]
    fn push_event(&mut self, row: EvRow) {
        let p = row.part();
        // SAFETY: p < EV_PARTS; the open block has room (it is replaced as
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

    /// Every block of each bucket (full ones and the open one).
    pub fn into_parts(self) -> (Vec<Vec<&'static [EvRow]>>, Vec<ClickRow>) {
        let mut full = self.full;
        for p in 0..EV_PARTS {
            if self.open_len[p] > 0 {
                // SAFETY: the first open_len rows of the open block are written.
                full[p].push(unsafe { std::slice::from_raw_parts(self.open[p], self.open_len[p] as usize) });
            }
        }
        (full, self.clicks)
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

pub fn events_chunk(chunk: &'static [u8], ctx: &Ctx, w: &mut Worker) {
    scan::<7>(chunk, |f| {
        let &[eid, user, tsf, ingf, typ, product, pricef] = f;
        if eid.is_empty() {
            return;
        }
        let ek = key(eid, ctx.exotic);
        // ingest_ts is always valid F1 per the rules; if it ever is not, the
        // copy sorts last in dedup and is invalid if kept.
        let ingest = ts::parse(ingf);
        let mut row = EvRow {
            eid: ek,
            ingest: ingest.unwrap_or(i64::MAX),
            ts: 0,
            user: 0,
            price_kind: 0,
            cat: 0,
        };
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
                            if kind == KIND_PURCHASE {
                                row.cat = ctx.lookups.products.get(product).unwrap_or(ctx.lookups.unknown_category) as u32;
                            }
                            row.ts = t;
                            row.price_kind = p | (kind as u32) << 24;
                            row.user = key(user, ctx.exotic);
                        }
                    }
                }
            }
        }
        w.push_event(row);
    });
}

pub fn clicks_chunk(chunk: &'static [u8], ctx: &Ctx, w: &mut Worker) {
    let lo = ctx.ws - 7 * DAY;
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
        w.clicks.push(ClickRow {
            ts: t,
            cid: key(cid, ctx.exotic),
            user: key(user, ctx.exotic),
            campaign: c,
        });
    });
}

pub fn links_chunk(chunk: &'static [u8], ctx: &Ctx, links: &mut Vec<(u64, u64)>) {
    scan::<2>(chunk, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let a = key(f[0], ctx.exotic);
        let b = key(f[1], ctx.exotic);
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
