//! Packed per-person rows for the sessions stage.
//!
//! An event is one word: `ts - window_start` on top, then price (purchases
//! only; 0 otherwise, and a valid purchase price is >= 1) and category. A
//! click is `ts - (window_start - 7 days)` on top, then its campaign. Sorting
//! the words sorts by ts, so the sessions stage sorts plain integers.
//!
//! `u64` (32-bit offset | 24-bit price | 8-bit category) fits whenever the
//! click window spans under 2^32 ms (~49.7 days) and there are at most 256
//! categories: every dataset the generator writes. `u128` is the general
//! fallback (64-bit offset | 32 | 32), chosen at run time.

pub trait Pack: Copy + Ord + Default + Send + Sync + 'static {
    fn ev(ts_rel: u64, price: u32, cat: u32) -> Self;
    fn ev_ts(self) -> u64;
    fn ev_price(self) -> u32;
    fn ev_cat(self) -> u32;
    fn click(ts_rel: u64, campaign: u32) -> Self;
    fn click_ts(self) -> u64;
    fn click_campaign(self) -> u32;
}

impl Pack for u64 {
    #[inline(always)]
    fn ev(ts_rel: u64, price: u32, cat: u32) -> Self {
        debug_assert!(ts_rel < 1 << 32 && price < 1 << 24 && cat < 256);
        ts_rel << 32 | (price as u64) << 8 | cat as u64
    }
    #[inline(always)]
    fn ev_ts(self) -> u64 {
        self >> 32
    }
    #[inline(always)]
    fn ev_price(self) -> u32 {
        (self >> 8) as u32 & 0x00FF_FFFF
    }
    #[inline(always)]
    fn ev_cat(self) -> u32 {
        self as u32 & 0xFF
    }
    #[inline(always)]
    fn click(ts_rel: u64, campaign: u32) -> Self {
        debug_assert!(ts_rel < 1 << 32);
        ts_rel << 32 | campaign as u64
    }
    #[inline(always)]
    fn click_ts(self) -> u64 {
        self >> 32
    }
    #[inline(always)]
    fn click_campaign(self) -> u32 {
        self as u32
    }
}

impl Pack for u128 {
    #[inline(always)]
    fn ev(ts_rel: u64, price: u32, cat: u32) -> Self {
        (ts_rel as u128) << 64 | (price as u128) << 32 | cat as u128
    }
    #[inline(always)]
    fn ev_ts(self) -> u64 {
        (self >> 64) as u64
    }
    #[inline(always)]
    fn ev_price(self) -> u32 {
        (self >> 32) as u32
    }
    #[inline(always)]
    fn ev_cat(self) -> u32 {
        self as u32
    }
    #[inline(always)]
    fn click(ts_rel: u64, campaign: u32) -> Self {
        (ts_rel as u128) << 64 | campaign as u128
    }
    #[inline(always)]
    fn click_ts(self) -> u64 {
        (self >> 64) as u64
    }
    #[inline(always)]
    fn click_campaign(self) -> u32 {
        self as u32
    }
}

/// Whether the `u64` layout fits this dataset.
pub fn narrow_fits(ws: i64, we: i64, n_categories: usize, week: i64) -> bool {
    let span = (we as i128 - ws as i128 + week as i128).max(0);
    span < 1i128 << 32 && n_categories <= 256
}

#[cfg(test)]
mod tests {
    use super::*;
    fn round<T: Pack>() {
        for &(t, p, c) in &[(0u64, 0u32, 0u32), (1, 1, 1), ((1 << 32) - 1, 10_000_000, 255), (12345, 250, 7)] {
            let e = T::ev(t, p, c);
            assert_eq!((e.ev_ts(), e.ev_price(), e.ev_cat()), (t, p, c));
            let k = T::click(t, c * 1000 + 1);
            assert_eq!((k.click_ts(), k.click_campaign()), (t, c * 1000 + 1));
        }
        // ordering by ts first
        assert!(T::ev(5, 9_999_999, 255) < T::ev(6, 0, 0));
        assert!(T::click(5, u32::MAX) < T::click(6, 0));
    }
    #[test]
    fn layouts() {
        round::<u64>();
        round::<u128>();
        assert!(narrow_fits(0, 14 * 86_400_000, 21, 7 * 86_400_000));
        assert!(!narrow_fits(0, 60 * 86_400_000, 21, 7 * 86_400_000));
        assert!(!narrow_fits(0, 86_400_000, 300, 7 * 86_400_000));
    }
}
