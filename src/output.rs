//! Parquet output (via the small PLAIN writer in pq.rs): no dictionary, no
//! compression, no statistics. `day` is INT32 annotated DATE.

use crate::pq::{self, Col, Field};
use std::path::Path;

pub type Res = pq::Res;

pub struct UserCols<'a> {
    pub user_id: Vec<&'a [u8]>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue: Vec<i64>,
    /// One entry per row; None is null.
    pub top: Vec<Option<&'a [u8]>>,
}

pub struct CampaignCols<'a> {
    pub campaign_id: Vec<&'a [u8]>,
    pub channel: Vec<&'a [u8]>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub purchasing: Vec<i64>,
    pub revenue: Vec<i64>,
    pub assisted: Vec<i64>,
}

pub fn write_user(path: &Path, c: &UserCols) -> Res {
    pq::write(path, "user_daily", &[
        Field { name: "user_id", col: Col::Str(&c.user_id), converted: Some(0) },
        Field { name: "day", col: Col::I32(&c.day), converted: Some(6) },
        Field { name: "sessions", col: Col::I64(&c.sessions), converted: None },
        Field { name: "events", col: Col::I64(&c.events), converted: None },
        Field { name: "revenue_cents", col: Col::I64(&c.revenue), converted: None },
        Field { name: "top_category", col: Col::OptStr(&c.top), converted: Some(0) },
    ])
}

pub fn write_campaign(path: &Path, c: &CampaignCols) -> Res {
    pq::write(path, "campaign_daily", &[
        Field { name: "campaign_id", col: Col::Str(&c.campaign_id), converted: Some(0) },
        Field { name: "channel", col: Col::Str(&c.channel), converted: Some(0) },
        Field { name: "day", col: Col::I32(&c.day), converted: Some(6) },
        Field { name: "sessions", col: Col::I64(&c.sessions), converted: None },
        Field { name: "purchasing_sessions", col: Col::I64(&c.purchasing), converted: None },
        Field { name: "revenue_cents", col: Col::I64(&c.revenue), converted: None },
        Field { name: "assisted_sessions", col: Col::I64(&c.assisted), converted: None },
    ])
}
