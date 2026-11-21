//! Parquet output. Plain encoding, no dictionary, no compression and no
//! statistics: the outputs are small next to the inputs and this is the
//! cheapest thing to write. `day` is INT32 annotated DATE (days since epoch).

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

pub struct UserCols {
    pub user_id: Vec<ByteArray>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue: Vec<i64>,
    pub top: Vec<ByteArray>,     // present values only
    pub top_def: Vec<i16>,       // 1 present, 0 null
}

pub struct CampaignCols {
    pub campaign_id: Vec<ByteArray>,
    pub channel: Vec<ByteArray>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub purchasing: Vec<i64>,
    pub revenue: Vec<i64>,
    pub assisted: Vec<i64>,
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_size_limit(1 << 20)
            .build(),
    )
}

type Res = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub fn write_user(path: &Path, c: &UserCols) -> Res {
    let schema = Arc::new(parse_message_type(
        "message user_daily {
            REQUIRED BYTE_ARRAY user_id (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 events;
            REQUIRED INT64 revenue_cents;
            OPTIONAL BYTE_ARRAY top_category (UTF8);
        }",
    )?);
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut w = SerializedFileWriter::new(file, schema, props())?;
    if !c.day.is_empty() {
        let mut rg = w.next_row_group()?;
        let mut i = 0;
        while let Some(mut col) = rg.next_column()? {
            match i {
                0 => { col.typed::<ByteArrayType>().write_batch(&c.user_id, None, None)?; }
                1 => { col.typed::<Int32Type>().write_batch(&c.day, None, None)?; }
                2 => { col.typed::<Int64Type>().write_batch(&c.sessions, None, None)?; }
                3 => { col.typed::<Int64Type>().write_batch(&c.events, None, None)?; }
                4 => { col.typed::<Int64Type>().write_batch(&c.revenue, None, None)?; }
                5 => { col.typed::<ByteArrayType>().write_batch(&c.top, Some(&c.top_def), None)?; }
                _ => unreachable!(),
            }
            col.close()?;
            i += 1;
        }
        rg.close()?;
    }
    w.close()?;
    Ok(())
}

pub fn write_campaign(path: &Path, c: &CampaignCols) -> Res {
    let schema = Arc::new(parse_message_type(
        "message campaign_daily {
            REQUIRED BYTE_ARRAY campaign_id (UTF8);
            REQUIRED BYTE_ARRAY channel (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 purchasing_sessions;
            REQUIRED INT64 revenue_cents;
            REQUIRED INT64 assisted_sessions;
        }",
    )?);
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut w = SerializedFileWriter::new(file, schema, props())?;
    if !c.day.is_empty() {
        let mut rg = w.next_row_group()?;
        let mut i = 0;
        while let Some(mut col) = rg.next_column()? {
            match i {
                0 => { col.typed::<ByteArrayType>().write_batch(&c.campaign_id, None, None)?; }
                1 => { col.typed::<ByteArrayType>().write_batch(&c.channel, None, None)?; }
                2 => { col.typed::<Int32Type>().write_batch(&c.day, None, None)?; }
                3 => { col.typed::<Int64Type>().write_batch(&c.sessions, None, None)?; }
                4 => { col.typed::<Int64Type>().write_batch(&c.purchasing, None, None)?; }
                5 => { col.typed::<Int64Type>().write_batch(&c.revenue, None, None)?; }
                6 => { col.typed::<Int64Type>().write_batch(&c.assisted, None, None)?; }
                _ => unreachable!(),
            }
            col.close()?;
            i += 1;
        }
        rg.close()?;
    }
    w.close()?;
    Ok(())
}
