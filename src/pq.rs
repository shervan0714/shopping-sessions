//! Minimal, dependency-free Parquet writer: one row group per file (or a few),
//! one PLAIN data page (v1) per column chunk, no compression, no statistics.
//! Supports REQUIRED INT32/INT64/BYTE_ARRAY and OPTIONAL BYTE_ARRAY with
//! RLE-encoded definition levels. Enough for the two output tables.

use std::fs::File;
use std::io::Write;
use std::path::Path;

pub type Res = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Thrift compact protocol writer.
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, 8);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        self.list_hdr(elem, n);
    }
    fn list_hdr(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    /// A struct as a list element.
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
}

pub enum Col<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
    /// REQUIRED strings
    Str(&'a [&'a [u8]]),
    /// OPTIONAL strings: one entry per row, None for null
    OptStr(&'a [Option<&'a [u8]>]),
}

pub struct Field<'a> {
    pub name: &'a str,
    pub col: Col<'a>,
    /// converted_type: Some(0)=UTF8, Some(6)=DATE
    pub converted: Option<i32>,
}

impl Col<'_> {
    fn len(&self) -> usize {
        match self {
            Col::I32(v) => v.len(),
            Col::I64(v) => v.len(),
            Col::Str(v) => v.len(),
            Col::OptStr(v) => v.len(),
        }
    }
    fn ptype(&self) -> i32 {
        match self {
            Col::I32(_) => 1,
            Col::I64(_) => 2,
            Col::Str(_) | Col::OptStr(_) => 6,
        }
    }
    fn optional(&self) -> bool {
        matches!(self, Col::OptStr(_))
    }
    /// Page body: [def levels] + PLAIN values.
    fn encode(&self, out: &mut Vec<u8>, a: usize, b: usize) {
        match self {
            Col::I32(v) => {
                for x in &v[a..b] {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::I64(v) => {
                for x in &v[a..b] {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::Str(v) => {
                for s in &v[a..b] {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s);
                }
            }
            Col::OptStr(v) => {
                // RLE runs of definition levels (bit width 1), 4-byte length prefix.
                let at = out.len();
                out.extend_from_slice(&[0; 4]);
                let v = &v[a..b];
                let mut i = 0;
                while i < v.len() {
                    let d = v[i].is_some();
                    let mut j = i + 1;
                    while j < v.len() && v[j].is_some() == d {
                        j += 1;
                    }
                    let mut run = ((j - i) as u64) << 1;
                    while run >= 0x80 {
                        out.push((run as u8) | 0x80);
                        run >>= 7;
                    }
                    out.push(run as u8);
                    out.push(d as u8);
                    i = j;
                }
                let n = (out.len() - at - 4) as u32;
                out[at..at + 4].copy_from_slice(&n.to_le_bytes());
                for s in v.iter().flatten() {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s);
                }
            }
        }
    }
}

const ROWS_PER_GROUP: usize = 1 << 20;

pub fn write(path: &Path, schema_name: &str, fields: &[Field]) -> Res {
    let nrows = fields.first().map_or(0, |f| f.col.len());
    let mut out: Vec<u8> = Vec::with_capacity(64 << 10);
    out.extend_from_slice(b"PAR1");
    // (per row group) per column: (data_page_offset, size, num_values)
    let mut groups: Vec<(usize, Vec<(usize, usize)>)> = Vec::new();
    let mut page = Vec::new();
    let mut a = 0;
    while a < nrows {
        let b = (a + ROWS_PER_GROUP).min(nrows);
        let mut cols = Vec::new();
        for f in fields {
            page.clear();
            f.col.encode(&mut page, a, b);
            let mut h = T::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, page.len() as i32);
            h.i32(3, page.len() as i32);
            h.begin(5);
            h.i32(1, (b - a) as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // RLE
            h.i32(4, 3); // RLE
            h.end();
            h.b.push(0);
            let off = out.len();
            out.extend_from_slice(&h.b);
            out.extend_from_slice(&page);
            cols.push((off, out.len() - off));
        }
        groups.push((b - a, cols));
        a = b;
    }

    let mut m = T::new();
    m.i32(1, 1);
    m.list(2, 12, fields.len() + 1);
    m.elem_begin();
    m.bin(4, schema_name.as_bytes());
    m.i32(5, fields.len() as i32);
    m.end();
    for f in fields {
        m.elem_begin();
        m.i32(1, f.col.ptype());
        m.i32(3, if f.col.optional() { 1 } else { 0 });
        m.bin(4, f.name.as_bytes());
        if let Some(c) = f.converted {
            m.i32(6, c);
        }
        m.end();
    }
    m.i64(3, nrows as i64);
    m.list(4, 12, groups.len());
    for (n, cols) in &groups {
        m.elem_begin();
        m.list(1, 12, cols.len());
        let mut total = 0i64;
        for (f, &(off, size)) in fields.iter().zip(cols) {
            total += size as i64;
            m.elem_begin();
            m.i64(2, off as i64);
            m.begin(3);
            m.i32(1, f.col.ptype());
            m.list(2, 5, if f.col.optional() { 2 } else { 1 });
            m.zz(0); // PLAIN
            if f.col.optional() {
                m.zz(3); // RLE
            }
            m.list(3, 8, 1);
            m.varint(f.name.len() as u64);
            m.b.extend_from_slice(f.name.as_bytes());
            m.i32(4, 0); // UNCOMPRESSED
            m.i64(5, *n as i64);
            m.i64(6, size as i64);
            m.i64(7, size as i64);
            m.i64(9, off as i64);
            m.end();
            m.end();
        }
        m.i64(2, total);
        m.i64(3, *n as i64);
        m.end();
    }
    m.bin(6, b"shopping-sessions");
    m.b.push(0);
    out.extend_from_slice(&m.b);
    out.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    File::create(path)?.write_all(&out)?;
    Ok(())
}
