# Shopping Sessions — native Rust solution

A single, dependency-light Rust binary that implements `rules.md` exactly and
runs the whole pipeline (CSV → dedup → identity resolution → sessions →
attribution → Parquet) in one process with no engine startup cost.

`rules.md` (copied here from the track page) is the specification. The
36 edge-case fixtures, `scripts/run_fixtures.py`, `scripts/compare.py` and
`scripts/docker_run.sh` are the organisers' files, unmodified.

## Build and run

```sh
make build            # docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
make fixtures         # image on all 36 fixtures, scorer flags, exact diff
make dev              # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev          # image on dev/ with scorer flags, timed
make check-dev        # exact diff against dev-answer/ (dev-answer.zip unzipped here)
```

Without Docker (Rust ≥ 1.85, Python with `polars` for the checkers):

```sh
make native-fixtures                 # cargo build --release + 36 fixtures
make native-dev check-dev            # dev set + exact diff
make unit                            # unit tests (timestamps, CSV dialect, SIMD scanner fuzz, keys)
make cross-check                     # Rust vs scripts/reference.py on freshly generated seeds
```

The binary reads `DATA_DIR`/`OUTPUT_DIR` (default `/data`, `/output`) or the
last two command-line arguments, and `NCPU` for its thread count. It writes
only `$OUTPUT_DIR/user_daily/*.parquet` and `$OUTPUT_DIR/campaign_daily/*.parquet`;
diagnostics (stage timings) go to stderr. It needs no scratch space.

## Why Rust (and not Polars / DuckDB / Spark)

* The CSV dialect (quotes only around a whole field, `\r` as data, malformed
  lines dropped, empty line malformed, exact ASCII digits) does not match any
  engine's reader defaults; matching it in an engine means reading lines as
  strings and re-parsing them with regexes, which is slower than parsing once.
* The work is dominated by string handling (IDs) and per-person sequential
  logic (sessions, latest-click-with-tie-break, distinct assists). A
  hand-written pass does each in one go with no joins or intermediate tables.
* No JVM / interpreter start-up, no query planning, tiny image, predictable
  memory. Spark alone spends seconds starting.

## Architecture

| Stage | What | Complexity / memory |
|---|---|---|
| 1. map | `mmap` every CSV, cut bodies into ~4 MB line-aligned chunks. A background thread issues `MADV_WILLNEED` at most ~128 chunks ahead of the parsers; each parsed chunk is released with `MADV_DONTNEED`, so the inputs' page-cache footprint stays bounded. | O(files) |
| 2. parse | Work-stealing over chunks on `NCPU` threads. A SIMD structural scanner (`src/scan.rs`, AVX2/SSE2 bitmasks for `\n`, `,`, `"`) finds fields in one pass; quote validity is checked by popcount (quotes in the line must equal 2 × wrapped fields), which is provably the same rule as the simple splitter in `src/csv.rs` and is fuzz-tested against it. Timestamps are parsed by hand (`src/ts.rs`). Events are validated (including the late check, which depends only on the copy itself) and appended to fixed 256-row blocks in one of 256 `event_id`-hash buckets. Valid clicks and link pairs are collected. User IDs are interned in a 1024-shard locked table whose entries carry an exact inline encoding of the key, so lookups do not dereference the input. | O(bytes); ~40 B per event row, 32 B per click |
| 3. people | Union–find (union by size, path halving: no recursion, any chain length) over link pairs; each component is named by its byte-wise smallest ID. IDs that only appear in links are included. | O(links · α) |
| 4. dedup | Per bucket, in parallel: sort by (hash, ingest_ts); in each equal-hash run keep the first (smallest ingest_ts) copy; it survives only if it was valid. 64-bit hash collisions between different event_ids are detected by comparing the bytes and regrouped exactly. | O(n log(n/256)) |
| 5. group | Counting sort of surviving events and valid clicks by person (atomic counters + scatter). | O(n) |
| 6. sessions | Per person, in parallel (work-stealing, so whale persons do not serialise the rest; huge persons sort in parallel): sort events by ts, cut sessions at gaps > 30 min (equal timestamps always share a session), day = UTC date of the start. Credit: binary search in the person's clicks sorted by (ts, click_id) for the last click with ts ≤ start, then the first click with that ts (smallest click_id), organic if none within 7 days. Assists: per campaign, the click intervals [ts, ts+7d] are merged and every session whose start falls in a merged interval is counted once, by binary search over the session starts and the person's day segments — O(clicks log clicks + intervals · (log sessions + days)), never sessions × clicks. top_category: highest purchase sum, ties to the byte-wise smallest name, `unknown` included. | O(events log events) per person |
| 7. output | Parquet (plain, uncompressed, no dictionary), one `user_daily` file per thread written in parallel plus one `campaign_daily` file; `day` is INT32 `DATE`. | O(rows) |

Click duplicates are not removed: rules.md guarantees copies are identical,
and neither crediting (latest ts, then smallest id) nor assisting (distinct
campaigns) changes when an identical row appears twice.

## Correctness evidence

All of these were run, not assumed:

* 36/36 fixtures pass (`make native-fixtures`).
* `check-dev` on the 1M-event dev set (seed 1): **MATCH** on both tables.
* `scripts/reference.py` is an independent, deliberately naive Python
  implementation of rules.md (dict dedup, explicit per-session click scans).
  It matches `dev-answer` exactly, and the Rust binary matches it on 13 more
  generator seeds (2–12, 15, 16; 150K–2M events; windows from 2025 to 2028,
  including seed 15 whose window contains 2028-02-29).
* The Docker image, run with the scorer's flags (read-only root, uid 1000,
  no network, 64 MB tmpfs `/tmp`), passes all 36 fixtures and matches the dev
  answer.
* Unit tests: every timestamp example and bad form from rules.md, the CSV
  dialect cases, price parsing, inline-key exactness, and a 60 000-case fuzz of
  the SIMD scanner against the simple splitter.

## Performance

See `PERFORMANCE.md` for measured numbers and how they were taken.

## Limitations

See `PERFORMANCE.md` → "Limitations and untested cases".
