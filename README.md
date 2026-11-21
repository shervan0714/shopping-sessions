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

## Architecture (v0.2, `fast-pipeline` branch)

No crate dependencies (std only; Parquet is written by `src/pq.rs`).

| Stage | What |
|---|---|
| 1. links | Parsed first. Both IDs go into a lock-free insert-only table (`src/utab.rs`, 32-byte slots, CAS publish) and are unioned with a concurrent union-find stored in the same slots. Each component's name is a lock-free minimum over its keys; every linked ID's slot then holds its person. |
| 2. events + clicks | Line-at-a-time SIMD scanner (`scan_lines`: one 128-byte window gives newline/comma/quote masks). IDs pack into exact, order-preserving 128-bit keys (`src/key.rs`; >15 bytes go to a locked side interner). User and product lookups are batched 16 rows at a time behind prefetches; unknown users are inserted with themselves as person. Every event copy is scattered by event_id hash into P1 partitions (per-thread blocks in huge-page arenas); valid clicks are scattered by person into P2 partitions. |
| 3. dedup | Per P1 partition, a cache-resident hash table keeps the smallest-ingest copy; surviving valid events are scattered by person into P2. |
| 4. sessions | Per P2 partition (largest first): group by person (local hash + counting sort), sort each person's events/clicks, sessionize, credit (moving pointer), assists (merged 7-day intervals), aggregate. |
| 5. output | One user_daily file per thread + one campaign_daily file, written in parallel. |

If the user table passes 75 % load (only on data far from the generator's shape) the binary re-executes itself with a 4x larger table before writing anything.

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
