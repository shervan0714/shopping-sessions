# Shopping Sessions — native Rust solution

A single, dependency-free (std only) Rust binary that implements `rules.md` exactly and
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

Without Docker (any recent stable Rust; Python with `polars` for the checkers):

```sh
make native-fixtures                 # cargo build --release + 36 fixtures
make native-dev check-dev            # dev set + exact diff
make unit                            # unit tests (timestamps, CSV dialect, SIMD scanner fuzz, keys)
make cross-check                     # Rust vs scripts/reference.py on freshly generated seeds
make fuzz                            # mutation fuzzer vs scripts/reference.py (required before any speed change)
make ab A=old B=new DATA=d20         # alternating A/B, medians per stage (scripts/ab.sh; PERFORMANCE.md)
sh scripts/id_lengths.sh DATA        # ID byte lengths and the share that misses the packed fast path
```

The binary reads `DATA_DIR`/`OUTPUT_DIR` (default `/data`, `/output`) or the
last two command-line arguments, and `NCPU` for its thread count. It writes
only `$OUTPUT_DIR/user_daily/*.parquet` and `$OUTPUT_DIR/campaign_daily/*.parquet`;
diagnostics (stage timings) go to stderr. It needs no scratch space.

The image holds one static binary built for `TARGET_CPU` (build arg, default
`cascadelake`: runs on every GCP n2, Cascade Lake or Ice Lake) with
profile-guided optimization recorded **inside** `docker build`
(`scripts/build.sh`): the build compiles the published generator
(`tools/generator`), makes a 2M-event training set, runs an instrumented
binary on it and on the fixtures, merges the profile with `llvm-profdata`
and builds again. The profile therefore always matches the compiler and the
source; nothing is checked in. If training cannot run on the build host, the
final build is the same minus PGO (the build log says which).

Tuning knobs (environment; defaults are what the scorer gets):
`SS_INPUT=populate|pread`, `SS_READAHEAD=1` / `SS_NO_READAHEAD=1`,
`SS_EV_BITS` / `SS_PP_BITS` (log2 bucket counts), `SS_KEEP_MAPS=1`; test-only:
`SS_WIDE=1` (u128 rows), `SS_SMALL_CLICKS=N` (assist path threshold).

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
| 1. map | `mmap` every CSV (in parallel), cut bodies into 4 MB line-aligned chunks. Each chunk is prefaulted with one `MADV_POPULATE_READ` and parsed from the mapping (`SS_INPUT=pread`: 512 KB `pread` copies plus a readahead thread). Bucket counts are sized from the input bytes: ~40K rows per dedup bucket, ~20K events per person partition. After parsing, the input mappings are unmapped on a background thread. | O(files) |
| 2. parse | Link files first (small), in parallel; then the people table is built on its own thread while all event and click chunks are parsed by work-stealing on `NCPU` threads. A SIMD structural scanner (`src/scan.rs`, AVX2/SSE2 bitmasks for `\n`, `,`, `"`) finds fields in one pass; quote validity is checked by popcount (quotes in the line must equal 2 × wrapped fields), which is provably the same rule as the simple splitter in `src/csv.rs` and is fuzz-tested against it. Timestamps: SWAR time-of-day plus a small per-thread memo of validated dates (`src/ts.rs`, fuzzed against the plain parser). **Every ID becomes an exact 64-bit key** (`src/idkey.rs`): 1–10 bytes of `[0-9A-Z_a-z]` are packed 6 bits per byte so that integer order is byte order and integer equality is string equality; any other ("exotic") ID gets a flagged 62-bit hash key, with a second, independent 64-bit hash written to the worker's own log (`src/intern.rs`); after parsing the logs are sorted by key in parallel and any key with two different second hashes means a collision, which restarts the run on an exact locked interner. Comparisons involving an exotic ID use its real bytes, copied once into a read-only table. No locks or shared writes on the hot path. Each event copy is validated (including the late check) and writes a 24-byte dedup record (event key, ingest_ts, payload pointer) into fixed 2048-row blocks, one chain per `event_id`-key bucket, carved from per-worker huge-page arenas (`src/alloc.rs`); a valid copy also writes its payload (person key + packed event) once, straight into its person partition: users in no link (a bit filter over all link IDs, built before events are parsed) are their own person, the others are resolved in prefetched batches of 64 once the people table is built (copies parsed before that wait in a per-worker list). Products are looked up only for purchases. | O(bytes); 24 B record + 16 B payload per event row, 32 B per click |
| 3. people | Union–find (union by size, path halving) over the linked IDs only; each component is named by its byte-wise smallest ID. The result is an open-addressing table linked-ID-key → person-key with a bit filter in front; every unlinked ID is its own person (`src/people.rs`). | O(links · α) |
| 4. dedup | Per bucket, in parallel: one hash table per bucket over the 24-byte records (event key → copy with the smallest ingest_ts; buckets are sized to keep it cache-resident); every other copy's payload is marked dead in place, and a kept copy that is invalid or late has no payload. Nothing is copied. Payloads still waiting for the people table are then resolved (person lookup prefetched two stages ahead) and moved to their person partition. Keys are exact (verified), so there are no collisions to resolve. | O(n) |
| 5. clicks | Valid clicks get their person key and are scattered into the same person partitions. | O(clicks) |
| 6. sessions | Per person partition, in parallel, largest first: group the live (not dead) events by person with a hash table, attach each person's clicks (clicks of persons with no events are dropped: they cannot credit or assist), events and clicks are packed into `u64` words (`src/pack.rs`: ts offset on top, then price and category, or campaign; `u128` when the window or category count does not fit), so sorting is plain integer sorting; a person with two clicks at the same ms is sorted by (ts, click_id) exactly. Then sessions are cut at gaps > 30 min (equal timestamps always share a session), day = UTC date of the start. Credit: one forward pass over the clicks for all of a person's sessions (latest click with ts ≤ start, first of its ts run = smallest click_id; organic if none within 7 days). Assists: a person with ≤ 16 clicks slides the 7-day window over clicks and sessions and counts distinct campaigns directly; otherwise per campaign the click intervals [ts, ts+7d] are merged and every session whose start falls in one is counted once, by binary search over the session starts, starting at the right day segment. A partition's row blocks go back to the worker's pool once grouped; user rows reuse them. top_category: highest purchase sum, ties to the byte-wise smallest name, `unknown` included. | O(n log k), k = events per person |
| 7. output | Parquet via `src/pq.rs` (PLAIN, uncompressed, no dictionary, no statistics; thrift-compact footer), `user_daily` cut into ~4 files per thread written in parallel plus one `campaign_daily` file; `day` is INT32 `DATE`. | O(rows) |

Click duplicates are not removed: rules.md guarantees copies are identical,
and neither crediting (latest ts, then smallest id) nor assisting (distinct
campaigns) changes when an identical row appears twice.

## Credits

All code here was written for this repo. Ideas we learned from other teams' public
write-ups and then implemented our own way: order-preserving packed 64-bit ID keys,
person partitions instead of a global sort, huge-page block arenas for partition
buffers, links-first scheduling, and prefetched link-table lookups (from the public
READMEs/notes of the `micah-andelman/shopping-sessions` and `jklein07/shopping-sessions`
repositories, which credit further teams in turn). Papers and references that shaped
the scanner: Langdale & Lemire, *Parsing Gigabytes of JSON per Second* (simdjson,
arXiv:1902.08318), and the SIMD CSV work in `minio/simdcsv`.

## Correctness evidence

All of these were run, not assumed:

* 36/36 fixtures pass (`make native-fixtures`).
* `check-dev` on the 1M-event dev set (seed 1): **MATCH** on both tables.
* `scripts/reference.py` is an independent, deliberately naive Python
  implementation of rules.md (dict dedup, explicit per-session click scans).
  It matches `dev-answer` exactly, and the Rust binary matches it on 13 more
  generator seeds (2–12, 15, 16; 150K–2M events; windows from 2025 to 2028,
  including seed 15 whose window contains 2028-02-29).
* `scripts/fuzz.py`, a mutation fuzzer, writes datasets aimed at the rules'
  edges (IDs of 11–300 bytes, spaces, `
`, tabs, non-ASCII and emoji, case
  variants; link chains, self-links, shared devices; duplicate events with
  other ingest times; every bad timestamp form and window edge; 30:00.000 and
  30:00.001 gaps; 7-day click edges and same-ms click ties; quoted fields,
  stray quotes, wrong field counts, CRLF, header-only files), runs the binary
  with a random `NCPU`, and diffs it against `scripts/reference.py`. Seeds 1
  and 2: 408/408 rounds match, including 8 rounds of 150K–400K events with up
  to 40 % odd IDs. It caught both deliberately planted bugs tried (a `<` for
  `<=` in the session gap; a wrong byte order for odd IDs) in its first round.
* The Docker image, run with the scorer's flags (read-only root, uid 1000,
  no network, 64 MB tmpfs `/tmp`), passes all 36 fixtures and matches the dev
  answer.
* Unit tests: every timestamp example and bad form from rules.md, the CSV
  dialect cases, price parsing, inline- and packed-key exactness and ordering, a 300 000-case fuzz of the SWAR
  timestamp parser against the plain one, and a 60 000-case fuzz of
  the SIMD scanner against the simple splitter.

## Performance

See `PERFORMANCE.md` for measured numbers and how they were taken.

## Limitations

See `PERFORMANCE.md` → "Limitations and untested cases".
