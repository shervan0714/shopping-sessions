# Performance report

## Round 4 (branch `perf-round4`)

Compared against a second implementation (u128 keys, mutex interner for
long IDs) on identical data. No correctness difference found: both pass the
36 fixtures, the dev answer and `scripts/fuzz.py` (204 rounds, seeds 11 and
23), and their outputs match on 2M, 5M, 5M-long-ID and 20M generated sets.
The work therefore went into parse CPU, the largest measured gap
(`perf record`, 1 thread: parse 2731 vs 1702 samples).

| Change | Why |
|---|---|
| `scan.rs`: 64-byte blocks, each field unwrapped when its separator is found (no per-line commas array walked again), no software prefetch | scanner micro-benchmark 0.14–0.175 s → 0.114–0.14 s on 5M events |
| `ts.rs`: SWAR epoch digits (≤16 digits, slow path kept and fuzzed against), per-slot `Cell` date cache, `parse` inlined | removes a per-digit loop and a `RefCell` borrow per row |
| `parse.rs` + `people.rs`: 64-row batches; link filter prefetched when the row is parsed, tested at flush | the filter probe was a cache miss per row |

Laptop (WSL2, P/E cores), interleaved runs, medians of 5 (20M: 3); wall s /
CPU s / peak RSS MB. `base` = 2bf0c82, `new` = this round, `other` = the
compared implementation.

| set | NCPU | base | new | other |
|---|---|---|---|---|
| 2M | 1 | 0.49 / 0.49 / 305 | 0.43 / 0.43 / 305 | 0.37 / 0.38 / 324 |
| 2M | 8 | 0.27 / 1.50 / 347 | 0.24 / 1.30 / 352 | 0.15 / 0.97 / 365 |
| 5M | 1 | 1.10 / 1.11 / 752 | 0.89 / 0.91 / 752 | 0.78 / 0.80 / 800 |
| 5M | 2 | 0.74 / 1.45 / 761 | 0.64 / 1.23 / 761 | 0.59 / 1.18 / 805 |
| 5M | 3 | 0.61 / 1.68 / 770 | 0.55 / 1.53 / 770 | 0.49 / 1.43 / 809 |
| 5M | 8 | 0.46 / 3.05 / 803 | 0.39 / 2.38 / 801 | 0.26 / 1.91 / 829 |
| 5M long IDs | 1 (9 runs) | 1.22 / 1.24 / 891 | 1.02 / 1.06 / 891 | 1.44 / 1.46 / 987 |
| 5M long IDs | 8 (15 runs) | 0.46 / 2.93 / 961 | 0.44 / 2.78 / 960 | 3.07 / 11.08 / 1009 |
| 20M | 1 | 5.10 / 5.21 / 2962 | 3.67 / 3.77 / 2963 | 3.15 / 3.29 / 3198 |
| 20M | 2 | 3.10 / 6.20 / 2981 | 2.43 / 4.83 / 2981 | 1.95 / 4.06 / 3205 |
| 20M | 3 | 2.38 / 7.09 / 3003 | 1.99 / 5.88 / 3003 | 1.64 / 5.04 / 3214 |
| 20M | 8 | 1.90 / 13.70 / 3055 | 1.51 / 10.97 / 3058 | 1.13 / 9.12 / 3243 |

Still open: the other implementation is faster on short-ID data. Measured
causes: fresh huge-page zeroing (`clear_page_erms` 797 vs 366 samples:
events/clicks part blocks and the user writer), and person resolution done
inside parse. It uses ~7% more memory and is 2–7x slower on long IDs.

## Round 3 (branch `perf-round3`)

Eight changes, one commit each, every one gated on: unit tests, the 36
fixtures, the dev answer, byte-identical tables against `main` (c03ee97) on
seven generated sets (seeds 2, 3, 4, 7, 15 with a 2028-02-29 window, 21, and
a long-ID variant) at NCPU 1, 2 and 3, and `scripts/fuzz.py` (200 small + 4
large rounds). Every alternative path was forced and fuzzed too (u128 rows,
both assist paths, both input modes).

Measured on a 2-vCPU Xeon sandbox (AVX-512) with 5M events, alternating A/B,
medians (`scripts/ab.sh`). A shared, noisy box: same-binary runs drift by
±8% between batches, so only alternating pairs are compared.

| Change | Effect here |
|---|---|
| no crate dependencies (rayon, parquet, memmap2, memchr, hashbrown, xxhash, bytes) | neutral (0.968 vs 0.972 s); 0.74 MB binary, offline build |
| bucket counts from the input; one dedup table per bucket | dedup −5 to −10%; at 5M rows/40K = 128 buckets, 512 was ~10 ms faster still: tune `SS_EV_BITS` on n2 |
| packed u64 rows (u128 fallback) | neutral here; survivors 24 → 16 B |
| forward-pass credit, small-person assists, day-segment jump | sessions −9% (1 thread), −4% (2 threads) |
| per-worker pools, dead blocks reused, big-buffer recycling, background unmap | page faults per run 12.3K → 6.2K; sessions −9% |
| `MADV_POPULATE_READ` 4 MB chunks (vs pread + readahead) | warm: 1 thread −7%, 2 threads equal; cold: no clear difference |
| in-build PGO, one `cascadelake` binary, `rust:1` | not measurable here (no llvm-profdata offline); removes the re-exec |

Not measured here: 8 vCPUs, 20M/60M, PGO's effect. **Measure on GCP n2:**

```sh
gcloud compute instances create ss-bench --machine-type=n2-standard-8 \
  --min-cpu-platform="Intel Cascade Lake" --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud --boot-disk-size=100GB
# on the VM: install docker + rust, build tools/generator, then
generate --seed 5 --events 20000000 --clicks 5000000 --output d20
docker build -t ss-new . && git stash && docker build -t ss-old . && git stash pop
# or natively: two binaries, alternating, 11 rounds, pinned to 8 vCPUs
taskset -c 0-7 scripts/ab.sh old/shopping-sessions target/release/shopping-sessions d20 11 8
A_ENV="SS_INPUT=pread" taskset -c 0-7 scripts/ab.sh BIN BIN d20 11 8     # input modes
A_ENV="SS_EV_BITS=10" B_ENV="SS_EV_BITS=9" scripts/ab.sh BIN BIN d20 11 8 # bucket counts
python3 scripts/fuzz.py target/release/shopping-sessions                    # after every change
```


Every number here was measured; nothing is extrapolated.

## Machine

All measurements: a laptop, Intel Core Ultra 7 155U (15 W; 2 P-cores + 8 E-cores
+ 2 LP cores), inside a WSL2 VM with **7.6 GB RAM** and 2 GB swap, `NCPU=8`.
This is much weaker than the scorer (8 Xeon vCPUs, 24 GB) and noisy: the CPU
power-throttles and WSL grows/reclaims guest memory between runs, so the
first run after a pause is often 2x slower. Numbers are the best of 3–4
consecutive runs unless noted. Treat them as upper bounds for the scorer.

## Results (current build)

| Dataset | Input | Wall time | Notes |
|---|---|---|---|
| 1M dev (seed 1) | 96 MB, 84 files | ~0.1 s | `check-dev`: MATCH |
| 20M (seed 7, 5M clicks) | 1.92 GB, 182 files | **1.38–1.6 s** (best 1.38 s) | user ≈ 9–10 s, sys ≈ 1.5 s; warm page cache |

20M stage profile (typical warm run): parse + people (overlapped) 0.65–0.7 s ·
dedup 0.3 s · click partition 0.05–0.1 s · sessions 0.3 s · Parquet 0.09 s.

The previous build (commit c95e188) took 4.5–5.3 s on the same machine and
scored 3.3 s on the practice board, so the board seems to run about 1.4x faster
than this laptop. That ratio is an estimate, not a measurement.

### Memory and disk

* Anonymous working memory ≈ 24 B dedup record + 16 B payload per event row
  (huge-page block arenas; record blocks are recycled after dedup) + 2 × 32 B
  per valid click + per-worker logs of exotic IDs (16 B per long event ID,
  32 B per long user/click ID occurrence) until they are verified. 20M: max RSS 3.7 GB *including* ~1.9 GB of mapped input pages.
* Expected 60M peak on the scorer: ~6 GB anonymous + the inputs' page cache
  (5.8 GB), under 24 GB. Not measured on a 24 GB machine (see Limitations).
* `/scratch` is not used.

## Optimisation log (20M, best wall)

| Change | Wall |
|---|---|
| First correct version (per-field `memchr`, 1024 growing partition vectors) | 9.9 s |
| One vector per worker + radix scatter | ~10 s (no gain; noise) |
| SIMD structural scanner (one pass, popcount quote check) | 8.1 s |
| Interner entries carry hash + key; 40-byte event rows; reserved vectors | 6.95 s |
| Exact inline key encoding (no dereference of input on hash hits) for interner, products, campaigns | 4.55 s |
| Parse-time bucketing into fixed blocks (no second copy of all rows) | 4.8 s (same within noise; halves peak memory) |
| Interned IDs copied to compact arenas | 4.57 s; 60M people phase 300 s → 1.3 s under memory pressure |
| Fixed glibc mmap threshold + 80 KB blocks | 60M under memory pressure: 999 s → 121 s |
| **Round 2:** exact 64-bit packed ID keys (no interner locks, no hashing, no string reads) | 3.4 s (parse 2.6 → 1.2 s) |
| Huge-page block arenas + huge-page big allocations; dedup in 16 cache-sized sub-tables | 2.7–3.0 s |
| Survivors written straight into person-partition blocks (no extra scatter array) | 1.9 s |
| Links first; people table built while events parse; dedup reads winners in row order | ~1.9 s (people 0.2 s hidden) |
| Zero-copy names, user_daily split into ~32 files | output 0.25 → 0.09 s |
| Hash-group each person partition, sort per person (no 76K-row sorts) | 1.43 s |
| Two-stage prefetch of person lookups; SWAR time + date memo | 1.45–1.6 s (within noise) |
| **Round 3:** freed event blocks pooled and reused for survivors; clicks resolved to persons while parsing and written straight into person partitions (no click scatter pass); click lookups batched 64 at a time with prefetch; branchless quote unwrap; user_daily written during the session stage | min 1.30–1.38 s (perf: page zeroing 31 % → 13 % of CPU; output phase 0.09 → 0.02 s) |
| Chunks read with `pread` into a 512 KB per-worker buffer instead of parsed through the mapping (the scanner's input load was 76 % of its time: the hardware prefetcher stops at every 4 KB page) | profile: ~14.6 → ~11 CPU-s per run |
| SIMD ID packing (16-byte classify + two multiply-adds), fuzzed against the scalar packer; blocking wait (no spin) for the people table | best 1.38 s, ≈10.5 CPU-s (warm, 8 runs: 1.38–1.9 s once WSL memory is warm) |

| **Round 4:** static glibc (`+crt-static`), PGO (rustc 1.85.1, profile from the 20M set, dev and fixtures), plus a Cascade Lake build chosen at startup by CPUID | 15 interleaved runs each: dynamic median 1.90 s, static 1.87 s, static+PGO 1.82 s; minimums 1.57–1.62 s. No difference beyond noise here; kept because neither can change results and both may help on the scorer |
| Tried, not kept: 512 / 1024 person partitions (`PP_BITS` 9 / 10); `mallopt` (mmap threshold 1 MB, trim 1 GB, top pad 64 MB, 16 arenas) | 10 interleaved runs, median: base 1.82 s, 512 1.81 s, 1024 2.02 s, mallopt 1.84 s |
| **Round 5 (Nov 21 generator: 1 in 8 IDs has a 32-char hex suffix, so ~12 % of IDs miss the packed path):** exotic IDs keyed by a 62-bit hash with a second 64-bit hash in per-worker logs, verified collision-free after parsing (restart on the locked interner if not); no locks while parsing | emulated new data, 5M events, 2 vCPU, 9 interleaved runs: CPU 2.79 → 2.11 s, parse 0.92 → 0.62 s (the locked interner was the whole slowdown: old-style data parses in 0.53 s) |
| Dedup on 24-byte records (event_id key, ingest_ts, payload pointer); payloads written once while parsing, straight into person partitions (users in no link need no lookup; the rest are resolved in prefetched batches once the people table is built); dropped copies marked dead in place | same data, median of 5: 1 thread dedup 0.35 → 0.13 s, total 2.39 → 1.91 s; 2 threads dedup 0.17 → 0.075 s, total 1.14 → 0.99 s; peak RSS +12 % |

Profiles were taken with `perf record --call-graph fp`. The biggest wins
came from removing dependent cache misses (key bytes in the mmapped input),
not from arithmetic.

## Limitations and untested cases

* **The Cascade Lake (AVX-512) build has never run correctly here**: this
  laptop has no AVX-512, so that build dies of an illegal instruction. That
  failure is what tests the fallback: with `SS_FORCE_AVX512=1` (skip the CPU
  check) the main build clears the output and produces matching results,
  natively and in Docker. On the scorer, the AVX-512 build's results are
  compiler output from the same source, but unverified.
* Not tried: hand-written AVX-512 code (the scanner and ID packer). AVX-512
  intrinsics need Rust 1.89+ (the image uses 1.85.1 for the profile), and code
  that cannot be run here cannot be shown correct.
* **No measurement on scorer-class hardware or with 24 GB.** The old 60M
  run predates the round-2 rewrite and was not repeated (the laptop has 7.6 GB).
* Docker image verified on Docker Desktop (Windows) with the scorer's flags
  (read-only root, uid 1000, no network, tmpfs /tmp): fixtures and dev set.
  Not run on native Linux Docker or with the exact `scripts/docker_run.sh`
  (it needs Linux paths).
* 60M output was not compared against an independent implementation (the
  Python reference is too slow at that size); correctness at scale rests on
  the same code paths that match on dev and on 13 seeds up to 2M events.
* Inputs are assumed to be valid UTF-8 (rules.md says so); strings are copied
  into Parquet byte-for-byte.
* `ingest_ts` is assumed valid F1 as rules.md guarantees; an unparsable one is
  treated as the latest copy and invalid if kept.
* IDs outside the packed alphabet/length ("exotic": spaces, `\r`, non-ASCII,
  over 10 bytes; since Nov 21 about 1 in 8 generator IDs) are keyed by hash and
  verified after parsing (src/intern.rs). A key collision is caught for sure
  unless the second hash collides too (~1e-25 at 10M long IDs) and then the run
  restarts on the exact locked interner; `SS_HASH_BITS=3` forces that path for
  tests. Round 5 was measured on emulated new-generator data (the old
  generator plus the hex suffix, as rules.md describes it), not on the real
  one: check its ID lengths with `scripts/id_lengths.sh`.
* Timings are from a noisy 15 W laptop (±15–30 % run to run, worse when hot);
  round 3 decisions were made from `perf` CPU profiles rather than wall time; the first run after a
  pause is often much slower because WSL has to re-read the inputs.
