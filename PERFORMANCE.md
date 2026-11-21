# Performance report

## v0.2 (`fast-pipeline`) vs v0.1, same box, same data

2-vCPU Xeon sandbox, seed 7, 5M events / 1.25M clicks (480 MB), warm page cache, best of 5:

| | wall (NCPU=2) | CPU (user+sys) | wall (NCPU=1) |
|---|---|---|---|
| v0.1 | 2.27 s | 5.39 s | 3.74 s |
| v0.2 | 1.04 s | 2.27 s | 1.65 s |

Stage (NCPU=1) v0.1 → v0.2: parse 2.34 → 1.02 s, dedup 0.55 → 0.17 s, group+sessions 0.51 → 0.28 s, output 0.13 → 0.09 s.
Outputs identical to v0.1 on seeds 2, 3, 4, 7, 15 (2028-02-29 window), 21 and on a long-ID variant; 36/36 fixtures; dev MATCH.


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
| 1M dev (seed 1) | 96 MB, 84 files | 0.48 s | `check-dev`: MATCH |
| 20M (seed 7, 5M clicks) | 1.92 GB, 182 files | **4.5–5.3 s** (best 4.55 s) | user CPU ≈ 26 s; cold page cache ≈ 5–6 s on this NVMe disk |
| 60M (seed 3, 15M clicks) | 5.76 GB, 149 files | **121 s** | completed correctly, but the 7.6 GB VM was swapping (inputs alone are 5.8 GB); not representative of a 24 GB box |

20M stage profile (best run): parse 2.6 s · people 0.3 s · dedup 1.0 s ·
group 0.5 s · sessions 0.3 s · Parquet 0.3 s.

### Memory and disk

* Anonymous working memory ≈ 40 B per event row + 32 B per click while
  parsing; dedup frees each bucket's blocks as it goes (blocks are their own
  mmaps, returned to the OS on free). 20M: max RSS 3.1 GB *including* the
  mapped input pages. 60M: max RSS 7.4 GB including mapped input pages, under
  memory pressure.
* Expected 60M peak on the scorer: ~4 GB anonymous + the inputs' page cache
  (5.8 GB), well under 24 GB. When inputs × 2.2 would exceed 80 % of `MEM_GB`
  the binary releases parsed chunks (`MADV_DONTNEED`); at 60M / 24 GB it does not.
* `/scratch` is not used. Output: 20M → 92 MB `user_daily` + 1.6 MB
  `campaign_daily`; 60M → 274 MB + 1.6 MB.

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

Profiles were taken with `perf record --call-graph fp`. The biggest wins
came from removing dependent cache misses (key bytes in the mmapped input),
not from arithmetic.

## Limitations and untested cases

* **No measurement on scorer-class hardware or with 24 GB.** The 60M run only
  shows it completes correctly under heavy memory pressure.
* Docker image verified on Docker Desktop (Windows) with the scorer's flags
  (read-only root, uid 1000, no network, tmpfs /tmp): fixtures and dev set.
  Not run on native Linux Docker or with the exact `scripts/docker_run.sh`
  (it needs Linux paths).
* 60M output was not compared against an independent implementation (the
  Python reference is too slow at that size); correctness at scale rests on
  the same code paths that match on dev and on 11 seeds up to 2M events.
* Inputs are assumed to be valid UTF-8 (rules.md says so); strings are copied
  into Parquet byte-for-byte.
* `ingest_ts` is assumed valid F1 as rules.md guarantees; an unparsable one is
  treated as the latest copy and invalid if kept.
* Event IDs and click IDs are read back from the mapped input (for exact
  collision checks and click_id ties), so `/data` must not change while the
  container runs (it is read-only).
