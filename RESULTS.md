# Results

Measured findings from the benchmark harness ([src/bin/bench.rs](src/bin/bench.rs)). The goal, the bars, and what is being swept are in [PLAN.md](PLAN.md); the architecture in [spec.md](spec.md).

Each result records what was run, the raw numbers, the mechanism isolated, and the fix — in that order, so a fix is never stated without the evidence that motivated it.

---

## R1 — OLTP point-read latency vs cache ratio, one row-group size

**Date:** 2026-07-15
**Arm:** fork (A), the engine owns the write path.
**Config:** 200k keys, 256 B values, `ROW_GROUP_ROWS = 8192`, 20 files (~2 row groups each, 40 total). Uniform-random writes. S3 Standard latency, fitted lognormal (GET p50 26 ms / p99 86 ms). Working set measured at 3934 KiB resident (2000 hot keys). 1000 reads per point.
**Command:** `HG_LATENCY=s3 HG_BENCH_READS=1000 bench --write-dist random`

### Data

| ws/cache | cache | p50 | p99 | p99.9 | GET/read | hit% |
|---:|---:|---:|---:|---:|---:|---:|
| 0.5 | 7868 K | 0.4 ms | 0.7 ms | 1.1 ms | 0.20 | 100.0% |
| 1.0 | 3934 K | 0.3 ms | 0.6 ms | 0.9 ms | 0.20 | 100.0% |
| 2.0 | 1967 K | 32.6 ms | **432.4 ms** | 866.6 ms | 6.42 | 84.2% |
| 4.0 | 983 K | 50.7 ms | 712.8 ms | 909.4 ms | 12.61 | 69.4% |
| 8.0 | 491 K | 109.8 ms | 823.4 ms | 1008.3 ms | 22.59 | 45.3% |

Ratios 16 and 32 are missing: the run crashed there on `RequestTimeTooSkewed`, a MinIO clock-skew 403 from the Docker VM drifting during a host sleep, **not** an engine fault. Host and container clocks were confirmed back in agreement afterward. The 0.5–8.0 rows were all collected before the drift and are valid.

### Finding: it is a cliff, not a knee

p99 goes **0.6 ms → 432 ms between ratio 1.0 and 2.0** — roughly 700× for a 2× cache reduction. The OLTP-adjacent envelope (p99 ≤ 10 ms, per PLAN.md) holds **only while the cache holds essentially the entire working set.**

This contradicts the design's own premise. [DECISIONS.md](DECISIONS.md) states *"the cache is explicitly not a copy of the table."* At one row-group size, under random writes, the measurement says it has to be a copy of the working set, or p99 falls off a cliff. **As stated, the "disposable row tier as a cache" claim does not survive its own benchmark** — pending the fix below.

### Mechanism: bloom-filter eviction, isolated via the GET/read column

The whole latency curve is `GET/read × per-GET latency` — the only injected delay is per object-store op ([latency.rs](src/store/latency.rs)); everything else in a read is in-memory. So the cliff is a GET-*count* explosion, not a per-GET slowdown. `GET/read` climbs **0.20 → 6.42 → 12.61 → 22.59** as the cache shrinks.

Decomposing a fully-missed read (~22 GETs):

- **File-level interval prune is dead.** Random writes make every file's `[pk_min, pk_max]` span the keyspace, so all 20 files are candidates — nothing dismissed.
- **~20 of the 22 GETs are bloom fetches** — one per candidate file. Row-group min/max prune *does* work (a flush is PK-sorted, so a file's 2 row groups are disjoint and one is picked for free), so it is one bloom per file, not per row group. 19 of those 20 blooms exist only to answer "not here."
- **~3 GETs are the column chunks** (`pk`, `op`, `value`) of the one file that holds the key.

In the flat zone those ~20 bloom fetches are cache hits (all 40 blooms, ~660 KB, fit), which is the *only* thing holding p99 at 0.6 ms. The cliff is those blooms being evicted. The eviction is avoidable waste: the blooms are ~17% of the working set, and a plain LRU evicts them because it ranks bytes by recency, blind to leverage — a bloom dismisses a whole file per fetch; a column chunk answers one key; LRU keeps the frequently-touched chunks and discards the high-leverage blooms.

### Fix (proposed, not yet implemented): pin the bloom filters

Segment the cache — blooms pinned and never evicted, data through the normal LRU. Cost: ~660 KB permanently resident for the whole table. Expected effect: the ~20 bloom fetches become free at *every* cache size, GET/read drops from ~22 toward ~3 (column chunks only), and the cliff flattens into a graceful curve where shrinking the cache costs only *data* misses. ~30 lines in [cache.rs](src/cache.rs).

**Does not fix:** the residual ~3 GETs, and the fact that 19 blooms are still consulted (now for free) per read. Killing those is compaction's job — merge files back into disjoint PK ranges so the file-level interval prune revives and a read consults one file, not twenty. Pinning unblocks the honest OLTP curve; compaction is the structural win behind it.

### Not yet measured (as of R1)

- Ratios 16 and 32 (completed in R2 below).
- `HG_LATENCY=express` — shifts every number down but the cliff should persist, since a missed read still makes ~22 GETs, only cheaper ones. Confirms the cliff is GET-count, not per-GET.
- `--write-dist sequential` — the same sweep with interval pruning intact. The gap between the two arms is what compaction would buy.
- **The OLAP axis** — scan throughput via an external engine (DuckDB) across row-group sizes. R1 is a single row-group size; the actual deliverable (the HTAP joint frontier) needs the sweep.

---

## R2 — the same point-read curve, over DuckLake with catalog-side blooms

**Date:** 2026-07-25
**What changed since R1:** the store moved from Iceberg to **DuckLake** — Parquet on MinIO, catalog rows in Postgres, published by the forked DuckDB binary. R1's proposed fix (*pin the bloom filters in cache*) was **superseded** rather than built: DuckLake's fork writes an opt-in **catalog** bloom per file (`ducklake_file_column_blooms`, SBBF/murmur3), which the read path loads into the in-memory file index and probes **in-process**. So the pk bloom is no longer an object on S3 at all — dismissing a file costs zero GETs and zero cache bytes.
**Config:** identical *inputs* to R1 — 200k keys, 256 B values, `ROW_GROUP_ROWS = 8192`, 20 files, uniform-random writes, S3 Standard fitted lognormal. 500 reads per point. The inputs being identical does not make the sweeps comparable: the working set is measured per run, and it came out 3.5× smaller here (1114 K vs 3934 KiB), which moves the cache size at every ratio. See the confounds below before comparing any row to its R1 neighbour.
**Command:** `HG_LATENCY=s3 HG_BENCH_READS=500 bench --write-dist random`

### Data (R2), with R1 alongside

| ws/cache | cache | R2 p50 | R2 p99 | R2 GET/read | R2 hit% | R1 p99 | R1 GET/read |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.5 | 2229 K | 0.3 ms | 0.9 ms | 0.32 | 100.0% | 0.7 ms | 0.20 |
| 1.0 | 1114 K | 0.3 ms | 0.8 ms | 0.32 | 100.0% | 0.6 ms | 0.20 |
| 2.0 | 557 K | 20.5 ms | **117.4 ms** | 8.07 | 52.8% | 432.4 ms | 6.42 |
| 4.0 | 278 K | 42.4 ms | 160.5 ms | 12.36 | 27.9% | 712.8 ms | 12.61 |
| 8.0 | 139 K | 45.7 ms | 151.9 ms | 14.78 | 13.1% | 823.4 ms | 22.59 |
| 16.0 | 69 K | 46.0 ms | 138.7 ms | 16.19 | 4.9% | *(crashed)* | — |
| 32.0 | 34 K | 43.1 ms | 132.3 ms | 11.04 | 33.6% | *(crashed)* | — |

**`GET/read` is not the read path's request count, and reading it as one is a mistake this
section made in its first draft.** It is `store.gets / reads` ([bench.rs:328](src/bin/bench.rs#L328)),
so it counts only the requests that *missed* the cache. The read path's actual request count is
`GET/read ÷ (1 - hit%)`, valid because one range request is either a hit or exactly one store GET
([cache.rs:200-203](src/cache.rs#L200-L203)). Derived, from the same two columns:

| ws/cache | R1 GET/read | R1 hit% | **R1 req/read** | R2 GET/read | R2 hit% | **R2 req/read** |
|---:|---:|---:|---:|---:|---:|---:|
| 2.0 | 6.42 | 84.2% | **40.6** | 8.07 | 52.8% | **17.1** |
| 4.0 | 12.61 | 69.4% | **41.2** | 12.36 | 27.9% | **17.1** |
| 8.0 | 22.59 | 45.3% | **41.3** | 14.78 | 13.1% | **17.0** |
| 16.0 | — | — | — | 16.19 | 4.9% | **17.0** |
| 32.0 | — | — | — | 11.04 | 33.6% | **16.6** |

Ratios 0.5 and 1.0 are omitted: hit% is printed rounded to 100.0, so the quotient there is all
rounding error. Both are consistent with ~41 (R1) and ~17 (R2) for any true hit rate in the range
that prints as 100.0.

### Finding: catalog blooms turn R1's runaway cliff into a bounded plateau

Four things, in order of how much they matter:

1. **The bloom traffic left the read path, and `req/read` is where it shows.** Requests per read fall **41 → 17**, and both arms are flat across their whole sweep (R1: 40.6, 41.2, 41.3; R2: 17.1, 17.1, 17.0, 17.0, 16.6). A constant, cache-size-independent 24 requests disappeared, which is what a per-file cost that moved in-process looks like: ~20 bloom fetches plus the footer traffic of the files they used to force open. R2's read path issues the surviving file's ranges and nothing else.

   **`GET/read` does not show this, and at ratio 2 it moves the wrong way (6.42 → 8.07).** Two reasons, both mechanical. It counts only misses, so it is a *product* of requests and miss rate. And the sweep holds the ratio constant while measuring the working set per arm ([bench.rs:266-279](src/bin/bench.rs#L266-L279)), so R2's cache at a given ratio is 3.5× smaller in bytes than R1's (557 K vs 1967 K at ratio 2). Requests fell 2.4×, cache bytes fell 3.5×, the miss rate rose to cover the difference, and the two nearly cancel. Comparing the arms row-by-row understates the win, because equal-ratio rows are not equal-cache rows - see the confound below.

2. **The cliff's *height* drops 3–5×.** p99 at ratio 2 falls **432 → 117 ms**; at ratio 8, **823 → 152 ms**. Note this happens *while R2 makes more store GETs at ratio 2*, so the cause is not GET count. It is that the GETs stopped being serial. R1's per-file bloom fetch sat inside the candidate loop, which awaits one file at a time ([read.rs:66-80](src/read.rs#L66-L80)), so 20 dismissals were 20 round trips end to end. R2 dismisses in memory and only the surviving file does I/O, whose column-chunk ranges go through the `get_byte_ranges` → `try_join_all` override and issue concurrently.

   Wall-clock per GET (p99 ÷ GET/read) separates the arms cleanly against the injected GET p50 of 26 ms:

   | | ratio 2 | ratio 4 | ratio 8 | ratio 16 |
   |---|---:|---:|---:|---:|
   | R1 | 67 ms | 57 ms | 36 ms | — |
   | R2 | 14.5 ms | 13.0 ms | 10.3 ms | 8.6 ms |

   R1 sits at or above the per-GET p50 throughout; R2 sits well below it. Nothing serial lands under the p50 of its own per-op distribution, so this is sufficient to establish the split - but it divides a tail by a mean and is an indicator, not a measurement. A per-read round-trip-depth counter would settle it properly and does not exist yet.

3. **The tail stops climbing.** R1's p99 rose monotonically with the file count (432 → 713 → 823) because every extra file was another serial bloom fetch. R2's p99 **plateaus at ~130–160 ms** across ratios 2–32, on a flat 17 requests per read. Bounded, not runaway - the property R1 lacked, and the reason the sweep now completes at 16 and 32 where R1 crashed.

4. **The knee is now *honest*.** R1's cliff was partly an artefact - blooms being evicted from a cache whose size was the independent variable ([DECISIONS.md:342](DECISIONS.md#L342): "a bloom-eviction curve published as a working-set curve"). Under catalog blooms the pk filter costs **zero cache bytes**, so the ratio-1 knee is pure *data*-chunk caching: the envelope holds exactly while the cache holds the data working set, and breaks when it doesn't. That is the boundary the experiment was always trying to measure.

The 10 ms envelope still holds only to ws/cache = 1 - the fundamental "cache must hold the working set" limit is untouched, because it is not a bloom problem.

### Confounds, both unresolved

**Equal ratio is not equal cache, so the arms are not row-comparable.** The measured working set is 3934 KiB in R1 and 1114 K in R2, and `cache_bytes = working_set / ratio`, so every R2 row runs on a 3.5× smaller cache than the R1 row printed beside it. Matched on absolute bytes instead, the gap is far larger than the ratio table implies:

| cache | arm | p99 | GET/read |
|---:|---|---:|---:|
| 983 K | R1 (ratio 4) | 712.8 ms | 12.61 |
| 1114 K | R2 (ratio 1) | 0.8 ms | 0.32 |
| 491 K | R1 (ratio 8) | 823.4 ms | 22.59 |
| 557 K | R2 (ratio 2) | 117.4 ms | 8.07 |

Only ~660 KB of the 2820 KiB working-set drop is the blooms leaving (R1 measured them at ~17%). The rest is the two writers encoding the same rows differently, which is not a property of either architecture.

**The benchmark's value payload is a constant, so "256 B values" overstates the working set by ~70×.** [bench.rs:235](src/bin/bench.rs#L235) builds `vec![b'v'; 256]` once and [bench.rs:249](src/bin/bench.rs#L249) clones it for every row, so every value in the table is byte-identical and RLE/dictionary crushes the column. The measured working set works out to ~28 KiB per row group in R2 (1114 KiB over the ~40 row groups 2000 hot keys touch) against 2 MB raw. The swept numerator is therefore not the footprint a real 256 B-value workload produces, and it is also the most likely explanation for the codec gap above. Fix is one line - random bytes per value - and it invalidates the ws figure in both arms, so it needs a re-run rather than a correction.

This also retracts a claim the first draft of this section made: that the residual GET/read is "the 2 MB value column chunk fetched in cache-block-sized pieces". A 2 MB chunk cannot fit inside a 1114 KiB working set measured against an unbounded, non-evicting cache. The 2 MB was computed (8192 × 256 B), not measured, and the compressed chunk is roughly two orders of magnitude smaller. What the residual 17 requests per read actually consist of is **not yet decomposed** - R1's equivalent breakdown came from a MinIO request trace ([examples/trace_flush.rs](examples/trace_flush.rs)), and no such trace has been run against the DuckLake arm.

### What R2 settles, and what it leaves

- **Settled:** catalog-sourced blooms are strictly better than both R1's on-disk blooms *and* R1's proposed cache-pinning fix - they remove the bloom from the S3 path *and* from the cache budget at once, which pinning could not do (pinning still spends cache bytes). The evidence is the flat 41 → 17 requests per read, not the GET/read column.
- **Cross-implementation interop confirmed:** the Rust probe is validated against a Base64 blob the C++ extension wrote (`bloom::tests::probes_match_a_blob_the_fork_wrote`), and an external forked-DuckDB reads back rows holy-grail wrote. Same format, two independent implementations.
- **Still open, and still the same answer:** the ratio-1 knee is where the cache stops holding the working set. Only **compaction** - merging files back into disjoint PK ranges so the file-level interval prune revives and a read consults one file instead of twenty - moves it. That was R1's structural conclusion and R2 does not change it.
- **Quantitatively provisional.** Both confounds above sit under every number in this section. The direction of every finding survives them (requests/read is measured, not derived from the working set; the serial-vs-concurrent split is visible per GET). The *magnitudes* do not, because the ratio axis itself is built on a working set the benchmark measures from a pathologically compressible payload. Nothing here should be quoted as a number until the re-run.
- **Not yet measured:** `--write-dist sequential` (interval prune intact), `HG_LATENCY=express`, a request trace decomposing R2's residual 17 requests per read, and the OLAP axis (external-engine scan throughput) - the last now trivial, since the table is genuine DuckLake an external DuckDB queries directly.
- **Next run, in order:** random value bytes in the bench, then re-sweep both arms against **matched cache byte sizes** rather than matched ratios, so the two curves are comparable without the working-set denominator moving underneath them.
