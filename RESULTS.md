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

### Not yet measured

- Re-run with pinned blooms — the point of R1 is to motivate it.
- Ratios 16 and 32 (complete the curve).
- `HG_LATENCY=express` — shifts every number down but the cliff should persist, since a missed read still makes ~22 GETs, only cheaper ones. Confirms the cliff is GET-count, not per-GET.
- `--write-dist sequential` — the same sweep with interval pruning intact. The gap between the two arms is what compaction would buy.
- **The OLAP axis** — scan throughput via an external engine (DuckDB) across row-group sizes. R1 is a single row-group size; the actual deliverable (the HTAP joint frontier) needs the sweep.
