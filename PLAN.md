# Implementation Plan

## Framing

Iceberg is the primary store. The CDC stream of OLTP mutations already lands there, so the columnar table holds the full historical evolution of the row data. The OLTP engine built here is a *derived, disposable serving cache* over that table: an LSM tree whose bottom level is the Iceberg table itself, with a WAL-backed memtable in front of it.

This inverts the usual reading of the checklist:

- **Flush + idempotent recovery** is not durability plumbing. It is the contract that makes the row tier derivable. If flush is idempotent and watermark-stamped, any node can be rebuilt from `Iceberg + WAL suffix` with no coordination. This is the thesis.
- **Point read** is not just a read path. It is the proof that a columnar historical log can serve OLTP point lookups when the hot set is cached. The knee in the latency curve is the boundary of the claim.
- **WAL + memtable** is the only non-rebuildable state, and only for the LSN suffix past the published watermark. That suffix is small by design.

## Decisions

| Decision | Choice |
|---|---|
| Language | Rust |
| Deletes | Tombstones. A point read on a deleted key returns not-found, never a stale value. |
| Object-store latency | Injected in the client wrapper, so MinIO's ~1ms local latency can be shaped into realistic S3 latency. |
| Cache | Locally cached Parquet fetched from object storage. Bytes-bounded LRU — it is explicitly *not* the whole table. |
| Compaction | Out of scope. Flushed files accumulate and overlap on PK. |

### Record model

```
(pk: bytes, lsn: u64, op: Put | Delete, value: bytes)
```

### File ordering (consequence of no compaction)

Flushed files accumulate and overlap on PK. Each file's Iceberg snapshot carries the watermark LSN it covers, so file ordering is **total**, by watermark.

Point reads therefore scan files **newest-first, first hit wins**. No per-row LSN merge, no version reconciliation. A tombstone hit is a not-found and terminates the scan. Within a single flush a key appears at most once, because the memtable already collapsed it to its latest value.

## Gaps in the original checklist

The spec's six items are a capability list, not a build order. Three things that decide whether the prototype succeeds are absent from it:

1. **Latency injection layer** — must be built early and wrapped around the object store so every later phase inherits it. Retrofitting it invalidates all numbers taken before it existed.
2. **Local Parquet cache** — implied by goal #1 but not listed. It is the variable being swept.
3. **Bench harness and crash harness** — the two goals *are* deliverables. "Measure p99 vs cache ratio" and "prove flush is idempotent" do not happen unless they are built.

## Steps

| # | Step | Done when | State |
|---|---|---|---|
| 0 | Skeleton + record model | Cargo workspace, config, error types, record model, Iceberg schema | done |
| 1 | MinIO + Iceberg REST catalog | docker-compose up; table created; a hand-written Parquet file round-trips | done |
| 2 | Object-store client + latency injection | Configurable GET/PUT delay; all downstream code goes through this wrapper | done |
| 3 | WAL + memtable | fsync-before-ack; `kill -9`, restart, every acked write is present | done |
| 4 | Flush | Freeze memtable, PK-sort, write Parquet with bloom filter and PK min/max stats, upload, Iceberg commit carrying the watermark LSN, then truncate WAL | done |
| 5 | Idempotent recovery + crash harness | Deterministic snapshot ID; killing at each crash point still converges to the same state on recovery | done |
| 6 | Point read + local Parquet cache | memtable → manifest interval prune → bloom → cache or fetch → newest-first, first hit wins; LRU bounded in bytes | done |
| 7 | Bench harness | Zipfian read workload over uniform-random writes; sweep cache size; emit the p99 curve and the pass/fail below | |

Steps 3 → 4 → 5 are the correctness spine (claim #2: disposable row tier).
Steps 2 → 6 → 7 are the measurement spine (claim #1: OLTP-adjacent reads over Iceberg).

Step 6 depends on step 4 for real files, but can be unblocked early using hand-written Parquet from step 1.

## What step 7 measures — fixed 2026-07-14, before the harness produced a number

### The bar (fixed in advance, not negotiable after the fact)

> **"OLTP-adjacent" means p99 point-read latency ≤ 10 ms**, under S3 Standard latency and a uniform-random write distribution.

10 ms is about a slow local disk read; above it, "OLTP" is not a defensible use of the word. Uniform-random writes because that is what OLTP does — sequential writes give disjoint per-file PK ranges, the interval prune dismisses every file for free, and the result flatters. (The existing read-path test happens to cover only that flattering case: [tests/engine.rs](tests/engine.rs) builds "five files with disjoint key ranges.")

### The deliverable is an envelope, not a verdict

**Where does the p99 curve cross 10 ms, and what breaks when it does?**

"Does OLTP over Iceberg work?" is the wrong question. Nobody has shipped it; a naive build obviously has a wall. The useful output is *where the wall is* and *what is holding it up* — that names the next optimisation. A pass/fail bar cannot answer that.

The bar stays fixed anyway, because "find where to optimise next" without a pre-agreed definition of *good enough* is unfalsifiable — there is always a next optimisation. The bar is what the curve crosses; **where** it crosses is the finding.

### Withdrawn: the first criterion

The first version of this section read:

> ~~The claim dies if p99 exceeds 10 ms when the working set **equals** the cache size.~~

Withdrawn on the first smoke run, and worth recording rather than quietly overwriting. At a 1× ratio the cache holds the entire working set, so the measured hit rate was **100%** and object-store reads were **0.04 per read**. It measured a memory lookup and would have reported a triumphant PASS for a system that never touched Iceberg. It could be passed but never failed — worthless as a falsifier.

The reasoning behind it ("1× is the most favourable case, so a blown tail there is structural") is backwards: at 1× there is no tail to blow, because there is no I/O. The interesting region is **ratio > 1**, where the cache is genuinely smaller than the working set — which is the premise of the whole design, since *the row tier is a cache, not a copy*.

The sweep now runs 0.5× → 32×.

## Status — 2026-07-14

Steps 0–6 are done. 37 unit tests and 13 integration tests pass; the build is clean, with no warnings.

Step 7 (the benchmark harness) is all that remains before the prototype can produce its two numbers.

### Steps 4–6

- **`src/flush.rs`** — freeze, PK-sorted Parquet (bloom filter on `pk`, 8k-row row groups), Iceberg commit stamping the watermark into the snapshot summary, then WAL truncation. Split into `plan` / `write_files` / `commit_files` so the crash harness can die *between* them.
- **`src/index.rs`** — the file index, read out of the manifests at startup: one entry per live Parquet file with its watermark and PK bounds, sorted newest-first.
- **`src/cache.rs`** — bytes-bounded LRU behind a custom `AsyncFileReader`, so the cache holds exactly the byte ranges the Parquet reader asked for after pruning.
- **`src/read.rs`** — point read: interval prune → row-group prune → bloom filter → fetch the surviving row group. First hit wins.
- **`src/engine.rs`** — put/delete/get, flush, and recovery. `open` rebuilds every scrap of local state from `Iceberg + WAL` and nothing else.
- **`tests/crash.rs`** — a test per crash window. Each writes, dies, throws away all memory, reopens, and checks nothing was lost.

Measured: a point read costs **6 range fetches** on a cold cache (2 for the footer, 1 for the bloom filter, 3 for the surviving row group's column chunks), and that number is **constant in the file count** — a key no file can hold costs **zero** fetches. Repeated reads cost zero.

### Steps 0–3

What exists:

- **`src/record.rs`** — the `(pk, lsn, op, value)` model and its CRC-framed WAL encoding. A torn tail ends replay rather than failing it: a crash mid-append is expected, and the frames before the tear are still good.
- **`src/wal.rs`** — segmented, fsync-before-ack, batch-oriented. One fsync per `append` call, which is the hook group commit will hang off — not yet built, so the engine currently pays one fsync per write. `truncate_through(watermark)` drops whole sealed segments and is safe only *after* the Iceberg commit lands.
- **`src/memtable.rs`** — skiplist, with the active/frozen split so a flush to object storage never blocks a writer. Tombstones are stored as entries, and `Lookup::Deleted` vs `Lookup::Missing` is a real distinction the tests pin down. Backpressure on the frozen queue is in place — with an object store in the flush path it is load-bearing, not decorative.
- **`src/store/latency.rs`** — the `ObjectStore` wrapper. Only the trait primitives are intercepted; the trait's default methods are built on those, so they inherit the delay and there is no way to reach around it.
- **`src/catalog.rs`, `src/schema.rs`** — REST catalog, idempotent `ensure_table`, PK-sorted table, and the watermark property key.
- **`docker-compose.yml`** — MinIO plus `apache/iceberg-rest-fixture`.

Two things the implementation forced, both recorded below: the `object_store` version pin, and the hole in the latency shim.

## Crate picks

- `iceberg` + `iceberg-catalog-rest` 0.8 — REST catalog and the Parquet/DataFile writer
- `parquet` + `arrow` **57** — file format, bloom filters, row-group stats
- `object_store` **0.12** — S3 client; wrapped for latency injection
- `crossbeam-skiplist` — memtable
- `tokio` — async runtime

The three versions are not free choices. `iceberg` 0.8 builds against arrow/parquet 57, and parquet 57 against object_store 0.12. Taking newer versions of any of them puts *two* copies of that crate in the tree as distinct types — so an arrow `RecordBatch` cannot be handed to iceberg's writer, and our latency-wrapped `ObjectStore` cannot be handed to the Parquet reader at all. The shim would be bypassed precisely where it matters most. Iceberg's pins win.

## Open issue: the latency shim has a hole in it

`iceberg` does its own I/O through **opendal**, not through `object_store`. Our latency shim wraps `object_store`, so:

- **Parquet data-file reads and writes** go through the shim. These are the reads the p99 curve is about, and they are correctly charged.
- **Iceberg catalog, metadata, and manifest I/O** goes through opendal and pays **no injected latency**. Manifest reads in the read path will therefore look free when on real S3 they are a round trip each.

This does not affect correctness, only measurement, and only on the metadata side. It has to be resolved before step 7 produces a number anyone should believe. Options, roughly in order of preference:

1. Cache table metadata and manifests locally in the engine (which a real system would do anyway), so the uncharged reads happen once at startup rather than per point read. This makes the hole small and honest, and is useful work regardless.
2. Charge the manifest round trips explicitly at our own call sites — sleep the configured GET latency per manifest read. Crude, but it puts the cost where it belongs.
3. Attach a latency layer to the opendal `Operator` inside iceberg's `FileIO`, if 0.8 exposes a way to (it may not).

Deferred until step 6, when the read path exists and the real shape of the metadata traffic is visible.
