# Decisions

Implementation decisions and the reasoning behind them. Ordered by build step. A decision lands here when it was not forced — when a reasonable person could have gone the other way, and the choice has consequences someone will trip over later.

Status of each step is tracked in [PLAN.md](PLAN.md). The architecture and the two claims being tested are in [spec.md](spec.md).

---

## Cross-cutting

### The row tier is a cache; Iceberg is the store

Not "an LSM tree that happens to persist into Iceberg." The columnar table already holds the full historical evolution of the row data (it is fed by CDC), so the OLTP engine is a *derived, disposable serving layer* over it. `Iceberg + WAL suffix` is the complete truth.

This inverts what the code is for. Flush and recovery are not durability plumbing — they are the contract that makes the row tier rebuildable. The point read is not a feature — it is the evidence that a columnar historical log can serve OLTP lookups when the hot set is cached.

**Consequence:** anything that makes local state authoritative is a bug, no matter how convenient. If a fact exists only in the memtable or only in the WAL past the watermark, it must be reconstructible or it must not be relied on.

### Language: Rust

Latency measurement is the deliverable. A runtime whose own p99 noise is comparable to the effect being measured cannot produce the headline number. Python with pyiceberg would have been faster to write and useless to measure.

---

## Step 0 — Record model and schema

### Record is `(pk, lsn, op, value)`

One row per mutation. `op` is `Put` or `Delete`. This same shape is the WAL frame, the memtable entry, and the Parquet row — no translation layers, no second model.

### Deletes are tombstones, and tombstones are entries — never removals

A `Delete` inserts a record. It does **not** remove the key from the memtable.

This is the sharpest footgun in the whole design. If `delete(k)` removed `k` from the map, the read would find nothing at the memtable level, fall through to the columnar level, hit an older Parquet file, and **resurrect the value the delete was supposed to erase**. With no compaction, those older files never go away, so the resurrection is permanent.

This is why `Lookup` has three variants and not two:

```rust
enum Lookup { Found(Bytes), Deleted, Missing }
```

`Deleted` is a definitive answer that terminates the search. `Missing` means "not at this level, keep looking." Collapsing them into `Option` would make the bug unrepresentable-in-the-type-system in exactly the wrong direction — it would make the bug *easy*. Tests pin this behaviour directly (`delete_leaves_a_tombstone_not_a_hole`).

### `lsn` is stored per row, even though point reads never use it

Point reads resolve versions by *file* ordering (see "File ordering" below), not by row LSN. So per-row `lsn` is, strictly, dead weight for the current read path.

Kept anyway: it is nearly free, it makes a flushed file self-describing (you can tell what a file covers by reading it, without the catalog), and any later work that merges across files — compaction, MVCC, the deferred fresh-OLAP path — needs it. Removing it later is easy; adding it to files already written is not.

### The table is unpartitioned

Partitioning buys nothing for point reads. Pruning is done by the PK interval map over file and row-group statistics, not by partition values. A partition spec would add manifest overhead and a bucketing decision with no read-path payoff.

### Field IDs are fixed and mirrored into Arrow

`pk=1, lsn=2, op=3, value=4`. Arrow carries them in field metadata under `PARQUET:field_id` so the writer stamps them into the Parquet file and Iceberg binds columns by ID rather than by name. A test asserts the Arrow and Iceberg schemas agree on every ID, name, and nullability — a drift here produces files that read back as all-nulls, which is a miserable thing to debug.

`value` is nullable; the rest are required. A tombstone has no value.

---

## Step 1 — Infrastructure

### `apache/iceberg-rest-fixture` as the catalog

The reference REST catalog, backed by MinIO. Not production-grade, but it is the spec's actual semantics, which is what the flush protocol is being tested against.

### The watermark lives in the snapshot summary

Key: `holy-grail.watermark-lsn`, set as a snapshot property on every flush commit.

This is the single most load-bearing piece of the design. It is what tells a recovering node where the columnar level ends and the WAL suffix begins. It is *in the snapshot*, not in a side table or a local file, because it must be atomic with the data it describes: a commit either publishes the files and the watermark together, or neither.

---

## Step 2 — Object store and latency injection

### MinIO's latency is a lie, so the client tells the truth

MinIO answers in about a millisecond. Real S3 does not. Every number this prototype produces — where the p99 curve knees, how far flush falls behind, whether writes stall — is a function of object-store latency. So `LatencyStore` wraps the `ObjectStore` and sleeps before each operation, with configurable per-class delay (`get`/`put`/`list`/`delete`) plus uniform jitter.

Injection is **off by default** so correctness tests run at full speed. A benchmark has to ask for it explicitly (`HG_LATENCY=none|s3|express`).

### The injected distribution is lognormal, because uniform jitter cannot express a tail

The first version was `base + uniform(0, jitter)`, on the reasoning that constant latency makes tail percentiles a fiction. Correct reasoning, insufficient fix — uniform jitter is the same fiction one step out.

Work out what a uniform draw can express. `p50 = base + J/2`, `p99 ≈ base + 0.99·J`, so the largest `p99/p50` ratio it can produce — even with `base = 0` — is about **2**. Real S3 GET is 86/26 ≈ **3.3**. There is no `(base, jitter)` pair that fits both measured percentiles; solve the simultaneous equations and `base` comes out **negative**.

So the old model could not represent S3's tail at any parameterisation, and the tail *is* the deliverable. The p99 we published would have been an artifact of the distribution we chose, not a property of the storage we claim to model. Nothing in the test suite could have caught that — the same blind spot as the bloom filter, one layer up.

Now: each op class is a **lognormal fitted to a measured `(p50, p99)`**. Lognormal is the standard shape for request latency — a product of many multiplicative effects (queueing, retries, stragglers), heavy-tailed, strictly positive. The fit is exact rather than tuned: `median = exp(μ)`, `p99 = median · exp(σ·z99)`, so both parameters fall straight out of the two percentiles. `a_fitted_lognormal_reproduces_its_percentiles` draws 200k samples and asserts both come back.

The numbers are **cited, not invented**:

| Op | p50 | p99 | Source |
|---|---|---|---|
| GET | 26 ms | 86 ms | [nixiesearch](https://nixiesearch.substack.com/p/benchmarking-read-latency-of-aws), 0.5 MB, same region |
| PUT | 70 ms | 137 ms | [TopicPartition](https://topicpartition.io/misc/AWS-S3-PUT-latency-benchmark), 500 KB, eu-north-1 |
| LIST, DELETE | — | — | **uncited.** Guessed, and off the read path |

`s3_express()` is the second arm: GET p50 3 ms / p99 15 ms. If p99 knees on Standard but not on Express, the knee belongs to S3's tail rather than to this architecture — a result worth having either way.

Charging N uncharged round trips draws N times and **sums**, rather than drawing once and multiplying. Multiplying would understate the variance of the sum and make a slow commit slow in every op at once, which is not how a tail behaves.

### Only the trait primitives are intercepted

The `ObjectStore` trait builds `get`, `put`, `get_range` and friends on top of `get_opts` / `put_opts` as default methods. Wrapping only the primitives means the conveniences inherit the delay automatically — and, more importantly, that there is **no way to reach around the shim**. A future call site cannot accidentally bypass it by picking a different method.

### `get_ranges` is overridden to charge one round trip, not N

The default implementation fans a multi-range read out into one `get_range` per coalesced run, which would charge a full round trip per run. A real client issues those concurrently. One delay is the honest charge, and the read path fetches several row-group ranges at a time.

### Versions are dictated by iceberg, not chosen

`iceberg` 0.8 builds against **arrow/parquet 57**, and parquet 57 against **object_store 0.12**. We must match all three.

This is not a style preference. Cargo will happily put two major versions of a crate in one tree, and they are *distinct types*. Two concrete failures hit during implementation:

* With arrow 59 in our manifest, `RecordBatch` was a different type from the `RecordBatch` iceberg's writer expects, and the flush path simply would not compile.
* With object_store 0.13, our latency-wrapped store was a different `ObjectStore` from the one `ParquetObjectReader` accepts — so the shim could not be attached to the reader **at all**, and would have been silently bypassed on exactly the data-file reads the whole measurement is about.

The rule that came out of it: **whatever iceberg pins, we pin.** It sits at the bottom of the stack and everything else has to meet it there.

### Known hole: iceberg does its own I/O through opendal

`iceberg` 0.8 uses **opendal**, not `object_store`, for catalog, metadata, and manifest I/O. Our shim cannot see it.

- Parquet **data-file** reads and writes go through the shim and are charged correctly. These are the reads the p99 curve is about.
- Iceberg **metadata and manifest** I/O pays **no injected latency**. On real S3 each of those is a round trip.

Correctness is unaffected; only measurement is, and only on the metadata side. Handled in two places: the read path caches manifests in-process (see "File index" below), collapsing the uncharged reads into a one-time startup cost rather than a per-read lie; the flush path charges them by hand (see "Charging the opendal hole").

### Charging the opendal hole

The flush path used to charge `3` PUTs per commit — a guess, with a comment that said so. Guesses that feed backpressure are not acceptable in a benchmark, so the number was measured: `mc admin trace` against MinIO while `examples/trace_flush.rs` ran a single flush.

One commit, nine S3 requests:

| Issued by | Ops | What |
|---|---|---|
| Our process (opendal) | 1 PUT | the Parquet data file |
| Our process (opendal) | 2 PUT | manifest (`*-m0.avro`), manifest list (`snap-*.avro`) |
| Our process (opendal) | 2 GET | manifest list and manifest, read back for the index refresh |
| REST catalog, server-side | 3 GET, 1 PUT | current `metadata.json` (three times), then the new one |

So the guess was wrong twice over: it charged **4 PUTs and no GETs**, where the truth is **3 PUTs and 2 GETs** on our side alone. GET latency was being treated as free in a commit that makes five of them.

**The catalog's four are charged too.** We do not issue them, but we block on the HTTP call that does, so they are part of flush wall-clock and therefore part of what drives backpressure. Excluding them would make flush look faster than it can ever be.

The counts now live in named constants next to the code that charges them, with the measurement method recorded. They are specific to iceberg 0.8's `fast_append` against the REST catalog — a version bump can change them, and `examples/trace_flush.rs` is how you find out.

**Still undercharged:** one PUT per data file. A real 64 MiB upload is multipart and costs more than one round trip. This matters only if flush throughput becomes a headline number rather than a backpressure input.

---

## Step 3 — WAL and memtable

### fsync before the value is visible, not after

Order is: append to WAL → fsync → insert into memtable → ack.

The tempting inversion (insert first, fsync after) publishes a value to readers before it is durable. A crash then destroys a value some reader already acted on. That is worse than losing an unacknowledged write.

### The WAL is batch-oriented, but group commit is not built yet

`Wal::append` takes a slice, not a record, and pays a single fsync for the whole batch. Per-write fsync is 0.1–1ms and serializes everything.

That is the seam group commit will hang off. It is not yet built: `Engine::append` passes a one-record slice, so the write path today pays one fsync per write, and concurrent writers serialize on the WAL mutex with no amortisation. This is a throughput ceiling, not a durability hole — and worth stating plainly, because the two get conflated. Group commit gathers N concurrent writes, fsyncs them **once**, and acks all N only after that fsync returns. No writer is acked early; the fsync *cost* is amortised, the fsync *ordering* is not weakened. Losing acked writes requires decoupling ack from fsync (ack now, fsync on a timer), which this engine does not do and should not.

### Memtable insert is ordered by LSN, not by arrival

`Engine::put` takes its LSN from an atomic counter *before* it takes the WAL mutex, so two concurrent writes to the same key can reach the memtable in the opposite order to their LSNs. `Memtable::insert` therefore uses `compare_insert` and accepts a record only if it outranks the entry already there.

A plain last-write-wins insert is a silent correctness bug, not a race that resolves itself. The older value stands in the memtable, flush iterates the memtable and writes *that* value into Parquet, and the commit is stamped with the higher watermark — so the stale value becomes permanent in the columnar level, under a watermark that claims to cover the write that should have beaten it. WAL replay reproduces it faithfully, which makes recovery consistent and still wrong.

The alternative — allocate the LSN under the WAL lock, so arrival order *is* LSN order — was rejected: it forecloses group commit, which needs to assign LSNs to a batch before the batch takes the lock.

### WAL I/O is synchronous `std::fs`, on purpose

fsync blocks whatever thread it is on regardless, so async buys nothing here. Synchronous code keeps the crash-injection points in step 5 simple to reason about and simple to place. Callers on the runtime wrap it in `spawn_blocking`.

### A torn tail ends replay; it does not fail it

Each frame is `crc32 | len | payload`. On replay, a short read, an impossible length, or a bad CRC means "the log ends here" — not an error. A crash mid-append is the *expected* case, and the frames before the tear are still good. The file is then truncated back to the last intact frame so the next append starts clean.

Past the CRC check, malformed payloads *are* errors: the bytes are intact, so anything wrong is a bug or deliberate corruption, not a torn write. Different failure, different handling.

### Segments, deleted whole, never rewritten

Segments are named by the LSN they start at. Truncation deletes entire sealed segments whose highest LSN is at or below the watermark. No prefix rewriting, no compaction of the log.

The active segment is never deleted even when fully covered — truncating the file being appended to is not worth the complexity, and replay skips its covered records anyway.

### Memtable is a skiplist, not a hash map

A hash map is the obvious pick for a point-read-only workload — O(1) versus O(log n).

Rejected, for two reasons that both bite at flush time. Flush must emit **PK-sorted** Parquet, and a hash map means sorting the entire table under memory pressure at exactly the moment memory is tightest. And a lock-free skiplist lets flush **iterate a frozen memtable while writers continue on the active one** — with an object store in the flush path, taking a lock for the duration of a flush would stall writes for an S3 round trip.

`crossbeam-skiplist`. This is also what RocksDB does, for the same reasons.

### Active plus frozen, swapped under a short lock

One mutable active memtable, plus a list of immutable frozen ones awaiting flush. Freeze swaps in a fresh active under a write lock held only for the swap; flush then reads the frozen memtable with **no lock at all**.

Reads go active → frozen (newest first) → columnar, so a newer value always shadows an older one, including when the newer write is a delete.

### A flushed memtable is retired only after its commit is durable

Dropping it at flush *start* would make its records unreadable while still absent from the columnar level — a window where acknowledged writes vanish. `retire()` is called after the Iceberg commit lands.

### Backpressure on the frozen queue is load-bearing

Flush goes to object storage, with deliberately injected latency. It is slow **by construction** — that is the experiment. If writes outrun flush and the frozen list grows unbounded, the process dies of memory exhaustion and the benchmark produces an OOM instead of a curve.

So `max_frozen` is a hard cap and `freeze()` returns `Err(WriteStalled)` when it is hit. Stalling writes is the intended behaviour, not a failure mode.

### Memtable size accounting deliberately runs high

`approx_bytes` counts every insert, including overwrites of existing keys, so a write-heavy-on-few-keys workload over-reports. The bias is chosen: overestimating flushes early (harmless), underestimating overshoots the memory budget (not harmless). Exact accounting would need per-key delta tracking for no benefit.

---

## Step 4 — Flush

### File ordering is total, by watermark — so reads are newest-first, first-hit-wins

There is no compaction, so flushed files accumulate and overlap on PK. The naive consequence would be a per-row LSN merge across every candidate file.

Avoided: each file's Iceberg snapshot carries the watermark LSN it covers, and watermarks are monotonic, so **file ordering is total**. A point read scans candidate files newest-first and the **first hit wins** — no merge, no version reconciliation. A tombstone hit is a not-found and terminates the scan.

Within a single flush a key appears at most once, because the memtable already collapsed it to its latest value. That is what makes per-file ordering sufficient rather than merely convenient.

### Publish, then truncate

Order: write Parquet → commit to Iceberg (carrying the watermark) → **then** truncate the WAL.

The reverse loses data on a crash in between: the records would be gone from the WAL and not yet in the columnar level. This ordering means a crash in the window leaves the WAL holding records already published — which recovery handles by *skipping* them, a benign duplicate, rather than by inventing them, an impossible one.

### Idempotency comes from the watermark check, not from the commit UUID

Iceberg's `set_commit_uuid` does **not** deduplicate a snapshot. A retried commit with the same UUID still produces a *second* snapshot appending the same data files — i.e. duplicate rows. Relying on it for idempotency would be a silent correctness bug.

The actual mechanism: **before committing, read the published watermark from the table's current snapshot. If it is already at or above this flush's watermark, the flush has already landed — skip the commit entirely and proceed to truncate.** Watermarks are monotonic, so this is exact, not heuristic.

### Deterministic file and commit names, so a retry overwrites its own garbage

The watermark check makes a retry *correct*. Deterministic naming makes it *clean*.

Both the Parquet file name and the commit UUID are derived from the flush's watermark LSN. A flush that crashed after uploading its Parquet file but before committing leaves an orphan object; the retry writes to **the same object name** and overwrites it, rather than leaking a new one and abandoning the old. Same for the manifest list named by the commit UUID.

Without this, every crash-and-retry leaks a file that nothing references and nothing will ever clean up (there is no compaction or expiry to do it).

### Flush writes go through iceberg's writer, so their PUT latency is charged by hand

Iceberg's `ParquetWriter` produces the `DataFile` with correct statistics, bounds, and bloom filters — the hard part, and its stat-derivation helper is `pub(crate)` so it cannot be reused against a hand-rolled writer. But it writes through `FileIO`/opendal, which the latency shim cannot see (see the known hole above).

Hand-rolling the writer to route uploads through `LatencyStore` would mean hand-deriving Iceberg column statistics — bounds as `Datum`s, per-column sizes, value and null counts, split offsets — which is a large surface to get subtly wrong, and getting it wrong silently breaks the read path's pruning.

So: use iceberg's writer, and **charge the injected PUT latency explicitly at the call site**. Crude, but it puts the cost where it belongs, and the alternative risks the correctness of the thing being measured. Revisit if the write path becomes a headline number rather than a backpressure input.

---

## Step 5 — Recovery

### Recovery is a watermark read plus a WAL replay, and nothing else

On start: load the table, read the watermark from the current snapshot (absent snapshot ⇒ watermark 0), replay WAL records with `lsn > watermark` into a fresh memtable. Everything below the watermark is already columnar and is skipped.

No local checkpoint file, no local manifest of what was flushed. Local state that recovery *depended* on would contradict the thesis — the row tier has to be rebuildable from the catalog alone.

### The crash points that have to be survivable

The flush protocol has four windows. Each is exercised by the crash harness:

| Crash after | Consequence | Why it is safe |
|---|---|---|
| freeze, before Parquet write | Frozen memtable lost | Records still in WAL above the watermark; replay rebuilds them |
| Parquet upload, before commit | Orphan object in S3 | Unreferenced garbage, not corruption. Retry overwrites it (deterministic name) |
| commit, before WAL truncate | WAL holds published records | Replay skips them (`lsn > watermark`); truncation is retried |
| truncate, before retire | Nothing | Memtable is a cache of what is already in the columnar level |

The pattern: every window either loses something reconstructible, or duplicates something the watermark makes idempotent. Nothing in the protocol can lose an acknowledged write.

---

## Step 6 — Point read and cache

### The cache is byte ranges behind the Parquet reader, not whole files

"Locally cached Parquet" could mean caching whole files. Rejected: with a 64 MiB flush threshold, files are ~64 MiB, and a cache miss on a point read would fetch 64 MiB to answer a lookup for one key. That is not a system anyone would call OLTP-adjacent, and it would make the working-set-to-cache-ratio curve measure the wrong thing entirely.

Instead: a bytes-bounded LRU sits behind a custom `AsyncFileReader`, so **whatever granularity the Parquet reader asks for is what gets cached** — footer, then the specific column-chunk ranges of the specific row group that survived pruning. Two point reads to the same row group hit cache; a read to a cold row group fetches only that row group's ranges.

The cache is explicitly *not* a copy of the table. That is the entire experiment: sweep its size against the working set and find where p99 knees.

### File metadata is memoized per file, unbounded

Parquet footers are small and there are few files. Memoizing `ParquetMetaData` per path means the footer round trip is paid once per file rather than once per read. Real systems do exactly this.

It does mean the *first* read of a file pays footer latency and subsequent ones do not, which the benchmark must not mistake for a cache-size effect. Noted here so it is not rediscovered as a mystery.

### The read path, in order

1. **Memtable set** — active, then frozen newest-first. `Found` or `Deleted` terminates.
2. **File index** — candidate files, newest watermark first.
3. **PK interval prune** — skip any file whose `[pk_min, pk_max]` does not contain the key. Free: the bounds come from the manifest, already in memory.
4. **Bloom filter** — skip the file if the pk bloom says no. Costs one cached range read.
5. **Row-group prune** — PK min/max per row group, then fetch only the surviving row group's column chunks.
6. **First hit wins.** `Put` ⇒ value, `Delete` ⇒ not-found. Stop.

Steps 3 and 4 are what keep a point read from opening every file that has ever been flushed. With no compaction, the file count only grows, so pruning is not an optimisation here — it is what makes the read path viable at all.

### The file index is built from manifests and held in memory

Walk snapshots newest-first, read each one's manifests, collect the data files it added along with their PK bounds — all of which the manifest already carries, so the interval map costs no extra I/O.

Held in memory, and rebuilt from the post-commit table on each of our own flushes (`Engine::publish`). This is also the mitigation for the opendal latency hole: the uncharged manifest reads happen once at startup and once per flush, not once per point read, which is both faster and more honest than pretending manifests are free on every lookup.

### The cached index is sound only because this process is the sole writer

The index is a digest of the manifests, not a view of them: a point read consults the in-memory vector and never re-reads the catalog. Nothing invalidates it on a timer, and nothing checks the table's snapshot ID before serving a read. It is refreshed in exactly one place — after *our own* Iceberg commit lands.

So its correctness rests on an assumption that is true today and nowhere written down: **this engine is the only thing that mutates the table.** Single node, single writer, no compaction. Under that assumption the cache cannot be stale, because every mutation goes through the code path that rebuilds it.

Three things on the roadmap void it, and the failure modes are not symmetric:

* **A second writer.** Node B commits a file; node A's index never hears about it; a key B wrote reads back as `Missing` on A and falls through to nothing. A **silent wrong answer** — the dangerous one.
* **Compaction, or any Iceberg table maintenance** (`expire_snapshots`, `rewrite_manifests`, orphan cleanup). These delete files the cached index still points at, so a read 404s on an object that is genuinely gone. Loud, at least.
* **An external engine writing the table** — Spark, Trino. Either mode, depending on what it did.

**As soon as there is more than one writer, keeping this cache fresh stops being an optimisation and becomes a correctness requirement.** The fix is cheap and standard: record the snapshot ID the index was built from, compare it against `current_snapshot_id()` from the catalog, and reload when it has moved — on a background tick, or lazily on the read path. That reload is affordable precisely because the index is a digest: it is a manifest walk, not a data fetch. But it is a *read before serving a read*, and on the point-read path that is latency the current design does not pay. Budget for it when the second writer arrives; do not retrofit it as a bug fix under load.

### The bloom filter must be sized, or it is a megabyte

Enabling the `pk` bloom is not enough. Parquet's defaults are **NDV = 1,000,000** and **fpp = 0.05**, and a bloom filter is written per **column chunk** — that is, per *row group*, which holds `ROW_GROUP_ROWS` = 8192 keys, not a million. Left at the default, every filter is sized for 122× the keys it will ever hold: **~1 MiB each**.

Caught by tracing MinIO during a flush ([examples/trace_flush.rs](examples/trace_flush.rs)). A 100-record file was **1,053,621 bytes**, of which about 5 KB was data. A point read pulled a **1,048,684-byte** range off the object store to test one key.

The read-path cost was not the real damage. Cache size is the independent variable of the entire headline experiment — p99 against working-set-to-cache ratio. At 1 MiB per filter, an 8 MiB cache holds **eight bloom filters and no data**. The sweep would have produced a bloom-eviction curve and published it as a working-set curve. The number would have been wrong in a way nothing in the test suite could see.

Set both:

```rust
.set_column_bloom_filter_ndv("pk".into(), ROW_GROUP_ROWS as u64)
.set_column_bloom_filter_fpp("pk".into(), BLOOM_FPP)  // 0.01
```

`fpp` is bought down from the default 0.05 because a false positive costs a wasted row-group scan — real column-chunk fetches against an S3 round trip — and the filter is small either way.

Measured after: the file drops **1,053,621 → 21,423 bytes**, the bloom fetch **1,048,684 → 16,491**. The filter is still most of *that* file, but only because 100 records is a tenth of one row group; at a full 8192 the ratio inverts.

**The general lesson, which is the reason this is written down:** Parquet's tuning defaults assume analytical files — huge row groups, high cardinality. Every default this engine inherits should be read as a claim about a workload, and this is not that workload. `ROW_GROUP_ROWS` was already the same mistake caught once (a million-row default makes row-group pruning a no-op). The bloom sizing was the same mistake, one layer down, and it survived the first catch.

### Bloom filter reading needs an offset-shifting `ChunkReader`

`Sbbf::read_from_column_chunk` wants a `ChunkReader` whose offsets are absolute within the file, but we only fetch the bloom filter's byte range. So a small `OffsetChunkReader` wraps the fetched `Bytes` with a base offset and subtracts it. Twenty lines, and it keeps the bloom fetch to exactly the bytes needed rather than pulling the file.

If a column chunk has no `bloom_filter_length`, the bloom is skipped and the file is scanned — correct, just slower. Files written by this engine always have one.


---

## Things the implementation forced

Decisions that were not visible from the design and only surfaced once the code ran.

### Iceberg's `Binary` is arrow's `LargeBinary`

Iceberg maps its `Binary` primitive to `DataType::LargeBinary`, not `DataType::Binary`. Our Arrow schema originally said `Binary`.

The failure mode is nasty, and worth remembering. Writing `Binary` still produces a **valid Parquet file** — the physical type is `BYTE_ARRAY` either way, the writer accepts it, the commit succeeds, the file lands in the bucket. But the file's *embedded arrow schema* says `LargeBinary`, so every reader hands back a `LargeBinaryArray` while our code confidently downcasts to `BinaryArray` — and panics at runtime, in the read path, long after the write that caused it.

Nothing type-checks this. The engine's Arrow schema must mirror iceberg's type mapping exactly, and `arrow_mirrors_iceberg_field_ids` in `schema.rs` is the test that keeps it honest.

### The REST catalog fixture needs a file-backed database

`apache/iceberg-rest-fixture` defaults to an **in-memory** SQLite catalog. Under concurrent requests — which is to say, under `cargo test` running integration tests in parallel — each JDBC connection gets its *own empty database*, and calls fail with `no such table: iceberg_tables`.

This presents as a 500 from the catalog in the middle of a flush and looks exactly like an engine bug. It is not. `CATALOG_URI: jdbc:sqlite:file:/tmp/catalog.db` fixes it. (The path has to be one the container's user can write — a Docker volume mount is root-owned and fails with `SQLITE_CANTOPEN`.)

### Row groups must be small, or row-group pruning does nothing

Parquet's default row-group size is a million rows. At that size a flushed file is **one row group**, and "row-group pruning" prunes nothing — a point read fetches every column chunk in the file to find one key.

`ROW_GROUP_ROWS = 8192`. The cost is more metadata per file; the benefit is that a point read fetches one row group's column chunks instead of the file's. For this workload that is not close.

### What a point read actually costs

Measured on a cold cache, one file: **6 range fetches** — 2 for the footer (length, then the metadata block), 1 for the bloom filter, 3 for the surviving row group's `pk`/`op`/`value` column chunks. `lsn` is left out of the projection, so its chunk is never fetched. Confirmed against MinIO's request trace, not inferred from the code.

Count the *bytes*, not just the fetches. The bloom filter dominates them — see "The bloom filter must be sized" above, where a wrong default made that one fetch a megabyte while the fetch *count* stayed a reassuring 6.

The two properties that matter, both pinned by tests:

- **Constant in the file count.** Reading a key from the 1st file costs the same as from the 5th. Files are dismissed by the in-memory interval map.
- **A miss outside every file's PK range costs zero fetches.** Not one — zero. The manifest bounds are already in memory.

The 3 column-chunk fetches are issued **concurrently** (`get_byte_ranges` is overridden to `try_join_all`; the default runs them sequentially, which on a 20 ms link would turn one logical read into three serial round trips).

### Retiring the frozen memtable is what makes the columnar tests real

`Engine::flush` retires the frozen memtable after the commit lands. That means every read in the integration tests after a flush is a *genuine* columnar read — through the manifest, the bloom filter, and Parquet on MinIO — and not the memtable quietly answering and making the read path look correct when it isn't.

Worth stating because it would be easy, and disastrous, to "optimise" by keeping flushed data in memory: the tests would still pass and would no longer be testing anything.
