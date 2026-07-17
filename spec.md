**What we're building**

A single-node key-value storage engine that demonstrates the "row cache over columnar" architecture: an LSM tree whose durable bottom level is an Iceberg table on object storage, rather than proprietary SSTables on local disk.

Writes land in a WAL-backed memtable and are acknowledged at row-store latency. Accumulated writes are periodically flushed as PK-sorted Parquet files and committed to Iceberg, each commit stamped with the LSN watermark it covers. Reads consult the memtable first, then fall through to the columnar level via manifest range pruning and bloom filters. The result is a system where Iceberg plus the WAL suffix is the complete source of truth, and all local state — memtable, caches — is a disposable, rebuildable acceleration layer.

The prototype exists to replace two adjectives with numbers:

1. **"OLTP-adjacent reads over Iceberg"** — measure p99 point-read latency as a function of working-set-to-cache ratio, under realistic object-storage latency. Where the curve knees is where the architecture stops deserving the OLTP label.
2. **"Disposable row tier"** — the flush protocol (publish-then-truncate, idempotent recovery via watermark) is what makes local state safe to lose. Correctness here is what licenses the rebuild claim.

Out of scope: SQL, MVCC, distribution, compaction, and fresh OLAP (analytical scans merging the WAL suffix must be served through this engine and are deferred with MVCC; stale OLAP over Iceberg needs no work — external engines query the table directly, correct as of the last watermark).

**Checklist**

1. MinIO deployment — local S3 semantics
2. Iceberg REST catalog pointed at MinIO
3. WAL + memtable — durable write path, fsync-before-ack
4. Flush — freeze memtable, write PK-sorted Parquet, Iceberg commit carrying watermark LSN
5. Idempotent flush recovery — watermark check on WAL replay; dedup-on-retry via deterministic snapshot ID
6. Point read — memtable → manifest interval map → bloom filter → row-group fetch