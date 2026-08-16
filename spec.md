**What we're building**

A single-node key-value storage engine that demonstrates the "row cache over columnar" architecture: an LSM tree whose durable bottom level is a **DuckLake** table — Parquet on object storage with metadata (snapshots, file list, stats, blooms) in Postgres — rather than proprietary SSTables on local disk.

Writes land in a WAL-backed memtable and are acknowledged at row-store latency. Accumulated writes are periodically flushed as PK-sorted Parquet files and published to DuckLake (by the forked DuckDB binary), each flush carrying the LSN watermark it covers — derived from the `lsn` column's max stat, so it is atomic with the data by construction. Reads consult the memtable first, then fall through to the columnar level via the catalog's PK interval map and a catalog-side bloom filter probed in memory. The result is a system where DuckLake plus the WAL suffix is the complete source of truth, and all local state — memtable, caches — is a disposable, rebuildable acceleration layer.

The prototype exists to replace two adjectives with numbers:

1. **"OLTP-adjacent reads over the lakehouse"** — measure p99 point-read latency as a function of working-set-to-cache ratio, under realistic object-storage latency. Where the curve knees is where the architecture stops deserving the OLTP label.
2. **"Disposable row tier"** — the flush protocol (publish-then-truncate, idempotent recovery via watermark) is what makes local state safe to lose. Correctness here is what licenses the rebuild claim.

Out of scope: SQL, MVCC, distribution, compaction, and fresh OLAP (analytical scans merging the WAL suffix must be served through this engine and are deferred with MVCC; stale OLAP over DuckLake needs no work — external engines query the table directly, correct as of the last watermark; verified by pointing a stock DuckDB at the catalog and reading rows this engine wrote).

**Checklist**

1. MinIO deployment — local S3 semantics
2. DuckLake catalog on Postgres, bootstrapped by the forked DuckDB binary
3. WAL + memtable — durable write path, fsync-before-ack
4. Flush — freeze memtable, stage PK-sorted Parquet, publish via DuckDB (data file + catalog rows in one transaction), watermark derived from the `lsn` max stat
5. Idempotent flush recovery — watermark check on WAL replay; DuckDB's INSERT is the atomic unit, the watermark check prevents a double-publish
6. Point read — memtable → catalog interval map → in-memory catalog bloom → row-group fetch