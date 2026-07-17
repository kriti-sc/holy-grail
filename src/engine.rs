//! The engine: write path, read path, flush, recovery.
//!
//! Recovery is the whole thesis in one function. `open` reads the watermark off
//! the Iceberg table, replays the WAL suffix above it, and the node is back —
//! with no local checkpoint file, no local record of what was flushed, nothing
//! that would make local state authoritative. If `open` needed anything from
//! disk beyond the WAL, the row tier would not be disposable and the claim would
//! be false.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use iceberg::table::Table;
use iceberg_catalog_rest::RestCatalog;
use object_store::ObjectStore;

use crate::cache::{ByteCache, CacheStatsSnapshot, MetadataCache};
use crate::config::Config;
use crate::error::Result;
use crate::flush::{self, Flushed, Plan};
use crate::index::{published_watermark, FileIndex};
use crate::memtable::{Lookup, MemtableSet};
use crate::read::ColumnarReader;
use crate::record::{Lsn, Record};
use crate::store::{self, LatencyStore, StatsSnapshot};
use crate::wal::Wal;
use crate::{catalog, Error};

/// Where to stop, for the crash harness. Each variant is a window in the flush
/// protocol that a real crash could land in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashAt {
    /// After freezing the memtable, before any Parquet is written.
    BeforeWrite,
    /// After the Parquet file is in object storage, before the Iceberg commit.
    /// Leaves an orphan object.
    AfterWrite,
    /// After the commit is published, before the WAL is truncated. Leaves the
    /// WAL holding records that are already in the columnar level.
    AfterCommit,
    /// After truncation, before the memtable is retired.
    AfterTruncate,
}

pub struct Engine {
    cfg: Config,
    catalog: RestCatalog,

    table: RwLock<Table>,
    index: RwLock<Arc<FileIndex>>,

    memtables: Arc<MemtableSet>,
    wal: Arc<Mutex<Wal>>,
    next_lsn: AtomicU64,

    store: Arc<LatencyStore>,
    cache: Arc<ByteCache>,
    reader: ColumnarReader,
}

impl Engine {
    /// Open the engine, rebuilding all local state from `Iceberg + WAL`.
    pub async fn open(cfg: Config) -> Result<Self> {
        let store = store::build(&cfg.s3, cfg.latency)?;
        let catalog = catalog::connect(&cfg.catalog, &cfg.s3).await?;
        let table = catalog::ensure_table(&catalog, &cfg.catalog).await?;

        // The boundary between the columnar level and the WAL suffix. Read from
        // the catalog, which is the only thing authorised to know it.
        let watermark = published_watermark(&table);
        let index = FileIndex::load(&table).await?;

        // Replay only what the columnar level does not already have. Records at
        // or below the watermark are durable in Iceberg; replaying them would be
        // harmless but pointless.
        let (wal, replay) = Wal::open(&cfg.wal_dir, cfg.wal_segment_bytes, watermark)?;

        let memtables = Arc::new(MemtableSet::new(
            cfg.memtable_max_bytes,
            cfg.max_frozen_memtables,
        ));
        let replayed = replay.records.len();
        for rec in replay.records {
            memtables.insert(rec);
        }

        // The next LSN must clear everything on disk *and* everything published,
        // or a new write could reuse an LSN that a flushed file already claims.
        let next_lsn = replay.max_lsn.max(watermark) + 1;

        let cache = ByteCache::new(cfg.cache_bytes as u64);
        let metadata = MetadataCache::new();
        let reader = ColumnarReader::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            cfg.s3.bucket.clone(),
            Arc::clone(&cache),
            metadata,
        );

        tracing::info!(
            watermark,
            replayed,
            files = index.len(),
            next_lsn,
            "recovered"
        );

        Ok(Engine {
            cfg,
            catalog,
            table: RwLock::new(table),
            index: RwLock::new(Arc::new(index)),
            memtables,
            wal: Arc::new(Mutex::new(wal)),
            next_lsn: AtomicU64::new(next_lsn),
            store,
            cache,
            reader,
        })
    }

    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<Lsn> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        self.append(Record::put(key, lsn, value)).await?;
        Ok(lsn)
    }

    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<Lsn> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        self.append(Record::delete(key, lsn)).await?;
        Ok(lsn)
    }

    /// Durable, then visible. Never the other way round.
    async fn append(&self, rec: Record) -> Result<()> {
        let wal = Arc::clone(&self.wal);
        let durable = rec.clone();

        // fsync blocks. Keep it off the runtime's worker threads.
        tokio::task::spawn_blocking(move || wal.lock().unwrap().append(&[durable]))
            .await
            .map_err(|e| Error::Config(format!("wal append panicked: {e}")))??;

        // Only now is the value allowed to be seen. Inserting before the fsync
        // would publish a value that a crash could still destroy.
        self.memtables.insert(rec);

        if self.memtables.should_freeze() {
            self.flush().await?;
        }

        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // The row tier answers first, and its answer is final — including when
        // that answer is "deleted".
        match self.memtables.get(key) {
            Lookup::Found(v) => return Ok(Some(v)),
            Lookup::Deleted => return Ok(None),
            Lookup::Missing => {}
        }

        let index = Arc::clone(&self.index.read().unwrap());
        Ok(match self.reader.get(&index, key).await? {
            Lookup::Found(v) => Some(v),
            Lookup::Deleted | Lookup::Missing => None,
        })
    }

    pub async fn flush(&self) -> Result<Flushed> {
        self.flush_inner(None).await
    }

    /// Flush, optionally dying partway through.
    ///
    /// The crash points are the windows a real process death could land in. The
    /// protocol's claim is that every one of them either loses something
    /// reconstructible or duplicates something the watermark makes idempotent —
    /// and that none of them can lose an acknowledged write.
    pub async fn flush_inner(&self, crash: Option<CrashAt>) -> Result<Flushed> {
        let Some(frozen) = self.memtables.freeze()? else {
            return Ok(Flushed::Empty);
        };

        // Crash here: the frozen memtable is gone, but its records are still in
        // the WAL above the watermark. Replay rebuilds them.
        if crash == Some(CrashAt::BeforeWrite) {
            return Ok(Flushed::Empty);
        }

        let table = self.table.read().unwrap().clone();

        let watermark = match flush::plan(&table, &frozen) {
            Plan::Empty => {
                self.memtables.retire(&frozen);
                return Ok(Flushed::Empty);
            }
            // Already published: a retry after a crash between commit and
            // truncate. Skip write and commit entirely, and go finish the job.
            Plan::AlreadyPublished { watermark } => {
                self.truncate_wal(watermark).await?;
                self.memtables.retire(&frozen);
                return Ok(Flushed::AlreadyPublished { watermark });
            }
            Plan::Write { watermark } => watermark,
        };

        let data_files = flush::write_files(&table, &frozen, watermark, self.cfg.latency).await?;
        let files = data_files.len();

        // Crash here: an orphan Parquet object is left in the bucket. It is
        // unreferenced garbage, not corruption, and the retry overwrites it —
        // the file name is derived from the watermark.
        if crash == Some(CrashAt::AfterWrite) {
            return Ok(Flushed::Empty);
        }

        let table =
            flush::commit_files(&table, &self.catalog, data_files, watermark, self.cfg.latency)
                .await?;

        // Published. From this instant the columnar level owns these records,
        // and the WAL copy is redundant rather than load-bearing.
        self.publish(table).await?;

        // Crash here: the WAL still holds records that are already in Iceberg.
        // Replay skips them (`lsn > watermark`) and the truncation is retried.
        if crash == Some(CrashAt::AfterCommit) {
            return Ok(Flushed::Committed { watermark, files });
        }

        self.truncate_wal(watermark).await?;

        // Crash here: nothing is lost. The memtable is a cache of what is
        // already durable in the columnar level.
        if crash == Some(CrashAt::AfterTruncate) {
            return Ok(Flushed::Committed { watermark, files });
        }

        // Retire only now. Dropping the memtable any earlier would open a window
        // where acknowledged writes are in neither tier.
        self.memtables.retire(&frozen);

        Ok(Flushed::Committed { watermark, files })
    }

    /// Swap in the post-commit table and rebuild the file index from it.
    async fn publish(&self, table: Table) -> Result<()> {
        let index = FileIndex::load(&table).await?;
        *self.table.write().unwrap() = table;
        *self.index.write().unwrap() = Arc::new(index);
        Ok(())
    }

    async fn truncate_wal(&self, watermark: Lsn) -> Result<()> {
        let wal = Arc::clone(&self.wal);
        tokio::task::spawn_blocking(move || wal.lock().unwrap().truncate_through(watermark))
            .await
            .map_err(|e| Error::Config(format!("wal truncate panicked: {e}")))??;
        Ok(())
    }

    /// The watermark currently published in the catalog.
    pub fn watermark(&self) -> Lsn {
        published_watermark(&self.table.read().unwrap())
    }

    pub fn file_count(&self) -> usize {
        self.index.read().unwrap().len()
    }

    pub fn cache_stats(&self) -> CacheStatsSnapshot {
        self.cache.stats()
    }

    pub fn store_stats(&self) -> StatsSnapshot {
        self.store.stats().snapshot()
    }

    pub fn reset_stats(&self) {
        self.cache.reset_stats();
    }
}
