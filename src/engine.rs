//! The engine: write path, read path, flush, recovery.
//!
//! Recovery is the whole thesis in one function. `open` reads the watermark off
//! the DuckLake catalog, replays the WAL suffix above it, and the node is back —
//! with no local checkpoint file, no local record of what was flushed, nothing
//! that would make local state authoritative. If `open` needed anything from
//! disk beyond the WAL, the row tier would not be disposable and the claim would
//! be false.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use object_store::ObjectStore;

use crate::cache::{ByteCache, CacheStatsSnapshot, MetadataCache};
use crate::catalog::DuckLake;
use crate::config::Config;
use crate::error::Result;
use crate::flush::{self, Flushed, Plan};
use crate::index::FileIndex;
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
    /// After freezing the memtable, before anything is staged.
    BeforeWrite,
    /// After the memtable is staged to a local Parquet file, before DuckDB
    /// publishes it. Leaves an orphan staging file; nothing in the lake.
    AfterWrite,
    /// After DuckDB commits the snapshot, before the WAL is truncated. Leaves the
    /// WAL holding records that are already in the columnar level.
    AfterCommit,
    /// After truncation, before the memtable is retired.
    AfterTruncate,
}

pub struct Engine {
    cfg: Config,

    lake: DuckLake,
    index: RwLock<Arc<FileIndex>>,

    memtables: Arc<MemtableSet>,
    wal: Arc<Mutex<Wal>>,
    next_lsn: AtomicU64,

    /// Where flush stages local Parquet before DuckDB publishes it. Per-engine,
    /// so concurrent engines in tests do not collide on the deterministic name.
    staging_dir: std::path::PathBuf,

    store: Arc<LatencyStore>,
    cache: Arc<ByteCache>,
    reader: ColumnarReader,
}

impl Engine {
    /// Open the engine, rebuilding all local state from `DuckLake + WAL`.
    pub async fn open(cfg: Config) -> Result<Self> {
        let store = store::build(&cfg.s3, cfg.latency)?;
        let lake = catalog::DuckLake::connect(&cfg.catalog).await?;

        // The boundary between the columnar level and the WAL suffix. Read from
        // the catalog, which is the only thing authorised to know it.
        let watermark = lake.published_watermark().await?;
        let index = FileIndex::load(&lake).await?;

        // Replay only what the columnar level does not already have. Records at
        // or below the watermark are durable in DuckLake; replaying them would be
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

        let staging_dir = cfg.wal_dir.join("staging");
        std::fs::create_dir_all(&staging_dir)?;

        let cache = ByteCache::new(cfg.cache_bytes as u64);
        let metadata = MetadataCache::new();
        let reader = ColumnarReader::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            lake.bucket.clone(),
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
            lake,
            index: RwLock::new(Arc::new(index)),
            memtables,
            wal: Arc::new(Mutex::new(wal)),
            next_lsn: AtomicU64::new(next_lsn),
            staging_dir,
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
    /// protocol's claim is that every one either loses something reconstructible
    /// or duplicates something the watermark makes idempotent — and that none of
    /// them can lose an acknowledged write.
    pub async fn flush_inner(&self, crash: Option<CrashAt>) -> Result<Flushed> {
        let Some(frozen) = self.memtables.freeze()? else {
            return Ok(Flushed::Empty);
        };

        // Crash here: the frozen memtable is gone, but its records are still in
        // the WAL above the watermark. Replay rebuilds them.
        if crash == Some(CrashAt::BeforeWrite) {
            return Ok(Flushed::Empty);
        }

        let published = self.lake.published_watermark().await?;

        let watermark = match flush::plan(published, &frozen) {
            Plan::Empty => {
                self.memtables.retire(&frozen);
                return Ok(Flushed::Empty);
            }
            // Already published: a retry after a crash between publish and
            // truncate. Skip staging and publishing, and go finish the job.
            Plan::AlreadyPublished { watermark } => {
                self.truncate_wal(watermark).await?;
                self.memtables.retire(&frozen);
                return Ok(Flushed::AlreadyPublished { watermark });
            }
            Plan::Write { watermark } => watermark,
        };

        let staging = flush::staging_path(&self.staging_dir, watermark);
        flush::stage(&frozen, &staging).await?;

        // Crash here: an orphan local staging file is left. It is not in the
        // lake, so nothing references it; the retry re-stages to the same name.
        if crash == Some(CrashAt::AfterWrite) {
            return Ok(Flushed::Empty);
        }

        flush::publish(
            &self.cfg.duckdb,
            &self.cfg.catalog,
            &self.cfg.s3,
            &staging,
            self.cfg.latency,
        )
        .await?;

        // Published. From this instant the columnar level owns these records,
        // and the WAL copy is redundant rather than load-bearing.
        self.publish_index().await?;

        // Crash here: the WAL still holds records that are already in DuckLake.
        // Replay skips them (`lsn > watermark`) and the truncation is retried.
        if crash == Some(CrashAt::AfterCommit) {
            return Ok(Flushed::Committed { watermark, files: 1 });
        }

        self.truncate_wal(watermark).await?;

        // Crash here: nothing is lost. The memtable is a cache of what is
        // already durable in the columnar level.
        if crash == Some(CrashAt::AfterTruncate) {
            return Ok(Flushed::Committed { watermark, files: 1 });
        }

        // Retire only now. Dropping the memtable any earlier would open a window
        // where acknowledged writes are in neither tier.
        self.memtables.retire(&frozen);
        let _ = tokio::fs::remove_file(&staging).await;

        Ok(Flushed::Committed { watermark, files: 1 })
    }

    /// Rebuild the file index from the catalog after a publish.
    async fn publish_index(&self) -> Result<()> {
        let index = FileIndex::load(&self.lake).await?;
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

    /// The watermark currently published in the catalog, as reflected by the
    /// in-memory index (rebuilt after every publish).
    pub fn watermark(&self) -> Lsn {
        self.index.read().unwrap().watermark()
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
