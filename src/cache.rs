//! The local Parquet cache — the variable the whole p99 experiment sweeps.
//!
//! "Locally cached Parquet" could mean caching whole files. It doesn't, here.
//! With a 64 MiB flush threshold, files are ~64 MiB, and a miss on a point read
//! would fetch 64 MiB to answer a lookup for one key. Nothing about that
//! deserves the word OLTP, and it would make the working-set-to-cache curve
//! measure the wrong thing.
//!
//! Instead a bytes-bounded LRU sits *behind* the Parquet reader, so whatever
//! granularity the reader asks for is what gets cached: the footer, then the
//! specific column-chunk ranges of the specific row group that survived pruning.
//! Two reads landing in the same row group hit cache; a read into a cold row
//! group fetches only that row group's ranges.
//!
//! The cache is explicitly not a copy of the table. Shrinking it below the
//! working set is the experiment.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Key {
    path: String,
    start: u64,
    end: u64,
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub bytes_fetched: AtomicU64,
    pub evictions: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bytes_fetched: u64,
    pub evictions: u64,
    pub bytes_resident: u64,
}

/// Bytes-bounded LRU over byte ranges of Parquet objects.
pub struct ByteCache {
    inner: Mutex<Inner>,
    max_bytes: u64,
    stats: CacheStats,
}

struct Inner {
    /// Range → (bytes, recency tick).
    entries: HashMap<Key, (Bytes, u64)>,
    /// Recency tick → range. The LRU victim is the lowest tick.
    by_recency: BTreeMap<u64, Key>,
    resident: u64,
    tick: u64,
}

impl ByteCache {
    pub fn new(max_bytes: u64) -> Arc<Self> {
        Arc::new(ByteCache {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                by_recency: BTreeMap::new(),
                resident: 0,
                tick: 0,
            }),
            max_bytes,
            stats: CacheStats::default(),
        })
    }

    fn get(&self, key: &Key) -> Option<Bytes> {
        let mut inner = self.inner.lock().unwrap();

        let (bytes, old_tick) = inner.entries.get(key).cloned()?;
        inner.by_recency.remove(&old_tick);
        inner.tick += 1;
        let tick = inner.tick;
        inner.by_recency.insert(tick, key.clone());
        inner.entries.insert(key.clone(), (bytes.clone(), tick));

        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(bytes)
    }

    fn insert(&self, key: Key, bytes: Bytes) {
        let size = bytes.len() as u64;

        // A range larger than the whole cache would evict everything and then
        // itself. Refuse it: it is served, just not remembered.
        if size > self.max_bytes {
            return;
        }

        let mut inner = self.inner.lock().unwrap();

        if let Some((_, old_tick)) = inner.entries.remove(&key) {
            inner.by_recency.remove(&old_tick);
            inner.resident -= size.min(inner.resident);
        }

        while inner.resident + size > self.max_bytes {
            let Some((victim_tick, victim)) = inner.by_recency.pop_first() else {
                break;
            };
            let _ = victim_tick;
            if let Some((evicted, _)) = inner.entries.remove(&victim) {
                inner.resident -= (evicted.len() as u64).min(inner.resident);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        inner.tick += 1;
        let tick = inner.tick;
        inner.by_recency.insert(tick, key.clone());
        inner.entries.insert(key, (bytes, tick));
        inner.resident += size;
    }

    pub fn stats(&self) -> CacheStatsSnapshot {
        let resident = self.inner.lock().unwrap().resident;
        CacheStatsSnapshot {
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            bytes_fetched: self.stats.bytes_fetched.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            bytes_resident: resident,
        }
    }

    pub fn reset_stats(&self) {
        self.stats.hits.store(0, Ordering::Relaxed);
        self.stats.misses.store(0, Ordering::Relaxed);
        self.stats.bytes_fetched.store(0, Ordering::Relaxed);
        self.stats.evictions.store(0, Ordering::Relaxed);
    }
}

/// Parsed Parquet footers, memoized per file.
///
/// Unbounded: footers are small and files are few. A real system does this too.
/// The consequence to keep in mind when reading a benchmark: the *first* read of
/// a file pays footer latency and later ones do not, which is not a cache-size
/// effect even though it can look like one.
#[derive(Default)]
pub struct MetadataCache {
    inner: Mutex<HashMap<String, Arc<ParquetMetaData>>>,
}

impl MetadataCache {
    pub fn new() -> Arc<Self> {
        Arc::new(MetadataCache::default())
    }

    fn get(&self, path: &str) -> Option<Arc<ParquetMetaData>> {
        self.inner.lock().unwrap().get(path).cloned()
    }

    fn insert(&self, path: String, meta: Arc<ParquetMetaData>) {
        self.inner.lock().unwrap().insert(path, meta);
    }
}

/// Fetches byte ranges of one object, through the cache and then the (latency
/// wrapped) object store.
#[derive(Clone)]
pub struct RangeFetcher {
    store: Arc<dyn ObjectStore>,
    path: Path,
    cache: Arc<ByteCache>,
}

impl RangeFetcher {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path, cache: Arc<ByteCache>) -> Self {
        RangeFetcher { store, path, cache }
    }

    async fn fetch_range(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        let key = Key {
            path: self.path.as_ref().to_string(),
            start: range.start,
            end: range.end,
        };

        if let Some(hit) = self.cache.get(&key) {
            return Ok(hit);
        }
        self.cache.stats.misses.fetch_add(1, Ordering::Relaxed);

        // The only place a read reaches object storage. Everything above this
        // line is free; everything below pays a round trip.
        let bytes = self
            .store
            .get_range(&self.path, range)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        self.cache
            .stats
            .bytes_fetched
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.cache.insert(key, bytes.clone());

        Ok(bytes)
    }
}

impl MetadataFetch for RangeFetcher {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move { self.fetch_range(range).await }.boxed()
    }
}

/// An `AsyncFileReader` that serves the Parquet reader out of the cache.
///
/// Plugging in here rather than wrapping `ParquetObjectReader` is what gives the
/// cache the reader's own granularity for free: it caches exactly the ranges the
/// reader decided it needed after pruning.
pub struct CachedReader {
    fetcher: RangeFetcher,
    file_size: u64,
    metadata: Arc<MetadataCache>,
}

impl CachedReader {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
        cache: Arc<ByteCache>,
        metadata: Arc<MetadataCache>,
    ) -> Self {
        CachedReader {
            fetcher: RangeFetcher::new(store, path, cache),
            file_size,
            metadata,
        }
    }

    pub fn fetcher(&self) -> &RangeFetcher {
        &self.fetcher
    }

    pub async fn fetch(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        self.fetcher.fetch_range(range).await
    }
}

impl AsyncFileReader for CachedReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move { self.fetcher.fetch_range(range).await }.boxed()
    }

    /// Concurrent, not sequential: the default runs the ranges one after another,
    /// which on a 20ms link turns one logical read into N serial round trips.
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            let fetches = ranges.into_iter().map(|r| self.fetcher.fetch_range(r));
            futures::future::try_join_all(fetches).await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let path = self.fetcher.path.as_ref().to_string();
            if let Some(hit) = self.metadata.get(&path) {
                return Ok(hit);
            }

            let meta = ParquetMetaDataReader::new()
                .load_and_finish(self.fetcher.clone(), self.file_size)
                .await?;

            let meta = Arc::new(meta);
            self.metadata.insert(path, Arc::clone(&meta));
            Ok(meta)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str, start: u64, end: u64) -> Key {
        Key {
            path: name.to_string(),
            start,
            end,
        }
    }

    fn bytes(n: usize) -> Bytes {
        Bytes::from(vec![0u8; n])
    }

    #[test]
    fn hits_and_misses_are_counted() {
        let cache = ByteCache::new(1024);
        assert!(cache.get(&key("a", 0, 10)).is_none());

        cache.insert(key("a", 0, 10), bytes(10));
        assert_eq!(cache.get(&key("a", 0, 10)).unwrap().len(), 10);

        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.bytes_resident, 10);
    }

    #[test]
    fn eviction_is_least_recently_used() {
        let cache = ByteCache::new(30);
        cache.insert(key("a", 0, 10), bytes(10));
        cache.insert(key("b", 0, 10), bytes(10));
        cache.insert(key("c", 0, 10), bytes(10));

        // Touch `a`, making `b` the coldest.
        assert!(cache.get(&key("a", 0, 10)).is_some());

        cache.insert(key("d", 0, 10), bytes(10));

        assert!(cache.get(&key("b", 0, 10)).is_none(), "b was coldest");
        assert!(cache.get(&key("a", 0, 10)).is_some());
        assert!(cache.get(&key("c", 0, 10)).is_some());
        assert!(cache.get(&key("d", 0, 10)).is_some());
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn stays_within_its_byte_budget() {
        let cache = ByteCache::new(100);
        for i in 0..50u64 {
            cache.insert(key("f", i * 10, i * 10 + 10), bytes(10));
            assert!(
                cache.stats().bytes_resident <= 100,
                "cache overshot its budget"
            );
        }
        assert_eq!(cache.stats().bytes_resident, 100);
    }

    #[test]
    fn a_range_bigger_than_the_cache_is_not_admitted() {
        // Otherwise it evicts everything and then itself, and the cache spends
        // the rest of its life empty.
        let cache = ByteCache::new(50);
        cache.insert(key("a", 0, 10), bytes(10));
        cache.insert(key("big", 0, 100), bytes(100));

        assert!(cache.get(&key("big", 0, 100)).is_none());
        assert!(cache.get(&key("a", 0, 10)).is_some(), "a survived");
    }

    #[test]
    fn reinserting_the_same_range_does_not_double_count() {
        let cache = ByteCache::new(100);
        cache.insert(key("a", 0, 10), bytes(10));
        cache.insert(key("a", 0, 10), bytes(10));
        assert_eq!(cache.stats().bytes_resident, 10);
    }
}
