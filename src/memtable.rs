//! Memtable: the row tier's in-memory level.
//!
//! A `SkipMap` rather than a hash map, for two reasons. Flush must emit
//! PK-sorted Parquet, and a sorted structure means flush is a scan instead of a
//! full sort of the whole table under memory pressure. And a lock-free skiplist
//! lets flush iterate a frozen memtable while writers continue on the active
//! one — with an object store in the flush path, that matters.
//!
//! `MemtableSet` is one active memtable plus the frozen ones awaiting flush.
//! Reads go active → frozen (newest first) → columnar level, so a newer value
//! always shadows an older one.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::error::{Error, Result};
use crate::record::{Lsn, Op, Record};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub lsn: Lsn,
    pub op: Op,
    pub value: Bytes,
}

/// What a lookup found — or didn't.
///
/// The distinction between `Deleted` and `Missing` is the whole reason
/// tombstones exist. `Deleted` is a definitive answer and stops the search.
/// `Missing` means "not at this level, keep going" — and if a delete were
/// implemented as a removal from the map, it would return `Missing`, the read
/// would fall through to the columnar level, and an older Parquet file would
/// resurrect the value that was supposed to be gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    Found(Bytes),
    Deleted,
    Missing,
}

pub struct Memtable {
    map: SkipMap<Bytes, Entry>,
    approx_bytes: AtomicUsize,
    max_lsn: AtomicU64,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            map: SkipMap::new(),
            approx_bytes: AtomicUsize::new(0),
            max_lsn: AtomicU64::new(0),
        }
    }

    /// Insert a record. A `Delete` is stored as a tombstone entry, never as a
    /// removal.
    ///
    /// LSN order wins, not arrival order. Writers take their LSN before they
    /// take the WAL lock, so two concurrent writes to one key can fsync — and
    /// arrive here — in the opposite order to their LSNs. A plain `insert` would
    /// let the older one land last and stand, and flush would then carry that
    /// stale value into the columnar level under a watermark that covers both.
    pub fn insert(&self, rec: Record) {
        let size = rec.heap_size();
        let lsn = rec.lsn;

        self.map.compare_insert(
            rec.key,
            Entry {
                lsn,
                op: rec.op,
                value: rec.value,
            },
            |existing| existing.lsn < lsn,
        );

        // Counted even when the record lost: it is a real WAL record, and
        // over-counting only flushes early. `max_lsn` likewise advances for a
        // loser — it is covered by the flush watermark either way, being
        // superseded rather than absent.
        self.approx_bytes.fetch_add(size, Ordering::Relaxed);
        self.max_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    pub fn get(&self, key: &[u8]) -> Lookup {
        match self.map.get(key) {
            None => Lookup::Missing,
            Some(e) => match e.value().op {
                Op::Put => Lookup::Found(e.value().value.clone()),
                Op::Delete => Lookup::Deleted,
            },
        }
    }

    /// Size estimate driving the flush threshold.
    ///
    /// Counts every insert, including ones that overwrite an existing key, so it
    /// runs high on write-heavy-on-few-keys workloads. That bias is the safe
    /// one: it flushes early rather than overshooting the memory budget.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// Highest LSN in this memtable. Once frozen, this is exactly the watermark
    /// its flush stamps into the Iceberg commit — recovery replays from
    /// `watermark + 1`, so reporting it too high silently drops writes.
    pub fn max_lsn(&self) -> Lsn {
        self.max_lsn.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterate entries in PK order. Safe to call on a frozen memtable while
    /// writers run on the active one — this is the flush path.
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Entry)> + '_ {
        self.map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MemtableSet {
    inner: RwLock<Inner>,
    max_bytes: usize,
    max_frozen: usize,
}

struct Inner {
    active: Arc<Memtable>,
    /// Oldest first. Reads walk this in reverse.
    frozen: Vec<Arc<Memtable>>,
}

impl MemtableSet {
    /// `max_bytes` is the flush threshold. `max_frozen` is the backpressure
    /// limit: flush goes to object storage, so it is slow by construction, and
    /// without a cap on frozen memtables a write burst grows the queue until the
    /// process dies. Stalling writes is the intended behaviour, not a failure.
    pub fn new(max_bytes: usize, max_frozen: usize) -> Self {
        assert!(max_frozen > 0, "max_frozen must leave room for one flush");
        MemtableSet {
            inner: RwLock::new(Inner {
                active: Arc::new(Memtable::new()),
                frozen: Vec::new(),
            }),
            max_bytes,
            max_frozen,
        }
    }

    /// Insert into the active memtable.
    ///
    /// The caller must have already fsynced this record to the WAL. Inserting
    /// first would publish a value to readers that a crash could still destroy.
    pub fn insert(&self, rec: Record) {
        self.inner.read().unwrap().active.insert(rec);
    }

    pub fn get(&self, key: &[u8]) -> Lookup {
        let inner = self.inner.read().unwrap();

        match inner.active.get(key) {
            Lookup::Missing => {}
            hit => return hit,
        }
        for mt in inner.frozen.iter().rev() {
            match mt.get(key) {
                Lookup::Missing => continue,
                hit => return hit,
            }
        }
        Lookup::Missing
    }

    pub fn should_freeze(&self) -> bool {
        self.inner.read().unwrap().active.approx_bytes() >= self.max_bytes
    }

    /// True when the flush path has fallen far enough behind that writes must
    /// stall.
    pub fn stalled(&self) -> bool {
        self.inner.read().unwrap().frozen.len() >= self.max_frozen
    }

    /// Swap in a fresh active memtable and hand back the frozen one for flush.
    ///
    /// Returns `Err(WriteStalled)` when the frozen queue is full, and `Ok(None)`
    /// when the active memtable is empty (nothing to flush). The lock is held
    /// only for the swap — flush then reads the returned memtable with no lock
    /// at all, so an object-store round trip never blocks a writer.
    pub fn freeze(&self) -> Result<Option<Arc<Memtable>>> {
        let mut inner = self.inner.write().unwrap();

        if inner.frozen.len() >= self.max_frozen {
            return Err(Error::WriteStalled(inner.frozen.len()));
        }
        if inner.active.is_empty() {
            return Ok(None);
        }

        let frozen = std::mem::replace(&mut inner.active, Arc::new(Memtable::new()));
        inner.frozen.push(Arc::clone(&frozen));
        Ok(Some(frozen))
    }

    /// Retire a flushed memtable once its Iceberg commit has landed.
    ///
    /// Dropping it before the commit is durable would make its records
    /// unreadable while still absent from the columnar level.
    pub fn retire(&self, flushed: &Arc<Memtable>) {
        let mut inner = self.inner.write().unwrap();
        inner.frozen.retain(|mt| !Arc::ptr_eq(mt, flushed));
    }

    pub fn frozen_count(&self) -> usize {
        self.inner.read().unwrap().frozen.len()
    }

    pub fn approx_bytes(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.active.approx_bytes()
            + inner
                .frozen
                .iter()
                .map(|m| m.approx_bytes())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn put_get_overwrite() {
        let mt = Memtable::new();
        mt.insert(Record::put(key("a"), 1, &b"v1"[..]));
        assert_eq!(mt.get(b"a"), Lookup::Found(Bytes::from_static(b"v1")));

        mt.insert(Record::put(key("a"), 2, &b"v2"[..]));
        assert_eq!(mt.get(b"a"), Lookup::Found(Bytes::from_static(b"v2")));
        assert_eq!(mt.len(), 1);
        assert_eq!(mt.max_lsn(), 2);
    }

    #[test]
    fn an_out_of_order_arrival_does_not_beat_a_newer_lsn() {
        let mt = Memtable::new();

        // What two concurrent writers look like when the higher LSN wins the WAL
        // lock first: lsn 2 arrives, then lsn 1.
        mt.insert(Record::put(key("a"), 2, &b"new"[..]));
        mt.insert(Record::put(key("a"), 1, &b"old"[..]));

        assert_eq!(mt.get(b"a"), Lookup::Found(Bytes::from_static(b"new")));
        // The loser is still covered by the watermark its flush will stamp.
        assert_eq!(mt.max_lsn(), 2);

        // And the same for a tombstone arriving late — a stale delete must not
        // resurrect as an erasure.
        mt.insert(Record::delete(key("a"), 0));
        assert_eq!(mt.get(b"a"), Lookup::Found(Bytes::from_static(b"new")));
    }

    #[test]
    fn delete_leaves_a_tombstone_not_a_hole() {
        let mt = Memtable::new();
        mt.insert(Record::put(key("a"), 1, &b"v1"[..]));
        mt.insert(Record::delete(key("a"), 2));

        // The key must still be present, and must answer Deleted rather than
        // Missing — Missing would let the read fall through to an older Parquet
        // file and resurrect v1.
        assert_eq!(mt.get(b"a"), Lookup::Deleted);
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn iteration_is_pk_sorted() {
        let mt = Memtable::new();
        for k in ["delta", "alpha", "charlie", "bravo"] {
            mt.insert(Record::put(key(k), 1, &b"v"[..]));
        }
        let keys: Vec<_> = mt.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![key("alpha"), key("bravo"), key("charlie"), key("delta")]
        );
    }

    #[test]
    fn newer_memtable_shadows_older() {
        let set = MemtableSet::new(1 << 20, 4);
        set.insert(Record::put(key("a"), 1, &b"old"[..]));
        set.freeze().unwrap().unwrap();

        set.insert(Record::put(key("a"), 2, &b"new"[..]));
        assert_eq!(set.get(b"a"), Lookup::Found(Bytes::from_static(b"new")));

        // Including when the newer write is a delete.
        set.insert(Record::delete(key("a"), 3));
        assert_eq!(set.get(b"a"), Lookup::Deleted);
    }

    #[test]
    fn frozen_memtable_still_serves_reads() {
        let set = MemtableSet::new(1 << 20, 4);
        set.insert(Record::put(key("a"), 1, &b"v"[..]));
        let frozen = set.freeze().unwrap().unwrap();

        assert_eq!(set.get(b"a"), Lookup::Found(Bytes::from_static(b"v")));

        // Only once the flush is durable does the key stop being served here —
        // by then the columnar level answers for it.
        set.retire(&frozen);
        assert_eq!(set.get(b"a"), Lookup::Missing);
    }

    #[test]
    fn freeze_threshold_tracks_bytes() {
        let set = MemtableSet::new(512, 4);
        assert!(!set.should_freeze());

        for i in 0..8u64 {
            set.insert(Record::put(format!("k{i}").into_bytes(), i, vec![0u8; 16]));
        }
        assert!(set.should_freeze());
    }

    #[test]
    fn frozen_queue_backpressures_instead_of_growing() {
        let set = MemtableSet::new(64, 2);

        for i in 0..2u64 {
            set.insert(Record::put(format!("k{i}").into_bytes(), i, &b"v"[..]));
            set.freeze().unwrap().unwrap();
        }
        assert!(set.stalled());

        set.insert(Record::put(key("x"), 9, &b"v"[..]));
        assert!(matches!(set.freeze(), Err(Error::WriteStalled(2))));
    }

    #[test]
    fn freezing_an_empty_memtable_is_a_no_op() {
        let set = MemtableSet::new(64, 2);
        assert!(set.freeze().unwrap().is_none());
        assert_eq!(set.frozen_count(), 0);
    }
}
