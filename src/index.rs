//! The file index: the columnar level's table of contents.
//!
//! One entry per live Parquet file, carrying the watermark LSN it covers and its
//! PK bounds. Sorted by watermark, newest first — which is the order a point
//! read walks, taking the first hit.
//!
//! Two things fall out of building this once and holding it in memory:
//!
//! * **The interval map is free.** PK lower and upper bounds are already in the
//!   Iceberg manifest, so pruning a file by key range costs no I/O at all.
//! * **It plugs the opendal hole.** Iceberg does its manifest I/O through
//!   opendal, which our latency shim cannot see, so those reads pay no injected
//!   latency. Doing them once at startup rather than once per point read is both
//!   what a real system does and what keeps the benchmark honest — a per-read
//!   manifest fetch that costs nothing would be a lie about the read path.
//!
//! Precondition: this index is only ever rebuilt after *our own* commits
//! (`Engine::publish`). Nothing polls the catalog, so it is sound only while this
//! process is the table's sole writer — which today it is. A second writer, or
//! compaction, makes a freshness check on the read path mandatory rather than
//! optional. See DECISIONS.md, "The cached index is sound only because this
//! process is the sole writer".

use std::collections::HashMap;

use bytes::Bytes;
use iceberg::spec::{DataContentType, ManifestStatus};
use iceberg::table::Table;
use object_store::path::Path;

use crate::error::Result;
use crate::record::Lsn;
use crate::schema::{FIELD_ID_PK, WATERMARK_PROP};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Iceberg's full location, e.g. `s3://warehouse/holy_grail/kv/data/x.parquet`.
    pub location: String,
    /// Highest LSN this file covers. Total order across files — this is what
    /// makes newest-first, first-hit-wins correct without a per-row merge.
    pub watermark: Lsn,
    pub pk_min: Bytes,
    pub pk_max: Bytes,
    pub file_size: u64,
    pub record_count: u64,
}

impl FileEntry {
    /// Could this file contain `key`? Answered from the manifest bounds alone.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        key >= &self.pk_min[..] && key <= &self.pk_max[..]
    }

    /// Path within the bucket, for the object store.
    pub fn object_path(&self, bucket: &str) -> Path {
        let prefix = format!("s3://{bucket}/");
        let rel = self
            .location
            .strip_prefix(&prefix)
            .unwrap_or(&self.location)
            .trim_start_matches('/');
        Path::from(rel)
    }
}

/// Live data files, newest watermark first.
#[derive(Debug, Clone, Default)]
pub struct FileIndex {
    entries: Vec<FileEntry>,
}

impl FileIndex {
    pub fn empty() -> Self {
        FileIndex::default()
    }

    /// Read the whole index out of the table's current snapshot.
    ///
    /// Walks the current manifest list — which references every live manifest,
    /// not just this snapshot's — and attributes each manifest's data files to
    /// the snapshot that added it, and so to that snapshot's watermark.
    pub async fn load(table: &Table) -> Result<Self> {
        let metadata = table.metadata();
        let file_io = table.file_io();

        let Some(current) = metadata.current_snapshot() else {
            // No snapshot means nothing has ever been flushed. Everything lives
            // in the WAL, and a recovering node replays from LSN 0.
            return Ok(FileIndex::empty());
        };

        // snapshot id -> the watermark that snapshot published.
        let watermarks: HashMap<i64, Lsn> = metadata
            .snapshots()
            .map(|s| (s.snapshot_id(), watermark_of(s.as_ref())))
            .collect();

        let manifest_list = current.load_manifest_list(file_io, metadata).await?;
        let mut entries = Vec::new();

        for manifest_file in manifest_list.entries() {
            let watermark = watermarks
                .get(&manifest_file.added_snapshot_id)
                .copied()
                .unwrap_or(0);

            let manifest = manifest_file.load_manifest(file_io).await?;

            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::Data {
                    continue;
                }

                let (Some(pk_min), Some(pk_max)) = (
                    bound_bytes(data_file.lower_bounds()),
                    bound_bytes(data_file.upper_bounds()),
                ) else {
                    // A file with no PK bounds cannot be pruned, and silently
                    // scanning it on every read would quietly destroy the point
                    // of the interval map. Nothing this engine writes can hit
                    // this — the writer always emits bounds.
                    return Err(crate::Error::Config(format!(
                        "data file {} has no pk bounds; the interval map cannot prune it",
                        data_file.file_path()
                    )));
                };

                entries.push(FileEntry {
                    location: data_file.file_path().to_string(),
                    watermark,
                    pk_min,
                    pk_max,
                    file_size: data_file.file_size_in_bytes(),
                    record_count: data_file.record_count(),
                });
            }
        }

        // Newest first. Watermarks are monotonic, so this is a total order and
        // the first hit while scanning it is the current value.
        entries.sort_by(|a, b| b.watermark.cmp(&a.watermark));

        Ok(FileIndex { entries })
    }

    /// Files that could hold `key`, newest first.
    pub fn candidates<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a FileEntry> + 'a {
        self.entries.iter().filter(move |e| e.may_contain(key))
    }

    pub fn entries(&self) -> &[FileEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Highest watermark any file carries — the boundary between the columnar
    /// level and the WAL suffix.
    pub fn watermark(&self) -> Lsn {
        self.entries.first().map(|e| e.watermark).unwrap_or(0)
    }
}

/// The watermark a snapshot published, or 0 if it carries none (a snapshot this
/// engine did not write).
pub fn watermark_of(snapshot: &iceberg::spec::Snapshot) -> Lsn {
    snapshot
        .summary()
        .additional_properties
        .get(WATERMARK_PROP)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Read the published watermark straight off the table.
///
/// This is the number recovery turns on: WAL records at or below it are already
/// in the columnar level, and a flush that would publish at or below it has
/// already landed.
pub fn published_watermark(table: &Table) -> Lsn {
    table
        .metadata()
        .current_snapshot()
        .map(|s| watermark_of(s.as_ref()))
        .unwrap_or(0)
}

fn bound_bytes(bounds: &HashMap<i32, iceberg::spec::Datum>) -> Option<Bytes> {
    let datum = bounds.get(&FIELD_ID_PK)?;
    let buf = datum.to_bytes().ok()?;
    Some(Bytes::from(buf.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(watermark: Lsn, lo: &str, hi: &str) -> FileEntry {
        FileEntry {
            location: format!("s3://warehouse/data/{watermark}.parquet"),
            watermark,
            pk_min: Bytes::copy_from_slice(lo.as_bytes()),
            pk_max: Bytes::copy_from_slice(hi.as_bytes()),
            file_size: 1024,
            record_count: 10,
        }
    }

    #[test]
    fn interval_pruning_is_inclusive_at_both_ends() {
        let e = entry(1, "b", "d");
        assert!(e.may_contain(b"b"), "min is in range");
        assert!(e.may_contain(b"d"), "max is in range");
        assert!(e.may_contain(b"c"));
        assert!(!e.may_contain(b"a"));
        assert!(!e.may_contain(b"e"));
    }

    #[test]
    fn candidates_come_back_newest_first() {
        let mut index = FileIndex {
            entries: vec![entry(10, "a", "z"), entry(30, "a", "z"), entry(20, "a", "z")],
        };
        index.entries.sort_by(|a, b| b.watermark.cmp(&a.watermark));

        let got: Vec<_> = index.candidates(b"m").map(|e| e.watermark).collect();
        assert_eq!(got, vec![30, 20, 10], "a stale file must never win");
    }

    #[test]
    fn candidates_skip_files_whose_range_excludes_the_key() {
        let index = FileIndex {
            entries: vec![entry(30, "m", "z"), entry(20, "a", "f"), entry(10, "a", "z")],
        };

        let got: Vec<_> = index.candidates(b"c").map(|e| e.watermark).collect();
        assert_eq!(got, vec![20, 10], "the m..z file cannot hold c");
    }

    #[test]
    fn object_path_strips_the_bucket() {
        let e = FileEntry {
            location: "s3://warehouse/holy_grail/kv/data/wm-42-0.parquet".to_string(),
            ..entry(42, "a", "z")
        };
        assert_eq!(
            e.object_path("warehouse").as_ref(),
            "holy_grail/kv/data/wm-42-0.parquet"
        );
    }
}
