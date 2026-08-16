//! The file index: the columnar level's table of contents.
//!
//! One entry per live Parquet file, carrying the watermark LSN it covers and its
//! PK bounds. Sorted by watermark, newest first — which is the order a point
//! read walks, taking the first hit.
//!
//! Under DuckLake this is **one SQL query** against Postgres, not a walk of Avro
//! manifests on S3. The PK bounds and the per-file `lsn` max are columns in
//! `ducklake_file_column_stats`, so the interval map and the watermark both fall
//! out of the same read — no object-store I/O at all.
//!
//! Precondition: this index is only rebuilt after *our own* flushes
//! (`Engine::publish`). Nothing polls the catalog, so it is sound only while this
//! process is the table's sole writer — which today it is. A second writer makes
//! a freshness check on the read path mandatory; under DuckLake that check is
//! cheap (compare against `MAX(snapshot_id)`), but it is still not free and not
//! built. See DECISIONS.md, "The cached index is sound only because this process
//! is the sole writer".

use bytes::Bytes;
use object_store::path::Path;

use crate::bloom::CatalogBloom;
use crate::catalog::DuckLake;
use crate::error::{Error, Result};
use crate::record::Lsn;
use crate::schema::{FIELD_ID_LSN, FIELD_ID_PK};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Object key within the bucket, e.g. `hg/main/kv/ducklake-….parquet`.
    /// Already resolved from DATA_PATH + schema + table + relative path.
    pub location: String,
    /// Highest LSN this file covers — the `lsn` column's max stat. Total order
    /// across files, which is what makes newest-first, first-hit-wins correct
    /// without a per-row merge.
    pub watermark: Lsn,
    pub pk_min: Bytes,
    pub pk_max: Bytes,
    pub file_size: u64,
    pub record_count: u64,
    /// The `pk` catalog bloom for this file, parsed from
    /// `ducklake_file_column_blooms`. `None` if the file has no bloom (an older
    /// file, or blooms disabled) — the read path then cannot prune by bloom and
    /// falls back to opening the file, which is safe, only slower.
    pub bloom: Option<CatalogBloom>,
}

impl FileEntry {
    /// Could this file contain `key`? Answered from the catalog bounds alone.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        key >= &self.pk_min[..] && key <= &self.pk_max[..]
    }

    /// Might this file contain `key`, per its bloom? A `None` bloom cannot rule
    /// the key out, so it answers "maybe". Answered entirely in memory — no
    /// object-store or catalog I/O, which is why file dismissal here is free.
    pub fn bloom_may_contain(&self, key: &[u8]) -> bool {
        self.bloom.as_ref().map_or(true, |b| b.might_contain(key))
    }

    /// Path within the bucket, for the object store. `location` is already the
    /// bucket-relative key, so this is a straight conversion; the `bucket`
    /// argument is accepted for call-site compatibility and, when a full
    /// `s3://bucket/...` location sneaks in, stripped.
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

    /// Build the index from the catalog in one query.
    ///
    /// Joins `ducklake_data_file` to its `pk` and `lsn` column stats, keeping
    /// only live files (`end_snapshot IS NULL`). PK bounds come back hex-encoded
    /// (DuckLake's encoding for BLOB min/max); `lsn` max comes back as a decimal
    /// string.
    pub async fn load(lake: &DuckLake) -> Result<Self> {
        // LEFT JOIN the pk bloom: it is the only pk bloom DuckLake writes, and
        // pulling it here (once per index build, in the same round trip as the
        // stats) is what makes the read path's file dismissal a free in-memory
        // probe rather than a per-file object-store fetch.
        let rows = lake
            .client()
            .query(
                "SELECT f.path, f.file_size_bytes, f.record_count, \
                        pk.min_value, pk.max_value, lsn.max_value, b.bloom \
                 FROM ducklake_data_file f \
                 JOIN ducklake_file_column_stats pk \
                   ON pk.data_file_id = f.data_file_id AND pk.column_id = $2 \
                 JOIN ducklake_file_column_stats lsn \
                   ON lsn.data_file_id = f.data_file_id AND lsn.column_id = $3 \
                 LEFT JOIN ducklake_file_column_blooms b \
                   ON b.data_file_id = f.data_file_id AND b.column_id = $2 \
                 WHERE f.table_id = $1 AND f.end_snapshot IS NULL",
                &[
                    &lake.table_id,
                    &(FIELD_ID_PK as i64),
                    &(FIELD_ID_LSN as i64),
                ],
            )
            .await?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            let path: String = row.get(0);
            let file_size: i64 = row.get(1);
            let record_count: i64 = row.get(2);
            let pk_min_hex: String = row.get(3);
            let pk_max_hex: String = row.get(4);
            let lsn_max_str: String = row.get(5);
            let bloom_b64: Option<String> = row.get(6);

            let pk_min = decode_hex(&pk_min_hex, "pk min")?;
            let pk_max = decode_hex(&pk_max_hex, "pk max")?;
            let watermark: Lsn = lsn_max_str.parse().map_err(|_| {
                Error::Config(format!("lsn max stat is not an integer: {lsn_max_str:?}"))
            })?;
            let bloom = bloom_b64.as_deref().and_then(CatalogBloom::from_base64);

            entries.push(FileEntry {
                location: format!("{}{}", lake.key_prefix, path),
                watermark,
                pk_min,
                pk_max,
                file_size: file_size as u64,
                record_count: record_count as u64,
                bloom,
            });
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

fn decode_hex(s: &str, what: &str) -> Result<Bytes> {
    hex::decode(s)
        .map(Bytes::from)
        .map_err(|e| Error::Config(format!("{what} is not valid hex ({s:?}): {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(watermark: Lsn, lo: &str, hi: &str) -> FileEntry {
        FileEntry {
            location: format!("hg/main/kv/{watermark}.parquet"),
            watermark,
            pk_min: Bytes::copy_from_slice(lo.as_bytes()),
            pk_max: Bytes::copy_from_slice(hi.as_bytes()),
            file_size: 1024,
            record_count: 10,
            bloom: None,
        }
    }

    #[test]
    fn a_missing_bloom_cannot_prune() {
        // A file with no bloom must answer "maybe" for every key — never a false
        // negative, which would drop data.
        let e = entry(1, "a", "z");
        assert!(e.bloom_may_contain(b"anything"));
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
    fn object_path_is_the_bucket_relative_key() {
        let e = FileEntry {
            location: "hg/main/kv/ducklake-42.parquet".to_string(),
            ..entry(42, "a", "z")
        };
        assert_eq!(
            e.object_path("warehouse").as_ref(),
            "hg/main/kv/ducklake-42.parquet"
        );
    }

    #[test]
    fn hex_bounds_decode_to_raw_bytes() {
        // "k1" and "k999" as DuckLake stores them.
        assert_eq!(&decode_hex("6B31", "pk").unwrap()[..], b"k1");
        assert_eq!(&decode_hex("6B393939", "pk").unwrap()[..], b"k999");
    }
}
