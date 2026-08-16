//! Point read against the columnar level.
//!
//! Files are walked newest-watermark-first and the first hit wins — no merge
//! across files, because within a file a key appears at most once and file order
//! is total. A tombstone hit is a not-found and stops the walk.
//!
//! Everything else here is about *not* reading. With no compaction the file
//! count only grows, so pruning is not an optimisation, it is what makes the
//! read path viable at all:
//!
//! 1. **Interval prune** — the PK bounds are in the file index, in memory, so
//!    skipping a file costs nothing.
//! 2. **Catalog bloom** — the pk bloom, loaded from Postgres into the index and
//!    probed in memory (see `bloom.rs`). A file the bloom rules out is dismissed
//!    with **no object-store I/O at all** — not even a footer read. Under Iceberg
//!    this step was a per-file S3 fetch; sourcing the bloom from the catalog is
//!    what makes it free.
//! 3. **Row-group prune** — PK min/max per row group, from the footer.
//! 4. Only then fetch the surviving row group's column chunks.

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ColumnChunkMetaData;

use arrow::array::{Array, BinaryArray, Int32Array};

use crate::cache::{ByteCache, CachedReader, MetadataCache};
use crate::error::Result;
use crate::index::{FileEntry, FileIndex};
use crate::memtable::Lookup;
use crate::record::Op;

/// Column position of `pk` in the Parquet file. Fixed by the schema.
const PK_COL: usize = 0;

pub struct ColumnarReader {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    cache: Arc<ByteCache>,
    metadata: Arc<MetadataCache>,
}

impl ColumnarReader {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        bucket: String,
        cache: Arc<ByteCache>,
        metadata: Arc<MetadataCache>,
    ) -> Self {
        ColumnarReader {
            store,
            bucket,
            cache,
            metadata,
        }
    }

    /// Look `key` up in the columnar level.
    pub async fn get(&self, index: &FileIndex, key: &[u8]) -> Result<Lookup> {
        for entry in index.candidates(key) {
            // Free in-memory dismissal: the pk catalog bloom (loaded into the
            // index from Postgres, not fetched per read) rules a file out with no
            // object-store I/O at all — not even a footer read. This is the whole
            // point of catalog-sourced blooms: a missed file costs zero GETs.
            if !entry.bloom_may_contain(key) {
                continue;
            }
            match self.get_from_file(entry, key).await? {
                Lookup::Missing => continue,
                // Found or Deleted: this is the newest file that has an opinion
                // about this key, so its opinion is the answer.
                hit => return Ok(hit),
            }
        }
        Ok(Lookup::Missing)
    }

    async fn get_from_file(&self, entry: &FileEntry, key: &[u8]) -> Result<Lookup> {
        let mut reader = CachedReader::new(
            Arc::clone(&self.store),
            entry.object_path(&self.bucket),
            entry.file_size,
            Arc::clone(&self.cache),
            Arc::clone(&self.metadata),
        );

        let meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        let parquet_meta = meta.metadata().clone();

        for rg_idx in 0..parquet_meta.num_row_groups() {
            let column = parquet_meta.row_group(rg_idx).column(PK_COL);

            if !row_group_may_contain(column, key) {
                continue;
            }

            // The file is PK-sorted with unique keys, so at most one row group
            // can hold it. Whatever this row group says is final for this file.
            return self.scan_row_group(reader, meta, rg_idx, key).await;
        }

        Ok(Lookup::Missing)
    }

    async fn scan_row_group(
        &self,
        reader: CachedReader,
        meta: ArrowReaderMetadata,
        rg_idx: usize,
        key: &[u8],
    ) -> Result<Lookup> {
        // Only pk, op and value are needed. Leaving `lsn` out of the projection
        // means its column chunk is never fetched — the read pays for what it
        // reads.
        let mask = ProjectionMask::leaves(meta.parquet_schema(), [PK_COL, 2, 3]);

        let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta)
            .with_row_groups(vec![rg_idx])
            .with_projection(mask)
            .build()?;

        let batches: Vec<_> = stream.try_collect().await?;

        for batch in &batches {
            let pk = binary_col(batch, 0);
            let op = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("op is int32");
            let value = binary_col(batch, 2);

            // Sorted within the row group, so a binary search would do; at
            // ~8k rows a linear scan of a column already in memory is not
            // what the p99 is made of.
            for i in 0..batch.num_rows() {
                if pk.value(i) != key {
                    continue;
                }
                return Ok(if op.value(i) == Op::Delete as i32 {
                    Lookup::Deleted
                } else {
                    Lookup::Found(Bytes::copy_from_slice(value.value(i)))
                });
            }
        }

        // The bloom filter said maybe and was wrong. That is what a false
        // positive is, and it costs exactly this: one wasted row-group read.
        Ok(Lookup::Missing)
    }
}

// DuckDB writes BLOB columns as plain Parquet BYTE_ARRAY with no logical type,
// which the arrow reader maps to `BinaryArray` (32-bit offsets) — not the
// `LargeBinaryArray` iceberg's Binary→LargeBinary mapping used to produce. The
// staging file we hand DuckDB is LargeBinary, but DuckDB rewrites the lake file
// with its own encoding, and this is the file the read path actually sees.
fn binary_col(batch: &arrow::array::RecordBatch, idx: usize) -> &BinaryArray {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("pk and value are binary")
}

fn row_group_may_contain(column: &ColumnChunkMetaData, key: &[u8]) -> bool {
    let Some(stats) = column.statistics() else {
        return true;
    };
    let (Some(min), Some(max)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) else {
        return true;
    };
    key >= min && key <= max
}

