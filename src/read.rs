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
//! 1. **Interval prune** — the PK bounds are already in the file index, in
//!    memory, so skipping a file costs nothing.
//! 2. **Row-group prune** — PK min/max per row group, from the footer.
//! 3. **Bloom filter** — one cached range read, and the file is dismissed.
//! 4. Only then fetch the surviving row group's column chunks.

use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::ByteArray;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::reader::{ChunkReader, Length};

use arrow::array::{Array, Int32Array, LargeBinaryArray};

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
            if !self.bloom_may_contain(&reader, column, key).await? {
                continue;
            }

            // The file is PK-sorted with unique keys, so at most one row group
            // can hold it. Whatever this row group says is final for this file.
            return self.scan_row_group(reader, meta, rg_idx, key).await;
        }

        Ok(Lookup::Missing)
    }

    /// PK min/max for the row group, straight out of the footer.
    async fn bloom_may_contain(
        &self,
        reader: &CachedReader,
        column: &ColumnChunkMetaData,
        key: &[u8],
    ) -> Result<bool> {
        let (Some(offset), Some(length)) = (column.bloom_filter_offset(), column.bloom_filter_length())
        else {
            // No bloom filter: cannot rule the file out, so scan it. Correct,
            // just slower. Files this engine writes always have one.
            return Ok(true);
        };

        let offset = offset as u64;
        let bytes = reader.fetch(offset..offset + length as u64).await?;

        let chunk = OffsetChunkReader { base: offset, bytes };
        match Sbbf::read_from_column_chunk(column, &chunk)? {
            // The writer hashed the pk column's physical BYTE_ARRAY values, so
            // the probe has to hash the same bytes the same way.
            Some(sbbf) => Ok(sbbf.check(&ByteArray::from(key.to_vec()))),
            None => Ok(true),
        }
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

fn binary_col(batch: &arrow::array::RecordBatch, idx: usize) -> &LargeBinaryArray {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
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

/// A `ChunkReader` over a slice of a file, with absolute offsets.
///
/// `Sbbf::read_from_column_chunk` addresses the file absolutely, but we only
/// fetch the bloom filter's own byte range — fetching the whole file to read a
/// filter designed to save us from reading the file would be a poor trade. This
/// shifts the offsets back.
struct OffsetChunkReader {
    base: u64,
    bytes: Bytes,
}

impl Length for OffsetChunkReader {
    fn len(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }
}

impl ChunkReader for OffsetChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let offset = (start - self.base) as usize;
        Ok(Cursor::new(self.bytes.slice(offset..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let offset = (start - self.base) as usize;
        Ok(self.bytes.slice(offset..offset + length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_chunk_reader_addresses_the_file_not_the_slice() {
        let chunk = OffsetChunkReader {
            base: 1000,
            bytes: Bytes::from_static(b"abcdef"),
        };

        assert_eq!(chunk.len(), 1006);
        assert_eq!(&chunk.get_bytes(1000, 3).unwrap()[..], b"abc");
        assert_eq!(&chunk.get_bytes(1003, 3).unwrap()[..], b"def");
    }
}
