//! Flush: the frozen memtable becomes a Parquet file in the Iceberg table.
//!
//! This is the protocol that licenses the whole "disposable row tier" claim, so
//! its ordering is the most load-bearing thing in the codebase:
//!
//! 1. Check the published watermark. If it already covers this flush, the flush
//!    has landed — skip straight to truncation.
//! 2. Write PK-sorted Parquet (bloom filter on `pk`, per-row-group PK bounds).
//! 3. Commit to Iceberg, stamping the watermark LSN into the snapshot summary.
//! 4. **Then** truncate the WAL.
//! 5. Retire the memtable.
//!
//! Publish before truncate, never the reverse. Truncating first would, on a
//! crash in between, leave records gone from the WAL and absent from the
//! columnar level — an acknowledged write, lost. This order can only leave the
//! WAL holding records that are already published, which recovery *skips*. A
//! benign duplicate rather than an impossible hole.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, LargeBinaryArray, LargeBinaryBuilder, RecordBatch};
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg_catalog_rest::RestCatalog;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use uuid::Uuid;

use crate::error::Result;
use crate::index::published_watermark;
use crate::memtable::Memtable;
use crate::record::{Lsn, Op};
use crate::schema::{self, WATERMARK_PROP};
use crate::store::LatencyProfile;

/// Rows per Arrow batch handed to the writer. Purely a memory-shape knob.
const BATCH_ROWS: usize = 8192;

/// Rows per Parquet row group.
///
/// The default is a million, which would put the whole file in one row group and
/// make row-group pruning meaningless — a point read would fetch every column
/// chunk in the file to find one key. Small row groups are what let a point read
/// fetch only the slice that could hold the key. The cost is more metadata per
/// file, which is a trade this workload wants to make.
const ROW_GROUP_ROWS: usize = 8192;

/// False-positive rate for the `pk` bloom filter.
///
/// A false positive costs a wasted row-group scan — the column chunks are
/// fetched from the object store and the key is not there. Against an S3 round
/// trip that is expensive, and the filter is small either way, so 0.01 is
/// bought rather than parquet's default 0.05.
const BLOOM_FPP: f64 = 0.01;

/// Object-store round trips a commit makes that the latency shim cannot see.
///
/// Iceberg does its metadata I/O through opendal, not `object_store`, so none of
/// it passes through `LatencyStore`. These counts were measured, not guessed:
/// `mc admin trace` against MinIO while `examples/trace_flush.rs` performed one
/// flush. See DECISIONS.md, "Charging the opendal hole".
///
/// Our own process, via iceberg's `FileIO`:
///   PUT  manifest (`*-m0.avro`), PUT manifest list (`snap-*.avro`)
///   GET  manifest list, GET manifest — the post-commit index refresh reads back
///        what it just wrote.
const COMMIT_CLIENT_PUTS: u32 = 2;
const COMMIT_CLIENT_GETS: u32 = 2;

/// The REST catalog's own S3 I/O, performed server-side inside the commit call.
///
/// We do not issue these, but we *block* on the HTTP request that does, so they
/// are part of flush wall-clock time and therefore part of what drives
/// backpressure. Charging them is the honest choice; leaving them out would make
/// flush look faster than it can ever be.
///   GET  current metadata.json (x3), PUT new metadata.json
const COMMIT_CATALOG_PUTS: u32 = 1;
const COMMIT_CATALOG_GETS: u32 = 3;

/// Namespace for deriving deterministic commit UUIDs from watermarks.
const COMMIT_NAMESPACE: Uuid = Uuid::from_bytes([
    0x68, 0x6f, 0x6c, 0x79, 0x67, 0x72, 0x61, 0x69, 0x6c, 0x77, 0x61, 0x74, 0x65, 0x72, 0x6d, 0x6b,
]);

/// What a flush did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Flushed {
    /// Files were written and a snapshot was committed.
    Committed { watermark: Lsn, files: usize },
    /// The published watermark already covered this flush — a retry after a
    /// crash between commit and truncate. Nothing was written.
    AlreadyPublished { watermark: Lsn },
    /// The memtable held nothing.
    Empty,
}

impl Flushed {
    /// The watermark the WAL may now be truncated through, if any.
    pub fn watermark(&self) -> Option<Lsn> {
        match self {
            Flushed::Committed { watermark, .. } | Flushed::AlreadyPublished { watermark } => {
                Some(*watermark)
            }
            Flushed::Empty => None,
        }
    }
}

/// What a flush should do, decided before it does anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    Write { watermark: Lsn },
    /// The published watermark already covers this memtable — this is a retry
    /// after a crash between commit and truncate. Do not write, do not commit;
    /// just truncate.
    AlreadyPublished { watermark: Lsn },
    Empty,
}

/// Decide what to do. **This is where idempotency lives, and nowhere else.**
///
/// Note what it is *not*: iceberg's `set_commit_uuid` does not deduplicate. A
/// retried commit with the same UUID still produces a second snapshot appending
/// the same files — duplicate rows, silently. Relying on it would be a quiet
/// correctness bug. The watermark check is the actual mechanism, and because
/// watermarks are monotonic it is exact rather than heuristic.
pub fn plan(table: &Table, memtable: &Memtable) -> Plan {
    if memtable.is_empty() {
        return Plan::Empty;
    }
    let watermark = memtable.max_lsn();

    if watermark <= published_watermark(table) {
        Plan::AlreadyPublished { watermark }
    } else {
        Plan::Write { watermark }
    }
}

/// Write a frozen memtable to the columnar level and publish it.
///
/// Returns the updated table. The caller truncates the WAL *after* this returns,
/// never before.
///
/// The two halves — write and commit — are also exposed separately, because the
/// crash harness has to be able to die in the window between them.
pub async fn flush(
    table: &Table,
    catalog: &RestCatalog,
    memtable: &Memtable,
    latency: LatencyProfile,
) -> Result<(Table, Flushed)> {
    let watermark = match plan(table, memtable) {
        Plan::Empty => return Ok((table.clone(), Flushed::Empty)),
        Plan::AlreadyPublished { watermark } => {
            return Ok((table.clone(), Flushed::AlreadyPublished { watermark }))
        }
        Plan::Write { watermark } => watermark,
    };

    let data_files = write_files(table, memtable, watermark, latency).await?;
    let files = data_files.len();
    let table = commit_files(table, catalog, data_files, watermark, latency).await?;

    Ok((table, Flushed::Committed { watermark, files }))
}

/// Commit written files, stamping the watermark into the snapshot summary.
pub async fn commit_files(
    table: &Table,
    catalog: &RestCatalog,
    data_files: Vec<iceberg::spec::DataFile>,
    watermark: Lsn,
    latency: LatencyProfile,
) -> Result<Table> {
    // Iceberg's own I/O (manifest, manifest list, table metadata) goes through
    // opendal, which the latency shim cannot see. Charge it by hand so the flush
    // duration — and the backpressure it drives — is not fiction. The counts are
    // measured; see the constants.
    charge(
        latency,
        COMMIT_CLIENT_PUTS + COMMIT_CATALOG_PUTS,
        COMMIT_CLIENT_GETS + COMMIT_CATALOG_GETS,
    )
    .await;

    let tx = Transaction::new(table);
    let action = tx
        .fast_append()
        .add_data_files(data_files)
        .set_snapshot_properties(HashMap::from([(
            WATERMARK_PROP.to_string(),
            watermark.to_string(),
        )]))
        // Deterministic, so a retry of an interrupted commit reuses the same
        // manifest-list object name instead of leaking a fresh one. It does not
        // make the commit idempotent — `plan` does that.
        .set_commit_uuid(commit_uuid(watermark));

    Ok(action.apply(tx)?.commit(catalog).await?)
}

/// Deterministic commit UUID for a given watermark.
fn commit_uuid(watermark: Lsn) -> Uuid {
    Uuid::new_v5(&COMMIT_NAMESPACE, &watermark.to_be_bytes())
}

/// Deterministic file-name prefix for a given watermark.
///
/// A flush that crashed after uploading its Parquet file but before committing
/// leaves an orphan object. The retry writes to the *same* name and overwrites
/// it, rather than leaking a new one and abandoning the old — and with neither
/// compaction nor snapshot expiry in this prototype, nothing would ever clean
/// that orphan up.
fn file_prefix(watermark: Lsn) -> String {
    format!("wm-{watermark:020}")
}

/// Write the memtable as one PK-sorted Parquet file. Does not publish it.
pub async fn write_files(
    table: &Table,
    memtable: &Memtable,
    watermark: Lsn,
    latency: LatencyProfile,
) -> Result<Vec<iceberg::spec::DataFile>> {
    let props = WriterProperties::builder()
        .set_max_row_group_size(ROW_GROUP_ROWS)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_column_bloom_filter_enabled("pk".into(), true)
        // Both of these must be set. Parquet's default NDV is a million, but a
        // bloom filter is written per *column chunk* — that is, per row group,
        // which holds ROW_GROUP_ROWS keys and not a million. Left at the default
        // each filter is ~1 MiB, so the filters outweigh the data they index and
        // a point read drags a megabyte off the object store to test one key.
        .set_column_bloom_filter_ndv("pk".into(), ROW_GROUP_ROWS as u64)
        .set_column_bloom_filter_fpp("pk".into(), BLOOM_FPP)
        .build();

    let schema = table.metadata().current_schema().clone();

    let parquet_writer = ParquetWriterBuilder::new(props, schema);
    let rolling = RollingFileWriterBuilder::new(
        parquet_writer,
        // One file per flush: the target size is set above what a memtable can
        // hold, so the rolling writer never rolls. File-per-flush is what makes
        // the watermark a property of the *file* and not just of the snapshot.
        usize::MAX,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata().clone())?,
        DefaultFileNameGenerator::new(file_prefix(watermark), None, DataFileFormat::Parquet),
    );

    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;

    // The memtable is already in PK order, so this is a scan, not a sort — the
    // reason it is a skiplist and not a hash map.
    for batch in batches(memtable) {
        writer.write(batch).await?;
    }
    let data_files = writer.close().await?;

    // One PUT per data file, also written through opendal and so also unseen by
    // the shim. A real 64 MiB upload would be multipart and cost more than one
    // round trip; this undercharges the write path and is the next thing to fix
    // if flush throughput ever becomes a headline number rather than a
    // backpressure input.
    charge(latency, data_files.len() as u32, 0).await;

    Ok(data_files)
}

/// Chunk the memtable into Arrow batches, in PK order.
fn batches(memtable: &Memtable) -> Vec<RecordBatch> {
    let arrow_schema = schema::arrow_schema();
    let mut out = Vec::new();

    let mut pks = LargeBinaryBuilder::new();
    let mut lsns: Vec<i64> = Vec::with_capacity(BATCH_ROWS);
    let mut ops: Vec<i32> = Vec::with_capacity(BATCH_ROWS);
    let mut values = LargeBinaryBuilder::new();
    let mut rows = 0;

    for (key, entry) in memtable.iter() {
        pks.append_value(&key);
        lsns.push(entry.lsn as i64);
        ops.push(entry.op as i32);

        // A tombstone is a row with a null value. It must be written, not
        // skipped: without it the read falls through to an older file and
        // resurrects the deleted value.
        match entry.op {
            Op::Put => values.append_value(&entry.value),
            Op::Delete => values.append_null(),
        }

        rows += 1;
        if rows == BATCH_ROWS {
            out.push(finish(&arrow_schema, &mut pks, &mut lsns, &mut ops, &mut values));
            rows = 0;
        }
    }

    if rows > 0 {
        out.push(finish(&arrow_schema, &mut pks, &mut lsns, &mut ops, &mut values));
    }

    out
}

fn finish(
    arrow_schema: &Arc<arrow::datatypes::Schema>,
    pks: &mut LargeBinaryBuilder,
    lsns: &mut Vec<i64>,
    ops: &mut Vec<i32>,
    values: &mut LargeBinaryBuilder,
) -> RecordBatch {
    let pk: LargeBinaryArray = pks.finish();
    let value: LargeBinaryArray = values.finish();
    let lsn = Int64Array::from(std::mem::take(lsns));
    let op = Int32Array::from(std::mem::take(ops));

    RecordBatch::try_new(
        Arc::clone(arrow_schema),
        vec![
            Arc::new(pk),
            Arc::new(lsn),
            Arc::new(op),
            Arc::new(value),
        ],
    )
    .expect("batch columns match the arrow schema")
}

/// Charge object-store round trips that the latency shim could not see.
///
/// Sequential, not concurrent: iceberg writes the manifest, then the manifest
/// list, then commits, each depending on the last. Summing independent draws is
/// what that dependency chain actually costs.
async fn charge(latency: LatencyProfile, puts: u32, gets: u32) {
    let total = latency.charge_for(puts, gets);
    if total.is_zero() {
        return;
    }
    tokio::time::sleep(total).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use arrow::array::Array;
    use bytes::Bytes;

    #[test]
    fn commit_uuid_is_a_function_of_the_watermark() {
        assert_eq!(commit_uuid(42), commit_uuid(42), "a retry must reuse it");
        assert_ne!(commit_uuid(42), commit_uuid(43));
    }

    #[test]
    fn file_prefix_is_a_function_of_the_watermark() {
        assert_eq!(file_prefix(42), file_prefix(42));
        assert_ne!(file_prefix(42), file_prefix(43));
        // Zero-padded so the names sort in watermark order, which makes a
        // bucket listing readable during a debugging session.
        assert_eq!(file_prefix(42), "wm-00000000000000000042");
    }

    #[test]
    fn batches_are_pk_sorted_and_keep_tombstones() {
        let mt = Memtable::new();
        mt.insert(Record::put(Bytes::from_static(b"c"), 1, &b"three"[..]));
        mt.insert(Record::put(Bytes::from_static(b"a"), 2, &b"one"[..]));
        mt.insert(Record::delete(Bytes::from_static(b"b"), 3));

        let batches = batches(&mt);
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 3, "the tombstone is a row, not an omission");

        let pk = b
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(pk.value(0), b"a");
        assert_eq!(pk.value(1), b"b");
        assert_eq!(pk.value(2), b"c");

        let value = b
            .column(3)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(value.is_null(1), "the tombstone's value is null");

        let op = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(op.value(1), Op::Delete as i32);
    }

    #[test]
    fn a_batch_boundary_does_not_lose_rows() {
        let mt = Memtable::new();
        for i in 0..(BATCH_ROWS + 5) {
            mt.insert(Record::put(format!("k{i:08}").into_bytes(), i as u64, &b"v"[..]));
        }

        let batches = batches(&mt);
        assert_eq!(batches.len(), 2);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, BATCH_ROWS + 5);
    }
}
