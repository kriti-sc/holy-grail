//! Step 1: MinIO + the DuckLake catalog (Postgres) are real, and a PK-sorted
//! Parquet file round-trips through the latency-wrapped object store.
//!
//! Requires MinIO, Postgres, and the forked DuckDB binary to be up. Ignored by
//! default so `cargo test` stays hermetic; run with:
//!
//!     cargo test --test infra -- --ignored --nocapture

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, LargeBinaryArray, RecordBatch};
use bytes::Bytes;
use holy_grail::config::Config;
use holy_grail::index::FileIndex;
use holy_grail::schema;
use holy_grail::store::{self, LatencyProfile};
use holy_grail::{catalog, Op};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::metadata::PageIndexPolicy;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use futures::TryStreamExt;

/// Ten rows, PK-sorted, with one tombstone. `value` is null for the tombstone —
/// the row's existence plus `op` is the delete.
fn sample_batch() -> RecordBatch {
    let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("key{i:04}").into_bytes()).collect();
    let pk = LargeBinaryArray::from_iter_values(&keys);
    let lsn = Int64Array::from_iter_values(1..=10);
    let op = Int32Array::from_iter_values(
        (0..10).map(|i| if i == 7 { Op::Delete } else { Op::Put } as i32),
    );
    let value = LargeBinaryArray::from_iter((0..10).map(|i| {
        if i == 7 {
            None
        } else {
            Some(format!("value-{i}").into_bytes())
        }
    }));

    RecordBatch::try_new(
        schema::arrow_schema(),
        vec![
            Arc::new(pk),
            Arc::new(lsn),
            Arc::new(op),
            Arc::new(value),
        ],
    )
    .unwrap()
}

fn write_parquet(batch: &RecordBatch) -> Bytes {
    // Bloom filter on pk and chunk-level statistics: these are what the point
    // read in step 6 prunes on. A file written without them is a file that has
    // to be scanned end to end.
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_enabled(true)
        .set_column_bloom_filter_enabled("pk".into(), true)
        .build();

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buf)
}

#[tokio::test]
#[ignore = "requires the DuckLake catalog, MinIO, and the forked duckdb binary"]
async fn catalog_bootstrap_is_idempotent_and_resolves() {
    let mut cfg = Config::from_env();
    cfg.catalog.table = format!("t_infra_{}", std::process::id());

    // Bootstrap is the DDL the engine relies on but never issues. It must be
    // idempotent — a second run loads rather than recreates, which recovery
    // depends on.
    catalog::bootstrap(&cfg.duckdb, &cfg.catalog, &cfg.s3).await.unwrap();
    catalog::bootstrap(&cfg.duckdb, &cfg.catalog, &cfg.s3).await.unwrap();

    // Connect resolves the table and verifies the column mapping (a mismatch
    // there would produce all-null reads).
    let lake = catalog::DuckLake::connect(&cfg.catalog).await.unwrap();

    // No data yet, so no files, so watermark 0. A recovering node here replays
    // the WAL from 0.
    assert_eq!(lake.published_watermark().await.unwrap(), 0);
    assert!(FileIndex::load(&lake).await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn parquet_roundtrips_through_the_latency_store() {
    let cfg = Config::from_env();

    // No injected latency here: this test is about correctness of the path, not
    // its timing. The benchmark opts in.
    let store = store::build(&cfg.s3, LatencyProfile::none()).unwrap();
    let path = Path::from("holy_grail/test/roundtrip.parquet");

    let batch = sample_batch();
    let bytes = write_parquet(&batch);
    store
        .put(&path, PutPayload::from_bytes(bytes.clone()))
        .await
        .unwrap();

    // Read it back the way the read path will: through the object store, not off
    // local disk.
    let dyn_store: Arc<dyn ObjectStore> = store.clone();
    let reader = ParquetObjectReader::new(dyn_store, path.clone());
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .await
    .unwrap();

    let read: Vec<RecordBatch> = builder.build().unwrap().try_collect().await.unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0], batch, "what went in must come back out");

    let stats = store.stats().snapshot();
    assert!(stats.puts >= 1 && stats.gets >= 1, "the shim saw the traffic");

    // The pruning metadata the read path depends on must actually be in the
    // file. Finding out in step 6 that it isn't would mean re-running every
    // measurement taken before then.
    let local = SerializedFileReader::new(bytes).unwrap();
    let rg = local.get_row_group(0).unwrap();
    let pk_meta = rg.metadata().column(0);

    let stats = pk_meta.statistics().expect("pk needs chunk statistics");
    assert!(stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some());

    assert!(
        pk_meta.bloom_filter_offset().is_some(),
        "pk needs a bloom filter, or every candidate file gets opened"
    );
}
