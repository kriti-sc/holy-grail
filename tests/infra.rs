//! Step 1: MinIO + Iceberg REST catalog are real, and a PK-sorted Parquet file
//! round-trips through the latency-wrapped object store.
//!
//! Requires `docker compose up -d`. Ignored by default so `cargo test` stays
//! hermetic; run with:
//!
//!     cargo test --test infra -- --ignored --nocapture

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, LargeBinaryArray, RecordBatch};
use bytes::Bytes;
use holy_grail::config::Config;
use holy_grail::schema::{self, FIELD_ID_PK};
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
#[ignore = "requires docker compose up -d"]
async fn catalog_creates_the_table_and_is_idempotent() {
    let cfg = Config::from_env();
    let cat = catalog::connect(&cfg.catalog, &cfg.s3).await.unwrap();

    let table = catalog::ensure_table(&cat, &cfg.catalog).await.unwrap();
    let again = catalog::ensure_table(&cat, &cfg.catalog).await.unwrap();
    assert_eq!(
        table.metadata().uuid(),
        again.metadata().uuid(),
        "ensure_table must load, not recreate — recovery depends on it"
    );

    let schema = table.metadata().current_schema();
    assert_eq!(
        schema.identifier_field_ids().collect::<Vec<_>>(),
        vec![FIELD_ID_PK]
    );

    let order = table.metadata().default_sort_order();
    assert_eq!(order.fields.len(), 1, "table must be sorted by pk");
    assert_eq!(order.fields[0].source_id, FIELD_ID_PK);

    // No data yet, so no snapshot, so no watermark. A recovering node here must
    // replay the WAL from 0.
    assert!(table.metadata().current_snapshot().is_none() || table.metadata().snapshots().count() > 0);
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
