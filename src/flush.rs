//! Flush: the frozen memtable becomes a Parquet file in the DuckLake table.
//!
//! holy-grail **stages** the frozen memtable to a local Parquet file, then the
//! forked DuckDB binary **publishes** it — reading the staging file and writing
//! the real DuckLake data file to object storage plus all catalog rows, in one
//! transaction. DuckDB is the "write the table" library here, the DuckLake analog
//! of iceberg's `fast_append`: holy-grail keeps the orchestration (freeze,
//! watermark, truncate, recovery), DuckDB does the metadata mechanics.
//!
//! The protocol's ordering is unchanged and still load-bearing:
//!
//! 1. Check the published watermark. If it already covers this flush, skip to
//!    truncation (a retry after a crash between publish and truncate).
//! 2. Stage the memtable to a local Parquet file.
//! 3. Publish via DuckDB — atomic: the whole snapshot lands or nothing does.
//! 4. **Then** truncate the WAL.
//! 5. Retire the memtable.
//!
//! Publish before truncate, never the reverse. A crash in the window can only
//! leave the WAL holding records that are already published, which recovery
//! *skips* — a benign duplicate rather than an impossible hole.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, LargeBinaryArray, LargeBinaryBuilder, RecordBatch};
use parquet::arrow::AsyncArrowWriter;
use tokio::process::Command;

use crate::config::{CatalogConfig, DuckDbConfig, S3Config};
use crate::error::{Error, Result};
use crate::memtable::Memtable;
use crate::record::{Lsn, Op};
use crate::schema::{self};
use crate::store::LatencyProfile;

/// Rows per Arrow batch handed to the staging writer. A memory-shape knob only —
/// the *lake* file's row groups are DuckDB's `parquet_row_group_size`, set at
/// bootstrap, not this.
const BATCH_ROWS: usize = 8192;

/// What a flush did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Flushed {
    /// A file was written and a snapshot committed by DuckDB.
    Committed { watermark: Lsn, files: usize },
    /// The published watermark already covered this flush — a retry after a
    /// crash between publish and truncate. Nothing was written.
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
    /// The published watermark already covers this memtable — a retry after a
    /// crash between publish and truncate. Do not write; just truncate.
    AlreadyPublished { watermark: Lsn },
    Empty,
}

/// Decide what to do. **This is where idempotency lives, and nowhere else.**
///
/// `published` is the derived watermark (`MAX(lsn)` over live files). DuckDB's
/// INSERT is not idempotent on its own — a retried publish would append a second
/// file — so the watermark check is the actual mechanism. Because watermarks are
/// monotonic it is exact, not heuristic: if `published >= this flush's max_lsn`,
/// the flush already landed.
pub fn plan(published: Lsn, memtable: &Memtable) -> Plan {
    if memtable.is_empty() {
        return Plan::Empty;
    }
    let watermark = memtable.max_lsn();

    if watermark <= published {
        Plan::AlreadyPublished { watermark }
    } else {
        Plan::Write { watermark }
    }
}

/// Deterministic staging file name for a watermark.
///
/// A flush that crashed after staging but before publishing leaves an orphan
/// local file; the retry writes to the *same* name and overwrites it. The name
/// sorts in watermark order, which makes a stray staging dir readable.
pub fn staging_path(dir: &Path, watermark: Lsn) -> PathBuf {
    dir.join(format!("hg-stage-{watermark:020}.parquet"))
}

/// Stage the frozen memtable to a local Parquet file. Does not publish it.
///
/// This file is transport only — DuckDB reads it and writes the real lake file
/// with its own layout. So the staging writer needs no bloom, no tuned row
/// groups; it only needs correct types and column order.
pub async fn stage(memtable: &Memtable, path: &Path) -> Result<()> {
    let schema = schema::arrow_schema();
    let file = tokio::fs::File::create(path).await?;
    let mut writer = AsyncArrowWriter::try_new(file, schema, None)?;

    for batch in batches(memtable) {
        writer.write(&batch).await?;
    }
    writer.close().await?;
    Ok(())
}

/// Publish a staged file via the forked DuckDB binary.
///
/// Shells out to the binary with a SQL script that attaches the catalog, points
/// it at MinIO, and `INSERT … SELECT`s the staging rows. DuckDB writes the lake
/// Parquet and all catalog rows in one transaction. Its S3 write goes through
/// httpfs, which the latency shim cannot see, so charge it here for the
/// backpressure number — one PUT, the same undercharge the write path always had.
pub async fn publish(
    duckdb: &DuckDbConfig,
    catalog: &CatalogConfig,
    s3: &S3Config,
    staging: &Path,
    latency: LatencyProfile,
) -> Result<()> {
    let script = format!(
        "{preamble}\
         INSERT INTO lake.{schema}.{table} SELECT pk, lsn, op, value FROM read_parquet('{staging}');\n",
        preamble = attach_preamble(catalog, s3),
        schema = catalog.schema,
        table = catalog.table,
        staging = staging.display(),
    );

    run_duckdb(duckdb, &script).await?;
    charge(latency, 1, 0).await;
    Ok(())
}

/// The ATTACH + S3 settings every DuckDB script this engine runs begins with.
/// Attaches the DuckLake catalog as `lake`, points httpfs at MinIO, and disables
/// inlining (holy-grail is the row tier; the format's memtable must stay off).
pub fn attach_preamble(catalog: &CatalogConfig, s3: &S3Config) -> String {
    // MinIO endpoint without the scheme; the shim's config carries `http://`.
    let endpoint = s3
        .endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let use_ssl = s3.endpoint.starts_with("https://");

    format!(
        "ATTACH 'ducklake:postgres:{conn}' AS lake (DATA_PATH '{data}', DATA_INLINING_ROW_LIMIT 0);\n\
         SET s3_endpoint='{endpoint}'; SET s3_access_key_id='{ak}'; SET s3_secret_access_key='{sk}';\n\
         SET s3_url_style='path'; SET s3_use_ssl={ssl}; SET s3_region='{region}';\n\
         SET httpfs_client_implementation='httplib';\n",
        conn = catalog.pg_conn,
        data = catalog.data_path,
        ak = s3.access_key,
        sk = s3.secret_key,
        region = s3.region,
        ssl = use_ssl,
    )
}

/// Run a SQL script through the forked DuckDB binary, erroring on nonzero exit.
pub async fn run_duckdb(duckdb: &DuckDbConfig, script: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut child = Command::new(&duckdb.binary)
        .arg("-unsigned")
        .arg(":memory:")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    {
        let mut stdin = child.stdin.take().expect("stdin was piped");
        stdin.write_all(script.as_bytes()).await?;
        // Dropping stdin closes it, so DuckDB sees EOF and runs the script.
    }

    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(Error::DuckDbExec {
            code: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(())
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
        vec![Arc::new(pk), Arc::new(lsn), Arc::new(op), Arc::new(value)],
    )
    .expect("batch columns match the arrow schema")
}

/// Charge object-store round trips the latency shim could not see (DuckDB's
/// httpfs write). Kept for the backpressure number; unlike the Iceberg version
/// there are no fabricated metadata ops to charge — the catalog is Postgres.
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
    fn staging_path_is_a_function_of_the_watermark() {
        let dir = Path::new("/tmp");
        assert_eq!(staging_path(dir, 42), staging_path(dir, 42));
        assert_ne!(staging_path(dir, 42), staging_path(dir, 43));
        assert!(staging_path(dir, 42)
            .to_string_lossy()
            .ends_with("hg-stage-00000000000000000042.parquet"));
    }

    #[test]
    fn plan_skips_a_flush_already_covered_by_the_watermark() {
        let mt = Memtable::new();
        mt.insert(Record::put(Bytes::from_static(b"a"), 5, &b"one"[..]));
        // published watermark 5 already covers max_lsn 5.
        assert_eq!(plan(5, &mt), Plan::AlreadyPublished { watermark: 5 });
        // published watermark 4 does not.
        assert_eq!(plan(4, &mt), Plan::Write { watermark: 5 });
    }

    #[test]
    fn plan_on_empty_memtable_is_empty() {
        assert_eq!(plan(0, &Memtable::new()), Plan::Empty);
    }

    #[test]
    fn attach_preamble_strips_the_endpoint_scheme_and_disables_inlining() {
        let cat = CatalogConfig {
            pg_conn: "host=127.0.0.1 dbname=holy_grail".into(),
            schema: "main".into(),
            table: "kv".into(),
            data_path: "s3://warehouse/hg/".into(),
        };
        let s3 = S3Config {
            endpoint: "http://localhost:9000".into(),
            bucket: "warehouse".into(),
            access_key: "admin".into(),
            secret_key: "password".into(),
            region: "us-east-1".into(),
        };
        let sql = attach_preamble(&cat, &s3);
        assert!(sql.contains("s3_endpoint='localhost:9000'"));
        assert!(sql.contains("s3_use_ssl=false"));
        assert!(sql.contains("DATA_INLINING_ROW_LIMIT 0"));
        assert!(sql.contains("ducklake:postgres:host=127.0.0.1 dbname=holy_grail"));
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

        let pk = b.column(0).as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq!(pk.value(0), b"a");
        assert_eq!(pk.value(1), b"b");
        assert_eq!(pk.value(2), b"c");

        let value = b.column(3).as_any().downcast_ref::<LargeBinaryArray>().unwrap();
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
