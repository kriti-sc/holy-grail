//! The DuckLake catalog: a Postgres connection, resolved once and read on the
//! hot path. The catalog is the only thing that knows where the columnar level
//! ends. Every flush is published here (by the forked DuckDB binary), and every
//! recovery reads it back. Nothing else is authoritative.
//!
//! holy-grail is a **read-only** client of the catalog. It never inserts
//! `ducklake_*` rows — the forked DuckDB binary does that at flush (see
//! `flush.rs`). So this module resolves the table, verifies the column mapping
//! matches what the read path assumes, and answers two questions: what files are
//! live (`index.rs`), and what watermark is published.

use tokio_postgres::{Client, NoTls};

use crate::config::{CatalogConfig, DuckDbConfig, S3Config};
use crate::error::{Error, Result};
use crate::flush;
use crate::record::Lsn;
use crate::schema::{EXPECTED_COLUMNS, FIELD_ID_LSN};

/// Create the DuckLake table if absent and set its write options, via the forked
/// DuckDB binary. This is the DDL the engine relies on but never issues itself —
/// the engine is a read-only catalog client. Idempotent (`CREATE TABLE IF NOT
/// EXISTS`), so it is safe to run on every start and in every test.
///
/// Sets `parquet_row_group_size` so DuckDB's data files have small row groups —
/// without it the default (a million rows) puts the file in one row group and
/// row-group pruning does nothing. `catalog_blooms` opts `pk` into catalog-side
/// bloom filters (Phase 2); off in Phase 1 so the read path stays a control.
pub async fn bootstrap(
    duckdb: &DuckDbConfig,
    catalog: &CatalogConfig,
    s3: &S3Config,
) -> Result<()> {
    let mut script = format!(
        "{preamble}\
         CREATE TABLE IF NOT EXISTS lake.{schema}.{table}\
             (pk BLOB, lsn BIGINT, op INTEGER, value BLOB);\n\
         CALL lake.set_option('parquet_row_group_size', '{rg}', table_name => '{table}');\n",
        preamble = flush::attach_preamble(catalog, s3),
        schema = catalog.schema,
        table = catalog.table,
        rg = duckdb.row_group_size,
    );
    if duckdb.catalog_blooms {
        script.push_str(&format!(
            "CALL lake.set_option('bloom_filter_columns', 'pk', table_name => '{}');\n",
            catalog.table
        ));
    }
    flush::run_duckdb(duckdb, &script).await
}

/// A resolved handle to the DuckLake table in Postgres.
pub struct DuckLake {
    client: Client,
    /// `ducklake_table.table_id` for our table.
    pub table_id: i64,
    /// Bucket the data files live in, parsed from the catalog's DATA_PATH.
    pub bucket: String,
    /// Object-key prefix every data file sits under: the DATA_PATH prefix plus
    /// the schema and table sub-paths, e.g. `hg/main/kv/`. A file's full object
    /// key is this plus the relative `path` from `ducklake_data_file`.
    pub key_prefix: String,
}

impl DuckLake {
    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    /// Connect to Postgres, resolve the table, and verify the column mapping.
    ///
    /// `ensure_table` in the Iceberg version *created* the table; here the DDL is
    /// the forked binary's job (bootstrap), so this is resolve-and-verify. A
    /// missing table is a loud error rather than a silent create — the engine
    /// must not invent a table the writer never made.
    pub async fn connect(cfg: &CatalogConfig) -> Result<DuckLake> {
        let (client, connection) = tokio_postgres::connect(&cfg.pg_conn, NoTls).await?;
        // The connection drives the socket; it must run for the client to work.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "postgres connection closed");
            }
        });

        let (table_id, schema_sub, table_sub) = resolve_table(&client, &cfg.schema, &cfg.table)
            .await?
            .ok_or_else(|| {
                Error::Config(format!(
                    "table {}.{} not found in the DuckLake catalog — bootstrap it with the \
                     forked duckdb binary first",
                    cfg.schema, cfg.table
                ))
            })?;

        let (bucket, data_prefix) = split_data_path(&cfg.data_path)?;
        let key_prefix = format!("{data_prefix}{schema_sub}{table_sub}");

        let lake = DuckLake {
            client,
            table_id,
            bucket,
            key_prefix,
        };
        lake.verify_columns().await?;
        Ok(lake)
    }

    /// Check the catalog's `ducklake_column` agrees with the schema the read path
    /// binds by. A drift here (a renamed column, a reordered id) produces reads
    /// that bind the wrong column and come back all-nulls — the miserable,
    /// silent failure the Iceberg `arrow_mirrors_iceberg_field_ids` test guarded
    /// against, now guarded at startup against the live catalog.
    async fn verify_columns(&self) -> Result<()> {
        let rows = self
            .client
            .query(
                "SELECT column_id, column_name, column_type FROM ducklake_column \
                 WHERE table_id = $1 AND end_snapshot IS NULL ORDER BY column_order",
                &[&self.table_id],
            )
            .await?;

        if rows.len() != EXPECTED_COLUMNS.len() {
            return Err(Error::Config(format!(
                "catalog has {} columns, engine expects {}",
                rows.len(),
                EXPECTED_COLUMNS.len()
            )));
        }
        for (row, (id, name, ty)) in rows.iter().zip(EXPECTED_COLUMNS) {
            let got_id: i64 = row.get(0);
            let got_name: String = row.get(1);
            let got_ty: String = row.get(2);
            if got_id != *id as i64 || got_name != *name || got_ty != *ty {
                return Err(Error::Config(format!(
                    "column mismatch: catalog has ({got_id}, {got_name}, {got_ty}), \
                     engine expects ({id}, {name}, {ty})"
                )));
            }
        }
        Ok(())
    }

    /// The published watermark: the highest `lsn` any live file covers, read
    /// straight from the column stats DuckDB commits atomically with each file.
    ///
    /// This replaces Iceberg's snapshot-summary watermark property. It needs no
    /// separate write and cannot be out of step with the data it describes,
    /// because it *is* a stat of that data. Absent files ⇒ watermark 0.
    pub async fn published_watermark(&self) -> Result<Lsn> {
        let row = self
            .client
            .query_one(
                "SELECT COALESCE(MAX(s.max_value::BIGINT), 0) \
                 FROM ducklake_file_column_stats s \
                 JOIN ducklake_data_file f ON f.data_file_id = s.data_file_id \
                 WHERE s.table_id = $1 AND s.column_id = $2 AND f.end_snapshot IS NULL",
                &[&self.table_id, &(FIELD_ID_LSN as i64)],
            )
            .await?;
        let watermark: i64 = row.get(0);
        Ok(watermark as Lsn)
    }
}

/// Resolve `(table_id, schema_path, table_path)` for a live table by name.
async fn resolve_table(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Option<(i64, String, String)>> {
    let row = client
        .query_opt(
            "SELECT t.table_id, s.path, t.path \
             FROM ducklake_table t JOIN ducklake_schema s ON s.schema_id = t.schema_id \
             WHERE t.table_name = $1 AND s.schema_name = $2 \
               AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL",
            &[&table, &schema],
        )
        .await?;
    Ok(row.map(|r| (r.get(0), r.get(1), r.get(2))))
}

/// Split `s3://bucket/prefix/` into `(bucket, "prefix/")`.
fn split_data_path(data_path: &str) -> Result<(String, String)> {
    let rest = data_path.strip_prefix("s3://").ok_or_else(|| {
        Error::Config(format!("DATA_PATH must be an s3:// URL, got {data_path:?}"))
    })?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    Ok((bucket.to_string(), prefix.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_path_splits_into_bucket_and_prefix() {
        let (bucket, prefix) = split_data_path("s3://warehouse/hg/").unwrap();
        assert_eq!(bucket, "warehouse");
        assert_eq!(prefix, "hg/");
    }

    #[test]
    fn data_path_without_prefix() {
        let (bucket, prefix) = split_data_path("s3://warehouse/").unwrap();
        assert_eq!(bucket, "warehouse");
        assert_eq!(prefix, "");
    }

    #[test]
    fn non_s3_data_path_is_rejected() {
        assert!(split_data_path("/local/path").is_err());
    }
}
