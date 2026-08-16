//! Configuration, read from the environment with defaults that match
//! `docker-compose.yml`.

use std::path::PathBuf;
use std::time::Duration;

use crate::store::{LatencyProfile, OpLatency};

#[derive(Debug, Clone)]
pub struct Config {
    pub wal_dir: PathBuf,
    pub wal_segment_bytes: u64,

    /// Flush threshold.
    pub memtable_max_bytes: usize,
    /// Frozen memtables allowed to queue before writes stall. Flush is an
    /// object-store round trip, so this is a real limit, not a formality.
    pub max_frozen_memtables: usize,

    pub s3: S3Config,
    pub catalog: CatalogConfig,
    pub duckdb: DuckDbConfig,
    pub latency: LatencyProfile,

    /// Bytes of Parquet held locally. The variable swept in the benchmark: it is
    /// a cache over the columnar level, never a copy of it.
    pub cache_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

/// The DuckLake catalog: a Postgres database plus the schema/table the engine
/// reads. Unlike the Iceberg REST catalog this replaced, holy-grail only ever
/// *reads* here — the forked DuckDB binary is what writes catalog rows.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// libpq-style connection string, e.g.
    /// `host=127.0.0.1 dbname=holy_grail user=admin`.
    pub pg_conn: String,
    /// DuckLake schema name (`main` by default).
    pub schema: String,
    /// Table name (`kv`).
    pub table: String,
    /// The `DATA_PATH` the catalog was attached with, e.g.
    /// `s3://warehouse/hg/`. File paths in `ducklake_data_file` are relative to
    /// this (plus the schema/table subpaths).
    pub data_path: String,
}

/// How to drive the forked DuckDB binary that publishes flushes. This is the
/// DuckLake "write the table" library — the analog of the iceberg crate.
#[derive(Debug, Clone)]
pub struct DuckDbConfig {
    /// Path to the forked `duckdb` binary (built with httpfs + postgres, and the
    /// catalog-bloom modification).
    pub binary: PathBuf,
    /// Rows per Parquet row group DuckDB writes. Must match the read path's
    /// assumption — the default (a million) would put the file in one row group
    /// and make row-group pruning a no-op.
    pub row_group_size: usize,
    /// Whether to opt the table into catalog-side bloom filters on `pk`. Off in
    /// Phase 1 (the read path still probes the Parquet on-disk bloom, so the R1
    /// curve stays a byte-identical control); on in Phase 2.
    pub catalog_blooms: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            wal_dir: PathBuf::from("./data/wal"),
            wal_segment_bytes: 64 * 1024 * 1024,
            memtable_max_bytes: 64 * 1024 * 1024,
            max_frozen_memtables: 2,
            s3: S3Config::default(),
            catalog: CatalogConfig::default(),
            duckdb: DuckDbConfig::default(),
            latency: LatencyProfile::none(),
            cache_bytes: 256 * 1024 * 1024,
        }
    }
}

impl Default for S3Config {
    fn default() -> Self {
        S3Config {
            endpoint: env("HG_S3_ENDPOINT", "http://localhost:9000"),
            bucket: env("HG_S3_BUCKET", "warehouse"),
            access_key: env("HG_S3_ACCESS_KEY", "admin"),
            secret_key: env("HG_S3_SECRET_KEY", "password"),
            region: env("HG_S3_REGION", "us-east-1"),
        }
    }
}

impl Default for CatalogConfig {
    fn default() -> Self {
        CatalogConfig {
            pg_conn: env(
                "HG_PG_CONN",
                "host=127.0.0.1 dbname=holy_grail",
            ),
            schema: env("HG_SCHEMA", "main"),
            table: env("HG_TABLE", "kv"),
            data_path: env("HG_DATA_PATH", "s3://warehouse/hg/"),
        }
    }
}

impl Default for DuckDbConfig {
    fn default() -> Self {
        DuckDbConfig {
            binary: PathBuf::from(env(
                "HG_DUCKDB_BIN",
                "/Users/kriti/Projects/ducklake/build/release/duckdb",
            )),
            row_group_size: 8192,
            // On: the catalog bloom on `pk` is the *only* pk bloom DuckLake
            // writes (DuckDB does not emit an on-disk Parquet bloom for a
            // high-cardinality key), and the read path probes it in memory.
            catalog_blooms: true,
        }
    }
}

impl Config {
    /// Build from the environment, overriding the defaults above.
    pub fn from_env() -> Self {
        let mut cfg = Config::default();

        if let Some(v) = env_opt("HG_WAL_DIR") {
            cfg.wal_dir = PathBuf::from(v);
        }
        if let Some(v) = env_parse("HG_MEMTABLE_MAX_BYTES") {
            cfg.memtable_max_bytes = v;
        }
        if let Some(v) = env_parse("HG_MAX_FROZEN") {
            cfg.max_frozen_memtables = v;
        }
        if let Some(v) = env_parse("HG_CACHE_BYTES") {
            cfg.cache_bytes = v;
        }

        // Latency injection: off unless asked for, so correctness tests run at
        // full speed and benchmarks have to opt in explicitly.
        cfg.latency = match env("HG_LATENCY", "none").as_str() {
            "s3" => LatencyProfile::s3_same_region(),
            "express" => LatencyProfile::s3_express(),
            "none" => LatencyProfile::none(),
            other => panic!("HG_LATENCY must be none|s3|express, got {other:?}"),
        };

        // Per-class overrides, as a measured (p50, p99) pair. Both or neither —
        // a p50 without a p99 would silently fit a zero-variance distribution and
        // hand back a tail that does not exist.
        if let (Some(p50), Some(p99)) = (
            env_parse::<u64>("HG_LATENCY_GET_P50_MS"),
            env_parse::<u64>("HG_LATENCY_GET_P99_MS"),
        ) {
            cfg.latency.get = OpLatency::from_p50_p99(
                Duration::from_millis(p50),
                Duration::from_millis(p99),
            );
        }
        if let (Some(p50), Some(p99)) = (
            env_parse::<u64>("HG_LATENCY_PUT_P50_MS"),
            env_parse::<u64>("HG_LATENCY_PUT_P99_MS"),
        ) {
            cfg.latency.put = OpLatency::from_p50_p99(
                Duration::from_millis(p50),
                Duration::from_millis(p99),
            );
        }

        cfg
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok()?.parse().ok()
}
