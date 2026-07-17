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

#[derive(Debug, Clone)]
pub struct CatalogConfig {
    pub uri: String,
    pub warehouse: String,
    pub namespace: String,
    pub table: String,
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
            uri: env("HG_CATALOG_URI", "http://localhost:8181"),
            warehouse: env("HG_WAREHOUSE", "s3://warehouse/"),
            namespace: env("HG_NAMESPACE", "holy_grail"),
            table: env("HG_TABLE", "kv"),
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
