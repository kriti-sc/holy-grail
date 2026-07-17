//! Object storage, and the latency shim that makes MinIO tell the truth.

pub mod latency;

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

use crate::config::S3Config;
use crate::error::Result;

pub use latency::{LatencyProfile, LatencyStore, OpLatency, Stats, StatsSnapshot};

/// Build the S3 client for MinIO, wrapped in the latency shim.
///
/// Everything that touches object storage — Parquet reads, Iceberg manifests,
/// flush uploads — must go through the store this returns. A path that reaches
/// past it is a path whose measurements are meaningless.
pub fn build(cfg: &S3Config, profile: LatencyProfile) -> Result<Arc<LatencyStore>> {
    let s3 = AmazonS3Builder::new()
        .with_endpoint(&cfg.endpoint)
        .with_bucket_name(&cfg.bucket)
        .with_access_key_id(&cfg.access_key)
        .with_secret_access_key(&cfg.secret_key)
        .with_region(&cfg.region)
        // MinIO speaks path-style over plain http locally.
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()?;

    Ok(Arc::new(LatencyStore::new(
        Arc::new(s3) as Arc<dyn ObjectStore>,
        profile,
    )))
}
