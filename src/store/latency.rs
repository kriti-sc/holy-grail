//! Latency injection for the object store.
//!
//! MinIO answers in about a millisecond. Real S3 does not. Every number this
//! prototype produces — where the p99 curve knees, how far flush falls behind —
//! is a function of object-store latency, so the store is wrapped here rather
//! than measured against MinIO's local timings and then hand-waved.
//!
//! This wrapper sits under *everything* that touches object storage: Parquet
//! reads, Iceberg catalog and manifest I/O, flush uploads. Anything that reaches
//! around it produces a number that means nothing.

use std::fmt::{self, Debug, Display};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result,
};

/// The standard-normal quantile at p99. Fixed constant, not worth a dependency.
const Z99: f64 = 2.326_347_874_040_841;

/// Latency of one class of object-store operation, as a **lognormal**
/// distribution fitted to a measured `(p50, p99)` pair.
///
/// The distribution matters as much as the magnitude, and it is easy to get
/// wrong. This used to be `base + uniform(0, jitter)`. Work out what that can
/// express: `p50 = base + J/2` and `p99 ≈ base + 0.99·J`, so the largest
/// `p99/p50` ratio a uniform draw can produce — even with `base = 0` — is about
/// **2**. Real S3 GET is 86/26 ≈ **3.3**. There is no `(base, jitter)` pair that
/// fits both; solve it and `base` comes out negative.
///
/// So a uniform jitter cannot represent S3's tail *at all*, and the tail is the
/// entire deliverable. A published p99 would have been an artifact of the
/// distribution we picked rather than a property of the storage we claim to
/// model.
///
/// Lognormal is the standard shape for this — request latency is a product of
/// many independent multiplicative effects (queueing, retries, stragglers), and
/// it is heavy-tailed and strictly positive, as latency is. Fitting is exact:
/// with `median = exp(μ)` and `p99 = median · exp(σ·z99)`, both parameters fall
/// straight out of the two measured percentiles.
#[derive(Debug, Clone, Copy)]
pub struct OpLatency {
    /// `exp(μ)` — the median, in the units the draw returns.
    median: Duration,
    /// `σ` of the underlying normal. Zero means a constant delay.
    sigma: f64,
}

impl OpLatency {
    pub const fn zero() -> Self {
        OpLatency {
            median: Duration::ZERO,
            sigma: 0.0,
        }
    }

    /// Fit a lognormal to a measured `(p50, p99)`.
    ///
    /// `σ = ln(p99 / p50) / z99`. A `p99` at or below `p50` is nonsense and
    /// collapses to a constant rather than panicking, so a badly configured
    /// benchmark degrades to the old behaviour instead of dying.
    pub fn from_p50_p99(p50: Duration, p99: Duration) -> Self {
        if p50.is_zero() {
            return Self::zero();
        }
        let ratio = p99.as_secs_f64() / p50.as_secs_f64();
        let sigma = if ratio > 1.0 {
            ratio.ln() / Z99
        } else {
            0.0
        };
        OpLatency {
            median: p50,
            sigma,
        }
    }

    /// A constant delay, no spread. Honest only when you have no tail data.
    pub const fn fixed(d: Duration) -> Self {
        OpLatency {
            median: d,
            sigma: 0.0,
        }
    }

    pub fn is_zero(&self) -> bool {
        self.median.is_zero()
    }

    /// One draw from the fitted distribution.
    pub fn draw(&self) -> Duration {
        if self.median.is_zero() {
            return Duration::ZERO;
        }
        if self.sigma == 0.0 {
            return self.median;
        }
        self.median.mul_f64((self.sigma * standard_normal()).exp())
    }
}

/// Box–Muller. `rand` gives uniforms; a normal is two lines from there, and not
/// worth pulling `rand_distr` in for.
fn standard_normal() -> f64 {
    // Half-open at zero: ln(0) is -inf.
    let u1: f64 = rand::random_range(f64::MIN_POSITIVE..1.0);
    let u2: f64 = rand::random_range(0.0..1.0);
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// Per-operation-class latency. Reads and writes are separated because they are
/// not symmetric on S3, and the read number is the one the p99 curve is
/// sensitive to.
#[derive(Debug, Clone, Copy)]
pub struct LatencyProfile {
    pub get: OpLatency,
    pub put: OpLatency,
    pub list: OpLatency,
    pub delete: OpLatency,
}

impl LatencyProfile {
    /// No injection — MinIO's own latency, straight through. Useful for
    /// correctness tests, useless for the benchmark.
    pub const fn none() -> Self {
        LatencyProfile {
            get: OpLatency::zero(),
            put: OpLatency::zero(),
            list: OpLatency::zero(),
            delete: OpLatency::zero(),
        }
    }

    /// Same-region S3 Standard, fitted to published measurements.
    ///
    /// GET  p50 26 ms, p99  86 ms — nixiesearch, 0.5 MB objects, same region.
    /// PUT  p50 70 ms, p99 137 ms — TopicPartition, 500 KB objects, eu-north-1.
    ///
    /// LIST and DELETE are **not** cited — no benchmark to hand. LIST borrows
    /// GET's shape scaled up, DELETE borrows PUT's. Neither is on the read path,
    /// so neither touches the headline number; if one ever does, measure it
    /// rather than inheriting this guess.
    pub fn s3_same_region() -> Self {
        LatencyProfile {
            get: OpLatency::from_p50_p99(Duration::from_millis(26), Duration::from_millis(86)),
            put: OpLatency::from_p50_p99(Duration::from_millis(70), Duration::from_millis(137)),
            list: OpLatency::from_p50_p99(Duration::from_millis(40), Duration::from_millis(120)),
            delete: OpLatency::from_p50_p99(Duration::from_millis(50), Duration::from_millis(100)),
        }
    }

    /// S3 Express One Zone — the "what if the storage got fast" arm.
    ///
    /// Single-digit-millisecond class: GET p50 3 ms / p99 15 ms, PUT p50 5 ms /
    /// p99 20 ms. If the p99 curve knees on Standard but not here, the knee is a
    /// property of S3's tail and not of this architecture — which is a result
    /// worth having either way.
    pub fn s3_express() -> Self {
        LatencyProfile {
            get: OpLatency::from_p50_p99(Duration::from_millis(3), Duration::from_millis(15)),
            put: OpLatency::from_p50_p99(Duration::from_millis(5), Duration::from_millis(20)),
            list: OpLatency::from_p50_p99(Duration::from_millis(5), Duration::from_millis(20)),
            delete: OpLatency::from_p50_p99(Duration::from_millis(5), Duration::from_millis(20)),
        }
    }

    pub fn is_zero(&self) -> bool {
        self.get.is_zero() && self.put.is_zero() && self.list.is_zero() && self.delete.is_zero()
    }

    /// Total delay for `puts` PUTs and `gets` GETs that the shim could not see.
    ///
    /// Each round trip is drawn **independently** and the draws are summed —
    /// they are sequential, dependent calls, so their latencies add. Drawing once
    /// and multiplying by `n` would understate the variance of the sum and, worse,
    /// make a slow commit slow in *every* op at once, which is not how a tail
    /// works.
    pub fn charge_for(&self, puts: u32, gets: u32) -> Duration {
        let mut total = Duration::ZERO;
        for _ in 0..puts {
            total += self.put.draw();
        }
        for _ in 0..gets {
            total += self.get.draw();
        }
        total
    }
}

#[derive(Debug, Default)]
pub struct Stats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub lists: AtomicU64,
    pub deletes: AtomicU64,
    pub injected_micros: AtomicU64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            injected: Duration::from_micros(self.injected_micros.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub gets: u64,
    pub puts: u64,
    pub lists: u64,
    pub deletes: u64,
    pub injected: Duration,
}

/// Wraps any `ObjectStore`, sleeping before each operation.
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    profile: LatencyProfile,
    stats: Arc<Stats>,
}

impl LatencyStore {
    pub fn new(inner: Arc<dyn ObjectStore>, profile: LatencyProfile) -> Self {
        LatencyStore {
            inner,
            profile,
            stats: Arc::new(Stats::default()),
        }
    }

    pub fn stats(&self) -> Arc<Stats> {
        Arc::clone(&self.stats)
    }

    async fn delay(&self, op: OpLatency, counter: &AtomicU64) {
        let d = op.draw();
        charge(&self.stats, counter, d);
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
    }

    async fn on_get(&self) {
        self.delay(self.profile.get, &self.stats.gets).await
    }
    async fn on_put(&self) {
        self.delay(self.profile.put, &self.stats.puts).await
    }
}

fn charge(stats: &Stats, counter: &AtomicU64, d: Duration) {
    counter.fetch_add(1, Ordering::Relaxed);
    stats
        .injected_micros
        .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
}

impl Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStore({})", self.inner)
    }
}

impl Debug for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatencyStore")
            .field("inner", &self.inner)
            .field("profile", &self.profile)
            .finish()
    }
}

/// The primitives are intercepted, and the trait's own default methods (`get`,
/// `head`, `put`, `get_range`) are built on those primitives — so they inherit
/// the delay without appearing here, and, more to the point, there is no way for
/// a future call site to reach around the shim by picking a different method.
#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.on_put().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.on_put().await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.on_get().await;
        self.inner.get_opts(location, options).await
    }

    /// Overridden because the default fans a multi-range read out into one
    /// `get_range` per coalesced run, which would charge a full round trip per
    /// run. A real client issues those concurrently, so a single delay is the
    /// honest charge — and the read path fetches several row-group ranges at a
    /// time.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.on_get().await;
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.on_get().await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.delay(self.profile.delete, &self.stats.deletes).await;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self.inner.list(prefix);
        let delay = self.profile.list.draw();
        charge(&self.stats, &self.stats.lists, delay);

        // Pay before the first item, not per item: a list is one request, and
        // pagination is the inner store's business.
        futures::stream::once(async move {
            tokio::time::sleep(delay).await;
            inner
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.delay(self.profile.list, &self.stats.lists).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.on_put().await;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.on_put().await;
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::time::Instant;

    fn store(profile: LatencyProfile) -> LatencyStore {
        LatencyStore::new(Arc::new(InMemory::new()), profile)
    }

    /// Fitting a lognormal to a measured (p50, p99) has to reproduce *both*, or
    /// the injected tail is a number we invented. Checked against the real S3
    /// GET figures the benchmark ships with.
    #[test]
    fn a_fitted_lognormal_reproduces_its_percentiles() {
        let op =
            OpLatency::from_p50_p99(Duration::from_millis(26), Duration::from_millis(86));

        let mut draws: Vec<u128> = (0..200_000).map(|_| op.draw().as_micros()).collect();
        draws.sort_unstable();

        let pct = |p: f64| draws[(draws.len() as f64 * p) as usize] as f64 / 1000.0;
        let (p50, p99) = (pct(0.50), pct(0.99));

        assert!((p50 - 26.0).abs() < 1.0, "p50 was {p50:.1}ms, wanted 26ms");
        assert!((p99 - 86.0).abs() < 4.0, "p99 was {p99:.1}ms, wanted 86ms");

        // The property uniform jitter could not express at any parameterisation:
        // its p99/p50 ratio caps out around 2, and S3's is 3.3.
        assert!(
            p99 / p50 > 3.0,
            "tail ratio {:.2} — too flat to be S3",
            p99 / p50
        );
    }

    #[tokio::test]
    async fn injects_the_configured_delay() {
        let profile = LatencyProfile {
            get: OpLatency::fixed(Duration::from_millis(50)),
            put: OpLatency::fixed(Duration::from_millis(50)),
            ..LatencyProfile::none()
        };
        let store = store(profile);
        let path = Path::from("k");

        let t = Instant::now();
        store.put(&path, PutPayload::from_static(b"v")).await.unwrap();
        store.get(&path).await.unwrap();
        let elapsed = t.elapsed();

        assert!(
            elapsed >= Duration::from_millis(100),
            "expected >=100ms of injected latency, got {elapsed:?}"
        );

        let stats = store.stats().snapshot();
        assert_eq!(stats.puts, 1);
        assert_eq!(stats.gets, 1);
        assert!(stats.injected >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn a_zero_profile_is_a_passthrough() {
        let store = store(LatencyProfile::none());
        let path = Path::from("k");

        let t = Instant::now();
        store.put(&path, PutPayload::from_static(b"v")).await.unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();

        assert_eq!(&got[..], b"v");
        assert!(t.elapsed() < Duration::from_millis(20));
        assert_eq!(store.stats().snapshot().injected, Duration::ZERO);
    }

    #[tokio::test]
    async fn range_reads_are_charged_a_round_trip() {
        let store = store(LatencyProfile {
            get: OpLatency::fixed(Duration::from_millis(30)),
            ..LatencyProfile::none()
        });
        let path = Path::from("k");
        store
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();

        let t = Instant::now();
        let got = store.get_range(&path, 2..5).await.unwrap();
        assert_eq!(&got[..], b"234");
        assert!(t.elapsed() >= Duration::from_millis(30));
    }

    #[tokio::test]
    async fn listing_pays_before_the_first_item() {
        let store = store(LatencyProfile {
            list: OpLatency::fixed(Duration::from_millis(40)),
            ..LatencyProfile::none()
        });
        store
            .put(&Path::from("a"), PutPayload::from_static(b"v"))
            .await
            .unwrap();

        let t = Instant::now();
        let found: Vec<_> = store.list(None).collect().await;
        assert_eq!(found.len(), 1);
        assert!(t.elapsed() >= Duration::from_millis(40));
        assert_eq!(store.stats().snapshot().lists, 1);
    }
}
