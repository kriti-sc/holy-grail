//! Step 7 — the benchmark. Finds the operating envelope for claim #1.
//!
//! **What this measures, and what it does not.** It does not return a verdict.
//! "Does OLTP over Iceberg work" is the wrong question — nobody has shipped it,
//! so of course a naive build has a wall. The right question is *where the wall
//! is* and *what is holding it up*, because that is what an optimisation has to
//! move. The output is therefore an envelope — "OLTP-grade p99 holds while the
//! cache is at least X% of the working set" — plus an attribution of whatever
//! gave way at the edge.
//!
//! The 10 ms bar is still fixed in advance (PLAN.md), because "find where to
//! optimise next" without a fixed definition of *good enough* is unfalsifiable:
//! there is always a next optimisation. The bar is what the curve crosses; where
//! it crosses is the finding.
//!
//! Run:
//!
//!     docker compose up -d
//!     cargo run --release --bin bench -- --write-dist random
//!     cargo run --release --bin bench -- --write-dist sequential   # the flattering case
//!
//! ## What is actually being swept
//!
//! The independent variable is **working-set bytes ÷ cache bytes**, and the
//! numerator is *measured, not computed*. A calibration pass runs the read
//! workload against an effectively unbounded cache and reads back how many bytes
//! ended up resident. That is the working set, by definition, and it is the only
//! honest way to get it: a key's footprint is not its value size but the byte
//! ranges its lookup touches — a bloom filter plus the surviving row group's
//! column chunks — and how many *distinct* row groups the hot keys land in
//! depends entirely on the write distribution. Computing it analytically would
//! mean assuming the very thing under test.
//!
//! ## Why the write distribution is the whole experiment
//!
//! Sequential writes give each flushed file a contiguous, disjoint PK range, so
//! the in-memory interval map dismisses every file but one for free, and a point
//! read costs O(1) in the file count. Uniform-random writes make every file's
//! `[pk_min, pk_max]` span the keyspace. The intervals overlap almost totally,
//! the prune dismisses nothing, and every candidate file costs a bloom fetch —
//! O(files), with no compaction to stop the file count from growing.
//!
//! OLTP writes randomly. `sequential` exists only to show what is being given up.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use holy_grail::config::Config;
use holy_grail::{catalog, Engine};

/// Total keys written to the table.
const KEYSPACE: u32 = 200_000;
/// Keys per flush, and so the number of files: KEYSPACE / FLUSH_EVERY.
const FLUSH_EVERY: u32 = 10_000;
/// Distinct keys the read workload touches. The working set, in keys.
const WORKING_SET: usize = 2_000;
/// Measured reads per sweep point. Sequential, never concurrent — concurrency
/// would confound queueing delay with storage latency, and it is storage latency
/// the claim is about.
///
/// Overridable, because under injected S3 latency a read that misses costs
/// hundreds of milliseconds and a sweep of seven points gets expensive. 1000 is
/// enough for a p99; it is thin for p99.9, which is why that column is reported
/// but not used for anything load-bearing.
fn reads() -> usize {
    std::env::var("HG_BENCH_READS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000)
}
const VALUE_BYTES: usize = 256;

/// Working-set-to-cache ratios to sweep.
///
/// Below 1.0 the cache holds the whole working set and every read is a memory
/// hit — included only to anchor the flat end of the curve, and worth nothing as
/// evidence. Everything interesting is above 1.0, because "the row tier is a
/// cache, not a copy" *means* the cache is smaller than what you read.
const RATIOS: &[f64] = &[0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0];

/// The line that defines "OLTP-adjacent", fixed in PLAN.md before any number
/// existed. Not a pass/fail on the project — it is the threshold the curve
/// crosses, and *where* it crosses is the deliverable.
const OLTP_P99: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, PartialEq)]
enum WriteDist {
    Random,
    Sequential,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dist = match args.iter().position(|a| a == "--write-dist") {
        Some(i) => match args.get(i + 1).map(String::as_str) {
            Some("random") => WriteDist::Random,
            Some("sequential") => WriteDist::Sequential,
            other => panic!("--write-dist must be random|sequential, got {other:?}"),
        },
        None => WriteDist::Random,
    };

    let mut cfg = Config::from_env();
    cfg.catalog.table = format!(
        "bench_{}_{}",
        if dist == WriteDist::Random { "rnd" } else { "seq" },
        std::process::id()
    );
    cfg.wal_dir = std::env::temp_dir().join(format!("bench-wal-{}", std::process::id()));
    std::fs::create_dir_all(&cfg.wal_dir).unwrap();
    // Flushes are driven explicitly, so the file layout is a property of the
    // experiment rather than of whenever the memtable happened to fill.
    cfg.memtable_max_bytes = 1 << 30;

    if cfg.latency.is_zero() {
        eprintln!(
            "WARNING: HG_LATENCY is unset, so the object store answers at MinIO speed (~1 ms).\n\
             Every number below will be a fiction. Re-run with HG_LATENCY=s3.\n"
        );
    }

    // The engine is a read-only catalog client, so the table must exist first.
    catalog::bootstrap(&cfg.duckdb, &cfg.catalog, &cfg.s3)
        .await
        .unwrap();

    let keys = write_table(&cfg, dist).await;
    let (working_set, hot) = calibrate(&cfg, &keys).await;

    println!("\n  write distribution : {}", match dist {
        WriteDist::Random => "uniform random  (files overlap on PK; interval prune is dead)",
        WriteDist::Sequential => "sequential      (files disjoint on PK; interval prune works)",
    });
    println!("  keyspace           : {KEYSPACE} keys, {VALUE_BYTES} B values");
    println!("  files              : {}", KEYSPACE / FLUSH_EVERY);
    println!("  working set        : {WORKING_SET} keys = {} KiB resident", working_set / 1024);
    println!("  reads per point    : {}\n", reads());

    println!(
        "  {:>6}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}  {:>8}",
        "ws/$", "cache", "p50", "p99", "p99.9", "GET/read", "hit%"
    );
    println!("  {}", "-".repeat(84));

    let mut curve: Vec<(f64, Point)> = Vec::new();

    for &ratio in RATIOS {
        let cache_bytes = (working_set as f64 / ratio) as usize;
        let point = measure(&cfg, cache_bytes, &hot).await;

        println!(
            "  {:>6.1}  {:>9} K  {:>7.1}ms  {:>7.1}ms  {:>7.1}ms  {:>9.2}  {:>7.1}%  {}",
            ratio,
            cache_bytes / 1024,
            ms(point.p50),
            ms(point.p99),
            ms(point.p999),
            point.gets_per_read,
            point.hit_rate * 100.0,
            if point.p99 <= OLTP_P99 { "" } else { "<- over 10ms" },
        );
        curve.push((ratio, point));
    }

    envelope(&curve, dist);
}

/// Report the operating envelope, and point at what to optimise past its edge.
///
/// Deliberately *not* a pass/fail. A verdict answers "does this work", which is
/// the wrong question — nobody has shipped OLTP over Iceberg, so of course the
/// naive build has a wall. The useful question is **where the wall is**, and
/// **what is holding it up**, because that is the thing an optimisation would
/// have to move.
fn envelope(curve: &[(f64, Point)], dist: WriteDist) {
    println!("\n  OLTP-adjacent is defined as p99 <= 10ms (PLAN.md, fixed before this ran).");

    let last_ok = curve
        .iter()
        .filter(|(_, p)| p.p99 <= OLTP_P99)
        .map(|(r, _)| *r)
        .fold(f64::MIN, f64::max);
    let first_bad = curve
        .iter()
        .filter(|(_, p)| p.p99 > OLTP_P99)
        .map(|(r, _)| *r)
        .fold(f64::MAX, f64::min);

    if last_ok == f64::MIN {
        println!("\n  ENVELOPE: empty. p99 is over 10ms even with the whole working set cached,");
        println!("  which means the floor itself is too slow — look at the read path, not the cache.");
        return;
    }
    if first_bad == f64::MAX {
        println!("\n  ENVELOPE: holds across the whole sweep, out to ws/cache = {last_ok:.0}.");
        println!("  Extend RATIOS — the wall is further out than we looked.");
        return;
    }

    println!("\n  ENVELOPE: OLTP-adjacent up to ws/cache = {last_ok:.0}");
    println!("            (cache >= {:.0}% of the working set). Breaks by {first_bad:.0}.", 100.0 / last_ok);

    // Which mechanism gave way at the edge? The two degrade differently, and
    // they want completely different fixes — so name the one that moved.
    let at_edge = curve.iter().find(|(r, _)| *r == first_bad).map(|(_, p)| p);
    if let Some(p) = at_edge {
        println!("\n  At the edge (ws/cache = {first_bad:.0}): {:.2} GET/read, {:.0}% hit rate.",
            p.gets_per_read, p.hit_rate * 100.0);

        println!("\n  WHERE TO OPTIMISE:");
        if p.gets_per_read > 8.0 {
            println!("  - GET/read is high. A point read is opening many files, so the interval");
            println!("    prune is not dismissing them — the expected result under random writes,");
            println!("    where every file's PK range spans the keyspace. This is what COMPACTION");
            println!("    fixes: merging files back into disjoint PK ranges restores the prune.");
            println!("    It is the single highest-value thing not yet built.");
        } else {
            println!("  - GET/read is low, so pruning is working and the read path is not the");
            println!("    problem. The cost is cache misses against S3's tail. Levers, in order:");
            println!("    a bigger cache (cheap, and the curve says exactly how much buys what),");
            println!("    S3 Express (HG_LATENCY=express — same sweep, ~9x lower tail), or");
            println!("    prefetching whole row groups on the assumption of locality.");
        }
        if dist == WriteDist::Random {
            println!("  - Re-run with --write-dist sequential to see the same curve with pruning");
            println!("    intact. The gap between the two arms is exactly what compaction buys.");
        }
    }
}

/// Write the table and flush it into files. Returns every key written.
async fn write_table(cfg: &Config, dist: WriteDist) -> Vec<Vec<u8>> {
    let engine = Engine::open(cfg.clone()).await.unwrap();
    let value = vec![b'v'; VALUE_BYTES];

    // Both distributions write the *same* keys, so the two arms are comparable.
    // Only the order changes — and order is what decides whether a flush's keys
    // form a contiguous PK range or a scattering across the whole keyspace.
    let mut order: Vec<u32> = (0..KEYSPACE).collect();
    if dist == WriteDist::Random {
        shuffle(&mut order);
    }

    eprint!("writing {KEYSPACE} keys");
    let mut keys = Vec::with_capacity(KEYSPACE as usize);
    for (i, &k) in order.iter().enumerate() {
        let key = key_of(k);
        engine.put(key.clone(), value.clone()).await.unwrap();
        keys.push(key);

        if (i as u32 + 1) % FLUSH_EVERY == 0 {
            engine.flush().await.unwrap();
            eprint!(".");
        }
    }
    eprintln!(" {} files", engine.file_count());
    keys
}

/// Measure the working set instead of computing it.
///
/// Run the read workload against a cache large enough never to evict, then ask
/// the cache how many bytes it is holding. Those bytes *are* the working set —
/// the footer, bloom and column-chunk ranges the hot keys actually touch.
async fn calibrate(cfg: &Config, keys: &[Vec<u8>]) -> (usize, Vec<Vec<u8>>) {
    let hot = pick_hot(keys);

    let mut cfg = cfg.clone();
    cfg.cache_bytes = 4 << 30;
    let engine = Engine::open(cfg).await.unwrap();

    for key in &hot {
        engine.get(key).await.unwrap();
    }

    let resident = engine.cache_stats().bytes_resident as usize;
    (resident, hot)
}

struct Point {
    p50: Duration,
    p99: Duration,
    p999: Duration,
    gets_per_read: f64,
    hit_rate: f64,
}

/// One sweep point: open at a given cache size, warm, then measure.
async fn measure(cfg: &Config, cache_bytes: usize, hot: &[Vec<u8>]) -> Point {
    let mut cfg = cfg.clone();
    cfg.cache_bytes = cache_bytes;
    let engine = Engine::open(cfg).await.unwrap();

    // Warm, so that what follows is steady-state behaviour and not a cold-start
    // transient. Where the cache is smaller than the working set this warming
    // achieves nothing — the reads thrash — which is precisely the effect being
    // measured, so it is left in rather than special-cased.
    for key in hot.iter().take(WORKING_SET.min(hot.len())) {
        engine.get(key).await.unwrap();
    }
    engine.reset_stats();

    let reads = reads();
    let mut latencies = Vec::with_capacity(reads);
    for i in 0..reads {
        // Uniform over the hot set. Deliberately not Zipfian: a Zipf
        // distribution has no well-defined "working set size", and the spec's
        // independent variable is a *ratio* against one. Skew would make the
        // headline number better and less meaningful.
        let key = &hot[i % hot.len()];

        let t = Instant::now();
        let got = engine.get(key).await.unwrap();
        latencies.push(t.elapsed());

        assert!(got.is_some(), "benchmark read missed a key that was written");
    }

    latencies.sort_unstable();
    let store = engine.store_stats();
    let cache = engine.cache_stats();

    Point {
        p50: pct(&latencies, 0.50),
        p99: pct(&latencies, 0.99),
        p999: pct(&latencies, 0.999),
        gets_per_read: store.gets as f64 / reads as f64,
        hit_rate: cache.hits as f64 / (cache.hits + cache.misses).max(1) as f64,
    }
}

/// The hot set: WORKING_SET keys drawn from across the keyspace.
fn pick_hot(keys: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut seen = HashSet::new();
    let mut hot = Vec::with_capacity(WORKING_SET);
    while hot.len() < WORKING_SET {
        let i = rand::random_range(0..keys.len());
        if seen.insert(i) {
            hot.push(keys[i].clone());
        }
    }
    hot
}

/// Zero-padded so that lexicographic byte order — which is what Parquet sorts on
/// and what the PK interval map compares — matches numeric order.
fn key_of(k: u32) -> Vec<u8> {
    format!("key{k:08}").into_bytes()
}

fn shuffle(v: &mut [u32]) {
    for i in (1..v.len()).rev() {
        v.swap(i, rand::random_range(0..=i));
    }
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    let i = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[i]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
