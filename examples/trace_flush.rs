//! Counts the object-store requests one flush actually makes.
//!
//! `flush.rs` charges iceberg's opendal I/O by hand, with a hardcoded guess of
//! three round trips per commit. This probe replaces the guess with a count.
//!
//! Phases are separated by pauses so a concurrent `mc admin trace` can attribute
//! each request to the phase that caused it. Run with:
//!
//!     cargo run --example trace_flush

use holy_grail::config::Config;
use holy_grail::Engine;
use std::time::Duration;

const GAP: Duration = Duration::from_secs(4);

#[tokio::main]
async fn main() {
    let mut cfg = Config::from_env();
    cfg.catalog.table = format!("t_trace_{}", std::process::id());
    cfg.wal_dir = std::env::temp_dir().join(format!("trace-wal-{}", std::process::id()));
    std::fs::create_dir_all(&cfg.wal_dir).unwrap();
    cfg.memtable_max_bytes = 1 << 30;

    eprintln!("--- phase: idle (baseline) ---");
    tokio::time::sleep(GAP).await;

    eprintln!("--- phase: open (table create + index load) ---");
    let engine = Engine::open(cfg).await.unwrap();
    tokio::time::sleep(GAP).await;

    eprintln!("--- phase: puts (WAL only, no object store expected) ---");
    for i in 0..100u32 {
        engine
            .put(
                format!("key{i:04}").into_bytes(),
                format!("value-{i}").into_bytes(),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(GAP).await;

    eprintln!("--- phase: FLUSH (parquet write + iceberg commit) ---");
    engine.flush().await.unwrap();
    tokio::time::sleep(GAP).await;

    eprintln!("--- phase: point read (cold cache) ---");
    engine.get(b"key0050").await.unwrap();
    tokio::time::sleep(GAP).await;

    eprintln!("--- done ---");
}
