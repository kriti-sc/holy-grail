//! Counts the object-store requests one flush actually makes.
//!
//! Under DuckLake there is no metadata I/O to the object store — snapshots, file
//! lists and stats are Postgres rows. So a publish should make exactly **one**
//! object-store write (the data file, by DuckDB) and land its catalog rows in
//! Postgres. This probe is the evidence for that claim: run a concurrent
//! `mc admin trace` on MinIO and confirm the FLUSH phase makes one PUT.
//!
//! Phases are separated by pauses so the trace can attribute each request to the
//! phase that caused it. Run with:
//!
//!     cargo run --example trace_flush

use holy_grail::config::Config;
use holy_grail::{catalog, Engine};
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

    eprintln!("--- phase: bootstrap (create table via forked duckdb) ---");
    catalog::bootstrap(&cfg.duckdb, &cfg.catalog, &cfg.s3)
        .await
        .unwrap();
    tokio::time::sleep(GAP).await;

    eprintln!("--- phase: open (catalog resolve + index load, no object store) ---");
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

    eprintln!("--- phase: FLUSH (stage local + DuckDB publish: expect 1 PUT) ---");
    engine.flush().await.unwrap();
    tokio::time::sleep(GAP).await;

    eprintln!("--- phase: point read (cold cache) ---");
    engine.get(b"key0050").await.unwrap();
    tokio::time::sleep(GAP).await;

    eprintln!("--- done ---");
}
