//! Step 5: the crash harness.
//!
//! The flush protocol has four windows a process death can land in. The claim is
//! that every one of them either loses something reconstructible or duplicates
//! something the watermark makes idempotent — and that **no acknowledged write is
//! ever lost**. That claim is what licenses calling the row tier disposable, so
//! it gets tested rather than asserted.
//!
//! Each test writes, crashes at one window, throws away every scrap of in-memory
//! state, reopens from `Iceberg + WAL`, and checks the data is all there.
//!
//!     cargo test --test crash -- --ignored --nocapture

mod common;

use common::Fixture;
use holy_grail::{CrashAt, Engine, Flushed};

const N: u32 = 50;

async fn write_batch(engine: &Engine) {
    for i in 0..N {
        engine
            .put(format!("key{i:04}").into_bytes(), format!("value-{i}").into_bytes())
            .await
            .unwrap();
    }
}

async fn assert_all_present(engine: &Engine, context: &str) {
    for i in 0..N {
        let got = engine.get(format!("key{i:04}").as_bytes()).await.unwrap();
        assert_eq!(
            got.as_deref(),
            Some(format!("value-{i}").as_bytes()),
            "{context}: key{i:04} was lost"
        );
    }
}

/// Crash after freezing, before any Parquet is written.
///
/// The frozen memtable is gone. Its records are still in the WAL above the
/// watermark, so replay rebuilds them.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn crash_before_write_loses_nothing() {
    let fx = Fixture::new("crash_before_write");

    let engine = fx.open().await;
    write_batch(&engine).await;
    engine.flush_inner(Some(CrashAt::BeforeWrite)).await.unwrap();
    drop(engine);

    let engine = fx.reopen().await;
    assert_eq!(engine.watermark(), 0, "nothing was ever published");
    assert_all_present(&engine, "after crash before write").await;

    // And the retried flush still works.
    let flushed = engine.flush().await.unwrap();
    assert!(matches!(flushed, Flushed::Committed { .. }));
    assert_all_present(&engine, "after the retried flush").await;
}

/// Crash after the Parquet file is uploaded, before the Iceberg commit.
///
/// The bucket holds an orphan object — unreferenced garbage, not corruption.
/// Nothing was published, so recovery replays the WAL and the retry rewrites the
/// file. Because the file name is derived from the watermark, the retry
/// *overwrites* the orphan instead of leaking a second one.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn crash_after_write_leaves_an_orphan_and_retries_cleanly() {
    let fx = Fixture::new("crash_after_write");

    let engine = fx.open().await;
    write_batch(&engine).await;
    engine.flush_inner(Some(CrashAt::AfterWrite)).await.unwrap();
    drop(engine);

    let engine = fx.reopen().await;
    assert_eq!(engine.watermark(), 0, "the commit never landed");
    assert_eq!(engine.file_count(), 0, "the orphan is referenced by nothing");
    assert_all_present(&engine, "after crash after write").await;

    let flushed = engine.flush().await.unwrap();
    assert!(matches!(flushed, Flushed::Committed { files: 1, .. }));
    assert_eq!(engine.file_count(), 1, "the retry did not leak a second file");
    assert_all_present(&engine, "after the retried flush").await;
}

/// Crash after the commit, before the WAL is truncated. The interesting one.
///
/// The records are now in *both* tiers. Recovery must not double-count them and
/// must not re-commit them: replay skips everything at or below the published
/// watermark, and the retried flush sees its watermark is already published and
/// declines to write a second snapshot.
///
/// This is exactly the case where a naive re-publish would silently produce
/// duplicate rows: DuckDB's INSERT is not idempotent on its own, so the watermark
/// check — not the write mechanism — is what makes the retry safe.
#[tokio::test]
#[ignore = "requires the DuckLake catalog, MinIO, and the forked duckdb binary"]
async fn crash_after_commit_does_not_duplicate_on_recovery() {
    let fx = Fixture::new("crash_after_commit");

    let engine = fx.open().await;
    write_batch(&engine).await;
    let flushed = engine.flush_inner(Some(CrashAt::AfterCommit)).await.unwrap();
    assert!(matches!(flushed, Flushed::Committed { .. }));
    drop(engine);

    let engine = fx.reopen().await;
    assert_eq!(engine.watermark(), N as u64, "the commit did land");
    assert_eq!(engine.file_count(), 1);
    assert_all_present(&engine, "after crash after commit").await;

    // The WAL still holds the published records. A flush now must recognise them
    // as already published and commit nothing.
    let flushed = engine.flush().await.unwrap();
    assert!(
        matches!(flushed, Flushed::Empty | Flushed::AlreadyPublished { .. }),
        "recovery re-flushed already-published records: {flushed:?}"
    );
    assert_eq!(
        engine.file_count(),
        1,
        "a second file means the same rows were written twice"
    );
    assert_all_present(&engine, "after the idempotent retry").await;
}

/// Crash after truncation, before the memtable is retired. Nothing is at risk:
/// the memtable is a cache of what is already durable in the columnar level.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn crash_after_truncate_loses_nothing() {
    let fx = Fixture::new("crash_after_truncate");

    let engine = fx.open().await;
    write_batch(&engine).await;
    engine
        .flush_inner(Some(CrashAt::AfterTruncate))
        .await
        .unwrap();
    drop(engine);

    let engine = fx.reopen().await;
    assert_eq!(engine.watermark(), N as u64);
    assert_eq!(engine.file_count(), 1);
    assert_all_present(&engine, "after crash after truncate").await;
}

/// Writes that arrive *after* a published flush must survive a crash too — they
/// live only in the WAL suffix, which is the one piece of local state that is
/// not rebuildable from Iceberg.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn the_wal_suffix_above_the_watermark_survives_a_restart() {
    let fx = Fixture::new("suffix");

    let engine = fx.open().await;
    write_batch(&engine).await;
    engine.flush().await.unwrap();

    // Past the watermark: in the WAL, not in Iceberg.
    engine.put(&b"late-1"[..], &b"a"[..]).await.unwrap();
    engine.delete(&b"key0007"[..]).await.unwrap();
    drop(engine);

    let engine = fx.reopen().await;

    assert_eq!(engine.watermark(), N as u64);
    assert_eq!(
        engine.get(b"late-1").await.unwrap().as_deref(),
        Some(&b"a"[..]),
        "an acknowledged write above the watermark was lost"
    );
    assert_eq!(
        engine.get(b"key0007").await.unwrap(),
        None,
        "a delete above the watermark was lost, resurrecting the value"
    );
    assert_eq!(
        engine.get(b"key0008").await.unwrap().as_deref(),
        Some(&b"value-8"[..]),
        "the flushed data is still readable"
    );
}
