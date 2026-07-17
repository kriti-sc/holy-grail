//! Steps 4 and 6 end to end: flush to Iceberg, then read back through the
//! columnar level.
//!
//! Requires `docker compose up -d`. Run with:
//!
//!     cargo test --test engine -- --ignored --nocapture

mod common;

use common::Fixture;
use holy_grail::Flushed;

/// After a flush the memtable is retired, so every read here is a genuine
/// columnar read — manifest prune, bloom filter, row group, the lot.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn a_flushed_value_reads_back_from_the_columnar_level() {
    let fx = Fixture::new("flush_read");
    let engine = fx.open().await;

    for i in 0..100u32 {
        engine
            .put(format!("key{i:04}").into_bytes(), format!("value-{i}").into_bytes())
            .await
            .unwrap();
    }

    let flushed = engine.flush().await.unwrap();
    assert!(matches!(flushed, Flushed::Committed { files: 1, .. }));
    assert_eq!(engine.watermark(), 100, "the snapshot carries the watermark");

    // Nothing is in the memtable now. These reads go to Parquet on MinIO.
    for i in 0..100u32 {
        let got = engine.get(format!("key{i:04}").as_bytes()).await.unwrap();
        assert_eq!(
            got.as_deref(),
            Some(format!("value-{i}").as_bytes()),
            "key{i:04} did not survive the flush"
        );
    }

    assert_eq!(engine.get(b"nonexistent").await.unwrap(), None);
}

/// The resurrection test, and the one that matters most.
///
/// A key is written, flushed, then deleted and flushed again. The old value is
/// still sitting in the first Parquet file — there is no compaction, so it will
/// be there forever. If the tombstone in the newer file is not honoured, or if
/// the delete were implemented as a removal rather than a tombstone, the read
/// falls through and the deleted value comes back from the dead.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn a_tombstone_in_a_newer_file_beats_a_value_in_an_older_one() {
    let fx = Fixture::new("tombstone");
    let engine = fx.open().await;

    engine.put(&b"doomed"[..], &b"still-here"[..]).await.unwrap();
    engine.put(&b"bystander"[..], &b"untouched"[..]).await.unwrap();
    engine.flush().await.unwrap();

    assert_eq!(
        engine.get(b"doomed").await.unwrap().as_deref(),
        Some(&b"still-here"[..])
    );

    engine.delete(&b"doomed"[..]).await.unwrap();
    engine.flush().await.unwrap();

    assert_eq!(engine.file_count(), 2, "the old value's file is still there");
    assert_eq!(
        engine.get(b"doomed").await.unwrap(),
        None,
        "the deleted value was resurrected from the older Parquet file"
    );
    assert_eq!(
        engine.get(b"bystander").await.unwrap().as_deref(),
        Some(&b"untouched"[..]),
        "the delete took an innocent key with it"
    );
}

/// Overlapping files, newest watermark wins. No merge, no per-row LSN.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn a_newer_file_shadows_an_older_one() {
    let fx = Fixture::new("shadow");
    let engine = fx.open().await;

    engine.put(&b"k"[..], &b"v1"[..]).await.unwrap();
    engine.flush().await.unwrap();

    engine.put(&b"k"[..], &b"v2"[..]).await.unwrap();
    engine.flush().await.unwrap();

    engine.put(&b"k"[..], &b"v3"[..]).await.unwrap();
    engine.flush().await.unwrap();

    assert_eq!(engine.file_count(), 3);
    assert_eq!(
        engine.get(b"k").await.unwrap().as_deref(),
        Some(&b"v3"[..]),
        "a stale file won"
    );
}

/// The memtable's answer is final, including when it is a delete — a read must
/// not fall through to the columnar level and find the old value.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn an_unflushed_delete_masks_a_flushed_value() {
    let fx = Fixture::new("mask");
    let engine = fx.open().await;

    engine.put(&b"k"[..], &b"flushed"[..]).await.unwrap();
    engine.flush().await.unwrap();

    engine.delete(&b"k"[..]).await.unwrap();

    assert_eq!(
        engine.get(b"k").await.unwrap(),
        None,
        "the read fell through the tombstone to the columnar level"
    );
}

/// A repeated read must not go back to object storage.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn a_repeated_read_is_served_from_cache() {
    let fx = Fixture::new("cache");
    let engine = fx.open().await;

    for i in 0..500u32 {
        engine
            .put(format!("key{i:04}").into_bytes(), vec![b'x'; 64])
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    // Warm: footer, bloom filter, row group.
    engine.get(b"key0250").await.unwrap().unwrap();
    let warm = engine.store_stats().gets;

    for _ in 0..20 {
        engine.get(b"key0250").await.unwrap().unwrap();
    }

    assert_eq!(
        engine.store_stats().gets,
        warm,
        "a cached read still went to object storage"
    );
    assert!(engine.cache_stats().hits > 0);
}

/// Pruning has to actually prune. With no compaction the file count only grows,
/// so a read that opens every file would make the whole design unviable.
#[tokio::test]
#[ignore = "requires docker compose up -d"]
async fn interval_pruning_skips_files_that_cannot_hold_the_key() {
    let fx = Fixture::new("prune");
    let engine = fx.open().await;

    // Five files with disjoint key ranges.
    for file in 0..5u32 {
        for i in 0..20u32 {
            let key = format!("f{file}-k{i:03}");
            engine.put(key.into_bytes(), vec![b'v'; 32]).await.unwrap();
        }
        engine.flush().await.unwrap();
    }
    assert_eq!(engine.file_count(), 5);

    // A key outside every file's PK range must cost *nothing*. The interval map
    // lives in memory, built from manifest bounds, so five files are dismissed
    // without a single byte leaving MinIO. This is the property that keeps the
    // read path viable as files accumulate.
    let before = engine.store_stats().gets;
    assert_eq!(engine.get(b"zzz-no-such-key").await.unwrap(), None);
    assert_eq!(
        engine.store_stats().gets - before,
        0,
        "an unprunable miss went to object storage"
    );

    // A hit costs one file's worth of reads — footer, bloom filter, and the
    // column chunks of the single surviving row group — not five files' worth.
    let before = engine.store_stats().gets;
    assert!(engine.get(b"f3-k010").await.unwrap().is_some());
    let hit_cost = engine.store_stats().gets - before;

    let before = engine.store_stats().gets;
    assert!(engine.get(b"f0-k005").await.unwrap().is_some());
    let other_file_cost = engine.store_stats().gets - before;

    assert_eq!(
        hit_cost, other_file_cost,
        "read cost depends on which file the key lives in; pruning is not working"
    );
    assert!(
        hit_cost <= 8,
        "a point read cost {hit_cost} range fetches — more than one file's \
         footer, bloom and row group"
    );
}
