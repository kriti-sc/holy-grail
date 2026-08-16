//! Shared setup for the integration tests. Requires the DuckLake catalog
//! (Postgres), MinIO, and the forked DuckDB binary to be up.

use holy_grail::catalog;
use holy_grail::config::Config;
use holy_grail::Engine;
use tempfile::TempDir;

/// A config pointed at its own DuckLake table and its own WAL directory, so tests
/// cannot see each other's state.
///
/// The WAL directory is *kept* across a simulated restart within one test — that
/// is the whole point of the crash tests — but a fresh one per test.
pub struct Fixture {
    pub cfg: Config,
    _wal_dir: TempDir,
}

impl Fixture {
    pub fn new(table: &str) -> Self {
        let wal_dir = TempDir::new().unwrap();

        let mut cfg = Config::from_env();
        cfg.catalog.table = format!("t_{table}_{}", std::process::id());
        cfg.wal_dir = wal_dir.path().to_path_buf();

        // Small enough that tests can trigger a flush without writing 64 MiB,
        // but only when they ask for it — every test here flushes explicitly.
        cfg.memtable_max_bytes = 1 << 30;
        cfg.max_frozen_memtables = 4;
        cfg.cache_bytes = 8 << 20;

        Fixture {
            cfg,
            _wal_dir: wal_dir,
        }
    }

    pub async fn open(&self) -> Engine {
        // The engine is a read-only catalog client, so the table must exist
        // before it opens. Bootstrap is idempotent, so running it on every open
        // (including reopen) is safe and mirrors what a real deployment does.
        catalog::bootstrap(&self.cfg.duckdb, &self.cfg.catalog, &self.cfg.s3)
            .await
            .unwrap();
        Engine::open(self.cfg.clone()).await.unwrap()
    }

    /// Reopen from scratch: the same WAL directory and the same DuckLake table,
    /// but every scrap of in-memory state thrown away. This is what "the row tier
    /// is rebuildable" has to mean in practice.
    pub async fn reopen(&self) -> Engine {
        self.open().await
    }
}
