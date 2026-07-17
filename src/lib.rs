//! A key-value engine whose durable bottom level is an Iceberg table on object
//! storage.
//!
//! The row tier — memtable, WAL, cache — is a disposable acceleration layer over
//! that table. `Iceberg + WAL suffix` is the complete source of truth; every
//! other piece of local state can be thrown away and rebuilt.
//!
//! See PLAN.md for the build order and the two claims this prototype exists to
//! turn into numbers.

pub mod cache;
pub mod catalog;
pub mod config;
pub mod engine;
pub mod error;
pub mod flush;
pub mod index;
pub mod memtable;
pub mod read;
pub mod record;
pub mod schema;
pub mod store;
pub mod wal;

pub use config::Config;
pub use engine::{CrashAt, Engine};
pub use error::{Error, Result};
pub use flush::Flushed;
pub use memtable::{Lookup, Memtable, MemtableSet};
pub use record::{Lsn, Op, Record};
