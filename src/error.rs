use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("corrupt wal record in {segment} at offset {offset}: {reason}")]
    CorruptWal {
        segment: String,
        offset: u64,
        reason: &'static str,
    },

    #[error("write stalled: {0} frozen memtables pending flush")]
    WriteStalled(usize),

    #[error("config: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;
