//! The table's column model, and its Arrow mirror.
//!
//! One row per mutation, sorted by `pk`. `value` is nullable because a tombstone
//! has no value — the row's existence and its `op` are the delete.
//!
//! `lsn` is carried per row even though point reads never need it to resolve
//! versions: file ordering by watermark already does that (see PLAN.md). It is
//! here because it is nearly free, it makes a flushed file self-describing, and
//! any later work that merges across files — compaction, MVCC — needs it. Under
//! DuckLake it earns a second keep: the watermark is *derived* from the `lsn`
//! column's max stat, which DuckDB records per file (see `index.rs`).
//!
//! Field IDs are the `column_id`s DuckLake assigns in `ducklake_column`. They are
//! fixed and must never be reused. Arrow carries them in field metadata under
//! `PARQUET:field_id` so the staging Parquet is self-describing; the numbers must
//! agree with what DuckDB put in the catalog, or reads bind the wrong column and
//! come back all-nulls. `catalog.rs` asserts the agreement at startup.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

/// Column IDs, matching the `column_id`s DuckLake assigns for `kv`
/// (`pk`=1, `lsn`=2, `op`=3, `value`=4 — verified against `ducklake_column`).
pub const FIELD_ID_PK: i32 = 1;
pub const FIELD_ID_LSN: i32 = 2;
pub const FIELD_ID_OP: i32 = 3;
pub const FIELD_ID_VALUE: i32 = 4;

/// The `(column_id, name, ducklake_type)` triples the engine expects, in column
/// order. `catalog.rs` checks these against `ducklake_column`. Nullability is
/// intentionally not checked: DuckLake declares every column nullable, while the
/// engine only ever writes non-null `pk`/`lsn`/`op`, so a mismatch there is
/// harmless where a type or id mismatch is not.
pub const EXPECTED_COLUMNS: &[(i32, &str, &str)] = &[
    (FIELD_ID_PK, "pk", "blob"),
    (FIELD_ID_LSN, "lsn", "int64"),
    (FIELD_ID_OP, "op", "int32"),
    (FIELD_ID_VALUE, "value", "blob"),
];

/// The Arrow schema the staging Parquet is written with, with field IDs attached
/// so Parquet carries them.
pub fn arrow_schema() -> Arc<ArrowSchema> {
    let field = |id: i32, name: &str, ty: DataType, nullable: bool| {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            id.to_string(),
        )]))
    };

    // LargeBinary, not Binary. This was once forced by iceberg's Binary→arrow
    // mapping; now the engine writes and reads the staging file itself, so it is
    // simply a consistent choice. It still matters: the read path downcasts to
    // LargeBinaryArray, so the write side must agree, or the downcast panics at
    // runtime rather than failing to compile.
    Arc::new(ArrowSchema::new(vec![
        field(FIELD_ID_PK, "pk", DataType::LargeBinary, false),
        field(FIELD_ID_LSN, "lsn", DataType::Int64, false),
        field(FIELD_ID_OP, "op", DataType::Int32, false),
        field(FIELD_ID_VALUE, "value", DataType::LargeBinary, true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_field_ids_match_expected_columns() {
        let arrow = arrow_schema();
        assert_eq!(arrow.fields().len(), EXPECTED_COLUMNS.len());

        for (f, (id, name, _ty)) in arrow.fields().iter().zip(EXPECTED_COLUMNS) {
            let got: i32 = f.metadata()["PARQUET:field_id"].parse().unwrap();
            assert_eq!(got, *id, "field id for {name}");
            assert_eq!(f.name(), name);
        }
    }

    #[test]
    fn pk_is_the_first_column() {
        // The read path hard-codes PK at column 0 (`read::PK_COL`); the staging
        // schema must keep it there.
        assert_eq!(EXPECTED_COLUMNS[0].0, FIELD_ID_PK);
        assert_eq!(arrow_schema().field(0).name(), "pk");
    }
}
