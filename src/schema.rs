//! The Iceberg table schema, and its Arrow mirror.
//!
//! One row per mutation, sorted by `pk`. `value` is nullable because a tombstone
//! has no value — the row's existence and its `op` are the delete.
//!
//! `lsn` is carried per row even though point reads never need it to resolve
//! versions: file ordering by watermark already does that (see PLAN.md). It is
//! here because it is nearly free, it makes a flushed file self-describing, and
//! any later work that merges across files — compaction, MVCC — needs it.
//!
//! Field IDs are fixed and must never be reused. Arrow carries them in field
//! metadata under `PARQUET:field_id`, which is how the Parquet writer stamps
//! them into the file so Iceberg can bind columns by ID rather than by name.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::spec::{
    NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder, Transform,
    Type,
};

use crate::error::Result;

pub const FIELD_ID_PK: i32 = 1;
pub const FIELD_ID_LSN: i32 = 2;
pub const FIELD_ID_OP: i32 = 3;
pub const FIELD_ID_VALUE: i32 = 4;

/// Key under which the watermark LSN is stamped into each Iceberg snapshot's
/// summary. This is the load-bearing piece of the whole design: it is what tells
/// a recovering node where the columnar level ends and the WAL suffix begins.
pub const WATERMARK_PROP: &str = "holy-grail.watermark-lsn";

pub fn iceberg_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![FIELD_ID_PK])
        .with_fields(vec![
            NestedField::required(FIELD_ID_PK, "pk", Type::Primitive(PrimitiveType::Binary))
                .into(),
            NestedField::required(FIELD_ID_LSN, "lsn", Type::Primitive(PrimitiveType::Long))
                .into(),
            NestedField::required(FIELD_ID_OP, "op", Type::Primitive(PrimitiveType::Int))
                .with_doc("0 = put, 1 = delete (tombstone)")
                .into(),
            NestedField::optional(
                FIELD_ID_VALUE,
                "value",
                Type::Primitive(PrimitiveType::Binary),
            )
            .with_doc("null for tombstones")
            .into(),
        ])
        .build()?;

    Ok(schema)
}

/// Sort by `pk` ascending. This is what makes PK min/max row-group statistics
/// tight enough for the interval pruning in the read path to be worth anything —
/// unsorted files have overlapping row groups and every one of them has to be
/// opened.
pub fn sort_order(schema: &Schema) -> Result<SortOrder> {
    let order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(FIELD_ID_PK)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        )
        .build(schema)?;

    Ok(order)
}

/// The Arrow schema the flush path writes, with field IDs attached so Parquet
/// carries them.
pub fn arrow_schema() -> Arc<ArrowSchema> {
    let field = |id: i32, name: &str, ty: DataType, nullable: bool| {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            id.to_string(),
        )]))
    };

    // LargeBinary, not Binary: iceberg maps its `Binary` primitive to arrow's
    // LargeBinary, and that mapping is what ends up in the Parquet file's
    // embedded arrow schema. Writing `Binary` here still produces a valid file —
    // the physical type is BYTE_ARRAY either way — but every reader then hands
    // back a LargeBinaryArray while this schema claims Binary, and the downcast
    // fails at runtime rather than at compile time.
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
    fn schema_builds_with_pk_as_the_identifier() {
        let schema = iceberg_schema().unwrap();
        assert_eq!(
            schema.identifier_field_ids().collect::<Vec<_>>(),
            vec![FIELD_ID_PK]
        );
        assert!(schema.field_by_id(FIELD_ID_VALUE).unwrap().required == false);
    }

    #[test]
    fn sort_order_is_pk_ascending() {
        let schema = iceberg_schema().unwrap();
        let order = sort_order(&schema).unwrap();
        assert_eq!(order.fields.len(), 1);
        assert_eq!(order.fields[0].source_id, FIELD_ID_PK);
        assert_eq!(order.fields[0].direction, SortDirection::Ascending);
    }

    #[test]
    fn arrow_mirrors_iceberg_field_ids() {
        let arrow = arrow_schema();
        let iceberg = iceberg_schema().unwrap();
        assert_eq!(arrow.fields().len(), iceberg.as_struct().fields().len());

        for f in arrow.fields() {
            let id: i32 = f.metadata()["PARQUET:field_id"].parse().unwrap();
            let ice = iceberg.field_by_id(id).expect("field id must exist");
            assert_eq!(ice.name, *f.name());
            assert_eq!(ice.required, !f.is_nullable());
        }
    }
}
