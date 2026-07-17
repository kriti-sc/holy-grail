//! Iceberg REST catalog: connect, and create the table if it isn't there.
//!
//! The catalog is the only thing that knows where the columnar level ends. Every
//! flush commits here with a watermark, and every recovery reads that watermark
//! back. Nothing else is authoritative.

use std::collections::HashMap;

use iceberg::spec::{DataFileFormat, TableProperties};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};

use crate::config::{CatalogConfig, S3Config};
use crate::error::Result;
use crate::schema;

pub async fn connect(cfg: &CatalogConfig, s3: &S3Config) -> Result<RestCatalog> {
    // The catalog's own FileIO reads and writes manifests and metadata directly,
    // so it needs MinIO credentials of its own. Note that this path does *not*
    // go through the latency shim — it is inside the iceberg crate. Manifest and
    // metadata I/O on the read path will therefore look faster than it should;
    // step 6 has to route those through our object store or account for it.
    let props = HashMap::from([
        ("uri".to_string(), cfg.uri.clone()),
        ("warehouse".to_string(), cfg.warehouse.clone()),
        ("s3.endpoint".to_string(), s3.endpoint.clone()),
        ("s3.access-key-id".to_string(), s3.access_key.clone()),
        ("s3.secret-access-key".to_string(), s3.secret_key.clone()),
        ("s3.region".to_string(), s3.region.clone()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ]);

    let catalog = RestCatalogBuilder::default()
        .load("holy-grail", props)
        .await?;

    Ok(catalog)
}

pub fn table_ident(cfg: &CatalogConfig) -> TableIdent {
    TableIdent::new(NamespaceIdent::new(cfg.namespace.clone()), cfg.table.clone())
}

/// Create the namespace and table if absent; otherwise load what is there.
///
/// Idempotent, so it is safe to run on every start. Recovery depends on being
/// able to load the table and read its watermark, not on having created it.
pub async fn ensure_table(catalog: &RestCatalog, cfg: &CatalogConfig) -> Result<Table> {
    let ns = NamespaceIdent::new(cfg.namespace.clone());
    if !catalog.namespace_exists(&ns).await? {
        catalog.create_namespace(&ns, HashMap::new()).await?;
    }

    let ident = table_ident(cfg);
    if catalog.table_exists(&ident).await? {
        return Ok(catalog.load_table(&ident).await?);
    }

    let schema = schema::iceberg_schema()?;
    let sort_order = schema::sort_order(&schema)?;

    let creation = TableCreation::builder()
        .name(cfg.table.clone())
        .schema(schema)
        .sort_order(sort_order)
        .properties(HashMap::from([
            (
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
                DataFileFormat::Parquet.to_string(),
            ),
            // Unpartitioned on purpose. Partitioning buys nothing for point
            // reads — pruning is done by the PK interval map over file and
            // row-group statistics, not by partition values.
        ]))
        .build();

    Ok(catalog.create_table(&ns, creation).await?)
}
