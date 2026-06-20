use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};

use kokedb_data_source::formats::remote_table::{PostgreSQLConfig, PostgreSQLTableProvider};
use log::error;
use sqlx::types::chrono;
use sqlx::{PgPool, Row};

use crate::catalog_list::{CatalogInfo, PostgreSQLMetaCatalogProviderList};

#[derive(Debug)]
pub struct PostgreSQLCatalogProvider {
    catalog_info: CatalogInfo,
    remote_pool: PgPool,
    /// Shared meta-store connection pool (reused on hot paths).
    meta_pool: PgPool,
    schema_cache: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl PostgreSQLCatalogProvider {
    pub fn new(catalog_info: CatalogInfo, remote_pool: PgPool, meta_pool: PgPool) -> Self {
        Self {
            catalog_info,
            remote_pool,
            meta_pool,
            schema_cache: DashMap::new(),
        }
    }

    async fn get_schema_names(&self) -> Result<Vec<String>> {
        let query = "SELECT schema_name FROM information_schema.schemata 
                     WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')";

        let rows = sqlx::query(query)
            .fetch_all(&self.remote_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let schema_names = rows
            .into_iter()
            .map(|row| row.get::<String, _>("schema_name"))
            .collect();

        Ok(schema_names)
    }
}

impl CatalogProvider for PostgreSQLCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Ok(names) = self.get_schema_names().await {
                    names
                } else {
                    Vec::new()
                }
            })
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(schema) = self.schema_cache.get(name) {
            return Some(Arc::clone(&schema));
        }

        let provider: Arc<dyn SchemaProvider> = Arc::new(PostgreSQLSchemaProvider::new(
            self.catalog_info.clone(),
            name.to_string(),
            self.remote_pool.clone(),
            self.meta_pool.clone(),
        ));

        self.schema_cache
            .insert(name.to_string(), Arc::clone(&provider));

        Some(provider)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schema_cache.insert(name.to_string(), schema))
    }

    fn deregister_schema(
        &self,
        name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schema_cache.remove(name).map(|(_, v)| v))
    }
}

#[derive(Debug)]
pub struct PostgreSQLSchemaProvider {
    catalog_info: CatalogInfo,
    schema_name: String,
    remote_pool: PgPool,
    /// Shared meta-store connection pool (reused on hot paths).
    meta_pool: PgPool,
    /// Cached listing tables, versioned by snapshot `local_path` so a re-synced
    /// table is rebuilt only when its snapshot actually changes.
    table_cache: DashMap<String, (String, Arc<dyn TableProvider>)>,
}

impl PostgreSQLSchemaProvider {
    fn new(
        catalog_info: CatalogInfo,
        schema_name: String,
        remote_pool: PgPool,
        meta_pool: PgPool,
    ) -> Self {
        Self {
            catalog_info,
            schema_name,
            remote_pool,
            meta_pool,
            table_cache: DashMap::new(),
        }
    }

    async fn get_table_names(&self) -> Result<Vec<String>> {
        let query = "SELECT table_name FROM information_schema.tables 
                     WHERE table_schema = $1 AND table_type = 'BASE TABLE'";

        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.remote_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let table_names = rows
            .into_iter()
            .map(|row| row.get::<String, _>("table_name"))
            .collect();

        Ok(table_names)
    }

    async fn build_listing_table(
        schema: Arc<datafusion::arrow::datatypes::Schema>,
        table_path: &str,
        partition_col: Option<&str>,
    ) -> Result<Arc<dyn TableProvider>> {
        let file_format: Arc<dyn datafusion::datasource::file_format::FileFormat> =
            Arc::new(ParquetFormat::default());
        let mut listing_options = ListingOptions::new(file_format);
        let table_url = ListingTableUrl::parse(table_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let config = match partition_col {
            // Hive-partitioned: the partition column is in the path, not the
            // files. Declare it as a partition column and infer the file schema
            // from the actual parquet files so the two don't collide.
            Some(col) => {
                let field = schema
                    .field_with_name(col)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                listing_options = listing_options
                    .with_table_partition_cols(vec![(col.to_string(), field.data_type().clone())]);
                let ctx = datafusion::prelude::SessionContext::new();
                ListingTableConfig::new(table_url)
                    .with_listing_options(listing_options)
                    .infer_schema(&ctx.state())
                    .await?
            }
            None => ListingTableConfig::new(table_url)
                .with_listing_options(listing_options)
                .with_schema(schema),
        };
        let listing_table = ListingTable::try_new(config)?;
        Ok(Arc::new(listing_table))
    }
}

#[async_trait]
impl SchemaProvider for PostgreSQLSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Ok(names) = self.get_table_names().await {
                    names
                } else {
                    Vec::new()
                }
            })
        })
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let catalog = self.catalog_info.name.clone();
        let schema = self.schema_name.clone();
        let dsn = self.catalog_info.dsn.clone();

        // Reuse the shared meta pool instead of opening a new connection.
        let meta_client = PostgreSQLMetaCatalogProviderList::from_pool(self.meta_pool.clone());

        // Record daily query stats off the hot path (fire-and-forget).
        {
            let meta_pool = self.meta_pool.clone();
            let (c, s, n) = (catalog.clone(), schema.clone(), name.to_string());
            tokio::spawn(async move {
                let today = chrono::Local::now().naive_local().date();
                let client = PostgreSQLMetaCatalogProviderList::from_pool(meta_pool);
                if let Err(e) = client.save_table_daily_stats(&c, &s, &n, today).await {
                    error!("Failed to store table daily stats: {:?}", e);
                }
            });
        }

        // One meta lookup yields both "is this table cached?" and the current
        // snapshot path. An empty path means it is not cached -> read remote.
        let (arrow_schema, local_path) =
            meta_client.get_table_schema(&catalog, &schema, name).await?;

        if local_path.is_empty() {
            let config = PostgreSQLConfig {
                connection_string: dsn,
                table_name: name.to_string(),
                schema_name: Some(schema),
            };
            let remote_table = PostgreSQLTableProvider::new(config).await?;
            return Ok(Some(Arc::new(remote_table)));
        }

        // Reuse the cached listing table if it points at the current snapshot;
        // otherwise rebuild (and re-list parquet files) only because it changed.
        if let Some(entry) = self.table_cache.get(name) {
            if entry.0 == local_path {
                return Ok(Some(entry.1.clone()));
            }
        }
        let partition_col = meta_client
            .get_table_partition_column(&catalog, &schema, name)
            .await
            .ok()
            .flatten();
        let table =
            Self::build_listing_table(arrow_schema, &local_path, partition_col.as_deref()).await?;
        self.table_cache
            .insert(name.to_string(), (local_path, Arc::clone(&table)));
        Ok(Some(table))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .table_cache
            .insert(name, (String::new(), table))
            .map(|(_, v)| v))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.table_cache.remove(name).map(|(_, (_, v))| v))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_cache.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::CatalogProviderList;
    use log::{error, info};

    use crate::catalog_list::PostgreSQLMetaCatalogProviderList;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    #[ignore = "requires PostgreSQL; run via `make integration-test`"]
    async fn test_catalog_provider_list() {
        match PostgreSQLMetaCatalogProviderList::new().await {
            Ok(catalog_list) => {
                let catalog_names = catalog_list.catalog_names();
                info!("Found catalogs: {:?}", catalog_names);
                let Some(catalog_name) = catalog_names.first() else {
                    info!("No catalogs registered; nothing to enumerate.");
                    return;
                };
                let Some(catalog) = catalog_list.catalog(catalog_name) else {
                    return;
                };
                let schemas = catalog.schema_names();
                info!("Found schemas: {:?}", schemas);

                for schema_name in schemas {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        let table_names = schema.table_names();
                        info!("{:?}: {:?}", &schema_name, table_names);
                    }
                }
            }
            Err(e) => {
                error!("Error creating catalog list: {}", e);
            }
        }
    }
}
