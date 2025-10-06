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
use sqlx::{PgPool, Row};

use crate::catalog_list::{CatalogInfo, PostgreSQLMetaCatalogProviderList};

#[derive(Debug)]
pub struct PostgreSQLCatalogProvider {
    catalog_info: CatalogInfo,
    remote_pool: PgPool,
    schema_cache: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl PostgreSQLCatalogProvider {
    pub fn new(catalog_info: CatalogInfo, remote_pool: PgPool) -> Self {
        Self {
            catalog_info,
            remote_pool,
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
    table_cache: DashMap<String, Arc<dyn TableProvider>>,
}

impl PostgreSQLSchemaProvider {
    fn new(catalog_info: CatalogInfo, schema_name: String, remote_pool: PgPool) -> Self {
        Self {
            catalog_info,
            schema_name,
            remote_pool,
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

    async fn create_listing_table(
        &self,
        table_name: &str,
        meta_client: &PostgreSQLMetaCatalogProviderList,
    ) -> Result<Arc<dyn TableProvider>> {
        let (schema, table_path) =
            meta_client.get_table_schema(&self.catalog_info.name, &self.schema_name, table_name)?;
        let file_format: Arc<dyn datafusion::datasource::file_format::FileFormat> =
            Arc::new(ParquetFormat::default());

        let listing_options = ListingOptions::new(file_format);

        let table_url = ListingTableUrl::parse(&table_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);
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
        if let Some(table) = self.table_cache.get(name) {
            return Ok(Some(Arc::clone(&table)));
        }

        let catalog = self.catalog_info.name.clone();
        let schema = self.schema_name.clone();
        let dsn = self.catalog_info.dsn.clone();

        let meta_client = PostgreSQLMetaCatalogProviderList::new().await?;
        let is_cached = meta_client
            .check_table_is_cached(&catalog, &schema, name)
            .await?;

        if is_cached {
            match self.create_listing_table(name, &meta_client).await {
                Ok(table) => {
                    self.table_cache
                        .insert(name.to_string(), Arc::clone(&table));
                    Ok(Some(table))
                }
                Err(e) => Err(e),
            }
        } else {
            let config = PostgreSQLConfig {
                connection_string: dsn.clone(),
                table_name: name.to_string(),
                schema_name: Some(schema.clone()),
            };
            let remote_table = PostgreSQLTableProvider::new(config).await?;
            Ok(Some(Arc::new(remote_table)))
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.table_cache.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.table_cache.remove(name).map(|(_, v)| v))
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
    async fn test_catalog_provider_list() {
        match PostgreSQLMetaCatalogProviderList::new().await {
            Ok(catalog_list) => {
                let catalog_names = catalog_list.catalog_names();
                info!("Found catalogs: {:?}", catalog_names);
                let catalog_name = catalog_names.first().unwrap();
                let catalog = catalog_list.catalog(&catalog_name).unwrap();
                let schemas = catalog.schema_names();
                info!("Found schemas: {:?}", schemas);

                for schema_name in schemas {
                    let schema = catalog.schema(&schema_name).unwrap();
                    let table_names = schema.table_names();
                    info!("{:?}: {:?}", &schema_name, table_names);
                }
            }
            Err(e) => {
                error!("Error creating catalog list: {}", e);
            }
        }
    }
}
