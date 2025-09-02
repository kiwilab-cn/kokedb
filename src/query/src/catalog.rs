use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use sqlx::{PgPool, Row};

#[derive(Debug, Clone)]
pub struct CatalogInfo {
    pub name: String,
    pub dsn: String,
}

#[derive(Debug)]
pub struct PostgreSQLMetaCatalogProviderList {
    local_pool: PgPool,
    catalog_cache: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl PostgreSQLMetaCatalogProviderList {
    pub async fn new(local_dsn: &str) -> Result<Self> {
        let local_pool = PgPool::connect(local_dsn)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Self {
            local_pool,
            catalog_cache: DashMap::new(),
        })
    }

    async fn load_catalog_info(&self) -> Result<Vec<CatalogInfo>> {
        let query = "SELECT name, dsn FROM system.catalog";

        let rows = sqlx::query(query)
            .fetch_all(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut catalogs = Vec::new();
        for row in rows {
            let catalog_info = CatalogInfo {
                name: row.get("name"),
                dsn: row.get("dsn"),
            };
            catalogs.push(catalog_info);
        }

        Ok(catalogs)
    }
}

impl CatalogProviderList for PostgreSQLMetaCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        //TODO: need store to postgresql.
        self.catalog_cache.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Ok(catalogs) = self.load_catalog_info().await {
                    catalogs.into_iter().map(|c| c.name.clone()).collect()
                } else {
                    Vec::new()
                }
            })
        })
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Ok(catalogs) = self.load_catalog_info().await {
                    for catalog_info in catalogs {
                        if catalog_info.name == name {
                            if let Ok(remote_pool) = PgPool::connect(&catalog_info.dsn).await {
                                let provider: Arc<dyn CatalogProvider> = Arc::new(
                                    PostgreSQLCatalogProvider::new(catalog_info, remote_pool),
                                );

                                self.catalog_cache
                                    .insert(name.to_string(), Arc::clone(&provider));

                                return Some(provider);
                            }
                        }
                    }
                }
                None
            })
        })
    }
}

#[derive(Debug)]
pub struct PostgreSQLCatalogProvider {
    catalog_info: CatalogInfo,
    remote_pool: PgPool,
    schema_cache: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl PostgreSQLCatalogProvider {
    fn new(catalog_info: CatalogInfo, remote_pool: PgPool) -> Self {
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

    async fn create_listing_table(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        let table_path = format!(
            "/tmp/{}/{}/{}",
            self.catalog_info.name, self.schema_name, table_name
        );

        let file_format: Arc<dyn datafusion::datasource::file_format::FileFormat> =
            Arc::new(CsvFormat::default());

        let listing_options = ListingOptions::new(file_format);

        let table_url = ListingTableUrl::parse(&table_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

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
        if !self.table_cache.is_empty() {
            return self
                .table_cache
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
        }

        let runtime = tokio::runtime::Handle::try_current();
        if let Ok(handle) = runtime {
            if let Ok(names) = handle.block_on(self.get_table_names()) {
                return names;
            }
        }

        Vec::new()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.table_cache.get(name) {
            return Ok(Some(Arc::clone(&table)));
        }

        match self.create_listing_table(name).await {
            Ok(table) => {
                self.table_cache
                    .insert(name.to_string(), Arc::clone(&table));
                Ok(Some(table))
            }
            Err(e) => Err(e),
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
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_catalog_provider_list() {
        let local_dsn = "postgresql://postgres:123456@192.168.0.227:25432/kokedb";

        match PostgreSQLMetaCatalogProviderList::new(local_dsn).await {
            Ok(catalog_list) => {
                let catalog_names = catalog_list.catalog_names();
                println!("Found catalogs: {:?}", catalog_names);
                let catalog_name = catalog_names.first().unwrap();
                let catalog = catalog_list.catalog(&catalog_name).unwrap();
                let schema = catalog.schema_names();
                println!("===================>>>{:?}", schema);
            }
            Err(e) => {
                eprintln!("Error creating catalog list: {}", e);
            }
        }
    }
}
