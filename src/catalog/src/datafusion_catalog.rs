use std::any::Any;
use std::sync::Arc;

use crate::error::{CatalogError, CatalogResult};
use crate::provider::{
    CreateDatabaseOptions, CreateTableOptions, CreateViewOptions, DatabaseStatus,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace, TableColumnStatus,
    TableKind, TableStatus,
};
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

pub struct DataFusionCatalogAdapter {
    inner: Arc<dyn CatalogProvider>,
    catalog_name: String,
}

impl DataFusionCatalogAdapter {
    pub fn new(inner: Arc<dyn CatalogProvider>, catalog_name: String) -> Self {
        Self {
            inner,
            catalog_name,
        }
    }

    pub fn inner(&self) -> &dyn CatalogProvider {
        self.inner.as_ref()
    }

    fn create_database_status(&self, schema_name: &str) -> DatabaseStatus {
        DatabaseStatus {
            catalog: self.catalog_name.clone(),
            database: vec![schema_name.to_string()],
            comment: None,
            location: None,
            properties: vec![],
        }
    }

    async fn create_table_status(
        &self,
        schema_name: &str,
        table_name: &str,
        table_provider: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> TableStatus {
        let schema = table_provider.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|field| TableColumnStatus {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                comment: None,
                default: None,
                generated_always_as: None,
                is_partition: false,
                is_bucket: false,
                is_cluster: false,
            })
            .collect();

        TableStatus {
            name: table_name.to_string(),
            kind: TableKind::Table {
                catalog: self.catalog_name.clone(),
                database: vec![schema_name.to_string()],
                columns,
                comment: None,
                constraints: vec![],
                location: None,
                format: "unknown".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                options: vec![],
                properties: vec![],
            },
        }
    }
}

#[async_trait]
impl crate::provider::CatalogProvider for DataFusionCatalogAdapter {
    fn get_name(&self) -> &str {
        &self.catalog_name
    }

    async fn create_database(
        &self,
        _database: &Namespace,
        _options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        Err(CatalogError::NotSupported("create_database".to_string()))
    }

    async fn drop_database(
        &self,
        _database: &Namespace,
        _options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported("drop_database".to_string()))
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let schema_name = &database.head;

        if self.inner.schema(schema_name).is_some() {
            Ok(self.create_database_status(schema_name))
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }

    async fn list_databases(
        &self,
        _prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let schema_names = self.inner.schema_names();
        let databases = schema_names
            .into_iter()
            .map(|name| self.create_database_status(&name))
            .collect();

        Ok(databases)
    }

    async fn create_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported("create_table".to_string()))
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let schema_name = &database.head;

        if let Some(schema) = self.inner.schema(schema_name) {
            if let Some(table_provider) = schema
                .table(table)
                .await
                .map_err(|e| CatalogError::Internal(format!("DataFusion error: {}", e)))?
            {
                Ok(self
                    .create_table_status(schema_name, table, table_provider)
                    .await)
            } else {
                Err(CatalogError::NotFound("table", table.to_string()))
            }
        } else {
            Err(CatalogError::NotFound("database", schema_name.to_string()))
        }
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let schema_name = &database.head;

        if let Some(schema) = self.inner.schema(schema_name) {
            let table_names = schema.table_names();
            let mut tables = Vec::new();

            for table_name in table_names {
                if let Ok(table_status) = self.get_table(database, &table_name).await {
                    tables.push(table_status);
                }
            }

            Ok(tables)
        } else {
            Err(CatalogError::NotFound("database", schema_name.to_string()))
        }
    }

    async fn drop_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: DropTableOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported("drop_table".to_string()))
    }

    async fn create_view(
        &self,
        _: &Namespace,
        _: &str,
        _: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported("create_view".to_string()))
    }

    async fn get_view(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported("get_view".to_string()))
    }

    async fn list_views(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        Ok(vec![])
    }

    async fn drop_view(&self, _: &Namespace, _: &str, _: DropViewOptions) -> CatalogResult<()> {
        Err(CatalogError::NotSupported("drop_view".to_string()))
    }
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
                let schemas = catalog.schema_names();
                println!("Found schemas: {:?}", schemas);

                for schema_name in schemas {
                    let schema = catalog.schema(&schema_name).unwrap();
                    let table_names = schema.table_names();
                    println!("{:?}: {:?}", &schema_name, table_names);
                }
            }
            Err(e) => {
                eprintln!("Error creating catalog list: {}", e);
            }
        }
    }
}
