use std::any::Any;
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow::array::ArrowNativeTypeOp;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};

use sqlx::{PgPool, Row};

use crate::schema::{binary_to_schema, schema_to_binary, SchemaTable};

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

// TODO: maybe support sqlite/tikv, so read/write data must change to interface.
impl PostgreSQLMetaCatalogProviderList {
    pub async fn new() -> Result<Self> {
        let local_dsn = std::env::var("PG_META_DSN")
            .unwrap_or("postgresql://postgres:123456@127.0.0.1:25432/kokedb".to_string());
        let local_pool = PgPool::connect(&local_dsn)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            local_pool,
            catalog_cache: DashMap::new(),
        })
    }

    pub async fn init_db(&self) -> Result<()> {
        let init_meta_tables_sql =
            r#"
            -- 创建 schema
            CREATE SCHEMA IF NOT EXISTS system;

            -- 创建触发器函数
            CREATE OR REPLACE FUNCTION system.update_modified_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            -- 创建 table_arrow_schema 表
            CREATE TABLE IF NOT EXISTS system.table_arrow_schema (
                id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
                catalog_name varchar,
                schema_name varchar,
                created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
                updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
                table_name varchar,
                local_path varchar,
                table_stats jsonb,
                description text,
                partition_info jsonb,
                arrow_schema bytea,
                CONSTRAINT unique_catalog_schema_table UNIQUE (catalog_name, schema_name, table_name)
            );

            -- 创建 table_arrow_schema 表的触发器
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = 'update_table_arrow_schema_modtime'
                    AND tgrelid = 'system.table_arrow_schema'::regclass
                ) THEN
                    CREATE TRIGGER update_table_arrow_schema_modtime
                    BEFORE UPDATE ON system.table_arrow_schema
                    FOR EACH ROW EXECUTE FUNCTION system.update_modified_column();
                END IF;
            END $$;

            -- 创建索引
            CREATE INDEX IF NOT EXISTS idx_table_arrow_schema_partition_info_gin
            ON system.table_arrow_schema USING gin (partition_info);

            CREATE INDEX IF NOT EXISTS idx_table_arrow_schema_table_stats_gin
            ON system.table_arrow_schema USING gin (table_stats);

            -- 创建 catalog 表
            CREATE TABLE IF NOT EXISTS system.catalog (
                id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
                name varchar,
                dsn varchar,
                created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
                updated_at timestamp DEFAULT CURRENT_TIMESTAMP
            );

            -- 创建触发器
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = 'update_catalog_modtime'
                    AND tgrelid = 'system.catalog'::regclass
                ) THEN
                    CREATE TRIGGER update_catalog_modtime
                    BEFORE UPDATE ON system.catalog
                    FOR EACH ROW EXECUTE FUNCTION system.update_modified_column();
                END IF;
            END $$;
            "#.to_string();
        sqlx::query(&init_meta_tables_sql)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
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

    async fn get_catalog(&self, name: &str) -> Result<CatalogInfo> {
        let query = format!("SELECT name, dsn FROM system.catalog where name='{}'", name);

        let row = sqlx::query(&query)
            .fetch_one(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let catalog_info = CatalogInfo {
            name: row.get("name"),
            dsn: row.get("dsn"),
        };

        Ok(catalog_info)
    }

    pub fn create_catalog(&self, catalog: &str, dsn: &str) -> Result<bool> {
        let insert_sql = "INSERT INTO system.catalog (name, dsn) VALUES ($1, $2)";

        let ret = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(insert_sql)
                    .bind(catalog)
                    .bind(dsn)
                    .execute(&self.local_pool)
                    .await
            })
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ret.rows_affected().is_eq(1))
    }

    pub fn save_table_schema(&self, schema_info: &SchemaTable) -> Result<bool> {
        let catalog = schema_info.catalog;
        let schema = schema_info.schema;
        let table = schema_info.table;
        let arrow_schema = schema_info.arrow_schema.clone();
        let arrow_schema_bin =
            schema_to_binary(arrow_schema).map_err(|x| DataFusionError::External(Box::new(x)))?;
        let local_path = schema_info.local_path;

        let upsert_sql = r#"
                    INSERT INTO system.table_arrow_schema (
                        catalog_name, 
                        schema_name, 
                        table_name, 
                        arrow_schema, 
                        local_path
                    ) VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (catalog_name, schema_name, table_name) 
                    DO UPDATE SET 
                        arrow_schema = EXCLUDED.arrow_schema,
                        local_path = EXCLUDED.local_path
                    RETURNING id
                    "#;
        let _ret = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(upsert_sql)
                    .bind(catalog)
                    .bind(schema)
                    .bind(table)
                    .bind(arrow_schema_bin)
                    .bind(local_path)
                    .execute(&self.local_pool)
                    .await
            })
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(true)
    }

    pub fn get_table_schema(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Arc<Schema>> {
        let sql = format!("select arrow_schema from system.table_arrow_schema where catalog_name = '{}' and schema_name='{}' and table_name='{}'", catalog, schema, table);

        let row = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { sqlx::query(&sql).fetch_one(&self.local_pool).await })
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let arrow_schema: Vec<u8> = row.get("arrow_schema");
        let schema = binary_to_schema(&arrow_schema)?;

        Ok(schema)
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
                if let Ok(catalog_info) = self.get_catalog(name).await {
                    if let Ok(remote_pool) = PgPool::connect(&catalog_info.dsn).await {
                        let provider: Arc<dyn CatalogProvider> =
                            Arc::new(PostgreSQLCatalogProvider::new(catalog_info, remote_pool));

                        self.catalog_cache
                            .insert(name.to_string(), Arc::clone(&provider));

                        return Some(provider);
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
            "file:///tmp/{}/{}/{}",
            self.catalog_info.name, self.schema_name, table_name
        );

        let meta_client = PostgreSQLMetaCatalogProviderList::new().await?;
        let schema =
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
        match PostgreSQLMetaCatalogProviderList::new().await {
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
