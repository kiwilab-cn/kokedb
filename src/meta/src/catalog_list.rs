use std::any::Any;
use std::sync::Arc;

use arrow_schema::Schema;
use dashmap::DashMap;
use datafusion::arrow::array::ArrowNativeTypeOp;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};

use datafusion::error::{DataFusionError, Result};

use datafusion::sql::sqlparser::parser::ParserError;
use kokedb_common::cache_policy::parse_cache_policy;
use sqlx::{PgPool, Row};

use crate::datafusion_catalog::PostgreSQLCatalogProvider;
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
        let mut tx = self
            .local_pool
            .begin()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let sql_statements = vec![
            "CREATE SCHEMA IF NOT EXISTS system;",
            r#"
            CREATE OR REPLACE FUNCTION system.update_modified_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            "#,
            r#"
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
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS system.catalog (
                id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
                name varchar,
                dsn varchar,
                db_type varchar,
                description varchar,
                cache_policy varchar,
                created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
                updated_at timestamp DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS system.sql_stats (
                sql_hash BIGINT PRIMARY KEY,
                sql_text TEXT NOT NULL,
                execution_time BIGINT NOT NULL DEFAULT 0,
                count INTEGER NOT NULL DEFAULT 0,
                min_time BIGINT,
                max_time BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            "#,
        ];

        for sql in sql_statements {
            sqlx::query(sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        let trigger_statements = vec![
            r#"
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
            "#,
            r#"
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
            "#,
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = 'update_sql_stats_modtime'
                    AND tgrelid = 'system.sql_stats'::regclass
                ) THEN
                    CREATE TRIGGER update_sql_stats_modtime
                        BEFORE UPDATE ON system.sql_stats
                FOR EACH ROW EXECUTE FUNCTION system.update_modified_column();
                END IF;
            END $$;
            "#,
        ];

        for sql in trigger_statements {
            sqlx::query(sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        let index_statements = vec![
            r#"
            CREATE INDEX IF NOT EXISTS idx_table_arrow_schema_partition_info_gin
            ON system.table_arrow_schema USING gin (partition_info);
            "#,
            r#"
            CREATE INDEX IF NOT EXISTS idx_table_arrow_schema_table_stats_gin
            ON system.table_arrow_schema USING gin (table_stats);
            "#,
        ];

        for sql in index_statements {
            sqlx::query(sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    pub async fn load_catalog_info(&self) -> Result<Vec<CatalogInfo>> {
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

    pub async fn get_catalog(&self, name: &str) -> Result<CatalogInfo> {
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

    pub async fn get_catalog_cache_policy(&self, name: &str) -> Result<String> {
        let query = format!(
            "SELECT cache_policy FROM system.catalog where name='{}'",
            name
        );

        let row = sqlx::query(&query)
            .fetch_one(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let cache_policy: String = row
            .try_get("cache_policy")
            .unwrap_or_else(|_| String::new());
        Ok(cache_policy)
    }

    pub fn create_catalog(
        &self,
        catalog: &str,
        dsn: &str,
        db_type: &str,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    ) -> Result<bool> {
        let cache_policy = parse_cache_policy(properties).map_err(|x| {
            DataFusionError::SQL(
                Box::new(ParserError::ParserError(
                    "Failed to get topk/all/select and k/table_set value from the properties."
                        .to_string(),
                )),
                Some(format!(
                    "Failed to get cache policy from properties:{:?}",
                    x
                )),
            )
        })?;

        let insert_sql =
            "INSERT INTO system.catalog (name, dsn, db_type, description, cache_policy) VALUES ($1, $2, $3, $4, $5)";

        let ret = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(insert_sql)
                    .bind(catalog)
                    .bind(dsn)
                    .bind(db_type)
                    .bind(comment)
                    .bind(cache_policy.to_string())
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

    pub async fn check_table_is_cached(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<bool> {
        let sql = format!(
            "select arrow_schema, local_path from system.table_arrow_schema \
            where catalog_name = '{}' and schema_name='{}' and table_name='{}'",
            catalog, schema, table
        );

        let row = sqlx::query(&sql)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(row.is_some())
    }

    pub fn get_table_schema(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<(Arc<Schema>, String)> {
        let sql = format!(
            "select arrow_schema, local_path from system.table_arrow_schema \
            where catalog_name = '{}' and schema_name='{}' and table_name='{}'",
            catalog, schema, table
        );

        let row = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { sqlx::query(&sql).fetch_optional(&self.local_pool).await })
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let (schema, local_path) = if row.is_some() {
            let row = row.unwrap();
            let arrow_schema: Vec<u8> = row.get("arrow_schema");
            let local_path: String = row.get("local_path");
            let schema = binary_to_schema(&arrow_schema)?;
            (schema, local_path)
        } else {
            (Arc::new(Schema::empty()), "".to_string())
        };

        Ok((schema, local_path))
    }

    pub fn save_sql_history(&self, sql: &str, key: u64, cost: u64) -> Result<bool> {
        let insert_sql =
            "INSERT INTO sql_stats (sql_hash, sql_text, execution_time, count, min_time, max_time)
            VALUES ($1, $2, $3, 1, $3, $3)
            ON CONFLICT (sql_hash) DO UPDATE SET
            execution_time = sql_stats.execution_time + EXCLUDED.execution_time,
            count = sql_stats.count + 1,
            min_time = LEAST(sql_stats.min_time, EXCLUDED.min_time),
            max_time = GREATEST(sql_stats.max_time, EXCLUDED.max_time)";

        let ret = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(insert_sql)
                    .bind(key as i64)
                    .bind(sql)
                    .bind(cost as i64)
                    .execute(&self.local_pool)
                    .await
            })
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ret.rows_affected().is_eq(1))
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
