use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;
use chrono::{Duration, Local, NaiveDate};
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

/// Persisted incremental-sync state for a single cached table.
#[derive(Debug, Clone, Default)]
pub struct TableSyncState {
    pub watermark_column: Option<String>,
    /// Comma-separated primary key columns, if any.
    pub pk_columns: Option<String>,
    /// Last synced max watermark value, serialized as text.
    pub last_watermark: Option<String>,
    pub sync_mode: String,
    /// Number of consecutive incremental runs since the last full refresh.
    pub incremental_runs: i32,
}

/// Per-table sync *decision* (how to sync and how often). Phase 1 populates the
/// `refresh_*` cadence and the observed signal snapshot; the `inc_*` fields are
/// reserved for the incremental-inference phases.
#[derive(Debug, Clone)]
pub struct TableSyncPolicy {
    pub refresh_bucket: String,
    pub refresh_interval_sec: i32,
    pub est_row_count: Option<i64>,
    pub est_size_bytes: Option<i64>,
    pub churn_per_hour: Option<f32>,
    pub access_per_day: Option<f32>,
    pub score: Option<f32>,
}

/// The inferred incremental strategy for a table (Phase 2 decision layer),
/// stored alongside the cadence in `system.table_sync_policy`.
#[derive(Debug, Clone)]
pub struct TableIncPolicy {
    pub inc_mode: String,
    /// Lifecycle: none | suggested | active | rejected.
    pub inc_status: String,
    /// trusted | probation | audited.
    pub inc_tier: String,
    pub watermark_column: Option<String>,
    /// Comma-separated primary key columns.
    pub pk_columns: Option<String>,
    pub source: String,
    pub confidence: Option<f32>,
    pub reason: Option<String>,
    pub audit_passes: i32,
    pub divergence_count: i32,
}

/// One sync-run record from `system.cache_job` (`SHOW CACHE JOBS`).
#[derive(Debug, Clone)]
pub struct CacheJob {
    pub id: i64,
    pub catalog: String,
    pub schema_name: String,
    pub table_name: String,
    pub status: String,
    pub error_message: Option<String>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// The full `table_sync_policy` row for `SHOW TABLE METADATA`.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub refresh_bucket: String,
    pub refresh_interval_sec: i32,
    pub est_row_count: Option<i64>,
    pub est_size_bytes: Option<i64>,
    pub churn_per_hour: Option<f32>,
    pub access_per_day: Option<f32>,
    pub inc_mode: String,
    pub inc_status: String,
    pub inc_tier: String,
    pub watermark_column: Option<String>,
    pub pk_columns: Option<String>,
    pub source: String,
    pub confidence: Option<f32>,
    pub reason: Option<String>,
    pub audit_passes: i32,
    pub divergence_count: i32,
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

    /// Wraps an existing meta connection pool without opening a new one. Used on
    /// hot paths (e.g. table resolution) to reuse the shared pool instead of
    /// reconnecting per query.
    pub fn from_pool(local_pool: PgPool) -> Self {
        Self {
            local_pool,
            catalog_cache: DashMap::new(),
        }
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
                partition_by varchar,
                created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
                updated_at timestamp DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            // Backfill for catalogs created before partition_by existed.
            "ALTER TABLE system.catalog ADD COLUMN IF NOT EXISTS partition_by varchar;",
            r#"
            CREATE TABLE IF NOT EXISTS system.sql_stats (
                sql_hash VARCHAR(20) PRIMARY KEY,
                sql_text TEXT NOT NULL,
                execution_time BIGINT NOT NULL DEFAULT 0,
                count INTEGER NOT NULL DEFAULT 0,
                min_time BIGINT,
                max_time BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS system.query_table_daily_stats (
                id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
                catalog VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                stat_date DATE NOT NULL,
                query_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(catalog, schema_name, table_name, stat_date)
            );
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS system.table_stats (
                id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
                catalog VARCHAR(255) NOT NULL,
                schema VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                cache_key_list VARCHAR(20)[] NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(catalog, schema, table_name)
            );
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS system.table_sync_state (
                catalog VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                watermark_column VARCHAR(255),
                pk_columns VARCHAR(1024),
                last_watermark VARCHAR(64),
                sync_mode VARCHAR(16) NOT NULL DEFAULT 'full',
                incremental_runs INTEGER NOT NULL DEFAULT 0,
                last_sync_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (catalog, schema_name, table_name)
            );
            "#,
            // Per-table sync policy: the *decision* layer (how to sync + how
            // often), separate from table_sync_state (the runtime cursor).
            // Phase 1 uses the refresh_* + signal columns; the inc_*/audit
            // columns are reserved for the incremental-inference phases.
            r#"
            CREATE TABLE IF NOT EXISTS system.table_sync_policy (
                catalog VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                refresh_bucket VARCHAR(16) NOT NULL DEFAULT 'normal',
                refresh_interval_sec INTEGER NOT NULL DEFAULT 900,
                est_row_count BIGINT,
                est_size_bytes BIGINT,
                churn_per_hour REAL,
                access_per_day REAL,
                score REAL,
                inc_mode VARCHAR(16) NOT NULL DEFAULT 'full',
                inc_status VARCHAR(16) NOT NULL DEFAULT 'none',
                inc_tier VARCHAR(16) NOT NULL DEFAULT 'audited',
                watermark_column VARCHAR(255),
                pk_columns VARCHAR(1024),
                source VARCHAR(16) NOT NULL DEFAULT 'rule',
                confidence REAL,
                reason TEXT,
                next_audit_at TIMESTAMPTZ,
                audit_passes INTEGER NOT NULL DEFAULT 0,
                divergence_count INTEGER NOT NULL DEFAULT 0,
                paused BOOLEAN NOT NULL DEFAULT false,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (catalog, schema_name, table_name)
            );
            "#,
            // Backfill for policy rows created before `paused` existed.
            "ALTER TABLE system.table_sync_policy ADD COLUMN IF NOT EXISTS paused BOOLEAN NOT NULL DEFAULT false;",
            // One row per table-sync run, for `SHOW CACHE JOBS` observability.
            r#"
            CREATE TABLE IF NOT EXISTS system.cache_job (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                catalog VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'running',
                error_message TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ended_at TIMESTAMPTZ
            );
            "#,
            "CREATE INDEX IF NOT EXISTS idx_cache_job_lookup \
             ON system.cache_job (catalog, schema_name, table_name, started_at DESC);",
            // Per-database cache override (the middle tier between catalog and
            // table policy). `mode`: all | none.
            r#"
            CREATE TABLE IF NOT EXISTS system.database_policy (
                catalog VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                mode VARCHAR(16) NOT NULL DEFAULT 'all',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (catalog, schema_name)
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
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = 'update_table_daily_stats_modtime'
                    AND tgrelid = 'system.query_table_daily_stats'::regclass
                ) THEN
                    CREATE TRIGGER update_table_daily_stats_modtime
                        BEFORE UPDATE ON system.query_table_daily_stats
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
                    WHERE tgname = 'update_table_stats_modtime'
                    AND tgrelid = 'system.table_stats'::regclass
                ) THEN
                    CREATE TRIGGER update_table_stats_modtime
                        BEFORE UPDATE ON system.table_stats
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
        let query = "SELECT name, dsn FROM system.catalog;";

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
        let sql = "SELECT name, dsn FROM system.catalog where name = $1;";

        let row = sqlx::query(sql)
            .bind(name)
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
        let sql = "SELECT cache_policy FROM system.catalog where name = $1;";

        let row = sqlx::query(sql)
            .bind(name)
            .fetch_one(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let cache_policy: String = row
            .try_get("cache_policy")
            .unwrap_or_else(|_| String::new());
        Ok(cache_policy)
    }

    /// Updates a catalog's cache policy (backs `ALTER CATALOG ... SET CACHE
    /// POLICY`). Returns the number of catalogs updated (0 if not found).
    pub async fn update_catalog_cache_policy(
        &self,
        name: &str,
        cache_policy: &str,
    ) -> Result<u64> {
        let res = sqlx::query(
            "UPDATE system.catalog SET cache_policy = $2, updated_at = CURRENT_TIMESTAMP \
             WHERE name = $1;",
        )
        .bind(name)
        .bind(cache_policy)
        .execute(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(res.rows_affected())
    }

    /// Sets a per-database cache override (`mode`: all | none).
    pub async fn upsert_database_policy(
        &self,
        catalog: &str,
        schema: &str,
        mode: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO system.database_policy (catalog, schema_name, mode, updated_at) \
             VALUES ($1, $2, $3, CURRENT_TIMESTAMP) \
             ON CONFLICT (catalog, schema_name) DO UPDATE SET \
                 mode = EXCLUDED.mode, updated_at = CURRENT_TIMESTAMP;",
        )
        .bind(catalog)
        .bind(schema)
        .bind(mode)
        .execute(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// Removes the per-table sync policies for a whole database (used when a
    /// database is disabled), so its tables stop being scheduled for refresh.
    /// Cached snapshots remain until the table is dropped.
    pub async fn delete_schema_table_policies(&self, catalog: &str, schema: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM system.table_sync_policy WHERE catalog = $1 AND schema_name = $2;",
        )
        .bind(catalog)
        .bind(schema)
        .execute(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// Lists the currently-cached `table_name`s in a database (have a snapshot).
    pub async fn list_cached_tables_in_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT table_name FROM system.table_arrow_schema \
             WHERE catalog_name = $1 AND schema_name = $2;",
        )
        .bind(catalog)
        .bind(schema)
        .fetch_all(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(rows.iter().map(|r| r.get::<String, _>("table_name")).collect())
    }

    /// Evicts a table from the cache at the meta level: returns its snapshot
    /// `local_path` and the cached query-result keys (for the caller to delete
    /// the files / invalidate the result cache), then removes the arrow-schema,
    /// sync-state, policy, and stats rows so the table resolves to its live
    /// remote source again.
    pub async fn evict_table_cache(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<(Option<String>, Vec<u64>)> {
        let local_path: Option<String> = sqlx::query(
            "SELECT local_path FROM system.table_arrow_schema \
             WHERE catalog_name = $1 AND schema_name = $2 AND table_name = $3;",
        )
        .bind(catalog)
        .bind(schema)
        .bind(table)
        .fetch_optional(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .and_then(|r| r.try_get::<String, _>("local_path").ok());

        let cache_keys: Vec<u64> = sqlx::query_as::<_, (Vec<String>,)>(
            "SELECT cache_key_list FROM system.table_stats \
             WHERE catalog = $1 AND schema = $2 AND table_name = $3;",
        )
        .bind(catalog)
        .bind(schema)
        .bind(table)
        .fetch_optional(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map(|(list,)| list.iter().filter_map(|s| s.parse::<u64>().ok()).collect())
        .unwrap_or_default();

        // Delete the cache metadata from all relevant tables (note the differing
        // catalog/schema column names per table).
        for sql in [
            "DELETE FROM system.table_arrow_schema WHERE catalog_name=$1 AND schema_name=$2 AND table_name=$3",
            "DELETE FROM system.table_sync_state WHERE catalog=$1 AND schema_name=$2 AND table_name=$3",
            "DELETE FROM system.table_sync_policy WHERE catalog=$1 AND schema_name=$2 AND table_name=$3",
            "DELETE FROM system.table_stats WHERE catalog=$1 AND schema=$2 AND table_name=$3",
        ] {
            sqlx::query(sql)
                .bind(catalog)
                .bind(schema)
                .bind(table)
                .execute(&self.local_pool)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        // Drop it from the in-memory provider cache so the next resolve rebuilds
        // it (as a live remote table) instead of serving the stale snapshot.
        self.catalog_cache.remove(catalog);

        Ok((local_path, cache_keys))
    }

    /// Returns the `(schema, mode)` database overrides for a catalog.
    pub async fn get_database_policies(&self, catalog: &str) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT schema_name, mode FROM system.database_policy WHERE catalog = $1;",
        )
        .bind(catalog)
        .fetch_all(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(rows
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("schema_name"),
                    r.get::<String, _>("mode"),
                )
            })
            .collect())
    }

    /// The catalog's declared Hive partition column, if any.
    pub async fn get_catalog_partition_by(&self, name: &str) -> Result<Option<String>> {
        let sql = "SELECT partition_by FROM system.catalog WHERE name = $1;";
        let row = sqlx::query(sql)
            .bind(name)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(row.and_then(|r| r.try_get::<Option<String>, _>("partition_by").ok().flatten()))
    }

    /// Records (or clears) the Hive partition column for a synced table.
    pub async fn set_table_partition_column(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_col: Option<&str>,
    ) -> Result<()> {
        let info = partition_col.map(|c| serde_json::json!({ "partition_col": c }));
        let sql = "UPDATE system.table_arrow_schema SET partition_info = $4 \
            WHERE catalog_name = $1 AND schema_name = $2 AND table_name = $3;";
        sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(info)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// The Hive partition column a table's snapshot is partitioned by, if any.
    pub async fn get_table_partition_column(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<String>> {
        let sql = "SELECT partition_info ->> 'partition_col' AS col \
            FROM system.table_arrow_schema \
            WHERE catalog_name = $1 AND schema_name = $2 AND table_name = $3;";
        let row = sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(row.and_then(|r| r.try_get::<Option<String>, _>("col").ok().flatten()))
    }

    /// Reads the persisted incremental-sync state for a table, if any.
    pub async fn get_table_sync_state(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableSyncState>> {
        let sql = "SELECT watermark_column, pk_columns, last_watermark, sync_mode, \
            incremental_runs FROM system.table_sync_state \
            WHERE catalog = $1 AND schema_name = $2 AND table_name = $3;";

        let row = sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(row.map(|row| TableSyncState {
            watermark_column: row.try_get("watermark_column").ok(),
            pk_columns: row.try_get("pk_columns").ok(),
            last_watermark: row.try_get("last_watermark").ok(),
            sync_mode: row.try_get("sync_mode").unwrap_or_default(),
            incremental_runs: row.try_get("incremental_runs").unwrap_or(0),
        }))
    }

    /// Upserts the incremental-sync state for a table after a sync run.
    pub async fn upsert_table_sync_state(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        state: &TableSyncState,
    ) -> Result<()> {
        let sql = "INSERT INTO system.table_sync_state \
            (catalog, schema_name, table_name, watermark_column, pk_columns, \
             last_watermark, sync_mode, incremental_runs, last_sync_at) \
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP) \
            ON CONFLICT (catalog, schema_name, table_name) DO UPDATE SET \
                watermark_column = EXCLUDED.watermark_column, \
                pk_columns = EXCLUDED.pk_columns, \
                last_watermark = EXCLUDED.last_watermark, \
                sync_mode = EXCLUDED.sync_mode, \
                incremental_runs = EXCLUDED.incremental_runs, \
                last_sync_at = CURRENT_TIMESTAMP;";

        sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(&state.watermark_column)
            .bind(&state.pk_columns)
            .bind(&state.last_watermark)
            .bind(&state.sync_mode)
            .bind(state.incremental_runs)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    /// Upserts the adaptive refresh policy for a table (Phase 1: cadence +
    /// observed signal snapshot). Preserves the `inc_*` columns' defaults/values.
    pub async fn upsert_table_sync_policy(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        policy: &TableSyncPolicy,
    ) -> Result<()> {
        let sql = "INSERT INTO system.table_sync_policy \
            (catalog, schema_name, table_name, refresh_bucket, refresh_interval_sec, \
             est_row_count, est_size_bytes, churn_per_hour, access_per_day, score, updated_at) \
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP) \
            ON CONFLICT (catalog, schema_name, table_name) DO UPDATE SET \
                refresh_bucket = EXCLUDED.refresh_bucket, \
                refresh_interval_sec = EXCLUDED.refresh_interval_sec, \
                est_row_count = EXCLUDED.est_row_count, \
                est_size_bytes = EXCLUDED.est_size_bytes, \
                churn_per_hour = EXCLUDED.churn_per_hour, \
                access_per_day = EXCLUDED.access_per_day, \
                score = EXCLUDED.score, \
                updated_at = CURRENT_TIMESTAMP;";

        sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(&policy.refresh_bucket)
            .bind(policy.refresh_interval_sec)
            .bind(policy.est_row_count)
            .bind(policy.est_size_bytes)
            .bind(policy.churn_per_hour)
            .bind(policy.access_per_day)
            .bind(policy.score)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    /// Returns the `schema.table` names in `catalog` whose adaptive refresh
    /// interval has elapsed (or that have never been synced). Drives the
    /// per-catalog refresh tick.
    /// Pauses or resumes a table's scheduled refresh. Upserts a policy row so it
    /// works even before the table has been evaluated.
    pub async fn set_table_paused(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        paused: bool,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO system.table_sync_policy (catalog, schema_name, table_name, paused, updated_at) \
             VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP) \
             ON CONFLICT (catalog, schema_name, table_name) DO UPDATE SET \
                 paused = EXCLUDED.paused, updated_at = CURRENT_TIMESTAMP;",
        )
        .bind(catalog)
        .bind(schema)
        .bind(table)
        .bind(paused)
        .execute(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    pub async fn get_due_refresh_tables(&self, catalog: &str) -> Result<Vec<String>> {
        let sql = "SELECT p.schema_name, p.table_name \
            FROM system.table_sync_policy p \
            LEFT JOIN system.table_sync_state s \
              ON p.catalog = s.catalog AND p.schema_name = s.schema_name \
             AND p.table_name = s.table_name \
            WHERE p.catalog = $1 \
              AND NOT p.paused \
              AND (s.last_sync_at IS NULL \
                   OR s.last_sync_at + (p.refresh_interval_sec || ' seconds')::interval \
                      <= CURRENT_TIMESTAMP) \
            ORDER BY p.schema_name, p.table_name;";

        let rows = sqlx::query(sql)
            .bind(catalog)
            .fetch_all(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(rows
            .iter()
            .map(|row| {
                let schema_name: String = row.get("schema_name");
                let table_name: String = row.get("table_name");
                format!("{}.{}", schema_name, table_name)
            })
            .collect())
    }

    /// Per-table query counts over the last 7 days as queries/day, keyed by
    /// `schema.table`. Feeds the "access heat" signal for adaptive scheduling.
    pub async fn get_access_per_day(&self, catalog: &str) -> Result<HashMap<String, f32>> {
        let seven_days_ago = Local::now() - Duration::days(7);
        let sql = "SELECT schema_name, table_name, SUM(query_count)::float8 / 7.0 AS per_day \
            FROM system.query_table_daily_stats \
            WHERE catalog = $1 AND stat_date >= $2 \
            GROUP BY schema_name, table_name;";

        let rows = sqlx::query(sql)
            .bind(catalog)
            .bind(seven_days_ago.date_naive())
            .fetch_all(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(rows
            .iter()
            .map(|row| {
                let schema_name: String = row.get("schema_name");
                let table_name: String = row.get("table_name");
                let per_day: f64 = row.try_get("per_day").unwrap_or(0.0);
                (format!("{}.{}", schema_name, table_name), per_day as f32)
            })
            .collect())
    }

    /// Upserts the inferred incremental strategy for a table. The detected fields
    /// (mode/watermark/pk/confidence/reason/source) always refresh, but the
    /// lifecycle `inc_status` and `inc_tier` are only seeded while still `none` —
    /// later re-inference must not clobber an `active`/`rejected`/in-validation
    /// decision the state machine owns.
    pub async fn upsert_table_inc_policy(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        inc: &TableIncPolicy,
    ) -> Result<()> {
        let sql = "INSERT INTO system.table_sync_policy \
            (catalog, schema_name, table_name, inc_mode, inc_status, inc_tier, \
             watermark_column, pk_columns, source, confidence, reason, updated_at) \
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP) \
            ON CONFLICT (catalog, schema_name, table_name) DO UPDATE SET \
                inc_mode = EXCLUDED.inc_mode, \
                watermark_column = EXCLUDED.watermark_column, \
                pk_columns = EXCLUDED.pk_columns, \
                source = EXCLUDED.source, \
                confidence = EXCLUDED.confidence, \
                reason = EXCLUDED.reason, \
                inc_status = CASE WHEN system.table_sync_policy.inc_status = 'none' \
                                  THEN EXCLUDED.inc_status \
                                  ELSE system.table_sync_policy.inc_status END, \
                inc_tier = CASE WHEN system.table_sync_policy.inc_status = 'none' \
                                THEN EXCLUDED.inc_tier \
                                ELSE system.table_sync_policy.inc_tier END, \
                updated_at = CURRENT_TIMESTAMP;";

        sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(&inc.inc_mode)
            .bind(&inc.inc_status)
            .bind(&inc.inc_tier)
            .bind(&inc.watermark_column)
            .bind(&inc.pk_columns)
            .bind(&inc.source)
            .bind(inc.confidence)
            .bind(&inc.reason)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    /// Applies a user override of a table's incremental strategy. The strategy is
    /// trusted (`source='user'`, activated without validation) and resets the
    /// audit state. Creates the policy row if absent (cadence defaults apply).
    pub async fn set_user_inc_policy(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        inc_mode: &str,
        watermark_column: Option<String>,
        pk_columns: Option<String>,
    ) -> Result<()> {
        // `full` disables incremental; anything else is trusted active.
        let status = if inc_mode == "full" { "none" } else { "active" };
        let sql = "INSERT INTO system.table_sync_policy \
            (catalog, schema_name, table_name, inc_mode, inc_status, inc_tier, \
             watermark_column, pk_columns, source, confidence, reason, \
             audit_passes, divergence_count, next_audit_at, updated_at) \
            VALUES ($1, $2, $3, $4, $5, 'trusted', $6, $7, 'user', NULL, \
                    '[user] manual override', 0, 0, NULL, CURRENT_TIMESTAMP) \
            ON CONFLICT (catalog, schema_name, table_name) DO UPDATE SET \
                inc_mode = EXCLUDED.inc_mode, \
                inc_status = EXCLUDED.inc_status, \
                inc_tier = 'trusted', \
                watermark_column = EXCLUDED.watermark_column, \
                pk_columns = EXCLUDED.pk_columns, \
                source = 'user', \
                confidence = NULL, \
                reason = '[user] manual override', \
                audit_passes = 0, \
                divergence_count = 0, \
                next_audit_at = NULL, \
                updated_at = CURRENT_TIMESTAMP;";

        sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(inc_mode)
            .bind(status)
            .bind(&watermark_column)
            .bind(&pk_columns)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    /// Reads the inferred incremental strategy for a table, if one exists.
    pub async fn get_table_inc_policy(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableIncPolicy>> {
        let sql = "SELECT inc_mode, inc_status, inc_tier, watermark_column, pk_columns, \
            source, confidence, reason, audit_passes, divergence_count \
            FROM system.table_sync_policy \
            WHERE catalog = $1 AND schema_name = $2 AND table_name = $3;";

        let row = sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(row.map(|row| TableIncPolicy {
            inc_mode: row.try_get("inc_mode").unwrap_or_else(|_| "full".to_string()),
            inc_status: row.try_get("inc_status").unwrap_or_else(|_| "none".to_string()),
            inc_tier: row
                .try_get("inc_tier")
                .unwrap_or_else(|_| "audited".to_string()),
            watermark_column: row.try_get("watermark_column").ok(),
            pk_columns: row.try_get("pk_columns").ok(),
            source: row.try_get("source").unwrap_or_else(|_| "rule".to_string()),
            confidence: row.try_get("confidence").ok(),
            reason: row.try_get("reason").ok(),
            audit_passes: row.try_get("audit_passes").unwrap_or(0),
            divergence_count: row.try_get("divergence_count").unwrap_or(0),
        }))
    }

    /// Reads the full sync-policy row for a table (backs `SHOW TABLE METADATA`).
    pub async fn get_table_metadata(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableMetadata>> {
        let sql = "SELECT refresh_bucket, refresh_interval_sec, est_row_count, est_size_bytes, \
            churn_per_hour, access_per_day, inc_mode, inc_status, inc_tier, watermark_column, \
            pk_columns, source, confidence, reason, audit_passes, divergence_count \
            FROM system.table_sync_policy \
            WHERE catalog = $1 AND schema_name = $2 AND table_name = $3;";

        let row = sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(row.map(|row| TableMetadata {
            refresh_bucket: row
                .try_get("refresh_bucket")
                .unwrap_or_else(|_| "normal".to_string()),
            refresh_interval_sec: row.try_get("refresh_interval_sec").unwrap_or(900),
            est_row_count: row.try_get("est_row_count").ok(),
            est_size_bytes: row.try_get("est_size_bytes").ok(),
            churn_per_hour: row.try_get("churn_per_hour").ok(),
            access_per_day: row.try_get("access_per_day").ok(),
            inc_mode: row.try_get("inc_mode").unwrap_or_else(|_| "full".to_string()),
            inc_status: row.try_get("inc_status").unwrap_or_else(|_| "none".to_string()),
            inc_tier: row
                .try_get("inc_tier")
                .unwrap_or_else(|_| "audited".to_string()),
            watermark_column: row.try_get("watermark_column").ok(),
            pk_columns: row.try_get("pk_columns").ok(),
            source: row.try_get("source").unwrap_or_else(|_| "rule".to_string()),
            confidence: row.try_get("confidence").ok(),
            reason: row.try_get("reason").ok(),
            audit_passes: row.try_get("audit_passes").unwrap_or(0),
            divergence_count: row.try_get("divergence_count").unwrap_or(0),
        }))
    }

    /// Records the start of a sync run and returns its job id.
    pub async fn insert_cache_job(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO system.cache_job (catalog, schema_name, table_name, status) \
             VALUES ($1, $2, $3, 'running') RETURNING id;",
        )
        .bind(catalog)
        .bind(schema)
        .bind(table)
        .fetch_one(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(row.try_get("id").unwrap_or(0))
    }

    /// Marks a sync run finished with its terminal status and optional error.
    pub async fn complete_cache_job(
        &self,
        id: i64,
        status: &str,
        error: Option<String>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE system.cache_job SET status = $2, error_message = $3, \
             ended_at = CURRENT_TIMESTAMP WHERE id = $1;",
        )
        .bind(id)
        .bind(status)
        .bind(&error)
        .execute(&self.local_pool)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// Returns recent sync-run records, optionally filtered to a catalog and/or a
    /// `schema.table`, newest first.
    pub async fn get_cache_jobs(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table: Option<&str>,
        limit: i64,
    ) -> Result<Vec<CacheJob>> {
        let sql = "SELECT id, catalog, schema_name, table_name, status, error_message, \
            started_at, ended_at FROM system.cache_job \
            WHERE ($1::text IS NULL OR catalog = $1) \
              AND ($2::text IS NULL OR schema_name = $2) \
              AND ($3::text IS NULL OR table_name = $3) \
            ORDER BY started_at DESC LIMIT $4;";
        let rows = sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(limit)
            .fetch_all(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(rows
            .iter()
            .map(|row| CacheJob {
                id: row.try_get("id").unwrap_or(0),
                catalog: row.try_get("catalog").unwrap_or_default(),
                schema_name: row.try_get("schema_name").unwrap_or_default(),
                table_name: row.try_get("table_name").unwrap_or_default(),
                status: row.try_get("status").unwrap_or_default(),
                error_message: row.try_get("error_message").ok(),
                started_at: row
                    .try_get("started_at")
                    .unwrap_or_else(|_| chrono::Utc::now()),
                ended_at: row.try_get("ended_at").ok(),
            })
            .collect())
    }

    /// Returns `schema.table` names whose shadow audit is due: an inferred,
    /// not-yet-rejected strategy whose `next_audit_at` has passed (or was never
    /// scheduled — a freshly `suggested` table audits as soon as possible).
    pub async fn get_due_audit_tables(&self, catalog: &str) -> Result<Vec<String>> {
        let sql = "SELECT schema_name, table_name FROM system.table_sync_policy \
            WHERE catalog = $1 \
              AND inc_mode <> 'full' AND inc_status IN ('suggested', 'active') \
              AND (next_audit_at IS NULL OR next_audit_at <= CURRENT_TIMESTAMP) \
            ORDER BY schema_name, table_name;";

        let rows = sqlx::query(sql)
            .bind(catalog)
            .fetch_all(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(rows
            .iter()
            .map(|row| {
                let schema_name: String = row.get("schema_name");
                let table_name: String = row.get("table_name");
                format!("{}.{}", schema_name, table_name)
            })
            .collect())
    }

    /// Persists the result of one shadow audit: the new lifecycle status,
    /// pass/divergence counters, and when to audit next (`None` = stop).
    pub async fn update_audit_result(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        status: &str,
        passes: i32,
        divergence: i32,
        next_audit_secs: Option<i64>,
    ) -> Result<()> {
        let sql = "UPDATE system.table_sync_policy SET \
                inc_status = $4, audit_passes = $5, divergence_count = $6, \
                next_audit_at = CASE WHEN $7::bigint IS NULL THEN NULL \
                    ELSE CURRENT_TIMESTAMP + ($7::bigint || ' seconds')::interval END, \
                updated_at = CURRENT_TIMESTAMP \
            WHERE catalog = $1 AND schema_name = $2 AND table_name = $3;";

        sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(status)
            .bind(passes)
            .bind(divergence)
            .bind(next_audit_secs)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    /// Returns `(catalog_name, db_type, cache_policy)` for every registered
    /// catalog in a single query. Used by `SHOW CACHE POLICIES`.
    pub async fn list_cache_policies(&self) -> Result<Vec<(String, String, String)>> {
        let sql =
            "SELECT name, db_type, cache_policy FROM system.catalog ORDER BY name;";

        let rows = sqlx::query(sql)
            .fetch_all(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let policies = rows
            .into_iter()
            .map(|row| {
                let name: String = row.try_get("name").unwrap_or_default();
                let db_type: String = row.try_get("db_type").unwrap_or_default();
                let cache_policy: String = row.try_get("cache_policy").unwrap_or_default();
                (name, db_type, cache_policy)
            })
            .collect();

        Ok(policies)
    }

    pub fn create_catalog(
        &self,
        catalog: &str,
        dsn: &str,
        db_type: &str,
        comment: Option<String>,
        properties: Vec<(String, String)>,
    ) -> Result<bool> {
        // Optional Hive partition column declared via `with properties(partition_by="col")`.
        let partition_by = properties
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("partition_by"))
            .map(|(_, v)| v.clone())
            .filter(|v| !v.is_empty());

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
            "INSERT INTO system.catalog (name, dsn, db_type, description, cache_policy, partition_by) VALUES ($1, $2, $3, $4, $5, $6);";

        let ret = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Validate the DSN by actually connecting before persisting, so a
                // bad DSN fails CREATE CATALOG instead of leaving a broken entry.
                let probe = PgPool::connect(dsn).await?;
                probe.close().await;

                sqlx::query(insert_sql)
                    .bind(catalog)
                    .bind(dsn)
                    .bind(db_type)
                    .bind(comment)
                    .bind(cache_policy.to_string())
                    .bind(&partition_by)
                    .execute(&self.local_pool)
                    .await
            })
        })
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ret.rows_affected().is_eq(1))
    }

    /// Removes a catalog registration and drops it from the provider cache.
    pub async fn delete_catalog(&self, name: &str) -> Result<bool> {
        let sql = "DELETE FROM system.catalog WHERE name = $1;";
        let ret = sqlx::query(sql)
            .bind(name)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.catalog_cache.remove(name);
        Ok(ret.rows_affected() > 0)
    }

    pub async fn save_table_schema(&self, schema_info: &SchemaTable<'_>) -> Result<bool> {
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
        sqlx::query(upsert_sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(arrow_schema_bin)
            .bind(local_path)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(true)
    }

    pub async fn save_table_cache_key(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        cache_key: u64,
    ) -> Result<bool> {
        let insert_sql = r#"
            INSERT INTO system.table_stats (catalog, schema, table_name, cache_key_list)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (catalog, schema, table_name) DO UPDATE SET
                cache_key_list = array_cat(table_stats.cache_key_list, EXCLUDED.cache_key_list)
            "#;
        let key_list = vec![cache_key.to_string()];

        let ret = sqlx::query(insert_sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .bind(key_list)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ret.rows_affected() >= 1)
    }

    pub async fn get_table_cache_key(
        &self,
        schema_info: &SchemaTable<'_>,
    ) -> Result<Option<Vec<u64>>> {
        let catalog = schema_info.catalog;
        let schema = schema_info.schema;
        let table_name = schema_info.table;
        let query_sql = r#"
            SELECT cache_key_list FROM system.table_stats
            WHERE catalog = $1 AND schema = $2 AND table_name = $3
        "#;

        let row: Option<(Vec<String>,)> = sqlx::query_as(query_sql)
            .bind(catalog)
            .bind(schema)
            .bind(table_name)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        match row {
            Some((cache_key_list,)) => {
                let u64_list: Result<Vec<u64>> = cache_key_list
                    .into_iter()
                    .map(|s| {
                        s.parse::<u64>().map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to parse u64 from string: {}",
                                e
                            ))
                        })
                    })
                    .collect();
                Ok(Some(u64_list?))
            }
            None => Ok(None),
        }
    }

    pub async fn check_table_is_cached(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<bool> {
        let sql = "select arrow_schema, local_path from system.table_arrow_schema \
            where catalog_name = $1 and schema_name = $2 and table_name = $3;";

        let row = sqlx::query(&sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(row.is_some())
    }

    pub async fn get_table_schema(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<(Arc<Schema>, String)> {
        let sql = "select arrow_schema, local_path from system.table_arrow_schema \
        where catalog_name = $1 and schema_name = $2 and table_name = $3;";

        let row = sqlx::query(sql)
            .bind(catalog)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if let Some(row) = row {
            let arrow_schema: Vec<u8> = row.get("arrow_schema");
            let local_path: String = row.get("local_path");
            let schema = binary_to_schema(&arrow_schema)?;
            Ok((schema, local_path))
        } else {
            Ok((Arc::new(Schema::empty()), String::new()))
        }
    }

    /// Looks up the original SQL text for a cached query by its plan-hash key.
    /// Used by proactive result refresh to re-execute and re-cache the query.
    pub async fn get_sql_text_by_hash(&self, key: u64) -> Result<Option<String>> {
        let sql = "SELECT sql_text FROM system.sql_stats WHERE sql_hash = $1;";
        let row: Option<(String,)> = sqlx::query_as(sql)
            .bind(key.to_string())
            .fetch_optional(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(row.map(|(sql_text,)| sql_text))
    }

    pub async fn save_sql_stats(&self, sql: &str, key: u64, cost: u64) -> Result<bool> {
        //TODO: store cache status / execute status.
        let insert_sql =
            "INSERT INTO system.sql_stats (sql_hash, sql_text, execution_time, count, min_time, max_time)
            VALUES ($1, $2, $3, 1, $3, $3)
            ON CONFLICT (sql_hash) DO UPDATE SET
            execution_time = sql_stats.execution_time + EXCLUDED.execution_time,
            count = sql_stats.count + 1,
            min_time = LEAST(sql_stats.min_time, EXCLUDED.min_time),
            max_time = GREATEST(sql_stats.max_time, EXCLUDED.max_time);";

        let ret = sqlx::query(insert_sql)
            .bind(key.to_string())
            .bind(sql)
            .bind(cost as i64)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ret.rows_affected().is_eq(1))
    }

    pub async fn save_table_daily_stats(
        &self,
        catalog: &str,
        schema_name: &str,
        table_name: &str,
        stat_date: NaiveDate,
    ) -> Result<bool> {
        let insert_sql =
            "INSERT INTO system.query_table_daily_stats (catalog, schema_name, table_name, stat_date, query_count)
            VALUES ($1, $2, $3, $4, 1)
            ON CONFLICT (catalog, schema_name, table_name, stat_date)
            DO UPDATE SET
                query_count = query_table_daily_stats.query_count + 1;";

        let ret = sqlx::query(insert_sql)
            .bind(catalog)
            .bind(schema_name)
            .bind(table_name)
            .bind(stat_date)
            .execute(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ret.rows_affected() == 1)
    }

    pub async fn get_recent_table_stats(&self, catalog: &str) -> Result<Vec<String>> {
        let seven_days_ago = Local::now() - Duration::days(7);

        let sql = "select distinct schema_name, table_name from system.query_table_daily_stats \
        where catalog = $1 and stat_date >= $2 \
        order by schema_name, table_name;";

        let rows = sqlx::query(sql)
            .bind(catalog)
            .bind(seven_days_ago.date_naive())
            .fetch_all(&self.local_pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let stats = rows
            .iter()
            .map(|row| {
                let schema_name: String = row.get("schema_name");
                let table_name: String = row.get("table_name");
                format!("{}.{}", schema_name, table_name)
            })
            .collect();

        Ok(stats)
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
                        let provider: Arc<dyn CatalogProvider> = Arc::new(
                            PostgreSQLCatalogProvider::new(
                                catalog_info,
                                remote_pool,
                                self.local_pool.clone(),
                            ),
                        );

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

#[cfg(test)]
mod policy_tests {
    use super::*;

    fn meta_dsn() -> String {
        std::env::var("PG_META_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/kokedb".into())
    }

    // Verifies the table_sync_policy schema is created and that cadence + inc
    // policy roundtrip, including that re-inference does NOT clobber a decided
    // lifecycle status.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via PG_META_DSN"]
    async fn sync_policy_roundtrip_and_status_preserved() {
        let pool = PgPool::connect(&meta_dsn()).await.unwrap();
        let meta = PostgreSQLMetaCatalogProviderList::from_pool(pool);
        meta.init_db().await.unwrap();

        let (cat, schema, table) = ("it_cat", "public", "it_policy");
        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();

        // Cadence upsert.
        let cadence = TableSyncPolicy {
            refresh_bucket: "fast".into(),
            refresh_interval_sec: 120,
            est_row_count: Some(1000),
            est_size_bytes: Some(4096),
            churn_per_hour: Some(10.0),
            access_per_day: Some(5.0),
            score: Some(3.5),
        };
        meta.upsert_table_sync_policy(cat, schema, table, &cadence)
            .await
            .unwrap();

        // First inference: probation -> suggested.
        let suggested = TableIncPolicy {
            inc_mode: "upsert".into(),
            inc_status: "suggested".into(),
            inc_tier: "probation".into(),
            watermark_column: Some("updated_at".into()),
            pk_columns: Some("id".into()),
            source: "rule".into(),
            confidence: Some(0.75),
            reason: Some("conventional watermark".into()),
            audit_passes: 0,
            divergence_count: 0,
        };
        meta.upsert_table_inc_policy(cat, schema, table, &suggested)
            .await
            .unwrap();

        // Manually promote to active (simulating validation).
        sqlx::query(
            "UPDATE system.table_sync_policy SET inc_status='active' \
             WHERE catalog=$1 AND schema_name=$2 AND table_name=$3",
        )
        .bind(cat)
        .bind(schema)
        .bind(table)
        .execute(&meta.local_pool)
        .await
        .unwrap();

        // Re-inference (e.g. next reeval) must NOT reset active -> suggested.
        meta.upsert_table_inc_policy(cat, schema, table, &suggested)
            .await
            .unwrap();

        let got = meta
            .get_table_inc_policy(cat, schema, table)
            .await
            .unwrap()
            .expect("policy row exists");
        assert_eq!(got.inc_status, "active", "lifecycle must be preserved");
        assert_eq!(got.inc_mode, "upsert");
        assert_eq!(got.watermark_column.as_deref(), Some("updated_at"));
        assert_eq!(got.pk_columns.as_deref(), Some("id"));

        // SHOW TABLE METADATA reads the combined row.
        let md = meta
            .get_table_metadata(cat, schema, table)
            .await
            .unwrap()
            .expect("metadata row exists");
        assert_eq!(md.refresh_bucket, "fast");
        assert_eq!(md.refresh_interval_sec, 120);
        assert_eq!(md.inc_mode, "upsert");
        assert_eq!(md.inc_status, "active");
        assert_eq!(md.est_row_count, Some(1000));

        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();
    }

    // A user override must persist as a trusted, active, user-sourced strategy.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via PG_META_DSN"]
    async fn user_override_sets_trusted_active() {
        let pool = PgPool::connect(&meta_dsn()).await.unwrap();
        let meta = PostgreSQLMetaCatalogProviderList::from_pool(pool);
        meta.init_db().await.unwrap();
        let cat = "it_user";
        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();

        // Override on a table with no prior policy row (must insert).
        meta.set_user_inc_policy(
            cat,
            "public",
            "orders",
            "upsert",
            Some("updated_at".into()),
            Some("id".into()),
        )
        .await
        .unwrap();

        let got = meta
            .get_table_inc_policy(cat, "public", "orders")
            .await
            .unwrap()
            .expect("policy row created");
        assert_eq!(got.source, "user");
        assert_eq!(got.inc_status, "active");
        assert_eq!(got.inc_tier, "trusted");
        assert_eq!(got.inc_mode, "upsert");
        assert_eq!(got.watermark_column.as_deref(), Some("updated_at"));

        // full disables incremental.
        meta.set_user_inc_policy(cat, "public", "orders", "full", None, None)
            .await
            .unwrap();
        let got2 = meta
            .get_table_inc_policy(cat, "public", "orders")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got2.inc_mode, "full");
        assert_eq!(got2.inc_status, "none");
        assert_eq!(got2.source, "user");

        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();
    }

    // A paused table is excluded from the refresh tick; resuming re-includes it.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via PG_META_DSN"]
    async fn paused_table_is_not_due() {
        let pool = PgPool::connect(&meta_dsn()).await.unwrap();
        let meta = PostgreSQLMetaCatalogProviderList::from_pool(pool);
        meta.init_db().await.unwrap();
        let cat = "it_pause";
        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();

        // A policy row with no sync_state is immediately due.
        let policy = TableSyncPolicy {
            refresh_bucket: "normal".into(),
            refresh_interval_sec: 900,
            est_row_count: None,
            est_size_bytes: None,
            churn_per_hour: None,
            access_per_day: None,
            score: None,
        };
        meta.upsert_table_sync_policy(cat, "public", "t1", &policy)
            .await
            .unwrap();
        assert!(meta.get_due_refresh_tables(cat).await.unwrap().contains(&"public.t1".to_string()));

        meta.set_table_paused(cat, "public", "t1", true).await.unwrap();
        assert!(
            !meta.get_due_refresh_tables(cat).await.unwrap().contains(&"public.t1".to_string()),
            "paused table must not be due"
        );

        meta.set_table_paused(cat, "public", "t1", false).await.unwrap();
        assert!(
            meta.get_due_refresh_tables(cat).await.unwrap().contains(&"public.t1".to_string()),
            "resumed table is due again"
        );

        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();
    }

    // evict_table_cache returns the snapshot path + result-cache keys and removes
    // the cache metadata so the table is no longer considered cached.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via PG_META_DSN"]
    async fn evict_table_cache_clears_metadata() {
        let pool = PgPool::connect(&meta_dsn()).await.unwrap();
        let meta = PostgreSQLMetaCatalogProviderList::from_pool(pool);
        meta.init_db().await.unwrap();
        let (cat, sch, tbl) = ("it_evict", "public", "t1");
        for sql in [
            "DELETE FROM system.table_arrow_schema WHERE catalog_name='it_evict'",
            "DELETE FROM system.table_sync_state WHERE catalog='it_evict'",
            "DELETE FROM system.table_stats WHERE catalog='it_evict'",
        ] {
            sqlx::query(sql).execute(&meta.local_pool).await.unwrap();
        }
        sqlx::query(
            "INSERT INTO system.table_arrow_schema (catalog_name, schema_name, table_name, local_path) \
             VALUES ($1,$2,$3,'/tmp/kokedb-evict-x/uuid')",
        )
        .bind(cat).bind(sch).bind(tbl)
        .execute(&meta.local_pool).await.unwrap();
        sqlx::query(
            "INSERT INTO system.table_stats (catalog, schema, table_name, cache_key_list) \
             VALUES ($1,$2,$3, ARRAY['111','222'])",
        )
        .bind(cat).bind(sch).bind(tbl)
        .execute(&meta.local_pool).await.unwrap();

        assert!(meta.check_table_is_cached(cat, sch, tbl).await.unwrap());
        let (path, keys) = meta.evict_table_cache(cat, sch, tbl).await.unwrap();
        assert_eq!(path.as_deref(), Some("/tmp/kokedb-evict-x/uuid"));
        assert_eq!(keys, vec![111u64, 222u64]);
        assert!(!meta.check_table_is_cached(cat, sch, tbl).await.unwrap(), "no longer cached");
    }

    // Catalog/database policy accessors roundtrip.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via PG_META_DSN"]
    async fn catalog_and_database_policy_roundtrip() {
        let pool = PgPool::connect(&meta_dsn()).await.unwrap();
        let meta = PostgreSQLMetaCatalogProviderList::from_pool(pool);
        meta.init_db().await.unwrap();
        let cat = "it_polcat";
        let _ = sqlx::query("DELETE FROM system.catalog WHERE name = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await;
        let _ = sqlx::query("DELETE FROM system.database_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await;

        // Catalog cache-policy update.
        sqlx::query(
            "INSERT INTO system.catalog (name, dsn, db_type, cache_policy) \
             VALUES ($1, 'postgresql://x', 'postgresql', 'smart')",
        )
        .bind(cat)
        .execute(&meta.local_pool)
        .await
        .unwrap();
        let n = meta
            .update_catalog_cache_policy(cat, "topk:k=5")
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(meta.get_catalog_cache_policy(cat).await.unwrap(), "topk:k=5");
        assert_eq!(meta.update_catalog_cache_policy("nope", "all").await.unwrap(), 0);

        // Database overrides.
        meta.upsert_database_policy(cat, "tmp", "none").await.unwrap();
        meta.upsert_database_policy(cat, "dw", "all").await.unwrap();
        meta.upsert_database_policy(cat, "tmp", "all").await.unwrap(); // upsert overwrites
        let mut policies = meta.get_database_policies(cat).await.unwrap();
        policies.sort();
        assert_eq!(
            policies,
            vec![("dw".to_string(), "all".to_string()), ("tmp".to_string(), "all".to_string())]
        );

        let _ = sqlx::query("DELETE FROM system.catalog WHERE name = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await;
        let _ = sqlx::query("DELETE FROM system.database_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await;
    }

    // get_due_audit_tables should surface inferred (non-full, non-rejected)
    // tables whose next_audit_at has passed; update_audit_result should persist
    // the lifecycle + reschedule.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via PG_META_DSN"]
    async fn audit_accessors_roundtrip() {
        let pool = PgPool::connect(&meta_dsn()).await.unwrap();
        let meta = PostgreSQLMetaCatalogProviderList::from_pool(pool);
        meta.init_db().await.unwrap();
        let cat = "it_audit";
        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();

        let inc = TableIncPolicy {
            inc_mode: "upsert".into(),
            inc_status: "suggested".into(),
            inc_tier: "probation".into(),
            watermark_column: Some("updated_at".into()),
            pk_columns: Some("id".into()),
            source: "rule".into(),
            confidence: Some(0.7),
            reason: None,
            audit_passes: 0,
            divergence_count: 0,
        };
        meta.upsert_table_inc_policy(cat, "public", "t1", &inc)
            .await
            .unwrap();

        // Freshly suggested (next_audit_at NULL) is due.
        let due = meta.get_due_audit_tables(cat).await.unwrap();
        assert!(due.contains(&"public.t1".to_string()));

        // Record a pass that reschedules far in the future -> no longer due.
        meta.update_audit_result(cat, "public", "t1", "active", 2, 0, Some(86_400))
            .await
            .unwrap();
        let due2 = meta.get_due_audit_tables(cat).await.unwrap();
        assert!(!due2.contains(&"public.t1".to_string()), "rescheduled audit not due");

        let got = meta
            .get_table_inc_policy(cat, "public", "t1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.inc_status, "active");
        assert_eq!(got.audit_passes, 2);

        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = $1")
            .bind(cat)
            .execute(&meta.local_pool)
            .await
            .unwrap();
    }
}
