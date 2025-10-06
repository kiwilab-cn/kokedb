use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Row};
use std::cmp::Ordering;

use crate::error::TaskError;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TableInfo {
    pub schema_name: String,
    pub table_name: String,
    pub row_count: i64,
    pub table_size_bytes: i64,
    pub table_size_pretty: String,
    pub total_size_bytes: i64,
    pub total_size_pretty: String,
}

impl TableInfo {
    fn sort_score(&self) -> f64 {
        let size_mb = self.total_size_bytes as f64 / (1024.0 * 1024.0);
        let row_count_f64 = self.row_count as f64;

        (row_count_f64 * 0.6) + (size_mb * 0.4)
    }
}

impl PartialEq for TableInfo {
    fn eq(&self, other: &Self) -> bool {
        self.schema_name == other.schema_name && self.table_name == other.table_name
    }
}

impl Eq for TableInfo {}

impl PartialOrd for TableInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TableInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .sort_score()
            .partial_cmp(&self.sort_score())
            .unwrap_or(Ordering::Equal)
    }
}

pub async fn get_postgres_top_tables(dsn: &str, k: usize) -> Result<Vec<String>, TaskError> {
    let pool = PgPool::connect(dsn).await.map_err(|x| {
        TaskError::DatabaseError(format!(
            "Failed to connect postgresql: {} with error: {}",
            dsn, x
        ))
    })?;
    let topk_tables = get_top_tables_info(&pool, k).await?;
    pool.close().await;
    return Ok(topk_tables);
}

pub async fn get_postgres_all_tables(dsn: &str) -> Result<Vec<String>, TaskError> {
    let pool = PgPool::connect(dsn).await.map_err(|x| {
        TaskError::DatabaseError(format!(
            "Failed to connect postgresql: {} with error: {}",
            dsn, x
        ))
    })?;
    let tables = get_all_tables_info(&pool).await?;
    pool.close().await;

    let all_tables = tables
        .iter()
        .map(|t| format!("{}.{}", t.schema_name, t.table_name))
        .collect();

    return Ok(all_tables);
}

pub async fn get_top_tables_info(pool: &PgPool, k: usize) -> Result<Vec<String>, TaskError> {
    let mut tables = get_all_tables_info(pool).await?;
    tables.sort();
    let topk_tables = tables
        .iter()
        .take(k)
        .map(|x| format!("{}.{}", x.schema_name, x.table_name))
        .collect();
    Ok(topk_tables)
}

pub async fn get_all_tables_info(pool: &PgPool) -> Result<Vec<TableInfo>, TaskError> {
    let query = r#"
        SELECT 
            schemaname as schema_name,
            relname as table_name,
            n_tup_ins - n_tup_del as row_count,
            pg_total_relation_size(schemaname||'.'||relname) as total_size_bytes,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size_pretty,
            pg_relation_size(schemaname||'.'||relname) as table_size_bytes,
            pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as table_size_pretty
        FROM pg_stat_user_tables 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    "#;

    let rows = sqlx::query(query).fetch_all(pool).await.map_err(|e| {
        TaskError::DatabaseError(format!(
            "Failed to read table info from postgres with error:{}",
            e
        ))
    })?;

    let mut tables: Vec<TableInfo> = Vec::new();

    for row in rows {
        let table_info = TableInfo {
            schema_name: row.get("schema_name"),
            table_name: row.get("table_name"),
            row_count: row.get("row_count"),
            table_size_bytes: row.get("table_size_bytes"),
            table_size_pretty: row.get("table_size_pretty"),
            total_size_bytes: row.get("total_size_bytes"),
            total_size_pretty: row.get("total_size_pretty"),
        };
        tables.push(table_info);
    }

    Ok(tables)
}
