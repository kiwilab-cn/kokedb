//! MySQL-specific pieces of the incremental-sync path: primary-key and
//! watermark detection, max-watermark / change probes, and delta WHERE-clause
//! building. The merge itself ([`crate::incremental::merge_snapshot`]) is
//! source-agnostic.
//!
//! A MySQL table is only synced incrementally when it has a primary key AND a
//! DB-enforced watermark — a `TIMESTAMP`/`DATETIME` column with
//! `ON UPDATE CURRENT_TIMESTAMP`, which MySQL bumps on every row change. That
//! guarantee is what makes the timestamp watermark trustworthy (the analogue of
//! PostgreSQL's trigger-enforced `updated_at`); without it we fall back to full
//! sync.

use sqlx::mysql::MySqlPool;
use sqlx::Row;

use crate::error::TaskError;

pub async fn connect(dsn: &str) -> Result<MySqlPool, TaskError> {
    MySqlPool::connect(dsn)
        .await
        .map_err(|e| TaskError::DatabaseError(format!("connect MySQL source failed: {e}")))
}

fn split_db_table(name: &str) -> (Option<&str>, &str) {
    match name.split_once('.') {
        Some((db, t)) => (Some(db), t),
        None => (None, name),
    }
}

fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn quote_table(name: &str) -> String {
    match split_db_table(name) {
        (Some(db), t) => format!("{}.{}", quote_ident(db), quote_ident(t)),
        (None, t) => quote_ident(t),
    }
}

/// Resolves `db` to the qualified name's database, or the connection default.
async fn database_of(pool: &MySqlPool, source_table: &str) -> Result<String, TaskError> {
    if let (Some(db), _) = split_db_table(source_table) {
        return Ok(db.to_string());
    }
    sqlx::query_scalar::<_, Option<String>>("SELECT DATABASE()")
        .fetch_one(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(e.to_string()))?
        .ok_or_else(|| TaskError::DatabaseError("no MySQL database selected".to_string()))
}

/// Primary-key columns of `source_table`, in key order (empty if none).
pub async fn detect_primary_keys(
    pool: &MySqlPool,
    source_table: &str,
) -> Result<Vec<String>, TaskError> {
    let db = database_of(pool, source_table).await?;
    let (_, table) = split_db_table(source_table);
    let rows = sqlx::query(
        "SELECT CAST(COLUMN_NAME AS CHAR) AS c \
         FROM information_schema.key_column_usage \
         WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' \
         ORDER BY ordinal_position",
    )
    .bind(&db)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(e.to_string()))?;
    Ok(rows.iter().filter_map(|r| r.try_get::<String, _>("c").ok()).collect())
}

/// A DB-enforced watermark column (`ON UPDATE CURRENT_TIMESTAMP`), if any.
pub async fn detect_watermark_column(
    pool: &MySqlPool,
    source_table: &str,
) -> Result<Option<String>, TaskError> {
    let db = database_of(pool, source_table).await?;
    let (_, table) = split_db_table(source_table);
    let row = sqlx::query(
        "SELECT CAST(COLUMN_NAME AS CHAR) AS c \
         FROM information_schema.columns \
         WHERE table_schema = ? AND table_name = ? \
           AND DATA_TYPE IN ('timestamp', 'datetime') \
           AND LOWER(CAST(EXTRA AS CHAR)) LIKE '%on update current_timestamp%' \
         ORDER BY ordinal_position LIMIT 1",
    )
    .bind(&db)
    .bind(table)
    .fetch_optional(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(e.to_string()))?;
    Ok(row.and_then(|r| r.try_get::<String, _>("c").ok()))
}

/// The current MAX of the watermark column, as a string (None if no rows).
pub async fn max_watermark(
    pool: &MySqlPool,
    source_table: &str,
    watermark_column: &str,
) -> Result<Option<String>, TaskError> {
    let sql = format!(
        "SELECT CAST(MAX({}) AS CHAR) AS m FROM {}",
        quote_ident(watermark_column),
        quote_table(source_table)
    );
    let v: Option<String> = sqlx::query_scalar(&sql)
        .fetch_one(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(e.to_string()))?;
    Ok(v)
}

/// Whether any row has a watermark strictly greater than `last_watermark`.
pub async fn has_changes_since(
    pool: &MySqlPool,
    source_table: &str,
    watermark_column: &str,
    last_watermark: &str,
) -> Result<bool, TaskError> {
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM {} WHERE {} > ?) AS e",
        quote_table(source_table),
        quote_ident(watermark_column)
    );
    let e: i64 = sqlx::query_scalar(&sql)
        .bind(last_watermark)
        .fetch_one(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(e.to_string()))?;
    Ok(e != 0)
}

/// Reads adaptive signals (row count, on-disk size) for a MySQL table from
/// information_schema. MySQL has no per-period write counters like PostgreSQL's
/// pg_stat, so `churn_per_hour` is left at 0 and cadence is driven by size and
/// access heat. `access_per_day` is supplied by the caller (from the meta store).
pub async fn collect_signals(
    pool: &MySqlPool,
    source_table: &str,
    access_per_day: f64,
) -> Result<crate::table_signals::TableSignals, TaskError> {
    let db = database_of(pool, source_table).await?;
    let (_, table) = split_db_table(source_table);
    let row = sqlx::query(
        "SELECT CAST(COALESCE(table_rows, 0) AS SIGNED) AS rows_est, \
                CAST(COALESCE(data_length, 0) + COALESCE(index_length, 0) AS SIGNED) AS size_bytes \
         FROM information_schema.tables \
         WHERE table_schema = ? AND table_name = ?",
    )
    .bind(&db)
    .bind(table)
    .fetch_optional(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(e.to_string()))?;

    let (est_rows, est_size_bytes) = match row {
        Some(r) => (
            r.try_get::<i64, _>("rows_est").unwrap_or(0).max(0) as f64,
            r.try_get::<i64, _>("size_bytes").unwrap_or(0).max(0) as f64,
        ),
        None => (0.0, 0.0),
    };
    Ok(crate::table_signals::TableSignals {
        est_rows,
        est_size_bytes,
        churn_per_hour: 0.0,
        access_per_day,
    })
}

/// The delta predicate `\`col\` > 'last'` for an incremental fetch.
pub fn incremental_where(watermark_column: &str, last_watermark: &str) -> String {
    let escaped = last_watermark.replace('\\', "\\\\").replace('\'', "''");
    format!("{} > '{}'", quote_ident(watermark_column), escaped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_where_quotes_and_escapes() {
        assert_eq!(
            incremental_where("updated_at", "2024-01-01 00:00:00"),
            "`updated_at` > '2024-01-01 00:00:00'"
        );
        assert_eq!(
            incremental_where("up`d", "a'b"),
            "`up``d` > 'a''b'"
        );
    }
}
