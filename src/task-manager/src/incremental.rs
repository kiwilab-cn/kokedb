//! Watermark-based incremental sync helpers.
//!
//! Strategy: detect a table's primary key and an "updated at" timestamp column.
//! When both exist (and a previous snapshot is present), fetch only rows changed
//! since the last watermark, then rebuild a fresh, fully-deduplicated snapshot by
//! merging the previous snapshot with the delta (delta wins per primary key).
//!
//! The query path is unchanged — it always reads a single, complete snapshot
//! directory — so incremental sync carries no query-correctness risk. The one
//! accepted limitation is that timestamp watermarks cannot observe hard deletes
//! in the source; a periodic full refresh reconciles those (see
//! `FORCE_FULL_EVERY` in the executor).

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use sqlx::{PgPool, Row};

use crate::error::TaskError;

/// Candidate "updated at" column names, in priority order. A timestamp column
/// is only used as a watermark if its name matches one of these — we never
/// guess an arbitrary timestamp column (e.g. `created_at` would miss updates).
const WATERMARK_COLUMN_CANDIDATES: &[&str] = &[
    "updated_at",
    "last_update_time",
    "last_updated",
    "last_modified",
    "modified_at",
    "update_time",
    "gmt_modified",
    "updated",
    "mtime",
];

fn split_schema_table(qualified: &str) -> (String, String) {
    qualified
        .split_once('.')
        .map(|(s, t)| (s.to_string(), t.to_string()))
        .unwrap_or_else(|| ("public".to_string(), qualified.to_string()))
}

/// Returns the primary key columns (in key order) for a source table.
pub async fn detect_primary_keys(
    pool: &PgPool,
    qualified_table: &str,
) -> Result<Vec<String>, TaskError> {
    let (schema, table) = split_schema_table(qualified_table);
    let sql = r#"
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
        ORDER BY kcu.ordinal_position
    "#;
    let rows = sqlx::query(sql)
        .bind(&schema)
        .bind(&table)
        .fetch_all(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(format!("Failed to detect primary keys: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<String, _>("column_name"))
        .collect())
}

/// Returns a watermark timestamp column name if the table has one matching the
/// candidate dictionary, else `None`.
pub async fn detect_watermark_column(
    pool: &PgPool,
    qualified_table: &str,
) -> Result<Option<String>, TaskError> {
    let (schema, table) = split_schema_table(qualified_table);
    let sql = r#"
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
          AND data_type IN ('timestamp without time zone', 'timestamp with time zone')
    "#;
    let rows = sqlx::query(sql)
        .bind(&schema)
        .bind(&table)
        .fetch_all(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(format!("Failed to detect watermark column: {e}")))?;

    let ts_columns: Vec<String> = rows
        .into_iter()
        .map(|r| r.get::<String, _>("column_name"))
        .collect();

    for candidate in WATERMARK_COLUMN_CANDIDATES {
        if let Some(col) = ts_columns
            .iter()
            .find(|c| c.eq_ignore_ascii_case(candidate))
        {
            return Ok(Some(col.clone()));
        }
    }
    Ok(None)
}

/// Returns the current maximum watermark value (as text) in the source table,
/// or `None` if the table is empty.
pub async fn max_watermark(
    pool: &PgPool,
    qualified_table: &str,
    watermark_column: &str,
) -> Result<Option<String>, TaskError> {
    let sql = format!(
        "SELECT MAX(\"{}\")::text AS wm FROM {}",
        watermark_column, qualified_table
    );
    let row = sqlx::query(&sql)
        .fetch_one(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(format!("Failed to read max watermark: {e}")))?;
    Ok(row.try_get::<Option<String>, _>("wm").unwrap_or(None))
}

/// Returns whether any source row has a watermark greater than `last_watermark`.
/// The comparison happens inside PostgreSQL (the literal is coerced to the
/// column type), so it is correct regardless of the timestamp text format.
pub async fn has_changes_since(
    pool: &PgPool,
    qualified_table: &str,
    watermark_column: &str,
    last_watermark: &str,
) -> Result<bool, TaskError> {
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM {} WHERE {}) AS changed",
        qualified_table,
        incremental_where(watermark_column, last_watermark)
    );
    let row = sqlx::query(&sql)
        .fetch_one(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(format!("Failed to check changes: {e}")))?;
    Ok(row.try_get::<bool, _>("changed").unwrap_or(true))
}

/// Builds the `WHERE` clause selecting rows newer than `last_watermark`.
pub fn incremental_where(watermark_column: &str, last_watermark: &str) -> String {
    // `last_watermark` is a value we previously read back from PostgreSQL, not
    // user input; it is quoted as a literal and cast by PostgreSQL to the
    // column type during comparison.
    let escaped = last_watermark.replace('\'', "''");
    format!("\"{}\" > '{}'", watermark_column, escaped)
}

/// Merges a previous snapshot directory with a delta directory into a fresh,
/// deduplicated snapshot written to `out_path`. Rows in `delta` win over rows in
/// `old` with the same primary key (anti-join + union).
pub async fn merge_snapshot(
    old_path: &str,
    delta_path: &str,
    out_path: &str,
    pk_columns: &[String],
    schema: Arc<Schema>,
    sort_column: Option<&str>,
) -> Result<(), TaskError> {
    if pk_columns.is_empty() {
        return Err(TaskError::ExecutionFailed(
            "merge_snapshot requires at least one primary key column".to_string(),
        ));
    }

    let ctx = SessionContext::new();
    register_parquet_dir(&ctx, "old", old_path, schema.clone()).await?;
    register_parquet_dir(&ctx, "delta", delta_path, schema.clone()).await?;

    let on = pk_columns
        .iter()
        .map(|c| format!("o.\"{c}\" = d.\"{c}\""))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Primary key columns are NOT NULL, so an unmatched left join row has a NULL
    // delta key — that is exactly the set of old rows not superseded by a delta.
    let anti_key = &pk_columns[0];

    // Sort the merged output so the rebuilt snapshot stays clustered for pruning.
    let order_sql = match sort_column {
        Some(col) => format!(" ORDER BY \"{}\"", col.replace('"', "\"\"")),
        None => String::new(),
    };
    let sql = format!(
        "SELECT * FROM (\
            SELECT * FROM delta \
            UNION ALL \
            SELECT o.* FROM old o LEFT JOIN delta d ON {on} WHERE d.\"{anti_key}\" IS NULL\
         ){order_sql}"
    );

    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| TaskError::ExecutionFailed(format!("merge query failed: {e}")))?;

    df.write_parquet(out_path, DataFrameWriteOptions::new(), None)
        .await
        .map_err(|e| TaskError::WriteParquetError(format!("failed to write merged snapshot: {e}")))?;

    Ok(())
}

async fn register_parquet_dir(
    ctx: &SessionContext,
    name: &str,
    path: &str,
    schema: Arc<Schema>,
) -> Result<(), TaskError> {
    let url = ListingTableUrl::parse(path)
        .map_err(|e| TaskError::ExecutionFailed(format!("invalid listing path {path}: {e}")))?;
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config)
        .map_err(|e| TaskError::ExecutionFailed(format!("failed to open {path}: {e}")))?;
    ctx.register_table(name, Arc::new(table))
        .map_err(|e| TaskError::ExecutionFailed(format!("failed to register {name}: {e}")))?;
    Ok(())
}
