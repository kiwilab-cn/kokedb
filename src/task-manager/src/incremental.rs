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

#[cfg(test)]
mod hive_bench {
    use arrow::datatypes::DataType;
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;
    use std::time::Instant;

    async fn time_ms(ctx: &SessionContext, sql: &str) -> u128 {
        let t = Instant::now();
        let df = ctx.sql(sql).await.unwrap();
        let _ = df.collect().await.unwrap();
        t.elapsed().as_millis()
    }

    // Reproduce the production path: write flat, repartition via SELECT *, then
    // register with infer_schema + partition col, and query the partition col.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "local repro; run explicitly"]
    async fn partitioned_query_does_not_duplicate_column() {
        let flat = "/tmp/kokedb-repro-flat";
        let out = "/tmp/kokedb-repro-out";
        let _ = std::fs::remove_dir_all(flat);
        let _ = std::fs::remove_dir_all(out);

        let ctx = SessionContext::new();
        // Flat dataset with a low-card column k.
        let df = ctx
            .sql("SELECT value AS id, CAST(value % 4 AS INT) AS k, CAST(value AS DOUBLE) AS v FROM range(0, 1000)")
            .await
            .unwrap();
        df.write_parquet(flat, DataFrameWriteOptions::new(), None)
            .await
            .unwrap();

        // Production repartition: register flat, SELECT *, write partition_by(k).
        let ctx2 = SessionContext::new();
        let opts = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let url = ListingTableUrl::parse(flat).unwrap();
        let cfg = ListingTableConfig::new(url)
            .with_listing_options(opts)
            .infer_schema(&ctx2.state())
            .await
            .unwrap();
        ctx2.register_table("flat", Arc::new(ListingTable::try_new(cfg).unwrap()))
            .unwrap();
        let merged = ctx2.sql("SELECT * FROM flat").await.unwrap();
        merged
            .write_parquet(
                out,
                DataFrameWriteOptions::new().with_partition_by(vec!["k".to_string()]),
                None,
            )
            .await
            .unwrap();

        // Inspect: does a partition file still contain column k?
        let probe = SessionContext::new();
        probe
            .register_parquet(
                "probe",
                &format!("{out}/k=0"),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let cols: Vec<String> = probe
            .table("probe")
            .await
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        println!("REPRO file columns in k=0: {cols:?}");

        // Now register Hive-partitioned like build_listing_table does.
        let qctx = SessionContext::new();
        let opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_table_partition_cols(vec![("k".to_string(), DataType::Int32)]);
        let url = ListingTableUrl::parse(out).unwrap();
        let cfg = ListingTableConfig::new(url)
            .with_listing_options(opts)
            .infer_schema(&qctx.state())
            .await
            .unwrap();
        qctx.register_table("t", Arc::new(ListingTable::try_new(cfg).unwrap()))
            .unwrap();
        let res = qctx.sql("SELECT count(*) FROM t WHERE k = 1").await;
        match res {
            Ok(df) => {
                let n = df.collect().await;
                println!("REPRO WHERE k=1 -> {:?}", n.map(|b| b[0].num_rows()));
            }
            Err(e) => println!("REPRO ERROR: {e}"),
        }
        let _ = std::fs::remove_dir_all(flat);
        let _ = std::fs::remove_dir_all(out);
    }

    // De-risk: does DataFusion Hive partition pruning give a measurable win at
    // ~10M rows? Generates a partitioned parquet dataset and times a
    // partition-filtered query vs a full scan vs a non-partition filter.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "local benchmark; run explicitly"]
    async fn hive_partition_pruning_benchmark() {
        let dir = "/tmp/kokedb-hive-bench";
        let _ = std::fs::remove_dir_all(dir);

        let ctx = SessionContext::new();
        // 10M rows, partition column `part` with 10 distinct values.
        let gen = ctx
            .sql(
                "SELECT CAST(value % 10 AS INT) AS part, value AS id, \
                 CAST(value AS DOUBLE) * 1.5 AS v FROM range(0, 10000000)",
            )
            .await
            .unwrap();
        gen.write_parquet(
            dir,
            DataFrameWriteOptions::new().with_partition_by(vec!["part".to_string()]),
            None,
        )
        .await
        .unwrap();

        // Register as a Hive-partitioned listing table.
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_table_partition_cols(vec![("part".to_string(), DataType::Int32)]);
        let url = ListingTableUrl::parse(dir).unwrap();
        let config = ListingTableConfig::new(url).with_listing_options(options);
        let config = config.infer_schema(&ctx.state()).await.unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();

        // Warm + measure (best of 3).
        let q_part = "SELECT count(*), sum(v) FROM t WHERE part = 3";
        let q_full = "SELECT count(*), sum(v) FROM t";
        let q_nonpart = "SELECT count(*), sum(v) FROM t WHERE id % 10 = 3";
        for _ in 0..1 {
            time_ms(&ctx, q_part).await;
            time_ms(&ctx, q_full).await;
        }
        let mut part = u128::MAX;
        let mut full = u128::MAX;
        let mut nonpart = u128::MAX;
        for _ in 0..3 {
            part = part.min(time_ms(&ctx, q_part).await);
            full = full.min(time_ms(&ctx, q_full).await);
            nonpart = nonpart.min(time_ms(&ctx, q_nonpart).await);
        }
        println!(
            "HIVE-BENCH  partition-filter(part=3)={part}ms  full-scan={full}ms  \
             nonpartition-filter(id%10=3)={nonpart}ms"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Rewrites a flat snapshot into a Hive-partitioned layout (`out_path/{col}=v/…`)
/// so DataFusion can prune whole partitions for predicates on `partition_col`.
pub async fn write_partitioned(
    flat_path: &str,
    out_path: &str,
    partition_col: &str,
    schema: Arc<Schema>,
) -> Result<(), TaskError> {
    let ctx = SessionContext::new();
    register_parquet_dir(&ctx, "flat", flat_path, schema).await?;
    let df = ctx
        .sql("SELECT * FROM flat")
        .await
        .map_err(|e| TaskError::ExecutionFailed(format!("partition read failed: {e}")))?;
    df.write_parquet(
        out_path,
        DataFrameWriteOptions::new().with_partition_by(vec![partition_col.to_string()]),
        None,
    )
    .await
    .map_err(|e| {
        TaskError::WriteParquetError(format!("failed to write partitioned snapshot: {e}"))
    })?;
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
