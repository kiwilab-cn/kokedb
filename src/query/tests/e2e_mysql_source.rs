//! End-to-end test for the MySQL source connector, driving the real server path
//! (init_shared_services -> CREATE CATALOG mysql:// -> sync -> query) against a
//! live MySQL source with a PostgreSQL meta store.
//!
//!   KOKEDB_MYSQL_TEST_DSN=mysql://root:root@127.0.0.1:13306/testdb
//!   PG_META_DSN=postgresql://postgres:123456@127.0.0.1:25433/kokedb
//!   cargo test -p kokedb-query --test e2e_mysql_source -- --ignored --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch};
use kokedb_cache::result_cache::ResultCache;
use kokedb_common::hash::get_plan_hash;
use kokedb_query::binder::{parser, query};
use kokedb_query::context::{create_session_context, init_shared_services, SharedServices};
use sqlx::mysql::MySqlPool;
use sqlx::postgres::PgPool;
use sqlx::Row;

fn mysql_dsn() -> String {
    std::env::var("KOKEDB_MYSQL_TEST_DSN")
        .unwrap_or_else(|_| "mysql://root:root@127.0.0.1:13306/testdb".into())
}

fn meta_dsn() -> String {
    std::env::var("PG_META_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25433/kokedb".into())
}

async fn run(shared: &SharedServices, sql: &str) -> Vec<RecordBatch> {
    run_inner(shared, sql, None).await
}

/// Runs with a freshness requirement, forcing a stale cached table to be read
/// live from the MySQL source.
async fn run_fresh(shared: &SharedServices, sql: &str, secs: u64) -> Vec<RecordBatch> {
    run_inner(shared, sql, Some(secs)).await
}

async fn run_inner(shared: &SharedServices, sql: &str, staleness: Option<u64>) -> Vec<RecordBatch> {
    use futures::TryStreamExt;
    let ctx = Arc::new(create_session_context(shared).expect("session ctx"));
    let plan = parser(sql).unwrap_or_else(|e| panic!("parse `{sql}`: {e}"));
    let key = get_plan_hash(&plan).unwrap();
    let stream = query(ctx, &plan, key, staleness)
        .await
        .unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
    stream.try_collect().await.expect("collect")
}

fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 count")
        .value(0)
}

async fn clean_meta(pool: &PgPool, catalog: &str) {
    for sql in [
        "DELETE FROM system.catalog WHERE name = $1",
        "DELETE FROM system.table_sync_policy WHERE catalog = $1",
        "DELETE FROM system.table_sync_state WHERE catalog = $1",
        "DELETE FROM system.table_arrow_schema WHERE catalog_name = $1",
        "DELETE FROM system.cache_job WHERE catalog = $1",
        "DELETE FROM system.database_policy WHERE catalog = $1",
    ] {
        let _ = sqlx::query(sql).bind(catalog).execute(pool).await;
    }
}

async fn wait_until_cached(meta: &PgPool, catalog: &str, table: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        let row = sqlx::query(
            "SELECT local_path FROM system.table_arrow_schema \
             WHERE catalog_name = $1 AND table_name = $2",
        )
        .bind(catalog)
        .bind(table)
        .fetch_optional(meta)
        .await
        .ok()
        .flatten();
        if let Some(r) = row {
            let p: Option<String> = r.try_get("local_path").ok();
            if p.map(|s| !s.is_empty()).unwrap_or(false) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MySQL source + PostgreSQL meta"]
async fn mysql_source_sync_and_query() {
    let catalog = "mysql_shop";
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    // Seed the MySQL source: 200 orders with a mix of column types.
    let my = MySqlPool::connect(&mysql_dsn()).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS m_orders",
        "CREATE TABLE m_orders (\
            id INT PRIMARY KEY, \
            amount DECIMAL(10,2) NOT NULL, \
            qty INT UNSIGNED NOT NULL, \
            status VARCHAR(32) NOT NULL, \
            is_paid TINYINT(1) NOT NULL, \
            created_at DATETIME NOT NULL, \
            updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
                ON UPDATE CURRENT_TIMESTAMP(6))",
    ] {
        sqlx::query(stmt).execute(&my).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO m_orders(id, amount, qty, status, is_paid, created_at) \
         WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 200) \
         SELECT n, n * 1.50, n, IF(n <= 100, 'new', 'done'), n % 2, NOW() FROM seq",
    )
    .execute(&my)
    .await
    .unwrap();

    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    clean_meta(&meta, catalog).await;

    let result_cache = ResultCache::local(2000, 40000).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();

    // Create the MySQL-backed catalog (triggers the initial full sync).
    let create = format!(
        "CREATE CATALOG {catalog} USING '{}' WITH properties(cache_policy=\"select\", \
         table_set=\"testdb.m_orders\")",
        mysql_dsn()
    );
    run(&shared, &create).await;

    assert!(
        wait_until_cached(&meta, catalog, "m_orders", Duration::from_secs(60)).await,
        "m_orders was not cached from MySQL in time"
    );

    // Query the cached snapshot: counts and typed predicates must be correct.
    let total = run(&shared, &format!("SELECT count(*) FROM {catalog}.testdb.m_orders")).await;
    assert_eq!(scalar_count(&total), 200, "row count");

    let paid = run(
        &shared,
        &format!("SELECT count(*) FROM {catalog}.testdb.m_orders WHERE is_paid = true"),
    )
    .await;
    assert_eq!(scalar_count(&paid), 100, "boolean (tinyint(1)) decode");

    let big = run(
        &shared,
        &format!("SELECT count(*) FROM {catalog}.testdb.m_orders WHERE amount > 150.0"),
    )
    .await;
    assert_eq!(scalar_count(&big), 100, "decimal decode + predicate");

    let done = run(
        &shared,
        &format!("SELECT count(*) FROM {catalog}.testdb.m_orders WHERE status = 'done'"),
    )
    .await;
    assert_eq!(scalar_count(&done), 100, "varchar decode + predicate");

    // Adaptive signals: reeval computed a data-driven policy (row estimate) and
    // inferred a trusted `upsert` strategy from the ON UPDATE watermark + PK.
    let mut adaptive_ok = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok((est, mode, tier)) = sqlx::query_as::<_, (Option<i64>, String, String)>(
            "SELECT est_row_count, inc_mode, inc_tier FROM system.table_sync_policy \
             WHERE catalog = $1 AND table_name = 'm_orders'",
        )
        .bind(catalog)
        .fetch_one(&meta)
        .await
        {
            if est.unwrap_or(0) > 0 && mode == "upsert" && tier == "trusted" {
                adaptive_ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        adaptive_ok,
        "adaptive signals + upsert inference were not persisted for the MySQL table"
    );

    // Incremental sync: the `ON UPDATE` watermark + primary key qualify the
    // table for incremental. Update one row and insert another (both bump the
    // microsecond watermark), refresh, and confirm the snapshot merges the delta
    // rather than rebuilding.
    sqlx::query("UPDATE m_orders SET status = 'changed', amount = 999.99 WHERE id = 1")
        .execute(&my)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO m_orders(id, amount, qty, status, is_paid, created_at) \
         VALUES (201, 5.0, 5, 'new', 1, NOW())",
    )
    .execute(&my)
    .await
    .unwrap();
    run(&shared, &format!("REFRESH CACHE FROM TABLE {catalog}.testdb.m_orders")).await;

    let mut merged = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        let c = run(&shared, &format!("SELECT count(*) FROM {catalog}.testdb.m_orders")).await;
        if scalar_count(&c) == 201 {
            merged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(merged, "incremental sync did not pick up the insert (count != 201)");
    let changed = run(
        &shared,
        &format!("SELECT count(*) FROM {catalog}.testdb.m_orders WHERE status = 'changed'"),
    )
    .await;
    assert_eq!(scalar_count(&changed), 1, "incremental merge applied the update");
    // The sync state records incremental mode after a delta merge.
    let mode: (String,) = sqlx::query_as(
        "SELECT sync_mode FROM system.table_sync_state \
         WHERE catalog=$1 AND table_name='m_orders'",
    )
    .bind(catalog)
    .fetch_one(&meta)
    .await
    .unwrap();
    assert_eq!(mode.0, "incremental", "second sync ran incrementally");

    // Live read via the freshness guard: add a row to the MySQL source the
    // snapshot lacks, mark the snapshot stale, and demand freshness -> the read
    // routes to the live MySqlTableProvider and sees 201 rows.
    sqlx::query("INSERT INTO m_orders(id, amount, qty, status, is_paid, created_at) VALUES (9999, 1.0, 1, 'live', 1, NOW())")
        .execute(&my)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE system.table_sync_state SET last_sync_at = now() - interval '1 hour' \
         WHERE catalog=$1 AND table_name='m_orders'",
    )
    .bind(catalog)
    .execute(&meta)
    .await
    .unwrap();
    let live = run_fresh(
        &shared,
        &format!("SELECT count(*) FROM {catalog}.testdb.m_orders"),
        1,
    )
    .await;
    assert_eq!(scalar_count(&live), 202, "freshness guard reads live MySQL source");

    // Cleanup.
    clean_meta(&meta, catalog).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS m_orders").execute(&my).await;
}
